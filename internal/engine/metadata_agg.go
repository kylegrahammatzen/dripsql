// Metadata-only aggregate short-circuit. Resolves count, min, max, and varbytes
// GROUP BY count from manifest row counts, per-column stats, and the dict-histogram
// sidecar. Skipped when any segment carries a deletion vector.
package engine

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func (db *DB) tryMetadataAggregate(plan *sql.Plan) (*Rows, bool, error) {
	if plan == nil || plan.Kind != sql.PlanQuery || plan.Rel == nil {
		return nil, false, nil
	}
	rel := plan.Rel
	if rel.Op != sql.RelProject || len(rel.Inputs) != 1 {
		return nil, false, nil
	}
	agg := rel.Inputs[0]
	if agg.Op != sql.RelAggregate || len(agg.Hidden) != 0 || agg.Having != nil || len(agg.Inputs) != 1 {
		return nil, false, nil
	}
	scan := agg.Inputs[0]
	if scan.Op != sql.RelScan || scan.Where != nil {
		return nil, false, nil
	}
	segs, err := db.openSegmentsForQuery(scan.Table.Name)
	if err != nil {
		return nil, false, err
	}
	for _, seg := range segs {
		if seg.DV != nil {
			return nil, false, nil
		}
	}
	if len(agg.GroupBy) == 1 {
		return db.tryGroupByDictHistogram(plan, rel, agg, scan, segs)
	}
	if len(agg.GroupBy) != 0 {
		return nil, false, nil
	}
	values := make(map[string]any, len(agg.Aggregates))
	for _, a := range agg.Aggregates {
		v, ok, err := computeMetadataAggregate(a, scan, segs)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}
		key := a.Alias
		if key == "" {
			key = defaultAggOutputName(a.Func)
		}
		values[key] = v
	}
	row := make([]any, len(rel.Outputs))
	for i, o := range rel.Outputs {
		if o.Expr.Op != sql.ExprColumn {
			return nil, false, nil
		}
		v, ok := values[o.Expr.Column]
		if !ok {
			return nil, false, nil
		}
		row[i] = v
	}
	rows := &Rows{Columns: planOutputNames(plan), Values: [][]any{row}}
	return rows, true, nil
}

// All aggregates must be count() (Star or over a non-nullable column). Every segment
// must have a .dh sidecar for the group column or we fall back to the operator path.
func (db *DB) tryGroupByDictHistogram(plan *sql.Plan, rel, agg, scan *sql.Rel, segs []*storage.Segment) (*Rows, bool, error) {
	if agg.GroupBy[0].Op != sql.ExprColumn {
		return nil, false, nil
	}
	for _, a := range agg.Aggregates {
		if a.Func != sql.AggregateCount {
			return nil, false, nil
		}
		if !a.Star {
			argCol, ok := scanColumnDefByID(scan, a.ArgColumn)
			if !ok || argCol.Nullable {
				return nil, false, nil
			}
		}
	}
	keyName := types.NormalizeName(agg.GroupBy[0].Column)
	keyCol, ok := scanColumnDefByID(scan, agg.GroupBy[0].ColumnID)
	if !ok {
		return nil, false, nil
	}
	switch keyCol.Type.Kind {
	case types.KindText, types.KindBytes, types.KindJSON:
	default:
		return nil, false, nil
	}
	merged := storage.DictHistogram{}
	for _, seg := range segs {
		h, ok := seg.DictHists[keyName]
		if !ok {
			return nil, false, nil
		}
		for k, v := range h {
			merged[k] += v
		}
	}
	if len(merged) == 0 {
		return &Rows{Columns: planOutputNames(plan)}, true, nil
	}
	rows := &Rows{Columns: planOutputNames(plan), Values: make([][]any, 0, len(merged))}
	countNames := make(map[string]struct{}, len(agg.Aggregates))
	for _, a := range agg.Aggregates {
		alias := a.Alias
		if alias == "" {
			alias = defaultAggOutputName(a.Func)
		}
		countNames[alias] = struct{}{}
	}
	for value, count := range merged {
		row := make([]any, len(rel.Outputs))
		ok := true
		for i, o := range rel.Outputs {
			if o.Expr.Op != sql.ExprColumn {
				ok = false
				break
			}
			if o.Expr.Column == agg.GroupBy[0].Column {
				row[i] = value
				continue
			}
			if _, isCount := countNames[o.Expr.Column]; isCount {
				row[i] = int64(count)
				continue
			}
			ok = false
			break
		}
		if !ok {
			return nil, false, nil
		}
		rows.Values = append(rows.Values, row)
	}
	return rows, true, nil
}

func computeMetadataAggregate(a sql.AggSpec, scan *sql.Rel, segs []*storage.Segment) (any, bool, error) {
	switch a.Func {
	case sql.AggregateCount:
		if a.Star {
			return countAllRows(segs), true, nil
		}
		col, ok := scanColumnDefByID(scan, a.ArgColumn)
		if !ok {
			return nil, false, fmt.Errorf("metadata aggregate: column id %d not in scan", a.ArgColumn)
		}
		return countColumn(segs, col)
	case sql.AggregateMin, sql.AggregateMax:
		col, ok := scanColumnDefByID(scan, a.ArgColumn)
		if !ok {
			return nil, false, fmt.Errorf("metadata aggregate: column id %d not in scan", a.ArgColumn)
		}
		return mergeMinMax(segs, col.Name, col.Type.Kind, a.Func == sql.AggregateMax)
	}
	return nil, false, nil
}

func countAllRows(segs []*storage.Segment) int64 {
	var total int64
	for _, seg := range segs {
		total += int64(seg.Rows())
	}
	return total
}

func countColumn(segs []*storage.Segment, col sql.BoundColumnDef) (any, bool, error) {
	if !col.Nullable {
		return countAllRows(segs), true, nil
	}
	var total int64
	for _, seg := range segs {
		sc, ok := findSegColumn(seg, col.Name)
		if !ok {
			return nil, false, nil
		}
		total += int64(sc.Rows) - int64(sc.NullCount)
	}
	return total, true, nil
}

func mergeMinMax(segs []*storage.Segment, name string, kind types.Kind, wantMax bool) (any, bool, error) {
	switch kind {
	case types.KindInt16, types.KindInt32, types.KindInt64, types.KindDate, types.KindTimestamp, types.KindTime, types.KindDecimal:
	default:
		return nil, false, nil
	}
	have := false
	var min64, max64 int64
	for _, seg := range segs {
		sc, ok := findSegColumn(seg, name)
		if !ok || sc.Rows == 0 || int64(sc.NullCount) == int64(sc.Rows) {
			continue
		}
		segMin, segMax, ok := segColInt64MinMax(sc)
		if !ok {
			return nil, false, nil
		}
		if !have {
			min64, max64 = segMin, segMax
			have = true
			continue
		}
		if segMin < min64 {
			min64 = segMin
		}
		if segMax > max64 {
			max64 = segMax
		}
	}
	if !have {
		return nil, true, nil
	}
	if wantMax {
		return narrowInt(max64, kind), true, nil
	}
	return narrowInt(min64, kind), true, nil
}

func segColInt64MinMax(c *storage.SegmentColumn) (int64, int64, bool) {
	switch c.Kind {
	case types.VecInt16, types.VecInt32, types.VecDate:
		s := storage.UnmarshalNumericStats[int32](c.Stats[:], true)
		return int64(s.Min), int64(s.Max), true
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		s := storage.UnmarshalNumericStats[int64](c.Stats[:], true)
		return s.Min, s.Max, true
	}
	return 0, 0, false
}

func narrowInt(v int64, kind types.Kind) any {
	switch kind {
	case types.KindInt16:
		if v >= math.MinInt16 && v <= math.MaxInt16 {
			return int16(v)
		}
	case types.KindInt32, types.KindDate:
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			return int32(v)
		}
	}
	return v
}

func findSegColumn(seg *storage.Segment, name string) (*storage.SegmentColumn, bool) {
	for i := range seg.Cols {
		if types.NormalizeName(seg.Cols[i].Name) == types.NormalizeName(name) {
			return &seg.Cols[i], true
		}
	}
	return nil, false
}

func scanColumnDefByID(scan *sql.Rel, id sql.ColumnID) (sql.BoundColumnDef, bool) {
	for _, c := range scan.Table.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return sql.BoundColumnDef{}, false
}

func defaultAggOutputName(f sql.AggregateFunc) string {
	switch f {
	case sql.AggregateSum:
		return "sum"
	case sql.AggregateMin:
		return "min"
	case sql.AggregateMax:
		return "max"
	}
	return "count"
}
