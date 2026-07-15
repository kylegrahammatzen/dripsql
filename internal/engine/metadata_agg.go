// Metadata-only aggregate short-circuit that resolves count plus min and max and sum and varbytes GROUP BY count from manifest row counts and per-column stats and the dict-histogram sidecar.
// count(*) and non-nullable count(col) survive a deletion vector via a popcount on the loaded DV bits, the other aggregates bail to the scan path because their math depends on individual row values.
package engine

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (db *DB) answerFromMetadata(plan *sql.Plan) (*Rows, bool, error) {
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
	if scan.Op != sql.RelScan {
		return nil, false, nil
	}
	// Metadata aggregates read manifest-wide stats that ignore commit_ts visibility, so
	// any AS OF (or future per-table snapshot) must bail to the scan path.
	if scan.Table.AsOf != 0 {
		return nil, false, nil
	}
	if scan.Where != nil {
		return db.tryCountWithFilter(plan, rel, agg, scan)
	}
	segs, err := db.openSegmentsForQuery(scan.Table.Name)
	if err != nil {
		return nil, false, err
	}
	if len(agg.GroupBy) == 1 {
		for _, seg := range segs {
			if seg.DV != nil {
				return nil, false, nil
			}
		}
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

// tryCountWithFilter answers count(*) WHERE varbytes_col = literal by summing dict-hist entries.
// Falls through to the operator path when the WHERE is not a single varbytes equality leaf or any segment lacks the dict-hist sidecar.
func (db *DB) tryCountWithFilter(plan *sql.Plan, rel, agg, scan *sql.Rel) (*Rows, bool, error) {
	if len(agg.GroupBy) != 0 || len(agg.Aggregates) != 1 {
		return nil, false, nil
	}
	a := agg.Aggregates[0]
	if a.Func != sql.AggregateCount || !a.Star {
		return nil, false, nil
	}
	where := *scan.Where
	col, lit, ok := singleEqLeaf(where)
	if !ok {
		return nil, false, nil
	}
	colDef, ok := scanColumnDefByName(scan, col)
	if !ok {
		return nil, false, nil
	}
	switch colDef.Type.Kind {
	case schema.KindText, schema.KindBytes, schema.KindJSON:
	default:
		return nil, false, nil
	}
	segs, err := db.openSegmentsForQuery(scan.Table.Name)
	if err != nil {
		return nil, false, err
	}
	keyNorm := schema.NormalizeName(colDef.Name)
	var total int64
	for _, seg := range segs {
		if seg.DV != nil {
			return nil, false, nil
		}
		hists, err := seg.DictHistograms()
		if err != nil || hists == nil {
			return nil, false, nil
		}
		hist, ok := hists[keyNorm]
		if !ok {
			return nil, false, nil
		}
		total += int64(hist[lit])
	}
	key := a.Alias
	if key == "" {
		key = defaultAggOutputName(a.Func)
	}
	row := make([]any, len(rel.Outputs))
	for i, o := range rel.Outputs {
		if o.Expr.Op != sql.ExprColumn || o.Expr.Column != key {
			return nil, false, nil
		}
		row[i] = total
	}
	rows := &Rows{Columns: planOutputNames(plan), Values: [][]any{row}}
	return rows, true, nil
}

func singleEqLeaf(e sql.BoundExpr) (col string, lit string, ok bool) {
	if e.Op != sql.ExprEqual || len(e.Args) != 2 {
		return "", "", false
	}
	c, l := e.Args[0], e.Args[1]
	if c.Op == sql.ExprLiteral && l.Op == sql.ExprColumn {
		c, l = l, c
	}
	if c.Op != sql.ExprColumn || l.Op != sql.ExprLiteral {
		return "", "", false
	}
	s, ok := l.Literal.(string)
	if !ok {
		return "", "", false
	}
	return c.Column, s, true
}

func scanColumnDefByName(scan *sql.Rel, name string) (sql.BoundColumnDef, bool) {
	want := schema.NormalizeName(name)
	for _, c := range scan.Table.Columns {
		if schema.NormalizeName(c.Name) == want {
			return c, true
		}
	}
	return sql.BoundColumnDef{}, false
}

// All aggregates must be count() (Star or over a non-nullable column). Every segment
// must have a .dh sidecar for the group column or we fall back to the operator path.
func (db *DB) tryGroupByDictHistogram(plan *sql.Plan, rel, agg, scan *sql.Rel, segs []*storage.Segment) (*Rows, bool, error) {
	if agg.GroupBy[0].Op != sql.ExprColumn {
		return nil, false, nil
	}
	type sumSpec struct {
		alias string
		col   string
	}
	var sumSpecs []sumSpec
	countAliases := make(map[string]struct{}, len(agg.Aggregates))
	for _, a := range agg.Aggregates {
		alias := a.Alias
		if alias == "" {
			alias = defaultAggOutputName(a.Func)
		}
		switch a.Func {
		case sql.AggregateCount:
			if !a.Star {
				argCol, ok := scanColumnDefByID(scan, a.ArgColumn)
				if !ok || argCol.Nullable {
					return nil, false, nil
				}
			}
			countAliases[alias] = struct{}{}
		case sql.AggregateSum:
			if a.ArgExpr != nil {
				return nil, false, nil
			}
			argCol, ok := scanColumnDefByID(scan, a.ArgColumn)
			if !ok || argCol.Nullable {
				return nil, false, nil
			}
			switch argCol.Type.Kind {
			case schema.KindInt16, schema.KindInt32, schema.KindInt64, schema.KindDate, schema.KindTimestamp, schema.KindTime, schema.KindDecimal:
			default:
				return nil, false, nil
			}
			sumSpecs = append(sumSpecs, sumSpec{alias: alias, col: schema.NormalizeName(argCol.Name)})
		default:
			return nil, false, nil
		}
	}
	keyName := schema.NormalizeName(agg.GroupBy[0].Column)
	keyCol, ok := scanColumnDefByID(scan, agg.GroupBy[0].ColumnID)
	if !ok {
		return nil, false, nil
	}
	switch keyCol.Type.Kind {
	case schema.KindText, schema.KindBytes, schema.KindJSON:
	default:
		return nil, false, nil
	}
	merged := storage.DictHistogram{}
	for _, seg := range segs {
		hists, err := seg.DictHistograms()
		if err != nil {
			return nil, false, err
		}
		h, ok := hists[keyName]
		if !ok {
			return nil, false, nil
		}
		for k, v := range h {
			merged[k] += v
		}
	}
	// Sums merge from the group sums sidecar, every segment must carry the pair.
	sumByAlias := make(map[string]storage.DictSums, len(sumSpecs))
	if len(sumSpecs) > 0 {
		mergedSums := make(map[string]storage.DictSums, len(sumSpecs))
		for _, seg := range segs {
			gs, err := seg.GroupSums()
			if err != nil {
				return nil, false, err
			}
			gc, ok := gs[keyName]
			if !ok {
				return nil, false, nil
			}
			for _, ss := range sumSpecs {
				sums, ok := gc[ss.col]
				if !ok {
					return nil, false, nil
				}
				dst := mergedSums[ss.col]
				if dst == nil {
					dst = storage.DictSums{}
					mergedSums[ss.col] = dst
				}
				for k, v := range sums {
					if addOverflows(dst[k], v) {
						return nil, false, nil
					}
					dst[k] += v
				}
			}
		}
		for _, ss := range sumSpecs {
			sumByAlias[ss.alias] = mergedSums[ss.col]
		}
	}
	if len(merged) == 0 {
		return &Rows{Columns: planOutputNames(plan)}, true, nil
	}
	rows := &Rows{Columns: planOutputNames(plan), Values: make([][]any, 0, len(merged))}
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
			if _, isCount := countAliases[o.Expr.Column]; isCount {
				row[i] = int64(count)
				continue
			}
			if sums, isSum := sumByAlias[o.Expr.Column]; isSum {
				s, present := sums[value]
				if !present {
					return nil, false, nil
				}
				row[i] = s
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
	// Computed arguments have no ArgColumn so manifest stats cannot answer them.
	if a.ArgExpr != nil {
		return nil, false, nil
	}
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
		if anySegHasDV(segs) {
			return nil, false, nil
		}
		col, ok := scanColumnDefByID(scan, a.ArgColumn)
		if !ok {
			return nil, false, fmt.Errorf("metadata aggregate: column id %d not in scan", a.ArgColumn)
		}
		return mergeMinMax(segs, col.Name, col.Type.Kind, a.Func == sql.AggregateMax)
	case sql.AggregateSum:
		if anySegHasDV(segs) {
			return nil, false, nil
		}
		col, ok := scanColumnDefByID(scan, a.ArgColumn)
		if !ok {
			return nil, false, fmt.Errorf("metadata aggregate: column id %d not in scan", a.ArgColumn)
		}
		return mergeSum(segs, col)
	}
	return nil, false, nil
}

func anySegHasDV(segs []*storage.Segment) bool {
	for _, seg := range segs {
		if seg.DV != nil {
			return true
		}
	}
	return false
}

func countAllRows(segs []*storage.Segment) int64 {
	var total int64
	for _, seg := range segs {
		rows := int(seg.Rows())
		if seg.DV != nil {
			rows -= seg.DV.NullCount(rows)
		}
		total += int64(rows)
	}
	return total
}

func countColumn(segs []*storage.Segment, col sql.BoundColumnDef) (any, bool, error) {
	if !col.Nullable {
		return countAllRows(segs), true, nil
	}
	if anySegHasDV(segs) {
		return nil, false, nil
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

func mergeMinMax(segs []*storage.Segment, name string, kind schema.Kind, wantMax bool) (any, bool, error) {
	switch kind {
	case schema.KindInt16, schema.KindInt32, schema.KindInt64, schema.KindDate, schema.KindTimestamp, schema.KindTime, schema.KindDecimal:
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

func mergeSum(segs []*storage.Segment, col sql.BoundColumnDef) (any, bool, error) {
	switch col.Type.Kind {
	case schema.KindInt16, schema.KindInt32, schema.KindInt64, schema.KindDate, schema.KindTimestamp, schema.KindTime, schema.KindDecimal:
	default:
		return nil, false, nil
	}
	name := schema.NormalizeName(col.Name)
	var total int64
	for _, seg := range segs {
		sums, err := seg.NumericSums()
		if err != nil {
			return nil, false, err
		}
		s, ok := sums[name]
		if !ok {
			return nil, false, nil
		}
		if addOverflows(total, s.Sum) {
			return nil, false, nil
		}
		total += s.Sum
	}
	return total, true, nil
}

func addOverflows(a, b int64) bool {
	const maxI64 = int64(^uint64(0) >> 1)
	const minI64 = -maxI64 - 1
	if b > 0 && a > maxI64-b {
		return true
	}
	if b < 0 && a < minI64-b {
		return true
	}
	return false
}

func segColInt64MinMax(c *storage.SegmentColumn) (int64, int64, bool) {
	switch c.Kind {
	case vector.VecInt16, vector.VecInt32, vector.VecDate:
		s := storage.UnmarshalNumericStats[int32](c.Stats[:], true)
		return int64(s.Min), int64(s.Max), true
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		s := storage.UnmarshalNumericStats[int64](c.Stats[:], true)
		return s.Min, s.Max, true
	}
	return 0, 0, false
}

func narrowInt(v int64, kind schema.Kind) any {
	switch kind {
	case schema.KindInt16:
		if v >= math.MinInt16 && v <= math.MaxInt16 {
			return int16(v)
		}
	case schema.KindInt32, schema.KindDate:
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			return int32(v)
		}
	}
	return v
}

func findSegColumn(seg *storage.Segment, name string) (*storage.SegmentColumn, bool) {
	for i := range seg.Cols {
		if schema.NormalizeName(seg.Cols[i].Name) == schema.NormalizeName(name) {
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
