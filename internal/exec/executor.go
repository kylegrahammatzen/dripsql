// BuildOperator turns a bound *sql.Plan into a chain of Operators by walking its Rel tree.
// SegmentsFn resolves a BoundTableDef to its storage segments at execution time so the
// engine layer can keep catalog -> segment mapping out of exec.
package exec

import (
	"fmt"
	"runtime"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type SegmentsFn func(def sql.BoundTableDef) ([]*storage.Segment, error)

func BuildOperator(plan *sql.Plan, segments SegmentsFn) (Operator, error) {
	if plan == nil {
		return nil, fmt.Errorf("BuildOperator: nil plan")
	}
	if segments == nil {
		return nil, fmt.Errorf("BuildOperator: nil SegmentsFn")
	}
	switch plan.Kind {
	case sql.PlanQuery:
		if plan.Rel == nil {
			return nil, fmt.Errorf("BuildOperator: query plan missing Rel")
		}
		return buildRel(plan.Rel, segments)
	case sql.PlanExplain:
		return nil, fmt.Errorf("BuildOperator: EXPLAIN execution belongs to the engine layer")
	}
	return nil, fmt.Errorf("BuildOperator: unsupported plan kind %v", plan.Kind)
}

func buildRel(rel *sql.Rel, segments SegmentsFn) (Operator, error) {
	if rel == nil {
		return nil, fmt.Errorf("buildRel: nil rel")
	}
	switch rel.Op {
	case sql.RelScan:
		return buildScan(rel, segments, nil)
	case sql.RelProject:
		if err := checkProjectionOps(rel.Projection); err != nil {
			return nil, err
		}
		source, err := buildRel(rel.Inputs[0], segments)
		if err != nil {
			return nil, err
		}
		return &ProjectOp{Source: source, Outputs: rel.Projection}, nil
	case sql.RelLimit:
		source, err := buildRel(rel.Inputs[0], segments)
		if err != nil {
			return nil, err
		}
		n := rel.Limit
		if n < 0 {
			n = -1
		}
		return &LimitOp{Source: source, N: n, Offset: rel.Offset}, nil
	case sql.RelAggregate:
		for _, expr := range rel.GroupBy {
			if err := checkExecExpr(expr); err != nil {
				return nil, err
			}
		}
		if rel.Having != nil {
			if err := checkExecExpr(*rel.Having); err != nil {
				return nil, err
			}
		}
		source, err := buildRel(rel.Inputs[0], segments)
		if err != nil {
			return nil, err
		}
		return &AggregateOp{
			Source:     source,
			GroupBy:    rel.GroupBy,
			Aggregates: rel.Aggregates,
			Hidden:     rel.Hidden,
			Having:     rel.Having,
		}, nil
	case sql.RelFilter:
		if err := checkExecExpr(rel.Predicate); err != nil {
			return nil, err
		}
		source, err := buildRel(rel.Inputs[0], segments)
		if err != nil {
			return nil, err
		}
		return &FilterOp{Source: source, Predicate: rel.Predicate}, nil
	case sql.RelJoin:
		if len(rel.JoinKeys) == 0 {
			return nil, fmt.Errorf("BuildOperator: join missing keys")
		}
		leftKeys := make([]sql.BoundExpr, len(rel.JoinKeys))
		rightKeys := make([]sql.BoundExpr, len(rel.JoinKeys))
		for i, k := range rel.JoinKeys {
			if err := checkExecExpr(k.Left); err != nil {
				return nil, err
			}
			if err := checkExecExpr(k.Right); err != nil {
				return nil, err
			}
			leftKeys[i] = k.Left
			rightKeys[i] = k.Right
		}
		left, err := buildRel(rel.Inputs[0], segments)
		if err != nil {
			return nil, err
		}
		right, err := buildRel(rel.Inputs[1], segments)
		if err != nil {
			return nil, err
		}
		return &HashJoinOp{
			Kind:         rel.JoinKind,
			Left:         left,
			Right:        right,
			LeftKeys:     leftKeys,
			RightKeys:    rightKeys,
			LeftOutputs:  rel.Inputs[0].Outputs,
			RightOutputs: rel.Inputs[1].Outputs,
		}, nil
	case sql.RelSort:
		for _, key := range rel.SortKeys {
			if err := checkExecExpr(key.Expr); err != nil {
				return nil, err
			}
		}
		source, err := buildSortInput(rel, segments)
		if err != nil {
			return nil, err
		}
		return &SortOp{Source: source, Keys: rel.SortKeys, K: rel.K, Offset: rel.Offset}, nil
	}
	return nil, fmt.Errorf("BuildOperator: unsupported rel op %v", rel.Op)
}

// checkProjectionOps rejects expressions the row-by-row eval has not implemented yet.
func checkProjectionOps(outs []sql.BoundOutput) error {
	for _, o := range outs {
		if err := checkExecExpr(o.Expr); err != nil {
			return err
		}
	}
	return nil
}

func checkExecExpr(expr sql.BoundExpr) error {
	if err := walkUnsupported(expr); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func walkUnsupported(expr sql.BoundExpr) error {
	if expr.Op == sql.ExprInvalid {
		return fmt.Errorf("invalid expression")
	}
	for _, a := range expr.Args {
		if err := walkUnsupported(a); err != nil {
			return err
		}
	}
	return nil
}

// buildSortInput detects RelSort directly over RelScan with a single int-like column
// SortKey and K > 0, and pushes top-K into ScanOpts. SortOp stays above for correctness.
func buildSortInput(sortRel *sql.Rel, segments SegmentsFn) (Operator, error) {
	child := sortRel.Inputs[0]
	if child.Op == sql.RelScan && child.Where == nil && sortRel.K > 0 && len(sortRel.SortKeys) == 1 {
		key := sortRel.SortKeys[0]
		if key.Expr.Op == sql.ExprColumn {
			colName := scanColumnName(child, key.Expr.ColumnID)
			if colName != "" && intLikeType(key.Expr.Type) {
				return buildScan(child, segments, &storage.TopKPushdown{
					Column: colName,
					Desc:   key.Desc,
					K:      sortRel.K,
					Offset: sortRel.Offset,
				})
			}
		}
	}
	return buildRel(child, segments)
}

func scanColumnName(scan *sql.Rel, id sql.ColumnID) string {
	for _, c := range scan.Table.Columns {
		if c.ID == id {
			return c.Name
		}
	}
	return ""
}

func intLikeType(t types.Type) bool {
	switch t.Kind {
	case types.KindInt16, types.KindInt32, types.KindInt64,
		types.KindDate, types.KindTimestamp, types.KindTime, types.KindDecimal:
		return true
	}
	return false
}

func scanColumnNames(rel *sql.Rel) ([]string, error) {
	names := make([]string, 0, len(rel.Columns))
	for _, id := range rel.Columns {
		found := false
		for _, c := range rel.Table.Columns {
			if c.ID == id {
				names = append(names, c.Name)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("BuildOperator: unknown ColumnID %d in scan for table %q", id, rel.Table.Name)
		}
	}
	return names, nil
}

func buildScan(rel *sql.Rel, segments SegmentsFn, topK *storage.TopKPushdown) (Operator, error) {
	if rel.Where != nil {
		if err := checkExecExpr(*rel.Where); err != nil {
			return nil, err
		}
	}
	segs, err := segments(rel.Table)
	if err != nil {
		return nil, fmt.Errorf("BuildOperator: resolve segments: %w", err)
	}
	names, err := scanColumnNames(rel)
	if err != nil {
		return nil, err
	}
	opts := storage.ScanOpts{Segments: segs, Columns: names, TopK: topK}
	var residual *sql.BoundExpr
	if rel.Where != nil {
		if pred, ok := loweredPredicate(*rel.Where); ok {
			opts.Predicate = pred
			if len(rel.PredicateOnly) > 0 {
				drop := make(map[sql.ColumnID]struct{}, len(rel.PredicateOnly))
				for _, id := range rel.PredicateOnly {
					drop[id] = struct{}{}
				}
				kept := make([]string, 0, len(names))
				for i, id := range rel.Columns {
					if _, ok := drop[id]; !ok {
						kept = append(kept, names[i])
					}
				}
				opts.Columns = kept
			}
		} else {
			residual = rel.Where
		}
	}
	parallelism := 1
	if topK == nil && len(segs) > 1 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	var op Operator = &ScanOp{Opts: opts, ColumnAlias: rel.Alias, Parallelism: parallelism}
	if residual != nil {
		op = &FilterOp{Source: op, Predicate: *residual}
	}
	return op, nil
}
