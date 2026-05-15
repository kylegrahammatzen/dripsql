// Per-operator wall-time + row-count counters for EXPLAIN ANALYZE. timingOperator
// wraps any Operator; BuildOperatorAnalyzed walks the Rel tree and returns the wrapper
// list in pre-order so callers can stitch counters back onto Rel.String output.
package exec

import (
	"context"
	"fmt"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type TimingStats struct {
	Label string
	Wall  time.Duration
	Rows  int64
	Calls int64
}

type timingOperator struct {
	inner Operator
	stats *TimingStats
}

func (t *timingOperator) Open(ctx context.Context) error { return t.inner.Open(ctx) }
func (t *timingOperator) Close() error                   { return t.inner.Close() }

func (t *timingOperator) Next() (types.Batch, bool, error) {
	start := time.Now()
	batch, ok, err := t.inner.Next()
	t.stats.Wall += time.Since(start)
	t.stats.Calls++
	if ok {
		if batch.Sel != nil {
			t.stats.Rows += int64(batch.Sel.PopCount())
		} else {
			t.stats.Rows += int64(batch.Len)
		}
	}
	return batch, ok, err
}

// BuildOperatorAnalyzed mirrors BuildOperator but wraps each constructed operator with a
// timingOperator. Returned stats are in pre-order matching Rel.String's depth-first walk.
func BuildOperatorAnalyzed(plan *sql.Plan, segments SegmentsFn) (Operator, []*TimingStats, error) {
	if plan == nil {
		return nil, nil, fmt.Errorf("BuildOperatorAnalyzed: nil plan")
	}
	if segments == nil {
		return nil, nil, fmt.Errorf("BuildOperatorAnalyzed: nil SegmentsFn")
	}
	if plan.Kind != sql.PlanQuery || plan.Rel == nil {
		op, err := BuildOperator(plan, segments)
		return op, nil, err
	}
	var stats []*TimingStats
	op, err := buildRelAnalyzed(plan.Rel, segments, &stats)
	return op, stats, err
}

func buildRelAnalyzed(rel *sql.Rel, segments SegmentsFn, stats *[]*TimingStats) (Operator, error) {
	if rel == nil {
		return nil, fmt.Errorf("buildRelAnalyzed: nil rel")
	}
	st := &TimingStats{Label: relTimingLabel(rel)}
	*stats = append(*stats, st)
	switch rel.Op {
	case sql.RelScan:
		op, err := buildScan(rel, segments, nil)
		if err != nil {
			return nil, err
		}
		return &timingOperator{inner: op, stats: st}, nil
	case sql.RelProject:
		if err := checkProjectionOps(rel.Projection); err != nil {
			return nil, err
		}
		source, err := buildRelAnalyzed(rel.Inputs[0], segments, stats)
		if err != nil {
			return nil, err
		}
		return &timingOperator{inner: &ProjectOp{Source: source, Outputs: rel.Projection}, stats: st}, nil
	case sql.RelLimit:
		source, err := buildRelAnalyzed(rel.Inputs[0], segments, stats)
		if err != nil {
			return nil, err
		}
		n := rel.Limit
		if n < 0 {
			n = -1
		}
		return &timingOperator{inner: &LimitOp{Source: source, N: n, Offset: rel.Offset}, stats: st}, nil
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
		source, err := buildRelAnalyzed(rel.Inputs[0], segments, stats)
		if err != nil {
			return nil, err
		}
		return &timingOperator{inner: &AggregateOp{
			Source:     source,
			GroupBy:    rel.GroupBy,
			Aggregates: rel.Aggregates,
			Hidden:     rel.Hidden,
			Having:     rel.Having,
		}, stats: st}, nil
	case sql.RelFilter:
		if err := checkExecExpr(rel.Predicate); err != nil {
			return nil, err
		}
		source, err := buildRelAnalyzed(rel.Inputs[0], segments, stats)
		if err != nil {
			return nil, err
		}
		return &timingOperator{inner: &FilterOp{Source: source, Predicate: rel.Predicate}, stats: st}, nil
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
		return &timingOperator{inner: &SortOp{Source: source, Keys: rel.SortKeys, K: rel.K, Offset: rel.Offset}, stats: st}, nil
	case sql.RelJoin:
		op, err := buildRel(rel, segments)
		if err != nil {
			return nil, err
		}
		return &timingOperator{inner: op, stats: st}, nil
	}
	return nil, fmt.Errorf("buildRelAnalyzed: unsupported rel op %v", rel.Op)
}

func relTimingLabel(rel *sql.Rel) string {
	switch rel.Op {
	case sql.RelScan:
		return "Scan " + rel.Table.Name
	case sql.RelProject:
		return "Project"
	case sql.RelLimit:
		return "Limit"
	case sql.RelAggregate:
		return "Aggregate"
	case sql.RelFilter:
		return "Filter"
	case sql.RelSort:
		return "Sort"
	case sql.RelJoin:
		return rel.JoinKind.String() + "Join"
	}
	return fmt.Sprintf("Rel(%v)", rel.Op)
}
