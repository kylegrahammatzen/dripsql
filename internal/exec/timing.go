// Per-operator wall-time and row-count counters for EXPLAIN ANALYZE.
// BuildOperatorAnalyzed wraps each Operator and returns a tree-rooted TimingStats.
package exec

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type TimingStats struct {
	Label    string
	Wall     time.Duration
	Rows     int64
	Calls    int64
	Children []*TimingStats
}

func (t *TimingStats) Tree() string {
	if t == nil {
		return ""
	}
	var sb strings.Builder
	t.write(&sb, 0)
	return sb.String()
}

func (t *TimingStats) write(sb *strings.Builder, depth int) {
	indent := strings.Repeat("  ", depth)
	fmt.Fprintf(sb, "%s%-22s wall=%s rows=%d calls=%d\n", indent, t.Label, t.Wall, t.Rows, t.Calls)
	for _, c := range t.Children {
		c.write(sb, depth+1)
	}
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

// BuildOperatorAnalyzed wraps each constructed Operator with a timingOperator and
// returns the root of the parent-child stats tree mirroring the Rel tree.
func BuildOperatorAnalyzed(plan *sql.Plan, segments SegmentsFn) (Operator, *TimingStats, error) {
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
	return buildRelAnalyzed(plan.Rel, segments)
}

func buildRelAnalyzed(rel *sql.Rel, segments SegmentsFn) (Operator, *TimingStats, error) {
	if rel == nil {
		return nil, nil, fmt.Errorf("buildRelAnalyzed: nil rel")
	}
	st := &TimingStats{Label: relTimingLabel(rel)}
	switch rel.Op {
	case sql.RelScan:
		op, err := buildScan(rel, segments, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		return &timingOperator{inner: op, stats: st}, st, nil
	case sql.RelProject:
		if err := checkProjectionOps(rel.Projection); err != nil {
			return nil, nil, err
		}
		source, child, err := buildRelAnalyzed(rel.Inputs[0], segments)
		if err != nil {
			return nil, nil, err
		}
		st.Children = append(st.Children, child)
		return &timingOperator{inner: &ProjectOp{Source: source, Outputs: rel.Projection}, stats: st}, st, nil
	case sql.RelLimit:
		source, child, err := buildRelAnalyzed(rel.Inputs[0], segments)
		if err != nil {
			return nil, nil, err
		}
		st.Children = append(st.Children, child)
		n := rel.Limit
		if n < 0 {
			n = -1
		}
		return &timingOperator{inner: &LimitOp{Source: source, N: n, Offset: rel.Offset}, stats: st}, st, nil
	case sql.RelAggregate:
		for _, expr := range rel.GroupBy {
			if err := checkExecExpr(expr); err != nil {
				return nil, nil, err
			}
		}
		if rel.Having != nil {
			if err := checkExecExpr(*rel.Having); err != nil {
				return nil, nil, err
			}
		}
		source, child, err := buildRelAnalyzed(rel.Inputs[0], segments)
		if err != nil {
			return nil, nil, err
		}
		st.Children = append(st.Children, child)
		return &timingOperator{inner: &AggregateOp{
			Source:     source,
			GroupBy:    rel.GroupBy,
			Aggregates: rel.Aggregates,
			Hidden:     rel.Hidden,
			Having:     rel.Having,
		}, stats: st}, st, nil
	case sql.RelFilter:
		if err := checkExecExpr(rel.Predicate); err != nil {
			return nil, nil, err
		}
		source, child, err := buildRelAnalyzed(rel.Inputs[0], segments)
		if err != nil {
			return nil, nil, err
		}
		st.Children = append(st.Children, child)
		return &timingOperator{inner: &FilterOp{Source: source, Predicate: rel.Predicate}, stats: st}, st, nil
	case sql.RelSort:
		for _, key := range rel.SortKeys {
			if err := checkExecExpr(key.Expr); err != nil {
				return nil, nil, err
			}
		}
		source, err := buildSortInput(rel, segments, nil)
		if err != nil {
			return nil, nil, err
		}
		return &timingOperator{inner: &SortOp{Source: source, Keys: rel.SortKeys, K: rel.K, Offset: rel.Offset}, stats: st}, st, nil
	case sql.RelJoin:
		op, err := buildRel(rel, segments)
		if err != nil {
			return nil, nil, err
		}
		return &timingOperator{inner: op, stats: st}, st, nil
	case sql.RelUnion:
		if len(rel.Inputs) != 2 {
			return nil, nil, fmt.Errorf("buildRelAnalyzed: RelUnion needs two inputs")
		}
		left, lc, err := buildRelAnalyzed(rel.Inputs[0], segments)
		if err != nil {
			return nil, nil, err
		}
		right, rc, err := buildRelAnalyzed(rel.Inputs[1], segments)
		if err != nil {
			_ = left.Close()
			return nil, nil, err
		}
		st.Children = append(st.Children, lc, rc)
		return &timingOperator{inner: &UnionOp{Left: left, Right: right}, stats: st}, st, nil
	}
	return nil, nil, fmt.Errorf("buildRelAnalyzed: unsupported rel op %v", rel.Op)
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
	case sql.RelUnion:
		return "Union"
	}
	return fmt.Sprintf("Rel(%v)", rel.Op)
}
