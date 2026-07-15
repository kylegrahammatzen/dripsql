// Per-operator wall-time and row-count counters for EXPLAIN ANALYZE.
// BuildOperatorAnalyzed wraps the real operator tree post build so ANALYZE always measures the plan that actually runs.
package exec

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
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

func (t *timingOperator) Next() (vector.Batch, bool, error) {
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

// BuildOperatorAnalyzed builds the normal operator tree, then wraps every node with
// a timingOperator so the measured plan is identical to the executed plan.
func BuildOperatorAnalyzed(plan *sql.Plan, segments SegmentsFn) (Operator, *TimingStats, error) {
	op, err := BuildOperator(plan, segments)
	if err != nil {
		return nil, nil, err
	}
	if plan.Kind != sql.PlanQuery {
		return op, nil, nil
	}
	wrapped, stats := wrapTimings(op)
	return wrapped, stats, nil
}

func wrapTimings(op Operator) (Operator, *TimingStats) {
	st := &TimingStats{Label: operatorLabel(op)}
	wrapChild := func(child Operator) Operator {
		wrapped, cs := wrapTimings(child)
		st.Children = append(st.Children, cs)
		return wrapped
	}
	switch o := op.(type) {
	case *ProjectOp:
		o.Source = wrapChild(o.Source)
	case *LimitOp:
		o.Source = wrapChild(o.Source)
	case *AggregateOp:
		o.Source = wrapChild(o.Source)
	case *FilterOp:
		o.Source = wrapChild(o.Source)
	case *SortOp:
		o.Source = wrapChild(o.Source)
	case *WindowOp:
		o.Source = wrapChild(o.Source)
	case *UnionOp:
		o.Left = wrapChild(o.Left)
		o.Right = wrapChild(o.Right)
	case *HashJoinOp:
		o.Left = wrapChild(o.Left)
		o.Right = wrapChild(o.Right)
	}
	return &timingOperator{inner: op, stats: st}, st
}

func operatorLabel(op Operator) string {
	switch o := op.(type) {
	case *ScanOp:
		return "Scan"
	case *TopKScanOp:
		return "TopKScan"
	case *ProjectOp:
		return "Project"
	case *LimitOp:
		return "Limit"
	case *AggregateOp:
		return "Aggregate"
	case *FilterOp:
		return "Filter"
	case *SortOp:
		return "Sort"
	case *WindowOp:
		return "Window"
	case *UnionOp:
		return "Union"
	case *HashJoinOp:
		return o.Kind.String() + "Join"
	}
	return fmt.Sprintf("%T", op)
}
