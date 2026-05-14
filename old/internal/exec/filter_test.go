package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestFilterPredicateOnlyVisitsSelectedRows(t *testing.T) {
	batch := execInt64Batch(t, []int64{1, 2, 3, 4}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(1)
	sel.Set(3)
	downstream := &collectConsumer{}
	predicate := &countingPredicate{matches: map[int]bool{3: true}, recordVisited: true}
	filter := &Filter{Predicate: predicate, Downstream: downstream}
	if err := filter.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := filter.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got, want := fmt.Sprint(predicate.visited), "[1 3]"; got != want {
		t.Fatalf("visited rows = %s, want %s", got, want)
	}
	if len(downstream.sels) != 1 {
		t.Fatalf("downstream pushes = %d, want 1", len(downstream.sels))
	}
	assertSelectionRows(t, downstream.sels[0], []int{3})
}

func TestFilterSparseSelectionNoFullBatchEval(t *testing.T) {
	batch := execInt64Batch(t, []int64{1, 2, 3, 4}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(2)
	predicate := &countingPredicate{matches: map[int]bool{2: true}, recordVisited: true}
	filter := &Filter{Predicate: predicate, Downstream: &collectConsumer{}}
	if err := filter.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := filter.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if predicate.fullEvalCalled {
		t.Fatal("Filter called full-batch Eval, want EvalSelected")
	}
	if got, want := len(predicate.visited), 1; got != want {
		t.Fatalf("visited rows = %d, want %d", got, want)
	}
}

func BenchmarkFilterSparseSelection(b *testing.B) {
	batch := execInt64Batch(b, benchmarkInt64Values(types.StandardBatchRows), nil)
	sel := types.NewSelectionMask(batch.Len)
	for row := 0; row < batch.Len; row += 64 {
		sel.Set(row)
	}
	filter := &Filter{
		Predicate:  &countingPredicate{matchAll: true},
		Downstream: noopConsumer{},
	}
	if err := filter.Open(context.Background()); err != nil {
		b.Fatalf("Open: %v", err)
	}
	if err := filter.Push(batch, sel); err != nil {
		b.Fatalf("prewarm Push: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := filter.Push(batch, sel); err != nil {
			b.Fatal(err)
		}
	}
}

type countingPredicate struct {
	matches        map[int]bool
	matchAll       bool
	recordVisited  bool
	visited        []int
	fullEvalCalled bool
}

func (e *countingPredicate) RequiredColumns() []string { return nil }

func (e *countingPredicate) Eval(batch types.Batch, sel *types.SelectionMask) (int, error) {
	e.fullEvalCalled = true
	sel.Resize(batch.Len)
	matched := 0
	for row := 0; row < batch.Len; row++ {
		if e.matchAll || e.matches[row] {
			sel.SetUnsafe(row)
			matched++
		}
	}
	return matched, nil
}

func (e *countingPredicate) EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
	sel.Resize(batch.Len)
	matched := 0
	input.IterSet(func(row int) {
		if e.recordVisited {
			e.visited = append(e.visited, row)
		}
		if e.matchAll || e.matches[row] {
			sel.SetUnsafe(row)
			matched++
		}
	})
	return matched, nil
}

type noopConsumer struct{}

func (noopConsumer) Open(context.Context) error { return nil }

func (noopConsumer) Push(types.Batch, types.SelectionMask) error { return nil }

func (noopConsumer) Close() error { return nil }

func benchmarkInt64Values(rows int) []int64 {
	values := make([]int64, rows)
	for row := range values {
		values[row] = int64(row)
	}
	return values
}
