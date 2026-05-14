package exec

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestFilterIntersectsExistingSelection(t *testing.T) {
	batch := execInt64Batch(t, []int64{42, 7, 42}, nil)
	in := types.NewSelectionMask(batch.Len)
	in.Set(1)
	in.Set(2)
	downstream := &collectConsumer{}
	filter := &Filter{
		Predicate:  storage.NewPredicateEvaluator(storage.Predicate{Column: "amount", Op: storage.PredicateOpEq, PredicateValue: storage.PredicateValue{Int64: 42}}),
		Downstream: downstream,
	}
	if err := filter.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := filter.Push(batch, in); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := filter.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(downstream.sels) != 1 {
		t.Fatalf("downstream pushes = %d, want 1", len(downstream.sels))
	}
	assertSelectionRows(t, downstream.sels[0], []int{2})
}

func TestProjectColumnSubsetPreservesSelection(t *testing.T) {
	batch := execTwoInt64Batch(t, []int64{1, 2}, []int64{10, 20})
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(1)
	downstream := &collectConsumer{}
	project := &Project{Columns: []string{"other"}, Downstream: downstream}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := project.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(downstream.batches) != 1 {
		t.Fatalf("downstream pushes = %d, want 1", len(downstream.batches))
	}
	got := downstream.batches[0]
	if got.Len != 2 || len(got.Columns) != 1 || got.Columns[0].Name != "other" {
		t.Fatalf("projected batch = %#v", got)
	}
	assertSelectionRows(t, downstream.sels[0], []int{1})
}

func TestProjectRejectsMissingColumn(t *testing.T) {
	batch := execInt64Batch(t, []int64{1}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	project := &Project{Columns: []string{"missing"}, Downstream: &collectConsumer{}}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err == nil {
		t.Fatal("expected missing projected column error")
	}
}

func TestLimitOffsetAcrossBatches(t *testing.T) {
	first := execInt64Batch(t, []int64{1, 2, 3}, nil)
	second := execInt64Batch(t, []int64{4, 5, 6}, nil)
	downstream := &collectConsumer{}
	limit := &Limit{Offset: 2, Limit: 3, Downstream: downstream}
	if err := limit.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	firstSel := types.NewSelectionMask(first.Len)
	firstSel.FillAll()
	if err := limit.Push(first, firstSel); err != nil {
		t.Fatalf("Push first: %v", err)
	}
	secondSel := types.NewSelectionMask(second.Len)
	secondSel.FillAll()
	if err := limit.Push(second, secondSel); err != nil {
		t.Fatalf("Push second: %v", err)
	}
	if err := limit.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(downstream.sels) != 2 {
		t.Fatalf("downstream pushes = %d, want 2", len(downstream.sels))
	}
	assertSelectionRows(t, downstream.sels[0], []int{2})
	assertSelectionRows(t, downstream.sels[1], []int{0, 1})
}

func TestScanFilterProjectLimitCountPipeline(t *testing.T) {
	batch := execTwoInt64Batch(t, []int64{42, 7, 42, 42}, []int64{1, 2, 3, 4})
	sink := &CountSink{}
	agg := &Aggregate{Sinks: []AggregateSink{sink}}
	limit := &Limit{Offset: 1, Limit: 1, Downstream: agg}
	project := &Project{Columns: []string{"other"}, Downstream: limit}
	filter := &Filter{Predicate: storage.NewPredicateEvaluator(storage.Predicate{Column: "amount", Op: storage.PredicateOpEq, PredicateValue: storage.PredicateValue{Int64: 42}}), Downstream: project}
	scan := &Scan{Iterator: storage.SegmentScanIterator{Segments: []storage.ScanSegment{{Pages: []storage.ScanPage{{Batch: batch}}}}}}
	if err := scan.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := scan.Run(filter); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 1 {
		t.Fatalf("Result = %v, %v; want 1, nil", got, err)
	}
}

type collectConsumer struct {
	batches []types.Batch
	sels    []types.SelectionMask
}

func (c *collectConsumer) Open(context.Context) error { return nil }

func (c *collectConsumer) Push(batch types.Batch, sel types.SelectionMask) error {
	c.batches = append(c.batches, batch)
	copyMask := types.NewSelectionMask(sel.Rows)
	copy(copyMask.Words, sel.Words)
	c.sels = append(c.sels, copyMask)
	return nil
}

func (c *collectConsumer) Close() error { return nil }

func execTwoInt64Batch(t *testing.T, amount []int64, other []int64) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amount), I64: append([]int64(nil), amount...)}},
		{Name: "other", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(other), I64: append([]int64(nil), other...)}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func assertSelectionRows(t *testing.T, sel types.SelectionMask, want []int) {
	t.Helper()
	rows := make([]int, 0, sel.PopCount())
	sel.IterSet(func(row int) { rows = append(rows, row) })
	if len(rows) != len(want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("rows = %v, want %v", rows, want)
		}
	}
}
