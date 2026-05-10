package exec

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestSortInt64AscAcrossBatches(t *testing.T) {
	first := execSortBatch(t, []int64{3, 1}, []string{"c", "a"}, nil)
	second := execSortBatch(t, []int64{2}, []string{"b"}, nil)
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "amount"}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	firstSel := types.NewSelectionMask(first.Len)
	firstSel.FillAll()
	if err := sortOp.Push(first, firstSel); err != nil {
		t.Fatalf("Push first: %v", err)
	}
	secondSel := types.NewSelectionMask(second.Len)
	secondSel.FillAll()
	if err := sortOp.Push(second, secondSel); err != nil {
		t.Fatalf("Push second: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := singleSortedBatch(t, downstream)
	assertInt64Column(t, got, "amount", []int64{1, 2, 3})
	assertTextColumn(t, got, "event_type", []string{"a", "b", "c"})
	assertSelectionRows(t, downstream.sels[0], []int{0, 1, 2})
}

func TestSortInt64Desc(t *testing.T) {
	batch := execSortBatch(t, []int64{1, 3, 2}, []string{"a", "c", "b"}, nil)
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "amount", Desc: true}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	if err := sortOp.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertInt64Column(t, singleSortedBatch(t, downstream), "amount", []int64{3, 2, 1})
}

func TestSortTextStableForEqualKeys(t *testing.T) {
	batch := execSortBatchWithSeq(t, []int64{1, 1, 1}, []string{"b", "a", "a"}, []int64{0, 1, 2})
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "event_type"}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	if err := sortOp.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := singleSortedBatch(t, downstream)
	assertTextColumn(t, got, "event_type", []string{"a", "a", "b"})
	assertInt64Column(t, got, "seq", []int64{1, 2, 0})
}

func TestSortMultiKey(t *testing.T) {
	batch := execSortBatch(t, []int64{2, 1, 2, 1}, []string{"b", "b", "a", "a"}, nil)
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "amount"}, {Column: "event_type", Desc: true}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	if err := sortOp.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := singleSortedBatch(t, downstream)
	assertInt64Column(t, got, "amount", []int64{1, 1, 2, 2})
	assertTextColumn(t, got, "event_type", []string{"b", "a", "b", "a"})
}

func TestSortNullsLastAndPreservesValidity(t *testing.T) {
	valid := types.NewValidity(3)
	types.SetInvalid(valid, 1)
	batch := execSortBatch(t, []int64{3, 0, 1}, []string{"c", "null", "a"}, valid)
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "amount"}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	if err := sortOp.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := singleSortedBatch(t, downstream)
	assertInt64Column(t, got, "amount", []int64{1, 3, 0})
	if types.IsValid(got.Columns[0].V.Valid, 2) {
		t.Fatalf("sorted null row should remain invalid")
	}
}

func TestSortEmptyInputEmitsNoBatch(t *testing.T) {
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "amount"}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := sortOp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(downstream.batches) != 0 {
		t.Fatalf("downstream batches = %d, want 0", len(downstream.batches))
	}
}

func TestSortRejectsMissingKey(t *testing.T) {
	batch := execSortBatch(t, []int64{1}, []string{"a"}, nil)
	downstream := &collectConsumer{}
	sortOp := &Sort{Keys: []SortKey{{Column: "missing"}}, Downstream: downstream}
	if err := sortOp.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	if err := sortOp.Push(batch, sel); err == nil {
		t.Fatal("expected missing sort key error")
	}
}

func singleSortedBatch(t *testing.T, downstream *collectConsumer) types.Batch {
	t.Helper()
	if len(downstream.batches) != 1 {
		t.Fatalf("downstream batches = %d, want 1", len(downstream.batches))
	}
	return downstream.batches[0]
}

func execSortBatch(t *testing.T, amounts []int64, events []string, amountValid types.Validity) types.Batch {
	t.Helper()
	if len(amounts) != len(events) {
		t.Fatalf("amounts/events length mismatch: %d/%d", len(amounts), len(events))
	}
	varText := types.NewVarBytes(len(events), 0)
	for row, value := range events {
		varText.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amounts), Valid: amountValid, I64: append([]int64(nil), amounts...)}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(events), Var: varText}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execSortBatchWithSeq(t *testing.T, amounts []int64, events []string, seq []int64) types.Batch {
	t.Helper()
	if len(amounts) != len(events) || len(amounts) != len(seq) {
		t.Fatalf("sort batch length mismatch")
	}
	varText := types.NewVarBytes(len(events), 0)
	for row, value := range events {
		varText.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amounts), I64: append([]int64(nil), amounts...)}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(events), Var: varText}},
		{Name: "seq", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(seq), I64: append([]int64(nil), seq...)}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func assertInt64Column(t *testing.T, batch types.Batch, name string, want []int64) {
	t.Helper()
	col, ok := columnByName(batch, name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	if len(col.V.I64) != len(want) {
		t.Fatalf("%s len = %d, want %d", name, len(col.V.I64), len(want))
	}
	for i, value := range want {
		if col.V.I64[i] != value {
			t.Fatalf("%s = %v, want %v", name, col.V.I64, want)
		}
	}
}

func assertTextColumn(t *testing.T, batch types.Batch, name string, want []string) {
	t.Helper()
	col, ok := columnByName(batch, name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	if col.V.Len != len(want) {
		t.Fatalf("%s len = %d, want %d", name, col.V.Len, len(want))
	}
	for i, value := range want {
		got, ok := TextValueCopy(col.V, i)
		if !ok || got != value {
			t.Fatalf("%s[%d] = %q, %v; want %q, true", name, i, got, ok, value)
		}
	}
}
