package storage

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestSortBufferNoOpWhenSortByEmpty(t *testing.T) {
	batches := []types.Batch{
		segmentBatch(t, []int64{3, 1}, []string{"c", "a"}),
	}
	out, err := sortBatchesBy(batches, nil)
	if err != nil {
		t.Fatalf("sortBatchesBy: %v", err)
	}
	if &out[0] != &batches[0] {
		t.Fatalf("expected pass-through batches when sortBy is empty")
	}
}

func TestSortBufferSingleColumnInt64(t *testing.T) {
	batches := []types.Batch{
		segmentBatch(t, []int64{5, 2, 9}, []string{"e", "b", "i"}),
		segmentBatch(t, []int64{4, 1}, []string{"d", "a"}),
	}
	out, err := sortBatchesBy(batches, []string{"tenant_id"})
	if err != nil {
		t.Fatalf("sortBatchesBy: %v", err)
	}
	if len(out) != 2 || out[0].Len != 3 || out[1].Len != 2 {
		t.Fatalf("batch shape changed: %d batches, lens %d/%d", len(out), out[0].Len, out[1].Len)
	}
	gotIDs := flattenInt64(out, "tenant_id")
	gotText := flattenText(out, "event_type")
	wantIDs := []int64{1, 2, 4, 5, 9}
	wantText := []string{"a", "b", "d", "e", "i"}
	for i, id := range gotIDs {
		if id != wantIDs[i] || gotText[i] != wantText[i] {
			t.Fatalf("row %d = (%d, %q), want (%d, %q)", i, id, gotText[i], wantIDs[i], wantText[i])
		}
	}
}

func TestSortBufferMultiColumn(t *testing.T) {
	batches := []types.Batch{
		segmentBatch(t, []int64{2, 1, 2, 1}, []string{"y", "y", "x", "x"}),
	}
	out, err := sortBatchesBy(batches, []string{"tenant_id", "event_type"})
	if err != nil {
		t.Fatalf("sortBatchesBy: %v", err)
	}
	gotIDs := flattenInt64(out, "tenant_id")
	gotText := flattenText(out, "event_type")
	wantIDs := []int64{1, 1, 2, 2}
	wantText := []string{"x", "y", "x", "y"}
	for i := range gotIDs {
		if gotIDs[i] != wantIDs[i] || gotText[i] != wantText[i] {
			t.Fatalf("row %d = (%d, %q), want (%d, %q)", i, gotIDs[i], gotText[i], wantIDs[i], wantText[i])
		}
	}
}

func TestSortBufferNullsLast(t *testing.T) {
	batches := []types.Batch{
		segmentBatch(t, []int64{3, 0, 1, 0}, []string{"c", "x", "a", "y"}, 1, 3),
	}
	out, err := sortBatchesBy(batches, []string{"tenant_id"})
	if err != nil {
		t.Fatalf("sortBatchesBy: %v", err)
	}
	col := out[0].Columns[0].V
	for row := range 2 {
		if !types.IsValid(col.Valid, row) {
			t.Fatalf("row %d expected valid", row)
		}
	}
	for row := 2; row < 4; row++ {
		if types.IsValid(col.Valid, row) {
			t.Fatalf("row %d expected null", row)
		}
	}
	if col.I64[0] != 1 || col.I64[1] != 3 {
		t.Fatalf("non-null prefix = %d,%d", col.I64[0], col.I64[1])
	}
}

func TestSortBufferRejectsEncodedBatch(t *testing.T) {
	batches := []types.Batch{segmentBatch(t, []int64{1}, []string{"a"})}
	batches[0].Columns[0].V.Encoding = types.EncodingDictionary
	if _, err := sortBatchesBy(batches, []string{"tenant_id"}); err == nil {
		t.Fatal("expected encoded-batch rejection")
	}
}

func TestSortBufferRejectsUnknownColumn(t *testing.T) {
	batches := []types.Batch{segmentBatch(t, []int64{1}, []string{"a"})}
	if _, err := sortBatchesBy(batches, []string{"missing"}); err == nil {
		t.Fatal("expected missing-column rejection")
	}
}

func TestStoreAppendBufferedAppliesSortBy(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Options.SortBy = []string{"tenant_id"}

	ctx := context.Background()
	if err := store.AppendBuffered(ctx, table, segmentBatch(t, []int64{9, 3, 7}, []string{"i", "c", "g"})); err != nil {
		t.Fatalf("AppendBuffered first: %v", err)
	}
	if err := store.AppendBuffered(ctx, table, segmentBatch(t, []int64{2, 8, 1, 4}, []string{"b", "h", "a", "d"})); err != nil {
		t.Fatalf("AppendBuffered second: %v", err)
	}
	if err := store.FlushBuffered(ctx, table); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}

	it, err := store.ScanIterator(ctx, table, NewPredicateEvaluator(Predicate{}), nil)
	if err != nil {
		t.Fatalf("ScanIterator: %v", err)
	}
	var ids []int64
	if err := it.ForEach(func(batch types.Batch, _ types.SelectionMask) error {
		col := batch.Columns[0].V
		ids = append(ids, col.I64[:batch.Len]...)
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] > ids[i] {
			t.Fatalf("not sorted: ids[%d]=%d > ids[%d]=%d (full: %v)", i-1, ids[i-1], i, ids[i], ids)
		}
	}
	if len(ids) != 7 {
		t.Fatalf("expected 7 rows, got %d", len(ids))
	}
}

func flattenInt64(batches []types.Batch, name string) []int64 {
	var out []int64
	for _, b := range batches {
		for ci, col := range b.Columns {
			if col.Name == name {
				out = append(out, b.Columns[ci].V.I64[:b.Len]...)
			}
		}
	}
	return out
}

func flattenText(batches []types.Batch, name string) []string {
	var out []string
	for _, b := range batches {
		for ci, col := range b.Columns {
			if col.Name == name {
				v := b.Columns[ci].V
				for r := 0; r < b.Len; r++ {
					out = append(out, v.Var.String(r))
				}
			}
		}
	}
	return out
}
