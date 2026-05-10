package storage

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestStoreAppendAndScanIterator(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()

	if _, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"})); err != nil {
		t.Fatalf("AppendBatch first: %v", err)
	}
	if _, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{1, 42, 3}, []string{"signup", "checkout", "login"})); err != nil {
		t.Fatalf("AppendBatch second: %v", err)
	}
	stats := &ExecStats{}
	it, err := store.ScanIterator(context.Background(), table, NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}), stats)
	if err != nil {
		t.Fatalf("ScanIterator: %v", err)
	}

	visits := 0
	matched := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		matched += sel.PopCount()
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 2 || matched != 2 {
		t.Fatalf("visits = %d matched = %d, want 2/2", visits, matched)
	}
	if stats.SegmentsTotal != 2 || stats.PagesTotal != 2 || stats.RowsTotal != 5 || stats.RowsMatched != 2 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestStoreAppendBatchesWritesSingleSegment(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()

	meta, err := store.AppendBatches(context.Background(), table, []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4, 5}, []string{"login", "signup", "checkout"}),
	})
	if err != nil {
		t.Fatalf("AppendBatches: %v", err)
	}
	if meta.Rows != 5 || len(meta.Columns[0].Pages) != 2 {
		t.Fatalf("meta = %#v", meta)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].Meta.ID != 1 {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestIngestBufferCopiesAndFlushesOwnedBatches(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 4})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	batch := segmentBatch(t, []int64{1, 2}, []string{"a", "b"})
	published, err := buffer.Append(context.Background(), batch)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(published) != 0 || buffer.BufferedRows() != 2 {
		t.Fatalf("published=%d buffered=%d, want 0/2", len(published), buffer.BufferedRows())
	}

	batch.Columns[0].V.I64[0] = 99
	batch.Columns[1].V.Var.Data[0] = 'z'
	meta, ok, err := buffer.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !ok || meta.Rows != 2 || buffer.BufferedRows() != 0 {
		t.Fatalf("flush ok=%v meta=%#v buffered=%d", ok, meta, buffer.BufferedRows())
	}
	batch, err = firstScannedBatch(t, store, table, nil)
	if err != nil {
		t.Fatalf("firstScannedBatch: %v", err)
	}
	if got := batch.Columns[0].V.I64[0]; got != 1 {
		t.Fatalf("copied int = %d, want 1", got)
	}
	if got := textValue(t, batch.Columns[1].V, 0); got != "a" {
		t.Fatalf("copied text = %q, want a", got)
	}
}

func TestIngestBufferAutoFlushesAtTargetRows(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 3})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	published, err := buffer.Append(context.Background(), segmentBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"}))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(published) != 1 || published[0].Rows != 3 || buffer.BufferedRows() != 0 {
		t.Fatalf("published=%#v buffered=%d", published, buffer.BufferedRows())
	}
}

func TestNewIngestBufferUsesTableSegmentRows(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Options.SegmentRows = types.SegmentRows(7)
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	if buffer.targetRows != 7 {
		t.Fatalf("targetRows = %d, want 7", buffer.targetRows)
	}
	published, err := buffer.Append(context.Background(), segmentBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"}))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(published) != 0 {
		t.Fatalf("published before flush = %d, want 0", len(published))
	}
	published, err = buffer.Append(context.Background(), segmentBatch(t, []int64{4, 5, 6, 7}, []string{"d", "e", "f", "g"}))
	if err != nil {
		t.Fatalf("Append 2nd: %v", err)
	}
	if len(published) != 1 || published[0].Rows != 7 {
		t.Fatalf("published=%#v", published)
	}
}

func TestNewIngestBufferDefaultsToDefaultSegmentRows(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	buffer, err := store.NewIngestBuffer(storeTableSpec(), IngestBufferOptions{})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	if buffer.targetRows != DefaultSegmentRows {
		t.Fatalf("targetRows = %d, want %d", buffer.targetRows, DefaultSegmentRows)
	}
}

func TestNewIngestBufferNilContextDefaultsToBackground(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	buffer, err := store.NewIngestBuffer(storeTableSpec(), IngestBufferOptions{TargetRows: 2})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	//lint:ignore SA1012 this test exercises the nil-context fallback path
	published, err := buffer.Append(nil, segmentBatch(t, []int64{1, 2}, []string{"a", "b"}))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(published) != 1 {
		t.Fatalf("published=%#v", published)
	}
	//lint:ignore SA1012 this test exercises the nil-context fallback path
	_, ok, err := buffer.Flush(nil)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if ok {
		t.Fatal("expected flush on empty buffer to return ok=false")
	}
}

func TestIngestBufferNilErrors(t *testing.T) {
	var buffer *IngestBuffer
	if _, err := buffer.Append(context.Background(), segmentBatch(t, []int64{1}, []string{"a"})); err == nil {
		t.Fatal("expected nil buffer error")
	}
	if _, _, err := buffer.Flush(context.Background()); err == nil {
		t.Fatal("expected nil buffer error")
	}
}

func TestStoreAppendBufferedFlushAllOwnsInput(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	batch := segmentBatch(t, []int64{1, 2}, []string{"a", "b"})
	if err := store.AppendBuffered(context.Background(), table, batch); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments before flush: %v", err)
	}
	if len(segments) != 1 || segments[0].Path != "" {
		t.Fatalf("segments before flush = %#v, want one in-memory segment", segments)
	}

	batch.Columns[0].V.I64[0] = 99
	batch.Columns[1].V.Var.Data[0] = 'z'
	if err := store.FlushAllBuffered(context.Background()); err != nil {
		t.Fatalf("FlushAllBuffered: %v", err)
	}
	out, err := firstScannedBatch(t, store, table, nil)
	if err != nil {
		t.Fatalf("firstScannedBatch: %v", err)
	}
	if got := out.Columns[0].V.I64[0]; got != 1 {
		t.Fatalf("copied int = %d, want 1", got)
	}
	if got := textValue(t, out.Columns[1].V, 0); got != "a" {
		t.Fatalf("copied text = %q, want a", got)
	}
}

func TestStoreAppendBufferedAutoFlushesAtTableTarget(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Options.SegmentRows = types.SegmentRows(3)
	if err := store.AppendBuffered(context.Background(), table, segmentBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].Meta.Rows != 3 {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestStoreAppendBufferedFlushWaitsForAsyncSeals(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Options.SegmentRows = types.SegmentRows(2)

	for _, batch := range []types.Batch{
		segmentBatch(t, []int64{1}, []string{"a"}),
		segmentBatch(t, []int64{2}, []string{"b"}),
		segmentBatch(t, []int64{3}, []string{"c"}),
		segmentBatch(t, []int64{4}, []string{"d"}),
		segmentBatch(t, []int64{5}, []string{"e"}),
	} {
		if err := store.AppendBuffered(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBuffered: %v", err)
		}
	}
	if err := store.FlushBuffered(context.Background(), table); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 3 {
		t.Fatalf("segments = %#v, want 3", segments)
	}
	for i, segment := range segments {
		wantID := SegmentID(i + 1)
		if segment.Meta.ID != wantID {
			t.Fatalf("segment %d ID = %d, want %d", i, segment.Meta.ID, wantID)
		}
	}
	if segments[0].Meta.Rows != 2 || segments[1].Meta.Rows != 2 || segments[2].Meta.Rows != 1 {
		t.Fatalf("segment rows = %d/%d/%d, want 2/2/1", segments[0].Meta.Rows, segments[1].Meta.Rows, segments[2].Meta.Rows)
	}
}

func TestStoreAppendBatchesWaitsForPendingBufferedSeal(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Options.SegmentRows = types.SegmentRows(2)

	if err := store.AppendBuffered(context.Background(), table, segmentBatch(t, []int64{1, 2}, []string{"a", "b"})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	meta, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{3}, []string{"c"}))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if meta.ID != 2 {
		t.Fatalf("AppendBatch segment ID = %d, want 2", meta.ID)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 2 || segments[0].Meta.ID != 1 || segments[1].Meta.ID != 2 {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestStoreAppendBufferedCloseFlushesForReopen(t *testing.T) {
	root := t.TempDir()
	table := storeTableSpec()
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.AppendBuffered(context.Background(), table, segmentBatch(t, []int64{1, 2}, []string{"a", "b"})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	segments, err := reopened.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].Meta.Rows != 2 {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestStoreReopenLoadsSegmentsAndContinuesIDs(t *testing.T) {
	root := t.TempDir()
	table := storeTableSpec()
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{1}, []string{"signup"})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	segments, err := reopened.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].Meta.ID != 1 {
		t.Fatalf("segments after reopen = %#v", segments)
	}
	meta, err := reopened.AppendBatch(context.Background(), table, segmentBatch(t, []int64{2}, []string{"checkout"}))
	if err != nil {
		t.Fatalf("AppendBatch after reopen: %v", err)
	}
	if meta.ID != 2 {
		t.Fatalf("next segment ID = %d, want 2", meta.ID)
	}
}

func TestStoreConcurrentAppendAssignsUniqueSegmentIDs(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	batch := segmentBatch(t, []int64{1}, []string{"signup"})
	const appends = 8
	ids := make(chan SegmentID, appends)
	errs := make(chan error, appends)
	var wg sync.WaitGroup
	for i := 0; i < appends; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			meta, err := store.AppendBatch(context.Background(), table, batch)
			if err != nil {
				errs <- err
				return
			}
			ids <- meta.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
	}
	seen := make(map[SegmentID]struct{}, appends)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate segment ID %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != appends {
		t.Fatalf("segment IDs = %d, want %d", len(seen), appends)
	}
	segments, err := store.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != appends {
		t.Fatalf("segments = %d, want %d", len(segments), appends)
	}
}

func TestStoreReopenUsesManifestNotDirectoryScan(t *testing.T) {
	root := t.TempDir()
	table := storeTableSpec()
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{1}, []string{"signup"})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	roguePath := filepath.Join(root, "tables", "events", "segments", "0000000000000099.dsv3")
	if _, err := WriteSegment(roguePath, 99, []types.Batch{segmentBatch(t, []int64{99}, []string{"rogue"})}); err != nil {
		t.Fatalf("WriteSegment rogue: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	segments, err := reopened.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 || segments[0].Meta.ID != 1 {
		t.Fatalf("segments = %#v, want only manifested segment", segments)
	}
}

func TestStoreManifestIgnoresAndTruncatesPartialTail(t *testing.T) {
	root := t.TempDir()
	table := storeTableSpec()
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{1}, []string{"signup"})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	manifestPath := filepath.Join(root, "tables", "events", manifestFileName)
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile manifest: %v", err)
	}
	if _, err := file.WriteString(`{"path":`); err != nil {
		_ = file.Close()
		t.Fatalf("WriteString manifest: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close manifest: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close store: %v", err)
	}

	reopened, err := Open(root)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	segments, err := reopened.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("segments = %#v, want one complete manifest record", segments)
	}
	if _, err := reopened.AppendBatch(context.Background(), table, segmentBatch(t, []int64{2}, []string{"checkout"})); err != nil {
		t.Fatalf("AppendBatch after partial tail: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	third, err := Open(root)
	if err != nil {
		t.Fatalf("third open: %v", err)
	}
	t.Cleanup(func() { _ = third.Close() })
	segments, err = third.ScanSegments(context.Background(), table)
	if err != nil {
		t.Fatalf("ScanSegments after truncation: %v", err)
	}
	if len(segments) != 2 || segments[1].Meta.ID != 2 {
		t.Fatalf("segments after truncation = %#v", segments)
	}
}

func TestStoreAppendAfterCloseErrors(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = store.AppendBatch(context.Background(), storeTableSpec(), segmentBatch(t, []int64{1}, []string{"signup"}))
	if err == nil {
		t.Fatal("expected append-after-close error")
	}
}

func TestStoreAppendRejectsSchemaMismatch(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	batch := segmentBatch(t, []int64{1}, []string{"signup"})
	batch.Columns[0].Name = "account_id"
	_, err = store.AppendBatch(context.Background(), storeTableSpec(), batch)
	if err == nil {
		t.Fatal("expected schema mismatch error")
	}
}

func TestStoreAppendRejectsNotNullViolation(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	table := storeTableSpec()
	table.Columns[0].Nullable = false
	_, err = store.AppendBatch(context.Background(), table, segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}, 1))
	if err == nil {
		t.Fatal("expected not-null violation")
	}
}

func storeTableSpec() types.TableSpec {
	return types.TableSpec{
		Name: "events",
		Columns: []types.ColumnSpec{
			{Name: "tenant_id", Type: types.Int64, Nullable: true},
			{Name: "event_type", Type: types.Text, Nullable: true},
		},
	}
}

func firstScannedBatch(t *testing.T, store *Store, table types.TableSpec, pred PredicateEvaluator) (types.Batch, error) {
	t.Helper()
	it, err := store.ScanIterator(context.Background(), table, pred, nil)
	if err != nil {
		return types.Batch{}, err
	}
	var out types.Batch
	visits := 0
	err = it.ForEach(func(batch types.Batch, _ types.SelectionMask) error {
		if visits == 0 {
			out = batch
		}
		visits++
		return nil
	})
	if err != nil {
		return types.Batch{}, err
	}
	if visits == 0 {
		t.Fatal("expected at least one scanned batch")
	}
	return out, nil
}
