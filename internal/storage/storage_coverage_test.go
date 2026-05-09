package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestFlushAllBufferedPublishesPendingRows(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2}, []string{"a", "b"}, nil)

	if err := store.AppendBuffered(context.Background(), table, batch); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	if err := store.FlushAllBuffered(context.Background()); err != nil {
		t.Fatalf("FlushAllBuffered: %v", err)
	}
	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 1 || segments[0].Rows != 2 {
		t.Fatalf("segments = %#v, want one 2-row segment", segments)
	}
}

func TestBufferedPublisherPublishesSealedRuns(t *testing.T) {
	store, table := newTestStore(t)
	state, err := store.appendTableState(table)
	if err != nil {
		t.Fatalf("appendTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: DefaultPageRows})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	runs, err := buffer.AppendSealed(context.Background(), makeSequentialIntTextBatch(t, 0, DefaultPageRows, "published"))
	if err != nil {
		t.Fatalf("AppendSealed: %v", err)
	}
	if len(runs) != 1 || runs[0].Rows != DefaultPageRows {
		t.Fatalf("runs = %#v", runs)
	}

	state.bufferMu.Lock()
	state.buffer = buffer
	store.enqueueBufferedRunsLocked(table, state, runs)
	for state.publishing {
		state.bufferCond.Wait()
	}
	publishErr := state.publishErr
	pending := len(state.pendingRuns)
	state.bufferMu.Unlock()
	if publishErr != nil {
		t.Fatalf("publishErr: %v", publishErr)
	}
	if pending != 0 {
		t.Fatalf("pending runs = %d, want 0", pending)
	}

	count, err := store.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != uint64(DefaultPageRows) {
		t.Fatalf("count = %d, want %d", count, DefaultPageRows)
	}
}

func TestIngestBufferRecyclesFullPageRuns(t *testing.T) {
	store, table := newTestStore(t)
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: DefaultPageRows})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}

	first := makeSequentialIntTextBatch(t, 0, DefaultPageRows, "first")
	runs, err := buffer.AppendSealed(context.Background(), first)
	if err != nil {
		t.Fatalf("AppendSealed first: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("first runs = %#v", runs)
	}
	first.Columns[0].V.I64[0] = 99
	first.Columns[1].V.Var.Data[0] = 'z'
	owned := runs[0].Batches[0]
	if got := owned.Columns[0].V.I64[0]; got != 0 {
		t.Fatalf("owned int = %d, want 0", got)
	}
	if got := owned.Columns[1].V.Var.String(0); got != "first" {
		t.Fatalf("owned text = %q, want first", got)
	}

	buffer.RecycleRun(runs[0])
	ints := make([]int64, DefaultPageRows)
	strings := make([]string, DefaultPageRows)
	for row := range ints {
		ints[row] = int64(row + 1000)
		strings[row] = fmt.Sprintf("event-%d", row)
	}
	second := makeIntTextBatch(t, ints, strings, map[int]bool{1: true})
	runs, err = buffer.AppendSealed(context.Background(), second)
	if err != nil {
		t.Fatalf("AppendSealed second: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("second runs = %#v", runs)
	}
	second.Columns[0].V.I64[0] = -1
	second.Columns[1].V.Var.Data[0] = 'x'
	reused := runs[0].Batches[0]
	if got := reused.Columns[0].V.I64[0]; got != 1000 {
		t.Fatalf("reused int = %d, want 1000", got)
	}
	if reused.Columns[0].V.Valid == nil || vector.IsValid(reused.Columns[0].V.Valid, 1) {
		t.Fatalf("reused validity = %#v, want row 1 invalid", reused.Columns[0].V.Valid)
	}
	if got := reused.Columns[1].V.Var.String(0); got != "event-0" {
		t.Fatalf("reused text = %q, want event-0", got)
	}
}

func TestIngestBufferRecyclesMutablePages(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		store, table := newTestStoreBool(t)
		buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: DefaultPageRows})
		if err != nil {
			t.Fatalf("NewIngestBuffer: %v", err)
		}
		if runs, err := buffer.AppendSealed(context.Background(), makeBoolTextBatch(t, []bool{true, false}, []string{"old-a", "old-b"}, nil)); err != nil || len(runs) != 0 {
			t.Fatalf("AppendSealed first runs=%#v err=%v", runs, err)
		}
		run, ok, err := buffer.FlushSealed(context.Background())
		if err != nil || !ok {
			t.Fatalf("FlushSealed first ok=%v err=%v", ok, err)
		}
		buffer.RecycleRun(run)
		if runs, err := buffer.AppendSealed(context.Background(), makeBoolTextBatch(t, []bool{false, true}, []string{"new-a", "new-b"}, nil)); err != nil || len(runs) != 0 {
			t.Fatalf("AppendSealed second runs=%#v err=%v", runs, err)
		}
		run, ok, err = buffer.FlushSealed(context.Background())
		if err != nil || !ok {
			t.Fatalf("FlushSealed second ok=%v err=%v", ok, err)
		}
		batch := run.Batches[0]
		if boolAt(batch.Columns[0].V.BoolBits, 0) || !boolAt(batch.Columns[0].V.BoolBits, 1) {
			t.Fatalf("bool bits = %#v, want false/true", batch.Columns[0].V.BoolBits)
		}
		if got := batch.Columns[1].V.Var.String(1); got != "new-b" {
			t.Fatalf("text row 1 = %q, want new-b", got)
		}
	})

	t.Run("int32", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: DefaultPageRows})
		if err != nil {
			t.Fatalf("NewIngestBuffer: %v", err)
		}
		if runs, err := buffer.AppendSealed(context.Background(), makeInt32TextBatch(t, []int32{1, 2}, []string{"old-a", "old-b"}, nil)); err != nil || len(runs) != 0 {
			t.Fatalf("AppendSealed first runs=%#v err=%v", runs, err)
		}
		run, ok, err := buffer.FlushSealed(context.Background())
		if err != nil || !ok {
			t.Fatalf("FlushSealed first ok=%v err=%v", ok, err)
		}
		buffer.RecycleRun(run)
		if runs, err := buffer.AppendSealed(context.Background(), makeInt32TextBatch(t, []int32{7, 8}, []string{"new-a", "new-b"}, nil)); err != nil || len(runs) != 0 {
			t.Fatalf("AppendSealed second runs=%#v err=%v", runs, err)
		}
		run, ok, err = buffer.FlushSealed(context.Background())
		if err != nil || !ok {
			t.Fatalf("FlushSealed second ok=%v err=%v", ok, err)
		}
		values := run.Batches[0].Columns[0].V.I32
		if len(values) != 2 || values[0] != 7 || values[1] != 8 {
			t.Fatalf("int32 values = %#v, want 7/8", values)
		}
	})
}

func TestCountValidityAwareNumericPredicates(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		store, table := newTestStore(t)
		batch := makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"a", "b", "c", "d"}, map[int]bool{1: true})
		if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		var scratch QueryScratch
		cases := []struct {
			name string
			pred Predicate
			want uint64
		}{
			{"eq skips null", Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 2}, 0},
			{"between skips null", Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 1, Hi: 3}, 2},
		}
		for _, tc := range cases {
			got, err := store.Count(context.Background(), table, tc.pred, &scratch)
			if err != nil {
				t.Fatalf("Count %s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("Count %s = %d, want %d", tc.name, got, tc.want)
			}
		}
	})

	t.Run("int32", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		batch := makeInt32TextBatch(t, []int32{1, 2, 3, 4}, []string{"a", "b", "c", "d"}, map[int]bool{1: true})
		if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		var scratch QueryScratch
		cases := []struct {
			name string
			pred Predicate
			want uint64
		}{
			{"eq skips null", Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int32: 2}, 0},
			{"between skips null", Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo32: 1, Hi32: 3}, 2},
		}
		for _, tc := range cases {
			got, err := store.Count(context.Background(), table, tc.pred, &scratch)
			if err != nil {
				t.Fatalf("Count %s: %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("Count %s = %d, want %d", tc.name, got, tc.want)
			}
		}
	})
}

func TestHotBufferCountPathsStayZeroAllocation(t *testing.T) {
	store, table := newTestStore(t)
	benchInstallLargeBufferedTarget(t, store, table)
	for _, batch := range benchBatches(t, 4) {
		if err := store.AppendBuffered(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBuffered: %v", err)
		}
	}
	ctx := context.Background()
	allRows := uint64(4 * vector.StandardBatchRows)
	if count, err := store.Count(ctx, table, Predicate{}, nil); err != nil || count != allRows {
		t.Fatalf("Count all = %d, %v; want %d", count, err, allRows)
	}
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
	if count, err := store.Count(ctx, table, pred, nil); err != nil || count == 0 {
		t.Fatalf("Count predicate = %d, %v; want non-zero", count, err)
	}

	var allCount uint64
	allAllocs := testing.AllocsPerRun(100, func() {
		count, err := store.Count(ctx, table, Predicate{}, nil)
		if err != nil {
			t.Fatalf("Count all: %v", err)
		}
		allCount = count
	})
	if allCount != allRows {
		t.Fatalf("Count all after alloc run = %d, want %d", allCount, allRows)
	}
	if allAllocs != 0 {
		t.Fatalf("Count all allocations = %.2f, want 0", allAllocs)
	}

	var predCount uint64
	predAllocs := testing.AllocsPerRun(100, func() {
		count, err := store.Count(ctx, table, pred, nil)
		if err != nil {
			t.Fatalf("Count predicate: %v", err)
		}
		predCount = count
	})
	if predCount == 0 {
		t.Fatalf("Count predicate after alloc run = 0, want non-zero")
	}
	if predAllocs != 0 {
		t.Fatalf("Count predicate allocations = %.2f, want 0", predAllocs)
	}
}

func TestOpenRejectsStorageRootFile(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	if err := os.WriteFile(root, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write root file: %v", err)
	}
	store, err := Open(root)
	if err == nil {
		_ = store.Close()
		t.Fatalf("Open succeeded with storage root file")
	}
}

func TestOpenRejectsTablesDirFile(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "tables"), []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write tables file: %v", err)
	}
	store, err := Open(root)
	if err == nil {
		_ = store.Close()
		t.Fatalf("Open succeeded with tables file")
	}
}

func TestAppendRejectsTableDirFile(t *testing.T) {
	store, table := newTestStore(t)
	if err := os.WriteFile(tableDir(store.root, table), []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write table dir file: %v", err)
	}
	batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
	if _, err := store.AppendBatch(context.Background(), table, batch); err == nil {
		t.Fatalf("AppendBatch succeeded with table dir file")
	}
}

func TestAppendRejectsSegmentsDirFile(t *testing.T) {
	store, table := newTestStore(t)
	dir := tableDir(store.root, table)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir table dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "segments"), []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("write segments file: %v", err)
	}
	batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
	if _, err := store.AppendBatch(context.Background(), table, batch); err == nil {
		t.Fatalf("AppendBatch succeeded with segments dir file")
	}
}

func TestCountTextIncompleteStatsReadsPayloadAndSkipsNulls(t *testing.T) {
	store, table := newTestStore(t)
	rows := TextStatsMaxValues + 3
	ints := make([]int64, rows)
	strings := make([]string, rows)
	for row := range strings {
		ints[row] = int64(row)
		strings[row] = fmt.Sprintf("event-%d", row)
	}
	strings[0] = "target"
	strings[1] = "target"
	batch := makeIntTextBatch(t, ints, strings, map[int]bool{1: true})
	meta, err := store.AppendBatch(context.Background(), table, batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if meta.Columns[1].Text == nil || meta.Columns[1].Text.Complete {
		t.Fatalf("text stats = %#v, want incomplete stats", meta.Columns[1].Text)
	}

	count, err := store.Count(context.Background(), table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "target"}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestWriteSegmentReadColumnPageWrapper(t *testing.T) {
	store, table := newTestStore(t)
	if err := ensureTableDirs(store.root, table); err != nil {
		t.Fatalf("ensureTableDirs: %v", err)
	}
	batch := makeIntTextBatch(t, []int64{1, 2}, []string{"alpha", "beta"}, nil)
	meta, err := writeSegment(context.Background(), store.root, table, batch, 7)
	if err != nil {
		t.Fatalf("writeSegment: %v", err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	col, err := readColumnPage(path, meta.Columns[1], meta.Columns[1].Pages[0])
	if err != nil {
		t.Fatalf("readColumnPage: %v", err)
	}
	if got := col.V.Var.String(1); got != "beta" {
		t.Fatalf("text row 1 = %q, want beta", got)
	}
}

func TestScanCorruptSegmentReportsScannerError(t *testing.T) {
	store, table := newTestStore(t)
	meta := appendRows(t, store, table, []int64{1, 2}, []string{"a", "b"})
	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	if err := os.WriteFile(path, []byte("bad segment"), 0o644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}

	var out vector.Batch
	if scanner.Next(&out) {
		t.Fatalf("Next returned true for corrupt segment")
	}
	if scanner.Err() == nil {
		t.Fatalf("scanner.Err = nil, want read error")
	}
}
