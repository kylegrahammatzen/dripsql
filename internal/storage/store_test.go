package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestAppendReopenAndReadManifest(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2}, []string{"signup", "checkout"}, nil)
	meta, err := store.AppendBatch(context.Background(), table, batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if meta.ID != 1 || meta.Rows != 2 || len(meta.Columns) != 2 {
		t.Fatalf("meta = %#v", meta)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	segments, err := reopened.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 1 || segments[0].Rows != 2 {
		t.Fatalf("segments = %#v", segments)
	}
}

func TestAppendBatchesWritesSingleMultiPageSegment(t *testing.T) {
	store, table := newTestStore(t)
	first := makeSequentialIntTextBatch(t, 0, vector.StandardBatchRows, "first")
	second := makeSequentialIntTextBatch(t, int64(vector.StandardBatchRows), vector.StandardBatchRows, "second")

	meta, err := store.AppendBatches(context.Background(), table, []vector.Batch{first, second})
	if err != nil {
		t.Fatalf("AppendBatches: %v", err)
	}
	if meta.Rows != uint32(2*vector.StandardBatchRows) {
		t.Fatalf("rows = %d, want %d", meta.Rows, 2*vector.StandardBatchRows)
	}
	if len(meta.Columns[0].Pages) != 2 || meta.Columns[0].Pages[1].RowStart != uint32(vector.StandardBatchRows) {
		t.Fatalf("int pages = %#v", meta.Columns[0].Pages)
	}
	if len(meta.Columns[1].Pages) != 2 || meta.Columns[1].Pages[1].RowStart != uint32(vector.StandardBatchRows) {
		t.Fatalf("text pages = %#v", meta.Columns[1].Pages)
	}

	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 1 || segments[0].Rows != meta.Rows {
		t.Fatalf("segments = %#v", segments)
	}

	count, err := store.Count(context.Background(), table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: int64(vector.StandardBatchRows - 1), Hi: int64(vector.StandardBatchRows)}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count across pages = %d, want 2", count)
	}
}

func TestConcurrentAppendAssignsUniqueSegmentIDs(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
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
	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != appends {
		t.Fatalf("segments = %d, want %d", len(segments), appends)
	}
}

func TestIngestBufferCopiesAndFlushesOwnedBatches(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2}, []string{"a", "b"}, nil)
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 4})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
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

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if got := out.Columns[0].V.I64[0]; got != 1 {
		t.Fatalf("copied int = %d, want 1", got)
	}
	if got := out.Columns[1].V.Var.String(0); got != "a" {
		t.Fatalf("copied text = %q, want a", got)
	}
}

func TestIngestBufferAutoFlushesAtTargetRows(t *testing.T) {
	store, table := newTestStore(t)
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 3})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	published, err := buffer.Append(context.Background(), makeIntTextBatch(t, []int64{1, 2, 3}, []string{"a", "b", "c"}, nil))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if len(published) != 1 || published[0].Rows != 3 || buffer.BufferedRows() != 0 {
		t.Fatalf("published=%#v buffered=%d", published, buffer.BufferedRows())
	}
	count, err := store.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
}

func TestAppendBufferedFlushesOnDemandAndOwnsInput(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2}, []string{"a", "b"}, nil)
	if err := store.AppendBuffered(context.Background(), table, batch); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments before flush: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("segments before flush = %d, want 0", len(segments))
	}
	count, err := store.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count before flush: %v", err)
	}
	if count != 2 {
		t.Fatalf("count before flush = %d, want 2", count)
	}
	count, err = store.Count(context.Background(), table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "a"}, nil)
	if err != nil {
		t.Fatalf("Count text before flush: %v", err)
	}
	if count != 1 {
		t.Fatalf("text count before flush = %d, want 1", count)
	}

	batch.Columns[0].V.I64[0] = 99
	batch.Columns[1].V.Var.Data[0] = 'z'
	if err := store.FlushBuffered(context.Background(), table); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if got := out.Columns[0].V.I64[0]; got != 1 {
		t.Fatalf("copied int = %d, want 1", got)
	}
	if got := out.Columns[1].V.Var.String(0); got != "a" {
		t.Fatalf("copied text = %q, want a", got)
	}
}

func TestAppendBufferedCloseFlushesForReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	table := catalog.TableDef{
		ID:      1,
		Name:    "events",
		Version: 1,
		Columns: []catalog.ColumnDef{
			{ID: 1, Name: "tenant_id", Type: sqltype.Int64, Nullable: true},
			{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
		},
	}
	if err := store.AppendBuffered(context.Background(), table, makeIntTextBatch(t, []int64{1, 2}, []string{"a", "b"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer reopened.Close()
	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after reopen = %d, want 2", count)
	}
}

func TestAppendScanInt32RoundTrip(t *testing.T) {
	store, table := newTestStoreInt32(t)
	meta, err := store.AppendBatch(context.Background(), table, makeInt32TextBatch(t, []int32{1, -2, 3}, []string{"a", "b", "c"}, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if meta.Columns[0].Int32 == nil || meta.Columns[0].Int32.Min != -2 || meta.Columns[0].Int32.Max != 3 {
		t.Fatalf("int32 stats = %#v", meta.Columns[0].Int32)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID)
	got := out.Columns[0].V.I32
	if len(got) != 3 || got[0] != 1 || got[1] != -2 || got[2] != 3 {
		t.Fatalf("int32 values = %#v", got)
	}
	assertCount(t, store, table, Predicate{}, 3)
}

func TestAppendScanBoolRoundTrip(t *testing.T) {
	store, table := newTestStoreBool(t)
	appendBool(t, store, table, []bool{true, false, true}, []string{"a", "b", "c"}, nil)
	out := scanFirstBatch(t, store, table, table.Columns[0].ID)
	bits := out.Columns[0].V.BoolBits
	if !boolAt(bits, 0) || boolAt(bits, 1) || !boolAt(bits, 2) {
		t.Fatalf("bool bits = %#v", bits)
	}
	assertCount(t, store, table, Predicate{}, 3)
}

func TestAppendScanTimestampDateRoundTrip(t *testing.T) {
	store, table := newTestStoreTimestampDate(t)
	timestamps := []int64{
		time.Date(2026, 5, 7, 12, 30, 0, 123, time.UTC).UnixNano(),
		time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC).UnixNano(),
	}
	dates := []int32{
		dateDays(time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC)),
		dateDays(time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC)),
	}
	meta, err := store.AppendBatch(context.Background(), table, makeTimestampDateTextBatch(t, timestamps, dates, []string{"signup", "checkout"}, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if meta.Columns[0].Int64 == nil || meta.Columns[0].Int64.Min != timestamps[0] || meta.Columns[0].Int64.Max != timestamps[1] {
		t.Fatalf("timestamp stats = %#v", meta.Columns[0].Int64)
	}
	if meta.Columns[1].Int32 == nil || meta.Columns[1].Int32.Min != dates[0] || meta.Columns[1].Int32.Max != dates[1] {
		t.Fatalf("date stats = %#v", meta.Columns[1].Int32)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID, table.Columns[1].ID)
	if out.Columns[0].V.Kind != vector.Timestamp || out.Columns[1].V.Kind != vector.Date {
		t.Fatalf("kinds = %s/%s", out.Columns[0].V.Kind, out.Columns[1].V.Kind)
	}
	if got := out.Columns[0].V.I64; len(got) != 2 || got[0] != timestamps[0] || got[1] != timestamps[1] {
		t.Fatalf("timestamps = %#v", got)
	}
	if got := out.Columns[1].V.I32; len(got) != 2 || got[0] != dates[0] || got[1] != dates[1] {
		t.Fatalf("dates = %#v", got)
	}
	assertCount(t, store, table, Predicate{}, 2)
}

func TestAppendScanFloatRoundTrip(t *testing.T) {
	store, table := newTestStoreFloat(t)
	f32 := []float32{1.25, -2.5, 0}
	f64 := []float64{2.5, -4.125, 0}
	if _, err := store.AppendBatch(context.Background(), table, makeFloatTextBatch(t, f32, f64, []string{"a", "b", "null"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID, table.Columns[1].ID)
	if out.Columns[0].V.Kind != vector.Float32 || out.Columns[1].V.Kind != vector.Float64 {
		t.Fatalf("kinds = %s/%s", out.Columns[0].V.Kind, out.Columns[1].V.Kind)
	}
	if got := out.Columns[0].V.F32; len(got) != 3 || got[0] != f32[0] || got[1] != f32[1] {
		t.Fatalf("float32 values = %#v", got)
	}
	if got := out.Columns[1].V.F64; len(got) != 3 || got[0] != f64[0] || got[1] != f64[1] {
		t.Fatalf("float64 values = %#v", got)
	}
	if vector.IsValid(out.Columns[0].V.Valid, 2) || vector.IsValid(out.Columns[1].V.Valid, 2) {
		t.Fatalf("float validity = %#v/%#v, want row 2 invalid", out.Columns[0].V.Valid, out.Columns[1].V.Valid)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[0].ID, table.Columns[1].ID}, Predicate{})
	if err != nil {
		t.Fatalf("ScanRows: %v", err)
	}
	want := [][]any{{float32(1.25), float64(2.5)}, {float32(-2.5), float64(-4.125)}, {nil, nil}}
	if len(rows) != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", len(rows), len(want), rows)
	}
	for i := range want {
		for j := range want[i] {
			if rows[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows[i][j], want[i][j], rows)
			}
		}
	}
}

func TestCountAndScanFloatPredicates(t *testing.T) {
	store, table := newTestStoreFloat(t)
	if _, err := store.AppendBatch(context.Background(), table, makeFloatTextBatch(t,
		[]float32{1.25, 2.5, 4.5},
		[]float64{1.25, 2.5, 4.5},
		[]string{"one", "two", "four"},
		nil,
	)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(context.Background(), table, makeFloatTextBatch(t,
		[]float32{6.5, 0},
		[]float64{6.5, 0},
		[]string{"six", "null"},
		map[int]bool{1: true},
	)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	assertCount(t, store, table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpGreaterEqual, Float64: 2.5}, 3)
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, LoFloat64: 1, HiFloat64: 5}, 3)
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpIn, Float64s: []float64{1.25, 6.5}}, 2)

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[2].ID}, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpNotIn, Float64s: []float64{1.25, 6.5}})
	if err != nil {
		t.Fatalf("ScanRows float not in: %v", err)
	}
	wantRows := [][]any{{"two"}, {"four"}}
	if len(rows) != len(wantRows) {
		t.Fatalf("scan rows = %d, want %d (%#v)", len(rows), len(wantRows), rows)
	}
	for i := range wantRows {
		if rows[i][0] != wantRows[i][0] {
			t.Fatalf("scan row %d = %#v, want %#v (rows=%#v)", i, rows[i], wantRows[i], rows)
		}
	}

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[2].ID}, Predicate: Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpLess, Float64: 3}})
	if err != nil {
		t.Fatalf("Scan float predicate: %v", err)
	}
	defer scanner.Close()
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if out.VisibleLen() != 2 {
		t.Fatalf("visible rows = %d, want 2", out.VisibleLen())
	}
	if got := out.Columns[0].V.Var.String(int(out.Sel[0])); got != "one" {
		t.Fatalf("first scan row = %q, want one", got)
	}
	if got := out.Columns[0].V.Var.String(int(out.Sel[1])); got != "two" {
		t.Fatalf("second scan row = %q, want two", got)
	}
}

func TestAppendScanUUIDRoundTrip(t *testing.T) {
	store, table := newTestStoreUUID(t)
	ids := mustParseUUIDs(t,
		"550e8400-e29b-41d4-a716-446655440000",
		"550e8400-e29b-41d4-a716-446655440001",
		"00000000-0000-0000-0000-000000000000",
	)
	if _, err := store.AppendBatch(context.Background(), table, makeUUIDTextBatch(t, ids, []string{"a", "b", "null"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID)
	if out.Columns[0].V.Kind != vector.UUID {
		t.Fatalf("kind = %s, want uuid", out.Columns[0].V.Kind)
	}
	if got := out.Columns[0].V.UUID; len(got) != 3 || got[0] != ids[0] || got[1] != ids[1] {
		t.Fatalf("uuid values = %#v", got)
	}
	if vector.IsValid(out.Columns[0].V.Valid, 2) {
		t.Fatalf("uuid validity = %#v, want row 2 invalid", out.Columns[0].V.Valid)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[0].ID}, Predicate{})
	if err != nil {
		t.Fatalf("ScanRows: %v", err)
	}
	want := [][]any{{"550e8400-e29b-41d4-a716-446655440000"}, {"550e8400-e29b-41d4-a716-446655440001"}, {nil}}
	if len(rows) != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i][0] != want[i][0] {
			t.Fatalf("row %d = %#v, want %#v (rows=%#v)", i, rows[i], want[i], rows)
		}
	}
}

func TestCountAndScanUUIDPredicates(t *testing.T) {
	store, table := newTestStoreUUID(t)
	ids := mustParseUUIDs(t,
		"550e8400-e29b-41d4-a716-446655440000",
		"550e8400-e29b-41d4-a716-446655440001",
		"550e8400-e29b-41d4-a716-446655440002",
	)
	if _, err := store.AppendBatch(context.Background(), table, makeUUIDTextBatch(t, ids[:2], []string{"one", "two"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(context.Background(), table, makeUUIDTextBatch(t, ids[2:], []string{"three"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, UUID: ids[0]}, 1)
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpIn, UUIDs: []vector.UUID16{ids[0], ids[2]}}, 2)

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[1].ID}, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotIn, UUIDs: []vector.UUID16{ids[1]}})
	if err != nil {
		t.Fatalf("ScanRows uuid not in: %v", err)
	}
	wantRows := [][]any{{"one"}, {"three"}}
	if len(rows) != len(wantRows) {
		t.Fatalf("scan rows = %d, want %d (%#v)", len(rows), len(wantRows), rows)
	}
	for i := range wantRows {
		if rows[i][0] != wantRows[i][0] {
			t.Fatalf("scan row %d = %#v, want %#v (rows=%#v)", i, rows[i], wantRows[i], rows)
		}
	}
}

func TestAppendScanBytesRoundTrip(t *testing.T) {
	store, table := newTestStoreBytes(t)
	values := []string{"alpha", "raw\x00byte", "missing"}
	if _, err := store.AppendBatch(context.Background(), table, makeBytesTextBatch(t, values, []string{"a", "b", "null"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID)
	if out.Columns[0].V.Kind != vector.Bytes {
		t.Fatalf("kind = %s, want bytes", out.Columns[0].V.Kind)
	}
	if got := string(out.Columns[0].V.Var.Bytes(1)); got != values[1] {
		t.Fatalf("bytes row 1 = %q, want %q", got, values[1])
	}
	if vector.IsValid(out.Columns[0].V.Valid, 2) {
		t.Fatalf("bytes validity = %#v, want row 2 invalid", out.Columns[0].V.Valid)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[0].ID}, Predicate{})
	if err != nil {
		t.Fatalf("ScanRows: %v", err)
	}
	want := [][]any{{"alpha"}, {"raw\x00byte"}, {nil}}
	if len(rows) != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i][0] != want[i][0] {
			t.Fatalf("row %d = %#v, want %#v (rows=%#v)", i, rows[i], want[i], rows)
		}
	}
}

func TestCountAndScanBytesPredicates(t *testing.T) {
	store, table := newTestStoreBytes(t)
	if _, err := store.AppendBatch(context.Background(), table, makeBytesTextBatch(t, []string{"aa", "bb"}, []string{"one", "two"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(context.Background(), table, makeBytesTextBatch(t, []string{"cc"}, []string{"three"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Text: "aa"}, 1)
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpIn, Texts: []string{"aa", "cc"}}, 2)

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[1].ID}, Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Text: "aa"}})
	if err != nil {
		t.Fatalf("Scan bytes eq: %v", err)
	}
	defer scanner.Close()
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if out.VisibleLen() != 1 || out.Columns[0].V.Var.String(int(out.Sel[0])) != "one" {
		t.Fatalf("scan bytes eq output = %#v", out)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[1].ID}, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotIn, Texts: []string{"bb"}})
	if err != nil {
		t.Fatalf("ScanRows bytes not in: %v", err)
	}
	wantRows := [][]any{{"one"}, {"three"}}
	if len(rows) != len(wantRows) {
		t.Fatalf("scan rows = %d, want %d (%#v)", len(rows), len(wantRows), rows)
	}
	for i := range wantRows {
		if rows[i][0] != wantRows[i][0] {
			t.Fatalf("scan row %d = %#v, want %#v (rows=%#v)", i, rows[i], wantRows[i], rows)
		}
	}
}

func TestAppendScanEnumRoundTrip(t *testing.T) {
	store, table := newTestStoreEnum(t)
	if _, err := store.AppendBatch(context.Background(), table, makeEnumTextBatch(t, []uint32{1, 2, 1}, []string{"a", "b", "null"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	out := scanFirstBatch(t, store, table, table.Columns[0].ID)
	if out.Columns[0].V.Kind != vector.Enum32 {
		t.Fatalf("kind = %s, want enum32", out.Columns[0].V.Kind)
	}
	if got := out.Columns[0].V.U32; len(got) != 3 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("enum values = %#v", got)
	}
	if vector.IsValid(out.Columns[0].V.Valid, 2) {
		t.Fatalf("enum validity = %#v, want row 2 invalid", out.Columns[0].V.Valid)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[0].ID}, Predicate{})
	if err != nil {
		t.Fatalf("ScanRows: %v", err)
	}
	want := [][]any{{"new"}, {"done"}, {nil}}
	if len(rows) != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", len(rows), len(want), rows)
	}
	for i := range want {
		if rows[i][0] != want[i][0] {
			t.Fatalf("row %d = %#v, want %#v (rows=%#v)", i, rows[i], want[i], rows)
		}
	}
}

func TestCountAndScanEnumPredicates(t *testing.T) {
	store, table := newTestStoreEnum(t)
	if _, err := store.AppendBatch(context.Background(), table, makeEnumTextBatch(t, []uint32{1, 2}, []string{"one", "two"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(context.Background(), table, makeEnumTextBatch(t, []uint32{3}, []string{"three"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Enum: 1}, 1)
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpIn, Enums: []uint32{1, 3}}, 2)

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[1].ID}, Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Enum: 1}})
	if err != nil {
		t.Fatalf("Scan enum eq: %v", err)
	}
	defer scanner.Close()
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if out.VisibleLen() != 1 || out.Columns[0].V.Var.String(int(out.Sel[0])) != "one" {
		t.Fatalf("scan enum eq output = %#v", out)
	}

	rows, err := store.ScanRows(context.Background(), table, []catalog.ColumnID{table.Columns[1].ID}, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotIn, Enums: []uint32{2}})
	if err != nil {
		t.Fatalf("ScanRows enum not in: %v", err)
	}
	wantRows := [][]any{{"one"}, {"three"}}
	if len(rows) != len(wantRows) {
		t.Fatalf("scan rows = %d, want %d (%#v)", len(rows), len(wantRows), rows)
	}
	for i := range wantRows {
		if rows[i][0] != wantRows[i][0] {
			t.Fatalf("scan row %d = %#v, want %#v (rows=%#v)", i, rows[i], wantRows[i], rows)
		}
	}
}

// scanFirstBatch opens a Scanner for colIDs, advances once, and returns the
// resulting batch; the scanner is registered for cleanup via t.Cleanup.
func scanFirstBatch(t *testing.T, store *Store, table catalog.TableDef, colIDs ...catalog.ColumnID) vector.Batch {
	t.Helper()
	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table, Columns: colIDs})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	t.Cleanup(func() { _ = scanner.Close() })
	var out vector.Batch
	if !scanner.Next(&out) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	return out
}

// assertCount runs Count(pred) and fails when err is non-nil or count != want.
func assertCount(t *testing.T, s *Store, table catalog.TableDef, pred Predicate, want uint64) {
	t.Helper()
	count, err := s.Count(context.Background(), table, pred, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != want {
		t.Fatalf("count = %d, want %d", count, want)
	}
}

// appendInt32 appends an int32+text batch via AppendBatch.
func appendInt32(t *testing.T, store *Store, table catalog.TableDef, ints []int32, texts []string, nulls map[int]bool) {
	t.Helper()
	if _, err := store.AppendBatch(context.Background(), table, makeInt32TextBatch(t, ints, texts, nulls)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
}

// appendBool appends a bool+text batch via AppendBatch.
func appendBool(t *testing.T, store *Store, table catalog.TableDef, bools []bool, texts []string, nulls map[int]bool) {
	t.Helper()
	if _, err := store.AppendBatch(context.Background(), table, makeBoolTextBatch(t, bools, texts, nulls)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
}

// appendInt64Nulls appends an int64+text batch via AppendBatch.
func appendInt64Nulls(t *testing.T, store *Store, table catalog.TableDef, ints []int64, texts []string, nulls map[int]bool) {
	t.Helper()
	if _, err := store.AppendBatch(context.Background(), table, makeIntTextBatch(t, ints, texts, nulls)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
}

func TestCountPredicates(t *testing.T) {
	t.Run("none across multiple segments", func(t *testing.T) {
		store, table := newTestStore(t)
		appendRows(t, store, table, []int64{1, 2, 3}, []string{"a", "b", "c"})
		appendRows(t, store, table, []int64{4, 5}, []string{"d", "e"})
		assertCount(t, store, table, Predicate{}, 5)
	})
	t.Run("int64_eq", func(t *testing.T) {
		store, table := newTestStore(t)
		appendRows(t, store, table, []int64{1, 2, 1, 3}, []string{"a", "b", "c", "d"})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1}, 2)
	})
	t.Run("int64_between", func(t *testing.T) {
		store, table := newTestStore(t)
		appendRows(t, store, table, []int64{1, 2, 3, 4, 5}, []string{"a", "b", "c", "d", "e"})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 2, Hi: 4}, 3)
	})
	t.Run("int64_eq skips nulls", func(t *testing.T) {
		store, table := newTestStore(t)
		appendInt64Nulls(t, store, table, []int64{1, 0, 1}, []string{"a", "", "c"}, map[int]bool{1: true})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 0}, 0)
	})
	t.Run("int32_eq", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		appendInt32(t, store, table, []int32{1, 2, 1, 3}, []string{"a", "b", "c", "d"}, nil)
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int32: 1}, 2)
	})
	t.Run("int32_between", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		appendInt32(t, store, table, []int32{1, 2, 3, 4, 5}, []string{"a", "b", "c", "d", "e"}, nil)
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo32: 2, Hi32: 4}, 3)
	})
	t.Run("int32_eq skips nulls", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		appendInt32(t, store, table, []int32{1, 0, 1}, []string{"a", "", "c"}, map[int]bool{1: true})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int32: 0}, 0)
	})
	t.Run("bool_eq true and false", func(t *testing.T) {
		store, table := newTestStoreBool(t)
		appendBool(t, store, table, []bool{true, false, true, false}, []string{"a", "b", "c", "d"}, nil)
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Bool: true}, 2)
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Bool: false}, 2)
	})
	t.Run("bool_eq skips nulls", func(t *testing.T) {
		store, table := newTestStoreBool(t)
		appendBool(t, store, table, []bool{true, false, false}, []string{"a", "", "c"}, map[int]bool{1: true})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Bool: false}, 1)
	})
	t.Run("text_eq missing returns zero", func(t *testing.T) {
		store, table := newTestStore(t)
		appendRows(t, store, table, []int64{1, 2, 3, 4}, []string{"signup", "checkout", "signup", "login"})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "missing"}, 0)
	})
	t.Run("text_eq skips nulls", func(t *testing.T) {
		store, table := newTestStore(t)
		appendInt64Nulls(t, store, table, []int64{1, 2, 3}, []string{"signup", "signup", "login"}, map[int]bool{1: true})
		assertCount(t, store, table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "signup"}, 1)
	})
}

// TestCountTextPredicateMetadataOnly is kept separate because it asserts that
// text-eq with complete stats hits the metadata-only path and never opens a file.
func TestCountTextPredicateMetadataOnly(t *testing.T) {
	store, table := newTestStore(t)
	meta := appendRows(t, store, table, []int64{1, 2, 3, 4}, []string{"signup", "checkout", "signup", "login"})
	if meta.Columns[1].Text == nil || !meta.Columns[1].Text.Complete {
		t.Fatalf("text stats = %#v", meta.Columns[1].Text)
	}
	assertCount(t, store, table, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "signup"}, 2)
	if got := store.files.Len(); got != 0 {
		t.Fatalf("cached files = %d, want 0 for metadata-only text count", got)
	}
}

func TestCountCachesSegmentFiles(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2}, []string{"a", "b"})
	appendRows(t, store, table, []int64{1, 3}, []string{"c", "d"})

	count, err := store.Count(context.Background(), table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if got := store.files.Len(); got != 2 {
		t.Fatalf("cached files = %d, want 2", got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := store.files.Len(); got != 0 {
		t.Fatalf("cached files after close = %d, want 0", got)
	}
}

func TestCountUsesCachedManifestAfterLoad(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("initial Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("initial count = %d, want 1", count)
	}

	manifestPath := filepath.Join(tableDir(reopened.root, table), manifestFileName)
	if err := os.WriteFile(manifestPath, []byte("{bad json\n"), 0o644); err != nil {
		t.Fatalf("corrupt manifest: %v", err)
	}
	count, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("cached Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("cached count = %d, want 1", count)
	}
}

func TestScanTextRoundTrip(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2}, []string{"signup", "checkout"})

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[1].ID}})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if len(batch.Columns) != 1 || batch.Len != 2 {
		t.Fatalf("batch = %#v", batch)
	}
	if got := batch.Columns[0].V.Var.String(0); got != "signup" {
		t.Fatalf("row 0 = %q", got)
	}
	if got := batch.Columns[0].V.Var.String(1); got != "checkout" {
		t.Fatalf("row 1 = %q", got)
	}
	if scanner.Next(&batch) {
		t.Fatalf("unexpected second batch")
	}
}

func TestScanDefaultsToAllColumnsWhenProjectionEmpty(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2}, []string{"signup", "checkout"})

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	got := make([]int64, 0, 2)
	gotText := make([]string, 0, 2)
	var batch vector.Batch
	for scanner.Next(&batch) {
		for row := 0; row < batch.VisibleLen(); row++ {
			rowIndex := row
			if len(batch.Sel) != 0 {
				rowIndex = int(batch.Sel[row])
			}
			got = append(got, batch.Columns[0].V.I64[rowIndex])
			gotText = append(gotText, batch.Columns[1].V.Var.String(rowIndex))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner.Err: %v", err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("scan int values = %#v, want [1 2]", got)
	}
	if len(gotText) != 2 || gotText[0] != "signup" || gotText[1] != "checkout" {
		t.Fatalf("scan text values = %#v, want [signup checkout]", gotText)
	}
	if len(batch.Columns) != len(table.Columns) {
		t.Fatalf("projection columns = %d, want %d", len(batch.Columns), len(table.Columns))
	}
	if batch.Columns[0].Name != table.Columns[0].Name || batch.Columns[1].Name != table.Columns[1].Name {
		t.Fatalf("projected names = %q / %q, want %q / %q", batch.Columns[0].Name, batch.Columns[1].Name, table.Columns[0].Name, table.Columns[1].Name)
	}
}

func TestScanCloseIsIdempotentAndStopsIteration(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2}, []string{"a", "b"})

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := scanner.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	var batch vector.Batch
	if scanner.Next(&batch) {
		t.Fatalf("Next after Close returned true")
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("Err after Close = %v, want nil", err)
	}
}

func TestScanLifecycleReleasesOpenSegmentFiles(t *testing.T) {
	store, table := newTestStore(t)
	big := makeSequentialIntTextBatch(t, 0, vector.StandardBatchRows, "event")
	if _, err := store.AppendBatch(context.Background(), table, big); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:   table,
		Columns: []catalog.ColumnID{table.Columns[1].ID},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(segments))
	}
	path := segments[0].AbsPath(tableDir(store.root, table))

	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if got := activeSegmentRefs(store, path); got != 1 {
		t.Fatalf("segment file refs while scanning = %d, want 1", got)
	}

	if err := scanner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := activeSegmentRefs(store, path); got != 0 {
		t.Fatalf("segment file refs after scanner close = %d, want 0", got)
	}

	if scanner.Next(&batch) {
		t.Fatalf("Next after Close returned true")
	}
}

func activeSegmentRefs(store *Store, path string) int {
	store.files.mu.Lock()
	defer store.files.mu.Unlock()
	entry := store.files.files[path]
	if entry == nil {
		return 0
	}
	return entry.refs
}

func TestScanUsesSnapshotFromCreation(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"before"})

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()
	appendRows(t, store, table, []int64{2}, []string{"after"})

	var got []string
	var batch vector.Batch
	for scanner.Next(&batch) {
		for row := 0; row < batch.VisibleLen(); row++ {
			rowIndex := row
			if len(batch.Sel) != 0 {
				rowIndex = int(batch.Sel[row])
			}
			got = append(got, batch.Columns[1].V.Var.String(rowIndex))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner.Err: %v", err)
	}
	if len(got) != 1 || got[0] != "before" {
		t.Fatalf("scan rows = %#v, want [before]", got)
	}
}

func TestScanUsesSnapshotFromCreationWithPredicate(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3}, []string{"before-1", "before-2", "before-3"})

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()
	appendRows(t, store, table, []int64{1, 4}, []string{"after-1", "after-4"})

	var got []string
	var batch vector.Batch
	for scanner.Next(&batch) {
		for row := 0; row < batch.VisibleLen(); row++ {
			rowIndex := row
			if len(batch.Sel) != 0 {
				rowIndex = int(batch.Sel[row])
			}
			got = append(got, batch.Columns[0].V.Var.String(rowIndex))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner.Err: %v", err)
	}
	if len(got) != 1 || got[0] != "before-1" {
		t.Fatalf("scan rows = %#v, want [before-1]", got)
	}
}

func TestScanInt64PredicateSetsSelection(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 1, 3}, []string{"a", "b", "c", "d"})

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if batch.VisibleLen() != 2 || len(batch.Sel) != 2 || batch.Sel[0] != 0 || batch.Sel[1] != 2 {
		t.Fatalf("selection = %#v visible=%d", batch.Sel, batch.VisibleLen())
	}
}

func TestScanInt64BetweenPredicateSkipsNulls(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2, 3, 2}, []string{"a", "b", "c", "d"}, map[int]bool{1: true})
	if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 2, Hi: 2},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batchOut vector.Batch
	if !scanner.Next(&batchOut) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if batchOut.VisibleLen() != 1 {
		t.Fatalf("visible len = %d, want 1", batchOut.VisibleLen())
	}
	rowIndex := 0
	if len(batchOut.Sel) != 0 {
		rowIndex = int(batchOut.Sel[0])
	}
	if got := batchOut.Columns[0].V.Var.String(rowIndex); got != "d" {
		t.Fatalf("value = %q, want d", got)
	}
	if scanner.Next(&batchOut) {
		t.Fatalf("unexpected second batch")
	}
}

func TestScanInt64PredicateSkipsNullRows(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 2, 2}, []string{"a", "null", "b"}, map[int]bool{1: true})
	if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 2},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batchOut vector.Batch
	if !scanner.Next(&batchOut) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if batchOut.VisibleLen() != 1 {
		t.Fatalf("visible len = %d, want 1", batchOut.VisibleLen())
	}
	rowIndex := 0
	if len(batchOut.Sel) != 0 {
		rowIndex = int(batchOut.Sel[0])
	}
	if got := batchOut.Columns[0].V.Var.String(rowIndex); got != "b" {
		t.Fatalf("value = %q, want b", got)
	}
	if scanner.Next(&batchOut) {
		t.Fatalf("unexpected second batch")
	}
}

func TestScanInt64BetweenPredicateSetsSelection(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3, 4, 5}, []string{"a", "b", "c", "d", "e"})

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 2, Hi: 4},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if batch.VisibleLen() != 3 {
		t.Fatalf("visible len = %d, want 3", batch.VisibleLen())
	}
	if len(batch.Sel) != 3 || batch.Sel[0] != 1 || batch.Sel[1] != 2 || batch.Sel[2] != 3 {
		t.Fatalf("selection = %#v visible=%d", batch.Sel, batch.VisibleLen())
	}

	if scanner.Next(&batch) {
		t.Fatalf("unexpected second batch")
	}
}

func TestScanPredicateSkipsUnreadNonMatchingPages(t *testing.T) {
	store, table := newTestStore(t)
	first := makeSequentialIntTextBatch(t, 0, vector.StandardBatchRows, "first")
	second := makeSequentialIntTextBatch(t, 10_000, vector.StandardBatchRows, "second")
	meta, err := store.AppendBatches(context.Background(), table, []vector.Batch{first, second})
	if err != nil {
		t.Fatalf("AppendBatches: %v", err)
	}

	firstPredicatePage := meta.Columns[0].Pages[0]
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteAt([]byte("corrupt skipped predicate page"), int64(firstPredicatePage.Offset)); err != nil {
		_ = file.Close()
		t.Fatalf("WriteAt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close file: %v", err)
	}

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 10_000, Hi: 10_002},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	if batch.VisibleLen() != 3 {
		t.Fatalf("visible len = %d, want 3", batch.VisibleLen())
	}
	for row := 0; row < batch.VisibleLen(); row++ {
		rowIndex := row
		if len(batch.Sel) != 0 {
			rowIndex = int(batch.Sel[row])
		}
		if got := batch.Columns[0].V.Var.String(rowIndex); got != "second" {
			t.Fatalf("row %d = %q, want second", row, got)
		}
	}
	if scanner.Next(&batch) {
		t.Fatalf("unexpected second matching page")
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner.Err: %v", err)
	}
}

func TestScanPredicateAcrossSegmentsProjectsBothColumns(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2}, []string{"seg1-a", "seg1-b"})
	appendRows(t, store, table, []int64{10, 11}, []string{"seg2-a", "seg2-b"})
	appendRows(t, store, table, []int64{12, 13}, []string{"seg3-a", "seg3-b"})

	scanner, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[0].ID, table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 10, Hi: 12},
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer scanner.Close()

	type row struct {
		id    int64
		event string
	}
	got := make([]row, 0, 3)
	var batch vector.Batch
	for scanner.Next(&batch) {
		if len(batch.Columns) != 2 {
			t.Fatalf("batch columns = %d, want 2", len(batch.Columns))
		}
		if batch.VisibleLen() == 0 {
			continue
		}
		for i := 0; i < batch.VisibleLen(); i++ {
			rowIndex := i
			if len(batch.Sel) != 0 {
				rowIndex = int(batch.Sel[i])
			}
			got = append(got, row{int64(batch.Columns[0].V.I64[rowIndex]), batch.Columns[1].V.Var.String(rowIndex)})
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner.Err: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("row count = %d, want 3", len(got))
	}
	want := []row{{10, "seg2-a"}, {11, "seg2-b"}, {12, "seg3-a"}}
	for i, row := range got {
		if row != want[i] {
			t.Fatalf("row %d = %#v, want %#v", i, row, want[i])
		}
	}
}

func TestScanPredicateMissingColumnRejected(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3}, []string{"a", "b", "c"})

	_, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: 99, Op: PredicateOpEq, Int64: 1},
	})
	if err == nil {
		t.Fatalf("Scan error = nil, want missing predicate column error")
	}
	want := "missing predicate column ID 99"
	if got := err.Error(); got != want {
		t.Fatalf("Scan error = %q, want %q", got, want)
	}
}

func TestScanUnsupportedPredicateRejectedEarly(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 1, 3}, []string{"a", "b", "c", "d"})
	_, err := store.Scan(context.Background(), ScanRequest{
		Table:     table,
		Columns:   []catalog.ColumnID{table.Columns[1].ID},
		Predicate: Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "a"},
	})
	if err == nil {
		t.Fatalf("Scan error = nil, want unsupported predicate error")
	}
	want := "scan predicate op"
	if got := err.Error(); !strings.Contains(got, want) {
		t.Fatalf("Scan error = %q, want prefix %q", got, want)
	}
}

func TestScanUnsupportedPredicatesRejectedExplicitly(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		batch := makeInt32TextBatch(t, []int32{1, 2}, []string{"a", "b"}, nil)
		if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}

		_, err := store.Scan(context.Background(), ScanRequest{
			Table:     table,
			Columns:   []catalog.ColumnID{table.Columns[1].ID},
			Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int32: 1},
		})
		if err == nil {
			t.Fatalf("Scan error = nil, want unsupported predicate op")
		}
		want := "scan predicate op"
		if got := err.Error(); !strings.Contains(got, want) {
			t.Fatalf("Scan error = %q, want prefix %q", got, want)
		}
	})

	t.Run("bool", func(t *testing.T) {
		store, table := newTestStoreBool(t)
		batch := makeBoolTextBatch(t, []bool{true, false}, []string{"a", "b"}, nil)
		if _, err := store.AppendBatch(context.Background(), table, batch); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}

		_, err := store.Scan(context.Background(), ScanRequest{
			Table:     table,
			Columns:   []catalog.ColumnID{table.Columns[1].ID},
			Predicate: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Bool: true},
		})
		if err == nil {
			t.Fatalf("Scan error = nil, want unsupported predicate op")
		}
		want := "scan predicate op"
		if got := err.Error(); !strings.Contains(got, want) {
			t.Fatalf("Scan error = %q, want prefix %q", got, want)
		}
	})
}

func TestScanDuplicateProjectedColumnsRejected(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3}, []string{"a", "b", "c"})

	_, err := store.Scan(context.Background(), ScanRequest{
		Table:   table,
		Columns: []catalog.ColumnID{table.Columns[0].ID, table.Columns[0].ID},
	})
	if err == nil {
		t.Fatalf("Scan error = nil, want duplicate projected column error")
	}
	want := fmt.Sprintf("duplicate projected column ID %d", table.Columns[0].ID)
	if got := err.Error(); got != want {
		t.Fatalf("Scan error = %q, want %q", got, want)
	}
}

func TestScanMissingProjectedColumnRejected(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3}, []string{"a", "b", "c"})

	_, err := store.Scan(context.Background(), ScanRequest{
		Table:   table,
		Columns: []catalog.ColumnID{table.Columns[0].ID, 99},
	})
	if err == nil {
		t.Fatalf("Scan error = nil, want missing projected column error")
	}
	want := "missing projected column ID 99"
	if got := err.Error(); got != want {
		t.Fatalf("Scan error = %q, want %q", got, want)
	}
}

func TestNullCountMetadataAndValidityPayload(t *testing.T) {
	store, table := newTestStore(t)
	batch := makeIntTextBatch(t, []int64{1, 0, 3}, []string{"a", "", "c"}, map[int]bool{1: true})
	meta, err := store.AppendBatch(context.Background(), table, batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	intCol := meta.Columns[0]
	if intCol.NullCount != 1 || intCol.AllValid || intCol.AllNull {
		t.Fatalf("int null metadata = %#v", intCol)
	}
	if intCol.Pages[0].Length != uint64(vector.ValidityWords(3)*8+3*8) {
		t.Fatalf("int page length = %d", intCol.Pages[0].Length)
	}

	allValid := appendRows(t, store, table, []int64{4, 5}, []string{"d", "e"})
	if !allValid.Columns[0].AllValid || allValid.Columns[0].Pages[0].Length != 2*8 {
		t.Fatalf("all-valid metadata = %#v", allValid.Columns[0])
	}
}

func TestInt64PruningDoesNotReadSkippedSegment(t *testing.T) {
	store, table := newTestStore(t)
	first := appendRows(t, store, table, []int64{1, 2}, []string{"a", "b"})
	appendRows(t, store, table, []int64{100, 101}, []string{"c", "d"})

	path := filepath.Join(tableDir(store.root, table), first.Path)
	if err := os.WriteFile(path, []byte("corrupt skipped segment"), 0o644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}

	count, err := store.Count(context.Background(), table, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 100}, nil)
	if err != nil {
		t.Fatalf("Count should skip corrupted segment: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestIncompleteTmpSegmentIgnored(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	tmp := filepath.Join(tableDir(store.root, table), "segments", "0000000000000002.dseg.tmp")
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}
	count, err := store.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func newTestStoreWithColumns(t testing.TB, cols []catalog.ColumnDef) (*Store, catalog.TableDef) {
	t.Helper()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, catalog.TableDef{ID: 1, Name: "events", Version: 1, Columns: cols}
}

func newTestStore(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "tenant_id", Type: sqltype.Int64, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreInt32(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "tenant_id", Type: sqltype.Int32, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreBool(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "active", Type: sqltype.Bool, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreTimestampDate(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "created_at", Type: sqltype.Timestamp, Nullable: true},
		{ID: 2, Name: "event_date", Type: sqltype.Date, Nullable: true},
		{ID: 3, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreFloat(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "f32", Type: sqltype.Float32, Nullable: true},
		{ID: 2, Name: "f64", Type: sqltype.Float64, Nullable: true},
		{ID: 3, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreUUID(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "id", Type: sqltype.UUID, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreBytes(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "payload", Type: sqltype.Bytes, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func newTestStoreEnum(t testing.TB) (*Store, catalog.TableDef) {
	t.Helper()
	return newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "status", Type: sqltype.Named("event_status"), Labels: []string{"new", "done", "archived"}, Nullable: true},
		{ID: 2, Name: "event_type", Type: sqltype.Text, Nullable: true},
	})
}

func appendRows(t *testing.T, store *Store, table catalog.TableDef, ints []int64, strings []string) SegmentMeta {
	t.Helper()
	meta, err := store.AppendBatch(context.Background(), table, makeIntTextBatch(t, ints, strings, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	return meta
}

func buildValidity(rows int, nullRows map[int]bool) vector.Validity {
	if len(nullRows) == 0 {
		return nil
	}
	valid := vector.NewValidity(rows)
	for row := range nullRows {
		vector.SetInvalid(valid, row)
	}
	return valid
}

func buildTextColumn(name string, rows int, valid vector.Validity, strings []string) vector.Column {
	varbytes := vector.NewVarBytes(rows, rows*8)
	for row, value := range strings {
		if valid == nil || vector.IsValid(valid, row) {
			varbytes.Data = append(varbytes.Data, value...)
		}
		varbytes.Offsets[row+1] = uint32(len(varbytes.Data))
	}
	return vector.Column{Name: name, Type: sqltype.Text, V: vector.Vec{Kind: vector.Text, Len: rows, Valid: valid, Var: varbytes}}
}

func buildBytesColumn(name string, rows int, valid vector.Validity, values []string) vector.Column {
	varbytes := vector.NewVarBytes(rows, rows*8)
	for row, value := range values {
		if valid == nil || vector.IsValid(valid, row) {
			varbytes.Data = append(varbytes.Data, value...)
		}
		varbytes.Offsets[row+1] = uint32(len(varbytes.Data))
	}
	return vector.Column{Name: name, Type: sqltype.Bytes, V: vector.Vec{Kind: vector.Bytes, Len: rows, Valid: valid, Var: varbytes}}
}

func mustNewBatch(t testing.TB, cols []vector.Column) vector.Batch {
	t.Helper()
	batch, err := vector.NewBatch(cols)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func makeIntTextBatch(t testing.TB, ints []int64, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(ints) != len(strings) {
		t.Fatalf("ints/strings length mismatch")
	}
	rows := len(ints)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "tenant_id", Type: sqltype.Int64, V: vector.Vec{Kind: vector.Int64, Len: rows, Valid: valid, I64: ints}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeInt32TextBatch(t testing.TB, ints []int32, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(ints) != len(strings) {
		t.Fatalf("ints/strings length mismatch")
	}
	rows := len(ints)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "tenant_id", Type: sqltype.Int32, V: vector.Vec{Kind: vector.Int32, Len: rows, Valid: valid, I32: ints}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeBoolTextBatch(t testing.TB, bools []bool, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(bools) != len(strings) {
		t.Fatalf("bools/strings length mismatch")
	}
	rows := len(bools)
	valid := buildValidity(rows, nullRows)
	bits := make([]uint64, vector.ValidityWords(rows))
	for row, value := range bools {
		if value {
			bits[row>>6] |= uint64(1) << uint(row&63)
		}
	}
	return mustNewBatch(t, []vector.Column{
		{Name: "active", Type: sqltype.Bool, V: vector.Vec{Kind: vector.Bool, Len: rows, Valid: valid, BoolBits: bits}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeTimestampDateTextBatch(t testing.TB, timestamps []int64, dates []int32, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(timestamps) != len(dates) || len(timestamps) != len(strings) {
		t.Fatalf("timestamps/dates/strings length mismatch")
	}
	rows := len(timestamps)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "created_at", Type: sqltype.Timestamp, V: vector.Vec{Kind: vector.Timestamp, Len: rows, Valid: valid, I64: timestamps}},
		{Name: "event_date", Type: sqltype.Date, V: vector.Vec{Kind: vector.Date, Len: rows, Valid: valid, I32: dates}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeFloatTextBatch(t testing.TB, f32 []float32, f64 []float64, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(f32) != len(f64) || len(f32) != len(strings) {
		t.Fatalf("float32/float64/strings length mismatch")
	}
	rows := len(f32)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "f32", Type: sqltype.Float32, V: vector.Vec{Kind: vector.Float32, Len: rows, Valid: valid, F32: f32}},
		{Name: "f64", Type: sqltype.Float64, V: vector.Vec{Kind: vector.Float64, Len: rows, Valid: valid, F64: f64}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeUUIDTextBatch(t testing.TB, ids []vector.UUID16, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(ids) != len(strings) {
		t.Fatalf("ids/strings length mismatch")
	}
	rows := len(ids)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "id", Type: sqltype.UUID, V: vector.Vec{Kind: vector.UUID, Len: rows, Valid: valid, UUID: ids}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeBytesTextBatch(t testing.TB, bytes []string, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(bytes) != len(strings) {
		t.Fatalf("bytes/strings length mismatch")
	}
	rows := len(bytes)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		buildBytesColumn("payload", rows, valid, bytes),
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func makeEnumTextBatch(t testing.TB, values []uint32, strings []string, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(values) != len(strings) {
		t.Fatalf("enum/strings length mismatch")
	}
	rows := len(values)
	valid := buildValidity(rows, nullRows)
	labels := []string{"new", "done", "archived"}
	return mustNewBatch(t, []vector.Column{
		{Name: "status", Type: sqltype.Named("event_status"), EnumLabels: labels, V: vector.Vec{Kind: vector.Enum32, Len: rows, Valid: valid, U32: values}},
		buildTextColumn("event_type", rows, valid, strings),
	})
}

func mustParseUUIDs(t testing.TB, values ...string) []vector.UUID16 {
	t.Helper()
	out := make([]vector.UUID16, 0, len(values))
	for _, value := range values {
		parsed, err := vector.ParseUUID(value)
		if err != nil {
			t.Fatalf("ParseUUID %q: %v", value, err)
		}
		out = append(out, parsed)
	}
	return out
}

func dateDays(value time.Time) int32 {
	return int32(value.Unix() / 86400)
}

func makeSequentialIntTextBatch(t testing.TB, start int64, rows int, text string) vector.Batch {
	t.Helper()
	ints := make([]int64, rows)
	strings := make([]string, rows)
	for row := range ints {
		ints[row] = start + int64(row)
		strings[row] = text
	}
	return makeIntTextBatch(t, ints, strings, nil)
}

func TestSumIntReturnsOverflowError(t *testing.T) {
	store, table := newTestStore(t)
	// Two near-MaxInt64 rows guarantee overflow on sum.
	appendRows(t, store, table, []int64{math.MaxInt64 - 1, math.MaxInt64 - 1}, []string{"a", "b"})

	_, err := store.SumInt(context.Background(), table, table.Columns[0].ID, Predicate{}, nil)
	if !errors.Is(err, ErrSumOverflow) {
		t.Fatalf("SumInt error = %v, want ErrSumOverflow", err)
	}
}

func TestSumIntReturnsCorrectSumWhenNoOverflow(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3, 4}, []string{"a", "b", "c", "d"})

	sum, err := store.SumInt(context.Background(), table, table.Columns[0].ID, Predicate{}, nil)
	if err != nil {
		t.Fatalf("SumInt: %v", err)
	}
	if sum != 10 {
		t.Fatalf("SumInt = %d, want 10", sum)
	}
}

func TestStoreCloseWaitsForActiveScanner(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1, 2, 3}, []string{"a", "b", "c"})

	scanner, err := store.Scan(context.Background(), ScanRequest{Table: table})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var batch vector.Batch
	if !scanner.Next(&batch) {
		t.Fatalf("Next false: %v", scanner.Err())
	}
	segments, err := store.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if got := activeSegmentRefs(store, segments[0].AbsPath(tableDir(store.root, table))); got != 1 {
		t.Fatalf("segment refs while scanning = %d, want 1", got)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()

	select {
	case err := <-closeDone:
		t.Fatalf("Store.Close returned before scanner closed: err=%v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := scanner.Close(); err != nil {
		t.Fatalf("scanner.Close: %v", err)
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Store.Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Store.Close did not return after scanner closed")
	}
}
