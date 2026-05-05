package table

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestCreateAppendOpenAndCount(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)

	appendBatch(t, tbl,
		[]int64{7, 42, 7},
		[]string{"signup", "checkout", "checkout"},
	)
	appendBatch(t, tbl,
		[]int64{11, 7},
		[]string{"cancel", "checkout"},
	)

	if tbl.Rows() != 5 {
		t.Fatalf("rows = %d, want 5", tbl.Rows())
	}
	if tbl.Segments() != 2 {
		t.Fatalf("segments = %d, want 2", tbl.Segments())
	}
	if tbl.Bytes() <= 0 {
		t.Fatalf("bytes = %d, want positive", tbl.Bytes())
	}
	if len(tbl.manifest.Segments) != 2 {
		t.Fatalf("manifest segments = %d, want 2", len(tbl.manifest.Segments))
	}
	if tbl.manifest.Segments[0].Offset != 0 {
		t.Fatalf("first offset = %d, want 0", tbl.manifest.Segments[0].Offset)
	}
	if tbl.manifest.Segments[1].Offset != tbl.manifest.Segments[0].Bytes {
		t.Fatalf("second offset = %d, want %d", tbl.manifest.Segments[1].Offset, tbl.manifest.Segments[0].Bytes)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Rows() != 5 {
		t.Fatalf("reopened rows = %d, want 5", reopened.Rows())
	}
	if reopened.Segments() != 2 {
		t.Fatalf("reopened segments = %d, want 2", reopened.Segments())
	}

	count, err := reopened.CountInt64Equal("tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("tenant count = %d, want 3", count)
	}

	count, err = reopened.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("event count = %d, want 3", count)
	}
}

func TestAppendRejectsSchemaMismatch(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())

	err := tbl.Append(mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7})},
	))
	if err == nil {
		t.Fatal("expected column order mismatch error")
	}
	if !strings.Contains(err.Error(), `batch column 0 is "event_type", want "tenant_id"`) {
		t.Fatalf("error = %q, want column order mismatch", err)
	}

	err = tbl.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewString([]string{"7"})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
	))
	if err == nil {
		t.Fatal("expected column kind mismatch error")
	}
	if !strings.Contains(err.Error(), `batch column "tenant_id" is string, want int64`) {
		t.Fatalf("error = %q, want kind mismatch", err)
	}
}

func TestAppenderPersistsMultipleSegmentsOnClose(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	appender, err := tbl.NewAppender()
	if err != nil {
		t.Fatal(err)
	}

	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "checkout"})},
	)); err != nil {
		t.Fatal(err)
	}
	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"cancel", "checkout"})},
	)); err != nil {
		t.Fatal(err)
	}

	uncommitted, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if uncommitted.Rows() != 0 || uncommitted.Segments() != 0 {
		t.Fatalf("uncommitted rows/segments = %d/%d, want 0/0", uncommitted.Rows(), uncommitted.Segments())
	}

	if err := appender.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Rows() != 5 || reopened.Segments() != 2 {
		t.Fatalf("reopened rows/segments = %d/%d, want 5/2", reopened.Rows(), reopened.Segments())
	}
	if reopened.manifest.Segments[0].Offset != 0 {
		t.Fatalf("first offset = %d, want 0", reopened.manifest.Segments[0].Offset)
	}
	if reopened.manifest.Segments[1].Offset != reopened.manifest.Segments[0].Bytes {
		t.Fatalf("second offset = %d, want %d", reopened.manifest.Segments[1].Offset, reopened.manifest.Segments[0].Bytes)
	}
}

func TestAppenderCloseRollsBackManifestFailure(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	appender, err := tbl.NewAppender()
	if err != nil {
		t.Fatal(err)
	}
	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
	)); err != nil {
		t.Fatal(err)
	}

	badDir := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(badDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	tbl.dir = badDir
	err = appender.Close()
	tbl.dir = dir
	if err == nil {
		t.Fatal("expected manifest write error")
	}
	if tbl.Rows() != 0 || tbl.Segments() != 0 {
		t.Fatalf("table rows/segments = %d/%d, want 0/0", tbl.Rows(), tbl.Segments())
	}
	info, err := os.Stat(filepath.Join(dir, dataFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("data file size = %d, want 0", info.Size())
	}
}

func TestCountRejectsMissingOrWrongType(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl, []int64{7}, []string{"signup"})

	_, err := tbl.CountInt64Equal("missing", 7)
	if err == nil || !strings.Contains(err.Error(), `missing column "missing"`) {
		t.Fatalf("missing int64 error = %v", err)
	}

	_, err = tbl.CountInt64Equal("event_type", 7)
	if err == nil || !strings.Contains(err.Error(), `column "event_type" is string, want int64`) {
		t.Fatalf("wrong int64 type error = %v", err)
	}

	_, err = tbl.CountStringEqual("tenant_id", "7")
	if err == nil || !strings.Contains(err.Error(), `column "tenant_id" is int64, want string`) {
		t.Fatalf("wrong string type error = %v", err)
	}
}

func TestCountInt64EqualPrunesSegmentsBeforeOpeningFiles(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl, []int64{1, 2, 3}, []string{"signup", "signup", "signup"})

	dataPath := filepath.Join(tbl.dir, dataFile)
	if err := os.WriteFile(dataPath, []byte("not-a-segment"), 0o644); err != nil {
		t.Fatal(err)
	}

	count, err := tbl.CountInt64Equal("tenant_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	_, err = tbl.CountInt64Equal("tenant_id", 2)
	if err == nil {
		t.Fatal("expected matching pruned-in corrupt segment to be opened")
	}
}

func TestEmptyTableCountsZero(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())

	count, err := tbl.CountInt64Equal("tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("int64 count = %d, want 0", count)
	}

	count, err = tbl.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("string count = %d, want 0", count)
	}
}

func TestScannerReusesBufferAcrossCounts(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl, []int64{7, 42, 7}, []string{"signup", "checkout", "checkout"})

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountInt64Equal("tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("tenant count = %d, want 2", count)
	}
	stats := scanner.Stats()
	if stats.RowsTotal != 3 || stats.RowsScanned != 3 || stats.BytesScanned <= 0 {
		t.Fatalf("stats = %+v, want 3-row scan", stats)
	}
	if cap(scanner.buf) == 0 {
		t.Fatal("expected scanner buffer to be retained")
	}
	bufCap := cap(scanner.buf)

	count, err = scanner.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("event count = %d, want 2", count)
	}
	if cap(scanner.buf) < bufCap {
		t.Fatalf("scanner buffer cap = %d, want at least %d", cap(scanner.buf), bufCap)
	}

	scanner.Reset()
	if scanner.buf != nil {
		t.Fatal("expected scanner reset to release buffer")
	}
	if scanner.Stats() != (ScanStats{}) {
		t.Fatalf("stats after reset = %+v, want zero", scanner.Stats())
	}
}

func TestScannerStatsTrackInt64Pruning(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl, []int64{1, 2, 3}, []string{"signup", "signup", "signup"})
	appendBatch(t, tbl, []int64{99, 99}, []string{"checkout", "checkout"})

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountInt64Equal("tenant_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	stats := scanner.Stats()
	if stats.SegmentsTotal != 2 || stats.SegmentsScanned != 1 || stats.SegmentsSkipped != 1 {
		t.Fatalf("segment stats = %+v, want 1 scanned and 1 skipped", stats)
	}
	if stats.RowsTotal != 5 || stats.RowsScanned != 2 || stats.RowsSkipped != 3 {
		t.Fatalf("row stats = %+v, want total 5 scanned 2 skipped 3", stats)
	}
	if stats.BytesTotal != tbl.Bytes() || stats.BytesScanned <= 0 || stats.BytesSkipped <= 0 {
		t.Fatalf("byte stats = %+v, table bytes = %d", stats, tbl.Bytes())
	}
}

func TestScannerStatsTrackSelectedColumnBytes(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	tenants := make([]int64, 1000)
	events := make([]string, 1000)
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	for row := range tenants {
		tenants[row] = int64(row)
		events[row] = choices[row%len(choices)]
	}
	appendBatch(t, tbl, tenants, events)

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if count != 250 {
		t.Fatalf("count = %d, want 250", count)
	}
	stats := scanner.Stats()
	if stats.BytesScanned <= 0 || stats.BytesScanned >= stats.BytesTotal {
		t.Fatalf("bytes scanned = %d, want selected-column scan below total %d", stats.BytesScanned, stats.BytesTotal)
	}
}

func TestGroupStringCounts(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl,
		[]int64{7, 42, 7},
		[]string{"signup", "checkout", "checkout"},
	)
	appendBatch(t, tbl,
		[]int64{11, 7},
		[]string{"cancel", "checkout"},
	)

	counts, err := tbl.GroupStringCounts("event_type")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"signup": 1, "checkout": 3, "cancel": 1}
	if !maps.Equal(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
}

func TestScannerGroupStringCountsInto(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl,
		[]int64{7, 42, 7},
		[]string{"signup", "checkout", "checkout"},
	)

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	counts, err := scanner.GroupStringCountsInto("event_type", map[string]int{"existing": 4})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"existing": 4, "signup": 1, "checkout": 2}
	if !maps.Equal(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
}

func TestManifestPersistsStats(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	appendBatch(t, tbl, []int64{7, 42, 11}, []string{"signup", "checkout", "cancel"})

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(reopened.manifest.Segments) != 1 {
		t.Fatalf("segments = %d, want 1", len(reopened.manifest.Segments))
	}
	segment := reopened.manifest.Segments[0]
	if segment.Rows != 3 || segment.Stats.Rows != 3 {
		t.Fatalf("segment rows = %d/%d, want 3/3", segment.Rows, segment.Stats.Rows)
	}
	if segment.Offset != 0 || segment.Bytes <= 0 {
		t.Fatalf("segment offset/bytes = %d/%d, want 0/positive", segment.Offset, segment.Bytes)
	}
	stats, ok := segment.Stats.Column("tenant_id")
	if !ok {
		t.Fatal("missing tenant_id stats")
	}
	if !stats.HasMinMax || stats.MinInt64 != 7 || stats.MaxInt64 != 42 {
		t.Fatalf("tenant stats = %+v, want min/max 7/42", stats)
	}
}

func TestCreateRejectsExistingManifest(t *testing.T) {
	dir := t.TempDir()
	createEventsTable(t, dir)

	_, err := Create(dir, eventsSchema())
	if err == nil {
		t.Fatal("expected existing manifest error")
	}
}

func BenchmarkCountInt64EqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := tbl.CountInt64Equal("tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 980 {
			b.Fatalf("count = %d, want 980", count)
		}
	}
}

func BenchmarkCountStringEqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := tbl.CountStringEqual("event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if count != 250_000 {
			b.Fatalf("count = %d, want 250000", count)
		}
	}
}

func BenchmarkScannerCountInt64EqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountInt64Equal("tenant_id", 7); err != nil {
		b.Fatal(err)
	} else if count != 980 {
		b.Fatalf("count = %d, want 980", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountInt64Equal("tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 980 {
			b.Fatalf("count = %d, want 980", count)
		}
	}
}

func BenchmarkScannerCountStringEqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountStringEqual("event_type", "checkout"); err != nil {
		b.Fatal(err)
	} else if count != 250_000 {
		b.Fatalf("count = %d, want 250000", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountStringEqual("event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if count != 250_000 {
			b.Fatalf("count = %d, want 250000", count)
		}
	}
}

func BenchmarkGroupStringCountsTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		counts, err := tbl.GroupStringCounts("event_type")
		if err != nil {
			b.Fatal(err)
		}
		if counts["checkout"] != 250_000 {
			b.Fatalf("checkout count = %d, want 250000", counts["checkout"])
		}
	}
}

func BenchmarkScannerGroupStringCountsTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	counts := make(map[string]int, 4)
	if _, err := scanner.GroupStringCountsInto("event_type", counts); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		clear(counts)
		counts, err := scanner.GroupStringCountsInto("event_type", counts)
		if err != nil {
			b.Fatal(err)
		}
		if counts["checkout"] != 250_000 {
			b.Fatalf("checkout count = %d, want 250000", counts["checkout"])
		}
	}
}

func createEventsTable(t testing.TB, dir string) *Table {
	t.Helper()
	tbl, err := Create(dir, eventsSchema())
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func eventsSchema() []Column {
	return []Column{
		{Name: "tenant_id", Kind: vector.KindInt64},
		{Name: "event_type", Kind: vector.KindString},
	}
}

func appendBatch(t testing.TB, tbl *Table, tenants []int64, events []string) {
	t.Helper()
	if err := tbl.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(tenants)},
		vector.Column{Name: "event_type", Vector: vector.FromString(events)},
	)); err != nil {
		t.Fatal(err)
	}
}

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func benchmarkEventsTable(b *testing.B, segments int, rowsPerSegment int) *Table {
	b.Helper()
	tbl := createEventsTable(b, b.TempDir())
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	for segment := 0; segment < segments; segment++ {
		tenants := make([]int64, rowsPerSegment)
		events := make([]string, rowsPerSegment)
		for row := range rowsPerSegment {
			tenants[row] = int64(row % 1024)
			events[row] = choices[row%len(choices)]
		}
		appendBatch(b, tbl, tenants, events)
	}
	return tbl
}
