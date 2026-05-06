package table

import (
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

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

func TestScannerReusesStorageReadersAcrossCounts(t *testing.T) {
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
	if len(scanner.storageOK) != 1 || !scanner.storageOK[0] {
		t.Fatal("expected scanner to cache the opened segment reader")
	}

	count, err = scanner.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("event count = %d, want 2", count)
	}
	if len(scanner.storageOK) != 1 || !scanner.storageOK[0] {
		t.Fatal("expected cached segment reader to survive repeated scans")
	}

	scanner.Reset()
	if scanner.buf != nil || scanner.storageReaders != nil || scanner.storageOK != nil {
		t.Fatal("expected scanner reset to release scratch and cached readers")
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
	if stats.BytesSkipped != stats.BytesTotal-stats.BytesScanned {
		t.Fatalf("bytes skipped = %d, want total-scanned %d", stats.BytesSkipped, stats.BytesTotal-stats.BytesScanned)
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
	if stats.BytesSkipped != stats.BytesTotal-stats.BytesScanned {
		t.Fatalf("bytes skipped = %d, want total-scanned %d", stats.BytesSkipped, stats.BytesTotal-stats.BytesScanned)
	}
}

func TestScannerStringBloomPrunesSegments(t *testing.T) {
	tbl, err := Create(t.TempDir(), []Column{{Name: "url", Kind: vector.KindString}})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]string, 5000)
	for row := range values {
		values[row] = "url-" + strconv.Itoa(row)
	}
	if err := tbl.Append(mustBatch(t, vector.Column{Name: "url", Vector: vector.FromString(values)})); err != nil {
		t.Fatal(err)
	}

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountStringEqual("url", values[123])
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("existing count = %d, want 1", count)
	}
	if stats := scanner.Stats(); stats.SegmentsScanned != 1 || stats.SegmentsSkipped != 0 {
		t.Fatalf("existing stats = %+v, want one scanned segment", stats)
	}

	for i := 0; i < 100; i++ {
		count, err = scanner.CountStringEqual("url", "missing-"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("missing count = %d, want 0", count)
		}
		stats := scanner.Stats()
		if stats.SegmentsSkipped == 1 && stats.SegmentsScanned == 0 && stats.RowsSkipped == int64(len(values)) {
			return
		}
	}
	t.Fatal("bloom did not skip any absent test values")
}

func TestScannerStringPageBloomSkipsRows(t *testing.T) {
	tbl, err := Create(t.TempDir(), []Column{{Name: "url", Kind: vector.KindString}})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]string, 5000)
	for row := range values {
		values[row] = strconv.FormatUint(testStringPageBloomHash(uint64(row+1)), 36)
	}
	if err := tbl.Append(mustBatch(t, vector.Column{Name: "url", Vector: vector.FromString(values)})); err != nil {
		t.Fatal(err)
	}

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountStringEqual("url", values[123])
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("existing count = %d, want 1", count)
	}
	if stats := scanner.Stats(); stats.SegmentsScanned != 1 || stats.RowsScanned == int64(len(values)) || stats.RowsSkipped == 0 {
		t.Fatalf("existing stats = %+v, want page bloom to scan fewer than all rows", stats)
	}

	for i := 0; i < 100; i++ {
		count, err = scanner.CountStringEqual("url", "/absent/"+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("missing count = %d, want 0", count)
		}
		stats := scanner.Stats()
		if stats.RowsSkipped == int64(len(values)) && stats.RowsScanned == 0 && stats.BytesScanned < stats.BytesTotal {
			return
		}
	}
	t.Fatal("storage page bloom did not skip all rows for any absent test value")
}

func testStringPageBloomHash(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	value ^= value >> 31
	return value
}

func TestScannerInt64BloomPrunesSegments(t *testing.T) {
	tbl, err := Create(t.TempDir(), []Column{{Name: "user_id", Kind: vector.KindInt64}})
	if err != nil {
		t.Fatal(err)
	}
	values := make([]int64, 5000)
	for row := range values {
		values[row] = int64(row*1000 + (row*row)%97)
	}
	if err := tbl.Append(mustBatch(t, vector.Column{Name: "user_id", Vector: vector.FromInt64(values)})); err != nil {
		t.Fatal(err)
	}

	scanner := tbl.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err := scanner.CountInt64Equal("user_id", values[123])
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("existing count = %d, want 1", count)
	}
	if stats := scanner.Stats(); stats.SegmentsScanned != 1 || stats.SegmentsSkipped != 0 {
		t.Fatalf("existing stats = %+v, want one scanned segment", stats)
	}

	for i := 0; i < 100; i++ {
		count, err = scanner.CountInt64Equal("user_id", int64(i*1000+500))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("missing count = %d, want 0", count)
		}
		stats := scanner.Stats()
		if stats.SegmentsSkipped == 1 && stats.SegmentsScanned == 0 && stats.RowsSkipped == int64(len(values)) {
			return
		}
	}
	t.Fatal("storage segment bloom did not skip any absent int64 test values")
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
