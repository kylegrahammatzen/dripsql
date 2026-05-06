package table

import (
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

func TestCreateAppendOpenParallelAndGroup(t *testing.T) {
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
	if tbl.Segments() != 2 {
		t.Fatalf("segments = %d, want 2", tbl.Segments())
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Rows() != 5 || reopened.Segments() != 2 {
		t.Fatalf("reopened rows/segments = %d/%d, want 5/2", reopened.Rows(), reopened.Segments())
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

	scanner := reopened.NewScanner()
	t.Cleanup(func() { _ = scanner.Close() })
	count, err = scanner.CountStringEqualParallel("event_type", "checkout", 4)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("parallel fallback count = %d, want 3", count)
	}
	counts, err := scanner.GroupStringCounts("event_type")
	if err != nil {
		t.Fatal(err)
	}
	if counts["checkout"] != 3 || counts["signup"] != 1 || counts["cancel"] != 1 {
		t.Fatalf("storage groups = %#v, want exact event counts", counts)
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
	if stats.Codec == 0 {
		t.Fatalf("tenant encoding stats were not persisted: %+v", stats)
	}
}

func TestColumnStorageStats(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	tenants := make([]int64, 1000)
	events := make([]string, 1000)
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	for row := range tenants {
		tenants[row] = int64(row % 4)
		events[row] = choices[row%len(choices)]
	}
	appendBatch(t, tbl, tenants, events)

	stats, err := tbl.ColumnStorageStats()
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 {
		t.Fatalf("stats columns = %d, want 2", len(stats))
	}

	tenant := columnStorageStats(t, stats, "tenant_id")
	if tenant.Rows != 1000 || tenant.Segments != 1 || tenant.Codec != "dictionary" {
		t.Fatalf("tenant storage stats = %+v, want 1000 rows in one dictionary segment", tenant)
	}
	if tenant.DictionarySegments != 1 || tenant.MinDictionaryValues != 4 || tenant.MaxDictionaryValues != 4 {
		t.Fatalf("tenant dictionary stats = %+v, want four dictionary values", tenant)
	}
	if tenant.PackedIDSegments != 1 || tenant.MinPackedIDBitWidth != 2 || tenant.MaxPackedIDBitWidth != 2 {
		t.Fatalf("tenant id stats = %+v, want packed 2-bit ids", tenant)
	}
	if tenant.CompressionRatio() <= 1 {
		t.Fatalf("tenant compression ratio = %.2f, want above 1", tenant.CompressionRatio())
	}

	event := columnStorageStats(t, stats, "event_type")
	if event.Codec != "dictionary" || event.FilterPath != "dictionary" || event.GroupPath != "dict-counts" {
		t.Fatalf("event storage stats = %+v, want dictionary equality/group stats", event)
	}
}

func TestCreateRejectsExistingManifest(t *testing.T) {
	dir := t.TempDir()
	createEventsTable(t, dir)

	_, err := Create(dir, []Column{
		{Name: "tenant_id", Kind: vector.KindInt64},
		{Name: "event_type", Kind: vector.KindString},
	})
	if err == nil {
		t.Fatal("expected existing manifest error")
	}
}

func createEventsTable(t testing.TB, dir string) *Table {
	t.Helper()
	tbl, err := Create(dir, []Column{
		{Name: "tenant_id", Kind: vector.KindInt64},
		{Name: "event_type", Kind: vector.KindString},
	})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
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

func columnStorageStats(t *testing.T, stats []ColumnStorageStats, name string) ColumnStorageStats {
	t.Helper()
	for _, col := range stats {
		if col.Name == name {
			return col
		}
	}
	t.Fatalf("missing column storage stats for %q", name)
	return ColumnStorageStats{}
}

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
