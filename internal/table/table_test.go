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
	if len(segment.Columns) != 2 {
		t.Fatalf("column ranges = %d, want 2", len(segment.Columns))
	}
	var tenantRange ColumnRange
	foundTenantRange := false
	for _, columnRange := range segment.Columns {
		if columnRange.Name == "tenant_id" {
			tenantRange = columnRange
			foundTenantRange = true
			break
		}
	}
	if !foundTenantRange {
		t.Fatal("missing tenant_id column range")
	}
	if tenantRange.Offset <= 0 || tenantRange.Bytes <= 0 || tenantRange.Offset+tenantRange.Bytes > segment.Bytes {
		t.Fatalf("tenant range = %+v, segment bytes = %d", tenantRange, segment.Bytes)
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

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
