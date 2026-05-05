package table

import (
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

	segmentPath := filepath.Join(tbl.dir, filepath.FromSlash(tbl.manifest.Segments[0].File))
	if err := os.WriteFile(segmentPath, []byte("not-a-segment"), 0o644); err != nil {
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
