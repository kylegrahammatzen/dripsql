package storage

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSegmentRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		batch vector.Batch
	}{
		{
			name: "int64 and string",
			batch: mustBatch(t,
				vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
				vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
			),
		},
		{
			name: "empty columns",
			batch: mustBatch(t,
				vector.Column{Name: "tenant_id", Vector: vector.NewInt64(nil)},
				vector.Column{Name: "event_type", Vector: vector.NewString(nil)},
			),
		},
		{
			name: "edge values",
			batch: mustBatch(t,
				vector.Column{Name: "value", Vector: vector.NewInt64([]int64{-10, 0, 99})},
				vector.Column{Name: "label", Vector: vector.NewString([]string{"", "signup", ""})},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeStats, got, readStats := roundTrip(t, tt.batch)

			if writeStats.Rows != tt.batch.Count {
				t.Fatalf("writeStats.Rows = %d, want %d", writeStats.Rows, tt.batch.Count)
			}
			if readStats.Rows != tt.batch.Count {
				t.Fatalf("readStats.Rows = %d, want %d", readStats.Rows, tt.batch.Count)
			}

			requireBatchEqual(t, tt.batch, got)
		})
	}
}

func TestSegmentPersistsInt64Stats(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		values     []int64
		wantMin    int64
		wantMax    int64
		encodedLen int
	}{
		{name: "positive", column: "tenant_id", values: []int64{7, 42, 7}, wantMin: 7, wantMax: 42, encodedLen: 24},
		{name: "negative", column: "value", values: []int64{-99, -7, -42}, wantMin: -99, wantMax: -7, encodedLen: 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := mustBatch(t,
				vector.Column{Name: tt.column, Vector: vector.NewInt64(tt.values)},
			)

			writeStats, _, readStats := roundTrip(t, batch)
			statsToCheck := []struct {
				label string
				stats ColumnStats
			}{
				{label: "written", stats: columnStats(t, writeStats, tt.column)},
				{label: "read", stats: columnStats(t, readStats, tt.column)},
			}

			for _, got := range statsToCheck {
				if !got.stats.HasMinMax {
					t.Fatalf("expected %s min/max stats", got.label)
				}
				if got.stats.MinInt64 != tt.wantMin || got.stats.MaxInt64 != tt.wantMax {
					t.Fatalf("%s min/max = %d/%d, want %d/%d", got.label, got.stats.MinInt64, got.stats.MaxInt64, tt.wantMin, tt.wantMax)
				}
				if got.stats.EncodedLen != tt.encodedLen {
					t.Fatalf("%s EncodedLen = %d, want %d", got.label, got.stats.EncodedLen, tt.encodedLen)
				}
			}
		})
	}
}

func TestReadSegmentStats(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
	)

	var buf bytes.Buffer
	writeStats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatal(err)
	}

	readStats, err := ReadSegmentStats(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if readStats.Rows != writeStats.Rows {
		t.Fatalf("rows = %d, want %d", readStats.Rows, writeStats.Rows)
	}
	if len(readStats.Columns) != len(writeStats.Columns) {
		t.Fatalf("column count = %d, want %d", len(readStats.Columns), len(writeStats.Columns))
	}

	tenantStats := columnStats(t, readStats, "tenant_id")
	if tenantStats.Kind != vector.KindInt64 || !tenantStats.HasMinMax {
		t.Fatalf("tenant stats = %+v, want int64 min/max", tenantStats)
	}
	if tenantStats.MinInt64 != 7 || tenantStats.MaxInt64 != 42 {
		t.Fatalf("tenant min/max = %d/%d, want 7/42", tenantStats.MinInt64, tenantStats.MaxInt64)
	}

	eventStats := columnStats(t, readStats, "event_type")
	if eventStats.Kind != vector.KindString {
		t.Fatalf("event kind = %s, want string", eventStats.Kind)
	}
	if eventStats.HasMinMax {
		t.Fatal("did not expect string min/max stats")
	}

	if _, ok := readStats.Column("missing"); ok {
		t.Fatal("did not expect missing column stats")
	}
}

func TestReadSegmentColumns(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}

	got, stats, err := ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 3 {
		t.Fatalf("count = %d, want 3", got.Count)
	}
	if len(got.Columns) != 1 {
		t.Fatalf("column count = %d, want 1", len(got.Columns))
	}
	if _, ok := got.Column("event_type"); ok {
		t.Fatal("did not expect unrequested event_type column")
	}

	tenantCol, ok := got.Column("tenant_id")
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{7, 42, 7}), tenantCol.Vector)

	if stats.Rows != 3 {
		t.Fatalf("stats rows = %d, want 3", stats.Rows)
	}
	if len(stats.Columns) != 1 {
		t.Fatalf("stats column count = %d, want 1", len(stats.Columns))
	}
	if stats.Columns[0].Name != "tenant_id" {
		t.Fatalf("stats column = %q, want tenant_id", stats.Columns[0].Name)
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "missing")
	if err == nil {
		t.Fatal("expected missing column error")
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "missing_a", "missing_b")
	if err == nil {
		t.Fatal("expected missing columns error")
	}
	if !strings.Contains(err.Error(), `"missing_a"`) {
		t.Fatalf("error = %q, want missing_a first", err)
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "tenant_id", "tenant_id")
	if err == nil {
		t.Fatal("expected duplicate requested column error")
	}
}

func TestReadSegmentColumnsConsumesFullSegment(t *testing.T) {
	first := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
	)
	second := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 12})},
	)

	reader := bytes.NewReader(append(first, second...))
	got, _, err := ReadSegmentColumns(reader, "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 2 {
		t.Fatalf("count = %d, want 2", got.Count)
	}

	next, _, err := ReadSegment(reader)
	if err != nil {
		t.Fatal(err)
	}
	nextCol, ok := next.Column("tenant_id")
	if !ok {
		t.Fatal("missing second tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{11, 12}), nextCol.Vector)
}

func FuzzSegmentRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3}, "signup")
	f.Add([]byte{}, "")
	f.Add([]byte{255, 0, 128}, "checkout")

	f.Fuzz(func(t *testing.T, raw []byte, label string) {
		ints := make([]int64, len(raw))
		strings := make([]string, len(raw))

		for i, value := range raw {
			ints[i] = int64(int8(value))
			strings[i] = label
		}

		batch := mustBatch(t,
			vector.Column{Name: "value", Vector: vector.NewInt64(ints)},
			vector.Column{Name: "label", Vector: vector.NewString(strings)},
		)

		_, got, _ := roundTrip(t, batch)
		requireBatchEqual(t, batch, got)
	})
}

func BenchmarkWriteSegmentInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	for b.Loop() {
		var buf bytes.Buffer
		buf.Grow(900_000)
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// Measures the write path when the caller owns and reuses the output buffer.
func BenchmarkWriteSegmentInt64ReuseBuffer(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	buf.Grow(900_000)

	for b.Loop() {
		buf.Reset()
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentStatsInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, err := ReadSegmentStats(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentInt64WithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}

	batch := mustBatch(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentColumnsInt64WithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		batch, _, err := ReadSegmentColumns(bytes.NewReader(data), "tenant_id")
		if err != nil {
			b.Fatal(err)
		}
		if batch.Count != 100_000 {
			b.Fatalf("count = %d, want 100000", batch.Count)
		}
	}
}

func BenchmarkReadSegmentColumnsInt64AfterString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkStringInt64Segment(b)
	b.ResetTimer()

	for b.Loop() {
		batch, _, err := ReadSegmentColumns(bytes.NewReader(data), "tenant_id")
		if err != nil {
			b.Fatal(err)
		}
		if batch.Count != 100_000 {
			b.Fatalf("count = %d, want 100000", batch.Count)
		}
	}
}

func BenchmarkWriteSegmentString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}

	batch := mustBatch(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)

	for b.Loop() {
		var buf bytes.Buffer
		buf.Grow(2_000_000)
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func roundTrip(t testing.TB, batch vector.Batch) (SegmentStats, vector.Batch, SegmentStats) {
	t.Helper()

	var buf bytes.Buffer

	writeStats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatal(err)
	}

	got, readStats, err := ReadSegment(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	return writeStats, got, readStats
}

func writeSegmentBytes(t testing.TB, columns ...vector.Column) []byte {
	t.Helper()

	batch := mustBatch(t, columns...)
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}

	return batch
}

func requireBatchEqual(t *testing.T, want, got vector.Batch) {
	t.Helper()

	if got.Count != want.Count {
		t.Fatalf("batch count = %d, want %d", got.Count, want.Count)
	}
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("column count = %d, want %d", len(got.Columns), len(want.Columns))
	}

	for _, wantCol := range want.Columns {
		gotCol, ok := got.Column(wantCol.Name)
		if !ok {
			t.Fatalf("missing column %q", wantCol.Name)
		}

		requireVectorEqual(t, wantCol.Vector, gotCol.Vector)
	}
}

func requireVectorEqual(t *testing.T, want, got vector.Vector) {
	t.Helper()

	if got.Kind() != want.Kind() {
		t.Fatalf("kind = %s, want %s", got.Kind(), want.Kind())
	}

	switch wantValues := want.(type) {
	case vector.Int64:
		gotValues, ok := got.(vector.Int64)
		if !ok {
			t.Fatalf("got %T, want vector.Int64", got)
		}
		if !slices.Equal(gotValues.Values, wantValues.Values) {
			t.Fatalf("values = %v, want %v", gotValues.Values, wantValues.Values)
		}

	case vector.String:
		gotValues, ok := got.(vector.String)
		if !ok {
			t.Fatalf("got %T, want vector.String", got)
		}
		if gotValues.Len() != wantValues.Len() {
			t.Fatalf("values len = %d, want %d", gotValues.Len(), wantValues.Len())
		}
		for i := 0; i < wantValues.Len(); i++ {
			if gotValues.Value(i) != wantValues.Value(i) {
				t.Fatalf("value[%d] = %q, want %q", i, gotValues.Value(i), wantValues.Value(i))
			}
		}

	default:
		t.Fatalf("unsupported vector type %T", want)
	}
}

func columnStats(t *testing.T, stats SegmentStats, name string) ColumnStats {
	t.Helper()

	for _, col := range stats.Columns {
		if col.Name == name {
			return col
		}
	}

	t.Fatalf("missing column stats for %q", name)
	return ColumnStats{}
}

func benchmarkInt64StringSegment(b *testing.B) []byte {
	b.Helper()

	ids := make([]int64, 100_000)
	for i := range ids {
		ids[i] = int64(i % 1024)
	}

	events := make([]string, 100_000)
	for i := range events {
		events[i] = "event"
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(ids)},
		vector.Column{Name: "event_type", Vector: vector.FromString(events)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func benchmarkStringInt64Segment(b *testing.B) []byte {
	b.Helper()

	events := make([]string, 100_000)
	for i := range events {
		events[i] = "event"
	}

	ids := make([]int64, 100_000)
	for i := range ids {
		ids[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "event_type", Vector: vector.FromString(events)},
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(ids)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}
