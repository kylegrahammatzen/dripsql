package storage

import (
	"bytes"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestCountInt64Equal(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, -3, 7, 9, -3})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "view", "checkout"})},
	)
	reader := mustOpenBytes(t, batch)
	tenantMeta := mustReaderColumn(t, reader, "tenant_id")

	count, scratch, stats, err := CountInt64Equal(reader, "tenant_id", 7, nil)
	if err != nil {
		t.Fatalf("CountInt64Equal() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: tenantMeta.Payload.Bytes, RowsScanned: 5, SegmentsScanned: 1})

	count, scratch, stats, err = CountInt64Equal(reader, "tenant_id", -3, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal(negative) error = %v", err)
	}
	if count != 2 {
		t.Fatalf("negative count = %d, want 2", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: tenantMeta.Payload.Bytes, RowsScanned: 5, SegmentsScanned: 1})

	count, scratch, stats, err = CountInt64Equal(reader, "tenant_id", 99, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal(absent) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{RowsSkipped: 5, SegmentsScanned: 1})

	if _, _, _, err := CountInt64Equal(reader, "missing", 7, scratch); err == nil {
		t.Fatalf("CountInt64Equal(missing) error = nil")
	}
	if _, _, _, err := CountInt64Equal(reader, "event_type", 7, scratch); err == nil {
		t.Fatalf("CountInt64Equal(wrong type) error = nil")
	}
}

func TestCountInt64EqualSequence(t *testing.T) {
	values := []int64{10, 13, 16, 19, 22}
	batch := mustBatch(t, vector.Column{Name: "id", Vector: vector.NewInt64(values)})
	reader := mustOpenBytes(t, batch)
	meta := mustReaderColumn(t, reader, "id")
	if meta.Codec != CodecInt64Sequence {
		t.Fatalf("id codec = %s, want sequence", meta.Codec)
	}

	count, scratch, stats, err := CountInt64Equal(reader, "id", 16, nil)
	if err != nil {
		t.Fatalf("CountInt64Equal() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: meta.Payload.Bytes, RowsSkipped: len(values), SegmentsScanned: 1})

	count, _, stats, err = CountInt64Equal(reader, "id", 17, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal(missing in range) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("missing in range count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: meta.Payload.Bytes, RowsSkipped: len(values), SegmentsScanned: 1})

	count, _, stats, err = CountInt64Equal(reader, "id", 99, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal(absent) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{RowsSkipped: len(values), SegmentsScanned: 1})
}

func TestCountStringEqual(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3, 4, 5})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "", "checkout", "signup", "checkout"})},
	)
	reader := mustOpenBytes(t, batch)
	eventMeta := mustReaderColumn(t, reader, "event_type")

	count, scratch, stats, err := CountStringEqual(reader, "event_type", "checkout", nil)
	if err != nil {
		t.Fatalf("CountStringEqual() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Payload.Bytes, RowsScanned: 5, SegmentsScanned: 1})

	count, scratch, stats, err = CountStringEqual(reader, "event_type", "", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual(empty) error = %v", err)
	}
	if count != 1 {
		t.Fatalf("empty count = %d, want 1", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Payload.Bytes, RowsScanned: 5, SegmentsScanned: 1})

	count, _, stats, err = CountStringEqual(reader, "event_type", "missing", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual(absent) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Payload.Bytes, RowsScanned: 5, SegmentsScanned: 1})

	if _, _, _, err := CountStringEqual(reader, "missing", "checkout", scratch); err == nil {
		t.Fatalf("CountStringEqual(missing) error = nil")
	}
	if _, _, _, err := CountStringEqual(reader, "tenant_id", "checkout", scratch); err == nil {
		t.Fatalf("CountStringEqual(wrong type) error = nil")
	}
}

func TestGroupStringCountsDictionary(t *testing.T) {
	const rows = 1000
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, rows)
	for row := range values {
		values[row] = choices[row%len(choices)]
	}
	batch := mustBatch(t, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	reader := mustOpenBytes(t, batch)
	eventMeta := mustReaderColumn(t, reader, "event_type")
	if eventMeta.Codec != CodecDictionary {
		t.Fatalf("event_type codec = %s, want dictionary", eventMeta.Codec)
	}

	counts, scratch, keyScratch, stats, err := GroupStringCounts(reader, "event_type", nil, nil, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts() error = %v", err)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	if len(keyScratch) != len(choices) {
		t.Fatalf("key scratch len = %d, want %d", len(keyScratch), len(choices))
	}
	for _, choice := range choices {
		if counts[choice] != rows/len(choices) {
			t.Fatalf("counts[%q] = %d, want %d", choice, counts[choice], rows/len(choices))
		}
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Dictionary.Bytes, RowsSkipped: rows, SegmentsScanned: 1})

	clear(counts)
	counts, _, keyScratch, _, err = GroupStringCounts(reader, "event_type", counts, scratch, keyScratch)
	if err != nil {
		t.Fatalf("GroupStringCounts(reuse) error = %v", err)
	}
	if len(keyScratch) != len(choices) {
		t.Fatalf("reused key scratch len = %d, want %d", len(keyScratch), len(choices))
	}
	if counts["checkout"] != rows/len(choices) {
		t.Fatalf("reused checkout count = %d, want %d", counts["checkout"], rows/len(choices))
	}
}

func TestGroupStringCountsPlain(t *testing.T) {
	values := []string{"signup", "", "checkout", "signup", "checkout"}
	batch := mustBatch(t, vector.Column{Name: "event_type", Vector: vector.NewString(values)})
	reader := mustOpenBytes(t, batch)
	eventMeta := mustReaderColumn(t, reader, "event_type")
	if eventMeta.Codec != CodecPlain {
		t.Fatalf("event_type codec = %s, want plain", eventMeta.Codec)
	}

	counts, scratch, keyScratch, stats, err := GroupStringCounts(reader, "event_type", nil, nil, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts() error = %v", err)
	}
	if scratch != nil || keyScratch != nil {
		t.Fatalf("scratch/keyScratch = %v/%v, want nil byte-view scratch", scratch, keyScratch)
	}
	want := map[string]int{"signup": 2, "": 1, "checkout": 2}
	if len(counts) != len(want) {
		t.Fatalf("counts len = %d, want %d: %#v", len(counts), len(want), counts)
	}
	for key, wantCount := range want {
		if counts[key] != wantCount {
			t.Fatalf("counts[%q] = %d, want %d", key, counts[key], wantCount)
		}
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Payload.Bytes, RowsScanned: len(values), SegmentsScanned: 1})

	if _, _, _, _, err := GroupStringCounts(reader, "missing", nil, nil, nil); err == nil {
		t.Fatalf("GroupStringCounts(missing) error = nil")
	}
	if _, _, _, _, err := GroupStringCounts(reader, "tenant_id", nil, nil, nil); err == nil {
		t.Fatalf("GroupStringCounts(wrong type) error = nil")
	}
}

func TestCountEqualSourcePaths(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3, 2})},
		vector.Column{Name: "path", Vector: vector.NewString([]string{"/a", "/b", "/a", "/c"})},
	)
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}

	t.Run("reader_at", func(t *testing.T) {
		source := &recordingReaderAt{data: buf.Bytes()}
		reader, scratch, err := OpenSegment(source, 0, int64(buf.Len()), nil)
		if err != nil {
			t.Fatalf("OpenSegment() error = %v", err)
		}
		baseReads := len(source.calls)
		tenantMeta := mustReaderColumn(t, reader, "tenant_id")
		pathMeta := mustReaderColumn(t, reader, "path")

		count, nextScratch, stats, err := CountInt64Equal(reader, "tenant_id", 2, scratch)
		if err != nil {
			t.Fatalf("CountInt64Equal() error = %v", err)
		}
		if count != 2 {
			t.Fatalf("int count = %d, want 2", count)
		}
		scratch = nextScratch
		assertScanStats(t, stats, ScanStats{BytesRead: tenantMeta.Payload.Bytes, BytesScanned: tenantMeta.Payload.Bytes, RowsScanned: 4, SegmentsScanned: 1})

		count, _, stats, err = CountStringEqual(reader, "path", "/a", scratch)
		if err != nil {
			t.Fatalf("CountStringEqual() error = %v", err)
		}
		if count != 2 {
			t.Fatalf("string count = %d, want 2", count)
		}
		assertScanStats(t, stats, ScanStats{BytesRead: pathMeta.Payload.Bytes, BytesScanned: pathMeta.Payload.Bytes, RowsScanned: 4, SegmentsScanned: 1})
		if got, want := len(source.calls), baseReads+2; got != want {
			t.Fatalf("payload reads = %d, want %d", got-baseReads, 2)
		}
		assertReadAtCall(t, source.calls[baseReads], tenantMeta.Payload)
		assertReadAtCall(t, source.calls[baseReads+1], pathMeta.Payload)
	})

	t.Run("byte_viewer", func(t *testing.T) {
		source := &recordingByteViewer{data: buf.Bytes()}
		reader, scratch, err := OpenSegment(source, 0, int64(buf.Len()), nil)
		if err != nil {
			t.Fatalf("OpenSegment() error = %v", err)
		}
		baseReadAt := len(source.readAtCalls)
		baseViews := len(source.viewCalls)
		pathMeta := mustReaderColumn(t, reader, "path")

		count, _, stats, err := CountStringEqual(reader, "path", "/a", scratch)
		if err != nil {
			t.Fatalf("CountStringEqual() error = %v", err)
		}
		if count != 2 {
			t.Fatalf("string count = %d, want 2", count)
		}
		assertScanStats(t, stats, ScanStats{BytesScanned: pathMeta.Payload.Bytes, RowsScanned: 4, SegmentsScanned: 1})
		if got := len(source.readAtCalls) - baseReadAt; got != 0 {
			t.Fatalf("ReadAt payload calls = %d, want 0", got)
		}
		if got, want := len(source.viewCalls), baseViews+1; got != want {
			t.Fatalf("View payload calls = %d, want %d", got-baseViews, 1)
		}
		assertReadAtCall(t, source.viewCalls[baseViews], pathMeta.Payload)
	})
}

func TestCountEqualZeroRows(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(nil)},
		vector.Column{Name: "event_type", Vector: vector.NewString(nil)},
	)
	reader := mustOpenBytes(t, batch)

	count, _, stats, err := CountInt64Equal(reader, "tenant_id", 7, nil)
	if err != nil {
		t.Fatalf("CountInt64Equal() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("int count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{SegmentsScanned: 1})

	count, _, stats, err = CountStringEqual(reader, "event_type", "signup", nil)
	if err != nil {
		t.Fatalf("CountStringEqual() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("string count = %d, want 0", count)
	}
	eventMeta := mustReaderColumn(t, reader, "event_type")
	assertScanStats(t, stats, ScanStats{BytesScanned: eventMeta.Payload.Bytes, SegmentsScanned: 1})
}

func mustOpenBytes(t *testing.T, batch vector.Batch) Reader {
	t.Helper()
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	return reader
}

func assertScanStats(t *testing.T, got ScanStats, want ScanStats) {
	t.Helper()
	if got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
}

var benchmarkCountResult int

func BenchmarkCountInt64EqualPlainBytes(b *testing.B) {
	const rows = 100_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = nonSequenceInt64Value(i)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)})
	reader := mustBenchmarkOpenBytes(b, batch)
	meta, _ := reader.Column("tenant_id")
	if meta.Codec != CodecPlain {
		b.Fatalf("tenant_id codec = %s, want plain", meta.Codec)
	}
	b.SetBytes(meta.Payload.Bytes)
	b.ReportAllocs()
	b.ResetTimer()

	count := 0
	for i := 0; i < b.N; i++ {
		var err error
		count, _, _, err = CountInt64Equal(reader, "tenant_id", values[rows/2], nil)
		if err != nil {
			b.Fatalf("CountInt64Equal() error = %v", err)
		}
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 1 {
		b.Fatalf("count = %d, want 1", benchmarkCountResult)
	}
}

func BenchmarkCountInt64EqualDictionaryBytes(b *testing.B) {
	const rows = 100_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i % 10)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)})
	reader := mustBenchmarkOpenBytes(b, batch)
	meta, _ := reader.Column("tenant_id")
	if meta.Codec != CodecDictionary {
		b.Fatalf("tenant_id codec = %s, want dictionary", meta.Codec)
	}
	b.ReportAllocs()
	count, _, stats, err := CountInt64Equal(reader, "tenant_id", 7, nil)
	if err != nil {
		b.Fatalf("CountInt64Equal() warmup error = %v", err)
	}
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, _, _, err = CountInt64Equal(reader, "tenant_id", 7, nil)
		if err != nil {
			b.Fatalf("CountInt64Equal() error = %v", err)
		}
	}
	benchmarkCountResult = count
	if benchmarkCountResult != rows/10 {
		b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/10)
	}
}

func BenchmarkCountInt64EqualPlainReaderAt(b *testing.B) {
	const rows = 100_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = nonSequenceInt64Value(i)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)})
	reader, scratch := mustBenchmarkOpenReaderAt(b, batch)
	meta, _ := reader.Column("tenant_id")
	if meta.Codec != CodecPlain {
		b.Fatalf("tenant_id codec = %s, want plain", meta.Codec)
	}
	b.SetBytes(meta.Payload.Bytes)
	b.ReportAllocs()

	count, nextScratch, _, err := CountInt64Equal(reader, "tenant_id", values[rows/2], scratch)
	if err != nil {
		b.Fatalf("CountInt64Equal() warmup error = %v", err)
	}
	scratch = nextScratch
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, nextScratch, _, err = CountInt64Equal(reader, "tenant_id", values[rows/2], scratch)
		if err != nil {
			b.Fatalf("CountInt64Equal() error = %v", err)
		}
		scratch = nextScratch
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 1 {
		b.Fatalf("count = %d, want 1", benchmarkCountResult)
	}
}

func BenchmarkCountInt64EqualDictionaryReaderAt(b *testing.B) {
	const rows = 100_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i % 10)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)})
	reader, scratch := mustBenchmarkOpenReaderAt(b, batch)
	meta, _ := reader.Column("tenant_id")
	if meta.Codec != CodecDictionary {
		b.Fatalf("tenant_id codec = %s, want dictionary", meta.Codec)
	}
	b.ReportAllocs()

	count, nextScratch, stats, err := CountInt64Equal(reader, "tenant_id", 7, scratch)
	if err != nil {
		b.Fatalf("CountInt64Equal() warmup error = %v", err)
	}
	scratch = nextScratch
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, nextScratch, _, err = CountInt64Equal(reader, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatalf("CountInt64Equal() error = %v", err)
		}
		scratch = nextScratch
	}
	benchmarkCountResult = count
	if benchmarkCountResult != rows/10 {
		b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/10)
	}
}

func BenchmarkCountStringEqualPageBloomBytes(b *testing.B) {
	const rows = 100_000
	values := make([]string, rows)
	for i := range values {
		values[i] = pageBloomValue(i)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	reader := mustBenchmarkOpenBytes(b, batch)
	meta, _ := reader.Column("event_type")
	if meta.Codec != CodecPlain {
		b.Fatalf("event_type codec = %s, want plain", meta.Codec)
	}
	b.ReportAllocs()
	target := pageBloomValue(50_000)
	count, _, stats, err := CountStringEqual(reader, "event_type", target, nil)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, _, _, err = CountStringEqual(reader, "event_type", target, nil)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 1 {
		b.Fatalf("count = %d, want 1", benchmarkCountResult)
	}
}

func BenchmarkCountStringEqualDictionaryBytes(b *testing.B) {
	const rows = 100_000
	choices := []string{"signup", "checkout", "view", "cancel"}
	values := make([]string, rows)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	reader := mustBenchmarkOpenBytes(b, batch)
	meta, _ := reader.Column("event_type")
	if meta.Codec != CodecDictionary {
		b.Fatalf("event_type codec = %s, want dictionary", meta.Codec)
	}
	b.ReportAllocs()
	count, _, stats, err := CountStringEqual(reader, "event_type", "checkout", nil)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, _, _, err = CountStringEqual(reader, "event_type", "checkout", nil)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
	}
	benchmarkCountResult = count
	if benchmarkCountResult != rows/len(choices) {
		b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/len(choices))
	}
}

func BenchmarkCountStringEqualPageBloomReaderAt(b *testing.B) {
	const rows = 100_000
	values := make([]string, rows)
	for i := range values {
		values[i] = pageBloomValue(i)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	reader, scratch := mustBenchmarkOpenReaderAt(b, batch)
	meta, _ := reader.Column("event_type")
	if meta.Codec != CodecPlain {
		b.Fatalf("event_type codec = %s, want plain", meta.Codec)
	}
	b.ReportAllocs()

	target := pageBloomValue(50_000)
	count, nextScratch, stats, err := CountStringEqual(reader, "event_type", target, scratch)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	scratch = nextScratch
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, nextScratch, _, err = CountStringEqual(reader, "event_type", target, scratch)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
		scratch = nextScratch
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 1 {
		b.Fatalf("count = %d, want 1", benchmarkCountResult)
	}
}

func BenchmarkCountStringEqualDictionaryReaderAt(b *testing.B) {
	const rows = 100_000
	choices := []string{"signup", "checkout", "view", "cancel"}
	values := make([]string, rows)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	reader, scratch := mustBenchmarkOpenReaderAt(b, batch)
	meta, _ := reader.Column("event_type")
	if meta.Codec != CodecDictionary {
		b.Fatalf("event_type codec = %s, want dictionary", meta.Codec)
	}
	b.ReportAllocs()

	count, nextScratch, stats, err := CountStringEqual(reader, "event_type", "checkout", scratch)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	scratch = nextScratch
	b.SetBytes(stats.BytesScanned)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, nextScratch, _, err = CountStringEqual(reader, "event_type", "checkout", scratch)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
		scratch = nextScratch
	}
	benchmarkCountResult = count
	if benchmarkCountResult != rows/len(choices) {
		b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/len(choices))
	}
}

func mustBenchmarkBatch(tb testing.TB, columns ...vector.Column) vector.Batch {
	tb.Helper()
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		tb.Fatalf("NewBatch() error = %v", err)
	}
	return batch
}

func mustBenchmarkOpenBytes(tb testing.TB, batch vector.Batch) Reader {
	tb.Helper()
	data := mustBenchmarkSegmentBytes(tb, batch)
	reader, err := OpenSegmentBytes(data)
	if err != nil {
		tb.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	return reader
}

func mustBenchmarkOpenReaderAt(tb testing.TB, batch vector.Batch) (Reader, []byte) {
	tb.Helper()
	data := mustBenchmarkSegmentBytes(tb, batch)
	reader, scratch, err := OpenSegment(bytes.NewReader(data), 0, int64(len(data)), nil)
	if err != nil {
		tb.Fatalf("OpenSegment() error = %v", err)
	}
	return reader, scratch
}

func mustBenchmarkSegmentBytes(tb testing.TB, batch vector.Batch) []byte {
	tb.Helper()
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		tb.Fatalf("WriteSegment() error = %v", err)
	}
	return buf.Bytes()
}
