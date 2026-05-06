package storage

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestPageBloomStringCounts(t *testing.T) {
	const rows = defaultPageRows * 2
	batch := mustBatch(t, vector.Column{Name: "url", Vector: pageBloomStringValues(rows)})
	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	urlStats, ok := stats.Column("url")
	if !ok {
		t.Fatalf("missing url column")
	}
	if urlStats.Codec != CodecPlain {
		t.Fatalf("url codec = %s, want plain", urlStats.Codec)
	}
	if urlStats.Pages.Bytes == 0 {
		t.Fatalf("url page directory is missing")
	}
	if urlStats.Filters.Bytes == 0 {
		t.Fatalf("url page bloom is missing")
	}

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	target, count, _, presentStats, err := pageBloomTargetWithOneScannedPage(reader, rows)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count(%q) = %d, want 1", target, count)
	}
	if presentStats.PagesScanned != 1 {
		t.Fatalf("present pages scanned = %d, want 1", presentStats.PagesScanned)
	}
	if presentStats.RowsScanned != defaultPageRows || presentStats.RowsSkipped != defaultPageRows {
		t.Fatalf("present stats = %+v, want one page scanned and one skipped", presentStats)
	}
	if presentStats.BytesRead != 0 {
		t.Fatalf("present bytes read = %d, want 0 for byte view", presentStats.BytesRead)
	}
	if presentStats.BytesScanned >= urlStats.Payload.Bytes {
		t.Fatalf("present bytes scanned = %d, want less than full payload %d", presentStats.BytesScanned, urlStats.Payload.Bytes)
	}

	absent, absentStats, err := pageBloomAbsentWithNoScannedPages(reader)
	if err != nil {
		t.Fatal(err)
	}
	if absentStats.PagesScanned != 0 || absentStats.RowsSkipped != rows {
		t.Fatalf("absent stats for %q = %+v, want all rows skipped", absent, absentStats)
	}
}

func TestPageBloomReaderAtReadsMetadataOnlyWhenAbsent(t *testing.T) {
	const rows = defaultPageRows * 2
	batch := mustBatch(t, vector.Column{Name: "url", Vector: pageBloomStringValues(rows)})
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	byteReader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	absent, _, err := pageBloomAbsentWithNoScannedPages(byteReader)
	if err != nil {
		t.Fatal(err)
	}

	source := &recordingReaderAt{data: buf.Bytes()}
	reader, scratch, err := OpenSegment(source, 0, int64(buf.Len()), nil)
	if err != nil {
		t.Fatalf("OpenSegment() error = %v", err)
	}
	meta := mustReaderColumn(t, reader, "url")
	baseReads := len(source.calls)
	count, _, stats, err := CountStringEqual(reader, "url", absent, scratch)
	if err != nil {
		t.Fatalf("CountStringEqual(absent) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent count = %d, want 0", count)
	}
	if stats.PagesScanned != 0 || stats.RowsSkipped != rows {
		t.Fatalf("stats = %+v, want all rows skipped", stats)
	}
	if stats.BytesRead != meta.Pages.Bytes+meta.Filters.Bytes {
		t.Fatalf("bytes read = %d, want metadata bytes %d", stats.BytesRead, meta.Pages.Bytes+meta.Filters.Bytes)
	}
	if got, want := len(source.calls), baseReads+1; got != want {
		t.Fatalf("reads after count = %d, want %d", got-baseReads, 1)
	}
	assertReadAtCall(t, source.calls[baseReads], Range{Offset: meta.Pages.Offset, Bytes: meta.Pages.Bytes + meta.Filters.Bytes})
}

func TestPageBloomSkipsLongStringColumns(t *testing.T) {
	const rows = defaultPageRows * 2
	values := make([]string, rows)
	for row := range values {
		values[row] = strconv.FormatInt(int64(row), 36) + ":payload-value-that-is-wide:" + strconv.FormatInt(int64(row*row+7), 36)
	}
	batch := mustBatch(t, vector.Column{Name: "payload", Vector: vector.FromString(values)})
	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	payloadStats, ok := stats.Column("payload")
	if !ok {
		t.Fatalf("missing payload column")
	}
	if payloadStats.Codec != CodecPlain && payloadStats.Codec != CodecStringTemplate {
		t.Fatalf("payload codec = %s, want plain or template", payloadStats.Codec)
	}
	if payloadStats.Codec == CodecPlain && payloadStats.Pages.Bytes == 0 {
		t.Fatalf("plain payload page directory is missing")
	}
	if payloadStats.Filters.Bytes != 0 {
		t.Fatalf("payload page bloom bytes = %d, want 0", payloadStats.Filters.Bytes)
	}

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	count, _, scanStats, err := CountStringEqual(reader, "payload", values[1234], nil)
	if err != nil {
		t.Fatalf("CountStringEqual() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	assertScanStats(t, scanStats, ScanStats{BytesScanned: payloadStats.Payload.Bytes, RowsScanned: rows, SegmentsScanned: 1})
}

func pageBloomStringValues(rows int) vector.String {
	values := make([]string, rows)
	for row := range values {
		values[row] = pageBloomValue(row)
	}
	return vector.FromString(values)
}

func pageBloomValue(row int) string {
	return strconv.FormatUint(mix64(uint64(row+1)), 36)
}

func pageBloomTargetWithOneScannedPage(reader Reader, rows int) (string, int, []byte, ScanStats, error) {
	var scratch []byte
	for row := 0; row < rows; row++ {
		value := pageBloomValue(row)
		count, nextScratch, stats, err := CountStringEqual(reader, "url", value, scratch)
		if err != nil {
			return "", 0, scratch, ScanStats{}, err
		}
		scratch = nextScratch
		if count == 1 && stats.PagesScanned == 1 {
			return value, count, scratch, stats, nil
		}
	}
	return "", 0, scratch, ScanStats{}, fmt.Errorf("no present page-bloom target scanned exactly one page")
}

func pageBloomAbsentWithNoScannedPages(reader Reader) (string, ScanStats, error) {
	var scratch []byte
	for i := 0; i < 1024; i++ {
		value := "/absent/" + strconv.Itoa(i)
		count, nextScratch, stats, err := CountStringEqual(reader, "url", value, scratch)
		if err != nil {
			return "", ScanStats{}, err
		}
		scratch = nextScratch
		if count == 0 && stats.PagesScanned == 0 {
			return value, stats, nil
		}
	}
	return "", ScanStats{}, fmt.Errorf("no absent page-bloom target skipped every page")
}

func BenchmarkCountStringEqualPageBloomAbsentBytes(b *testing.B) {
	const rows = 100_000
	batch := mustBenchmarkBatch(b, vector.Column{Name: "url", Vector: pageBloomStringValues(rows)})
	reader := mustBenchmarkOpenBytes(b, batch)
	target, _, err := pageBloomAbsentWithNoScannedPages(reader)
	if err != nil {
		b.Fatal(err)
	}
	count, _, stats, err := CountStringEqual(reader, "url", target, nil)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	b.SetBytes(stats.BytesScanned)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, _, _, err = CountStringEqual(reader, "url", target, nil)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 0 {
		b.Fatalf("count = %d, want 0", benchmarkCountResult)
	}
}

func BenchmarkCountStringEqualPageBloomAbsentReaderAt(b *testing.B) {
	const rows = 100_000
	batch := mustBenchmarkBatch(b, vector.Column{Name: "url", Vector: pageBloomStringValues(rows)})
	byteReader := mustBenchmarkOpenBytes(b, batch)
	target, _, err := pageBloomAbsentWithNoScannedPages(byteReader)
	if err != nil {
		b.Fatal(err)
	}
	reader, scratch := mustBenchmarkOpenReaderAt(b, batch)
	count, nextScratch, stats, err := CountStringEqual(reader, "url", target, scratch)
	if err != nil {
		b.Fatalf("CountStringEqual() warmup error = %v", err)
	}
	scratch = nextScratch
	b.SetBytes(stats.BytesScanned)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, nextScratch, _, err = CountStringEqual(reader, "url", target, scratch)
		if err != nil {
			b.Fatalf("CountStringEqual() error = %v", err)
		}
		scratch = nextScratch
	}
	benchmarkCountResult = count
	if benchmarkCountResult != 0 {
		b.Fatalf("count = %d, want 0", benchmarkCountResult)
	}
}
