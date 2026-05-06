package storage

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestStringTemplateCodecRoundTripAndCounts(t *testing.T) {
	const rows = 5000
	values := make([]string, rows)
	for row := range values {
		values[row] = templatePayloadValue(row)
	}
	batch := mustBatch(t, vector.Column{Name: "payload", Vector: vector.FromString(values)})

	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	assertColumnCodec(t, stats, "payload", CodecStringTemplate)
	payloadMeta, _ := stats.Column("payload")
	plainLen, err := stringPlainPayloadLen(vector.FromString(values))
	if err != nil {
		t.Fatalf("stringPlainPayloadLen() error = %v", err)
	}
	if payloadMeta.Payload.Bytes >= int64(plainLen) {
		t.Fatalf("template payload bytes = %d, want less than plain %d", payloadMeta.Payload.Bytes, plainLen)
	}

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	got, _, err := reader.ReadBatch(nil)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	assertStrings(t, mustColumn(t, got, "payload").Vector.(vector.String), values)

	count, scratch, scanStats, err := CountStringEqual(reader, "payload", values[1234], nil)
	if err != nil {
		t.Fatalf("CountStringEqual(present) error = %v", err)
	}
	if count != 1 {
		t.Fatalf("present count = %d, want 1", count)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	assertScanStats(t, scanStats, ScanStats{BytesScanned: payloadMeta.Payload.Bytes, RowsScanned: rows, SegmentsScanned: 1})

	count, _, scanStats, err = CountStringEqual(reader, "payload", "payload-missing", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual(missing shape) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("missing shape count = %d, want 0", count)
	}
	assertScanStats(t, scanStats, ScanStats{BytesScanned: payloadMeta.Payload.Bytes, RowsSkipped: rows, SegmentsScanned: 1})
}

func TestGroupStringCountsTemplate(t *testing.T) {
	const rows = 1024
	values := make([]string, rows)
	for row := range values {
		values[row] = templatePayloadValue(row)
	}
	batch := mustBatch(t, vector.Column{Name: "payload", Vector: vector.FromString(values)})
	reader := mustOpenBytes(t, batch)
	meta := mustReaderColumn(t, reader, "payload")
	if meta.Codec != CodecStringTemplate {
		t.Fatalf("payload codec = %s, want template", meta.Codec)
	}

	counts, scratch, keyScratch, stats, err := GroupStringCounts(reader, "payload", nil, nil, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts() error = %v", err)
	}
	if scratch != nil || keyScratch != nil {
		t.Fatalf("scratch/keyScratch = %v/%v, want nil byte-view scratch", scratch, keyScratch)
	}
	if len(counts) != rows {
		t.Fatalf("counts len = %d, want %d", len(counts), rows)
	}
	if counts[values[123]] != 1 {
		t.Fatalf("counts[%q] = %d, want 1", values[123], counts[values[123]])
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: meta.Payload.Bytes, RowsScanned: rows, SegmentsScanned: 1})
}

func templatePayloadValue(row int) string {
	return "id=" + strconv.FormatInt(int64(row%10_000_000), 36) + ";bucket=" + strconv.Itoa(row%97)
}
