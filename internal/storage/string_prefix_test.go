package storage

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestStringPrefixCodecRoundTripAndCounts(t *testing.T) {
	tests := []struct {
		name    string
		value   func(int) string
		missing string
	}{
		{
			name: "prefix_only",
			value: func(row int) string {
				return "/item/" + strconv.FormatInt(int64(row*17+3), 36)
			},
			missing: "missing/123",
		},
		{
			name: "prefix_and_suffix",
			value: func(row int) string {
				return "u" + strconv.FormatInt(int64(row*17+3), 36) + "@drip.test"
			},
			missing: "u123@example.test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const rows = 2048
			values := make([]string, rows)
			for row := range values {
				values[row] = tt.value(row)
			}
			batch := mustBatch(t, vector.Column{Name: "value", Vector: vector.FromString(values)})

			var buf bytes.Buffer
			stats, err := WriteSegment(&buf, batch)
			if err != nil {
				t.Fatalf("WriteSegment() error = %v", err)
			}
			assertColumnCodec(t, stats, "value", CodecStringPrefix)
			meta, _ := stats.Column("value")
			plainLen, err := stringPlainPayloadLen(vector.FromString(values))
			if err != nil {
				t.Fatalf("stringPlainPayloadLen() error = %v", err)
			}
			if meta.Payload.Bytes >= int64(plainLen) {
				t.Fatalf("prefix payload bytes = %d, want less than plain %d", meta.Payload.Bytes, plainLen)
			}

			reader, err := OpenSegmentBytes(buf.Bytes())
			if err != nil {
				t.Fatalf("OpenSegmentBytes() error = %v", err)
			}
			got, _, err := reader.ReadBatch(nil)
			if err != nil {
				t.Fatalf("ReadBatch() error = %v", err)
			}
			assertStrings(t, mustColumn(t, got, "value").Vector.(vector.String), values)

			count, scratch, scanStats, err := CountStringEqual(reader, "value", values[123], nil)
			if err != nil {
				t.Fatalf("CountStringEqual(present) error = %v", err)
			}
			if count != 1 {
				t.Fatalf("present count = %d, want 1", count)
			}
			if scratch != nil {
				t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
			}
			assertScanStats(t, scanStats, ScanStats{BytesScanned: meta.Payload.Bytes, RowsScanned: rows, SegmentsScanned: 1})

			count, _, scanStats, err = CountStringEqual(reader, "value", tt.missing, scratch)
			if err != nil {
				t.Fatalf("CountStringEqual(missing) error = %v", err)
			}
			if count != 0 {
				t.Fatalf("missing count = %d, want 0", count)
			}
			assertScanStats(t, scanStats, ScanStats{BytesScanned: meta.Payload.Bytes, RowsSkipped: rows, SegmentsScanned: 1})
		})
	}
}

func TestGroupStringCountsPrefix(t *testing.T) {
	const rows = 2048
	values := make([]string, rows)
	for row := range values {
		values[row] = "/item/" + strconv.FormatInt(int64(row), 36)
	}
	batch := mustBatch(t, vector.Column{Name: "url", Vector: vector.FromString(values)})
	reader := mustOpenBytes(t, batch)
	meta := mustReaderColumn(t, reader, "url")
	if meta.Codec != CodecStringPrefix {
		t.Fatalf("url codec = %s, want prefix", meta.Codec)
	}

	counts, scratch, keyScratch, stats, err := GroupStringCounts(reader, "url", nil, nil, nil)
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
