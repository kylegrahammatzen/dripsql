package storage

import (
	"bytes"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestDictionaryRoundTripAndCounts(t *testing.T) {
	const rows = 1024
	tenantValues := make([]int64, rows)
	eventValues := make([]string, rows)
	eventChoices := []string{"signup", "checkout", "view"}
	for row := 0; row < rows; row++ {
		tenantValues[row] = int64((row % 3) * 2)
		eventValues[row] = eventChoices[row%len(eventChoices)]
	}
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(tenantValues)},
		vector.Column{Name: "event_type", Vector: vector.FromString(eventValues)},
	)
	var buf bytes.Buffer
	writeStats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	assertColumnCodec(t, writeStats, "tenant_id", CodecDictionary)
	assertColumnCodec(t, writeStats, "event_type", CodecDictionary)

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	got, _, err := reader.ReadBatch(nil)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	gotTenant := mustColumn(t, got, "tenant_id").Vector.(vector.Int64)
	if !slices.Equal(gotTenant.Values, tenantValues) {
		t.Fatalf("tenant values do not round-trip")
	}
	assertStrings(t, mustColumn(t, got, "event_type").Vector.(vector.String), eventValues)

	tenantMeta := mustReaderColumn(t, reader, "tenant_id")
	tenantMetaLen := mustInt64DictionaryMetadataLen(t, 3)
	if tenantMeta.Payload.Bytes <= int64(tenantMetaLen) {
		t.Fatalf("tenant payload bytes = %d, want greater than metadata %d", tenantMeta.Payload.Bytes, tenantMetaLen)
	}
	count, scratch, stats, err := CountInt64Equal(reader, "tenant_id", 2, nil)
	if err != nil {
		t.Fatalf("CountInt64Equal() error = %v", err)
	}
	if count != 341 {
		t.Fatalf("tenant count = %d, want 341", count)
	}
	if scratch != nil {
		t.Fatalf("scratch = %v, want nil for byte-view path", scratch)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: int64(tenantMetaLen), RowsSkipped: rows, SegmentsScanned: 1})

	count, _, stats, err = CountInt64Equal(reader, "tenant_id", 1, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal(absent dictionary) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent tenant count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: int64(tenantMetaLen), RowsSkipped: rows, SegmentsScanned: 1})

	eventMeta := mustReaderColumn(t, reader, "event_type")
	eventMetaLen := mustStringDictionaryMetadataLen(t, eventChoices)
	if eventMeta.Payload.Bytes <= int64(eventMetaLen) {
		t.Fatalf("event payload bytes = %d, want greater than metadata %d", eventMeta.Payload.Bytes, eventMetaLen)
	}
	count, scratch, stats, err = CountStringEqual(reader, "event_type", "checkout", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual() error = %v", err)
	}
	if count != 341 {
		t.Fatalf("event count = %d, want 341", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: int64(eventMetaLen), RowsSkipped: rows, SegmentsScanned: 1})

	count, _, stats, err = CountStringEqual(reader, "event_type", "missing", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual(absent dictionary) error = %v", err)
	}
	if count != 0 {
		t.Fatalf("absent event count = %d, want 0", count)
	}
	assertScanStats(t, stats, ScanStats{BytesScanned: int64(eventMetaLen), RowsSkipped: rows, SegmentsScanned: 1})
}

func TestDictionaryReaderAtReadsMetadataOnly(t *testing.T) {
	const rows = 1024
	tenantValues := make([]int64, rows)
	eventValues := make([]string, rows)
	eventChoices := []string{"signup", "checkout", "view"}
	for row := 0; row < rows; row++ {
		tenantValues[row] = int64((row % 3) * 2)
		eventValues[row] = eventChoices[row%len(eventChoices)]
	}
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(tenantValues)},
		vector.Column{Name: "event_type", Vector: vector.FromString(eventValues)},
	)
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	source := &recordingReaderAt{data: buf.Bytes()}
	reader, scratch, err := OpenSegment(source, 0, int64(buf.Len()), nil)
	if err != nil {
		t.Fatalf("OpenSegment() error = %v", err)
	}
	baseReads := len(source.calls)

	tenantMeta := mustReaderColumn(t, reader, "tenant_id")
	tenantMetaLen := mustInt64DictionaryMetadataLen(t, 3)
	count, nextScratch, stats, err := CountInt64Equal(reader, "tenant_id", 2, scratch)
	if err != nil {
		t.Fatalf("CountInt64Equal() error = %v", err)
	}
	scratch = nextScratch
	if count != 341 {
		t.Fatalf("tenant count = %d, want 341", count)
	}
	assertScanStats(t, stats, ScanStats{BytesRead: int64(tenantMetaLen), BytesScanned: int64(tenantMetaLen), RowsSkipped: rows, SegmentsScanned: 1})
	if got, want := len(source.calls), baseReads+1; got != want {
		t.Fatalf("int dictionary metadata reads = %d, want %d", got-baseReads, 1)
	}
	assertReadAtCall(t, source.calls[baseReads], tenantMeta.Dictionary)

	eventMeta := mustReaderColumn(t, reader, "event_type")
	eventMetaLen := mustStringDictionaryMetadataLen(t, eventChoices)
	count, _, stats, err = CountStringEqual(reader, "event_type", "checkout", scratch)
	if err != nil {
		t.Fatalf("CountStringEqual() error = %v", err)
	}
	if count != 341 {
		t.Fatalf("event count = %d, want 341", count)
	}
	assertScanStats(t, stats, ScanStats{BytesRead: int64(eventMetaLen), BytesScanned: int64(eventMetaLen), RowsSkipped: rows, SegmentsScanned: 1})
	if got, want := len(source.calls), baseReads+2; got != want {
		t.Fatalf("dictionary metadata reads = %d, want %d", got-baseReads, 2)
	}
	assertReadAtCall(t, source.calls[baseReads+1], eventMeta.Dictionary)
}

func assertColumnCodec(t *testing.T, stats SegmentStats, name string, codec Codec) {
	t.Helper()
	col, ok := stats.Column(name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	if col.Codec != codec {
		t.Fatalf("column %q codec = %s, want %s", name, col.Codec, codec)
	}
}

func mustInt64DictionaryMetadataLen(t *testing.T, dictCount int) int {
	t.Helper()
	n, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		t.Fatalf("int64DictionaryMetadataLen() error = %v", err)
	}
	return n
}

func mustStringDictionaryMetadataLen(t *testing.T, values []string) int {
	t.Helper()
	dataLen := 0
	var err error
	for _, value := range values {
		dataLen, err = checkedAddInt("test string dictionary data length", dataLen, len(value))
		if err != nil {
			t.Fatalf("checkedAddInt() error = %v", err)
		}
	}
	sectionLen, err := stringDictionarySectionLen(len(values), dataLen)
	if err != nil {
		t.Fatalf("stringDictionarySectionLen() error = %v", err)
	}
	n, err := stringDictionaryMetadataLen(sectionLen, len(values))
	if err != nil {
		t.Fatalf("stringDictionaryMetadataLen() error = %v", err)
	}
	return n
}
