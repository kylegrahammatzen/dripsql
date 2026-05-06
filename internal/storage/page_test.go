package storage

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestPlainColumnPageDirectories(t *testing.T) {
	const rows = defaultPageRows + 3
	ids := make([]int64, rows)
	paths := make([]string, rows)
	for row := 0; row < rows; row++ {
		ids[row] = nonSequenceInt64Value(row)
		paths[row] = strconv.FormatUint(mix64(uint64(row+1)), 36)
	}
	batch := mustBatch(t,
		vector.Column{Name: "id", Vector: vector.FromInt64(ids)},
		vector.Column{Name: "path", Vector: vector.FromString(paths)},
	)
	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	assertColumnCodec(t, stats, "id", CodecPlain)
	assertColumnCodec(t, stats, "path", CodecPlain)

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	idMeta := mustReaderColumn(t, reader, "id")
	pathMeta := mustReaderColumn(t, reader, "path")
	if idMeta.Pages.Bytes == 0 {
		t.Fatalf("id page directory is missing")
	}
	if pathMeta.Pages.Bytes == 0 {
		t.Fatalf("path page directory is missing")
	}

	idPages, scratch, err := reader.ReadPages("id", nil)
	if err != nil {
		t.Fatalf("ReadPages(id) error = %v", err)
	}
	if len(idPages) != 2 {
		t.Fatalf("id pages = %d, want 2", len(idPages))
	}
	assertPage(t, idPages[0], Page{StartRow: 0, Count: defaultPageRows, Payload: Range{Offset: idMeta.Payload.Offset, Bytes: int64(defaultPageRows * 8)}, HasMinMax: true, MinInt64: ids[0], MaxInt64: ids[defaultPageRows-1]})
	assertPage(t, idPages[1], Page{StartRow: defaultPageRows, Count: 3, Payload: Range{Offset: idMeta.Payload.Offset + int64(defaultPageRows*8), Bytes: 3 * 8}, HasMinMax: true, MinInt64: ids[defaultPageRows], MaxInt64: ids[rows-1]})

	pathPages, scratch, err := reader.ReadPages("path", scratch)
	if err != nil {
		t.Fatalf("ReadPages(path) error = %v", err)
	}
	if len(pathPages) != 2 {
		t.Fatalf("path pages = %d, want 2", len(pathPages))
	}
	offsetBytes := int64((rows + 1) * stringOffsetWidth)
	pathPayload := buf.Bytes()[pathMeta.Payload.Offset : pathMeta.Payload.Offset+pathMeta.Payload.Bytes]
	firstPageDataStart := int64(binary.LittleEndian.Uint32(pathPayload[0:]))
	firstPageDataEnd := int64(binary.LittleEndian.Uint32(pathPayload[defaultPageRows*stringOffsetWidth:]))
	secondPageDataStart := firstPageDataEnd
	secondPageDataEnd := int64(binary.LittleEndian.Uint32(pathPayload[rows*stringOffsetWidth:]))
	assertPage(t, pathPages[0], Page{
		StartRow: 0,
		Count:    defaultPageRows,
		Payload:  Range{Offset: pathMeta.Payload.Offset, Bytes: int64((defaultPageRows + 1) * stringOffsetWidth)},
		Values:   Range{Offset: pathMeta.Payload.Offset + offsetBytes + firstPageDataStart, Bytes: firstPageDataEnd - firstPageDataStart},
	})
	assertPage(t, pathPages[1], Page{
		StartRow: defaultPageRows,
		Count:    3,
		Payload:  Range{Offset: pathMeta.Payload.Offset + int64(defaultPageRows*stringOffsetWidth), Bytes: 4 * stringOffsetWidth},
		Values:   Range{Offset: pathMeta.Payload.Offset + offsetBytes + secondPageDataStart, Bytes: secondPageDataEnd - secondPageDataStart},
	})

	got, _, err := reader.ReadBatch(scratch)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	assertStrings(t, mustColumn(t, got, "path").Vector.(vector.String), paths)
}

func TestDictionaryColumnsDoNotWritePageDirectories(t *testing.T) {
	const rows = defaultPageRows + 3
	ids := make([]int64, rows)
	events := make([]string, rows)
	choices := []string{"signup", "checkout", "view"}
	for row := 0; row < rows; row++ {
		ids[row] = int64(row % len(choices))
		events[row] = choices[row%len(choices)]
	}
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(ids)},
		vector.Column{Name: "event_type", Vector: vector.FromString(events)},
	)
	reader := mustOpenBytes(t, batch)
	tenantMeta := mustReaderColumn(t, reader, "tenant_id")
	eventMeta := mustReaderColumn(t, reader, "event_type")
	if tenantMeta.Codec != CodecDictionary || tenantMeta.Pages != (Range{}) {
		t.Fatalf("tenant metadata = %+v, want dictionary with no pages", tenantMeta)
	}
	if eventMeta.Codec != CodecDictionary || eventMeta.Pages != (Range{}) {
		t.Fatalf("event metadata = %+v, want dictionary with no pages", eventMeta)
	}
	pages, _, err := reader.ReadPages("tenant_id", nil)
	if err != nil {
		t.Fatalf("ReadPages(tenant_id) error = %v", err)
	}
	if pages != nil {
		t.Fatalf("tenant pages = %+v, want nil", pages)
	}
}

func TestReadPagesRejectsCorruption(t *testing.T) {
	const rows = defaultPageRows + 1
	ids := make([]int64, rows)
	for row := range ids {
		ids[row] = nonSequenceInt64Value(row)
	}
	batch := mustBatch(t, vector.Column{Name: "id", Vector: vector.FromInt64(ids)})
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	meta := mustReaderColumn(t, reader, "id")
	data := append([]byte(nil), buf.Bytes()...)
	pageDir := data[meta.Pages.Offset : meta.Pages.Offset+meta.Pages.Bytes]
	binary.LittleEndian.PutUint64(pageDir[4+8:], uint64(rows+1))
	reader, err = OpenSegmentBytes(data)
	if err != nil {
		t.Fatalf("OpenSegmentBytes(corrupt page dir) error = %v", err)
	}
	if _, _, err := reader.ReadPages("id", nil); err == nil {
		t.Fatalf("ReadPages() error = nil, want corrupt page directory error")
	}
}

func nonSequenceInt64Value(row int) int64 {
	return int64(row*3 + row%2)
}

func assertPage(t *testing.T, got Page, want Page) {
	t.Helper()
	if got != want {
		t.Fatalf("page = %+v, want %+v", got, want)
	}
}
