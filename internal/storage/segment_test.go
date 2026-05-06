package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestWriteOpenSegmentBytesRoundTrip(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{42, -7, 42, 99})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "", "checkout", "signup"})},
	)

	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	if stats.Rows != batch.Count || len(stats.Columns) != 2 {
		t.Fatalf("stats = %+v, want %d rows and 2 columns", stats, batch.Count)
	}

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	if reader.Directory().Rows != batch.Count {
		t.Fatalf("directory rows = %d, want %d", reader.Directory().Rows, batch.Count)
	}

	got, _, err := reader.ReadBatch(nil)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	tenantCol := mustColumn(t, got, "tenant_id")
	gotTenant, ok := tenantCol.Vector.(vector.Int64)
	if !ok {
		t.Fatalf("tenant_id type = %T, want vector.Int64", tenantCol.Vector)
	}
	if !slices.Equal(gotTenant.Values, []int64{42, -7, 42, 99}) {
		t.Fatalf("tenant_id = %v", gotTenant.Values)
	}
	eventCol := mustColumn(t, got, "event_type")
	gotEvent, ok := eventCol.Vector.(vector.String)
	if !ok {
		t.Fatalf("event_type type = %T, want vector.String", eventCol.Vector)
	}
	assertStrings(t, gotEvent, []string{"signup", "", "checkout", "signup"})
}

func TestInt64SequenceCodecRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{name: "ascending", values: []int64{10, 13, 16, 19, 22}},
		{name: "descending", values: []int64{10, 7, 4, 1, -2}},
		{name: "constant", values: []int64{42, 42, 42, 42, 42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := mustBatch(t, vector.Column{Name: "id", Vector: vector.NewInt64(tt.values)})
			var buf bytes.Buffer
			stats, err := WriteSegment(&buf, batch)
			if err != nil {
				t.Fatalf("WriteSegment() error = %v", err)
			}
			assertColumnCodec(t, stats, "id", CodecInt64Sequence)
			idStats, _ := stats.Column("id")
			if idStats.Payload.Bytes != int64SequencePayloadLen {
				t.Fatalf("sequence payload bytes = %d, want %d", idStats.Payload.Bytes, int64SequencePayloadLen)
			}

			reader, err := OpenSegmentBytes(buf.Bytes())
			if err != nil {
				t.Fatalf("OpenSegmentBytes() error = %v", err)
			}
			got, _, err := reader.ReadColumn("id", nil)
			if err != nil {
				t.Fatalf("ReadColumn() error = %v", err)
			}
			gotValues := got.Vector.(vector.Int64).Values
			if !slices.Equal(gotValues, tt.values) {
				t.Fatalf("decoded values = %v, want %v", gotValues, tt.values)
			}
		})
	}
}

func TestWriteSegmentWithCompactStringVector(t *testing.T) {
	strings, err := vector.NewStringData([]byte("alphabetagamma"), []uint64{
		vector.StringRange(0, 5),
		vector.StringRange(5, 4),
		vector.StringRange(9, 5),
	})
	if err != nil {
		t.Fatalf("NewStringData() error = %v", err)
	}
	batch := mustBatch(t, vector.Column{Name: "word", Vector: strings})

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	col, _, err := reader.ReadColumn("word", nil)
	if err != nil {
		t.Fatalf("ReadColumn() error = %v", err)
	}
	got, ok := col.Vector.(vector.String)
	if !ok {
		t.Fatalf("word type = %T, want vector.String", col.Vector)
	}
	assertStrings(t, got, []string{"alpha", "beta", "gamma"})
}

func TestReaderAtSelectedColumnReadsOnlyPayload(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})},
		vector.Column{Name: "path", Vector: vector.NewString([]string{"/a", "/b", "/c"})},
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
		tenantMeta := mustReaderColumn(t, reader, "tenant_id")
		pathMeta := mustReaderColumn(t, reader, "path")
		baseReads := len(source.calls)

		tenantCol, nextScratch, err := reader.ReadColumn("tenant_id", scratch)
		if err != nil {
			t.Fatalf("ReadColumn(tenant_id) error = %v", err)
		}
		scratch = nextScratch
		gotTenant, ok := tenantCol.Vector.(vector.Int64)
		if !ok {
			t.Fatalf("tenant_id type = %T, want vector.Int64", tenantCol.Vector)
		}
		if !slices.Equal(gotTenant.Values, []int64{1, 2, 3}) {
			t.Fatalf("tenant_id = %v", gotTenant.Values)
		}

		pathCol, nextScratch, err := reader.ReadColumn("path", scratch)
		if err != nil {
			t.Fatalf("ReadColumn(path) error = %v", err)
		}
		_ = nextScratch
		assertStrings(t, pathCol.Vector.(vector.String), []string{"/a", "/b", "/c"})

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
		tenantMeta := mustReaderColumn(t, reader, "tenant_id")
		pathMeta := mustReaderColumn(t, reader, "path")
		baseReadAt := len(source.readAtCalls)
		baseViews := len(source.viewCalls)

		tenantCol, nextScratch, err := reader.ReadColumn("tenant_id", scratch)
		if err != nil {
			t.Fatalf("ReadColumn(tenant_id) error = %v", err)
		}
		scratch = nextScratch
		gotTenant, ok := tenantCol.Vector.(vector.Int64)
		if !ok {
			t.Fatalf("tenant_id type = %T, want vector.Int64", tenantCol.Vector)
		}
		if !slices.Equal(gotTenant.Values, []int64{1, 2, 3}) {
			t.Fatalf("tenant_id = %v", gotTenant.Values)
		}

		pathCol, nextScratch, err := reader.ReadColumn("path", scratch)
		if err != nil {
			t.Fatalf("ReadColumn(path) error = %v", err)
		}
		_ = nextScratch
		assertStrings(t, pathCol.Vector.(vector.String), []string{"/a", "/b", "/c"})

		if got := len(source.readAtCalls) - baseReadAt; got != 0 {
			t.Fatalf("ReadAt payload calls = %d, want 0", got)
		}
		if got, want := len(source.viewCalls), baseViews+2; got != want {
			t.Fatalf("View payload calls = %d, want %d", got-baseViews, 2)
		}
		assertReadAtCall(t, source.viewCalls[baseViews], tenantMeta.Payload)
		assertReadAtCall(t, source.viewCalls[baseViews+1], pathMeta.Payload)
	})
}

func TestByteViewerFallsBackToReaderAt(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})},
		vector.Column{Name: "path", Vector: vector.NewString([]string{"/a", "/b", "/c"})},
	)
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	source := &fallbackByteViewer{data: buf.Bytes()}
	reader, scratch, err := OpenSegment(source, 0, int64(buf.Len()), nil)
	if err != nil {
		t.Fatalf("OpenSegment() error = %v", err)
	}
	pathMeta := mustReaderColumn(t, reader, "path")
	baseReads := len(source.readAtCalls)
	baseViews := len(source.viewCalls)

	pathCol, _, err := reader.ReadColumn("path", scratch)
	if err != nil {
		t.Fatalf("ReadColumn(path) error = %v", err)
	}
	assertStrings(t, pathCol.Vector.(vector.String), []string{"/a", "/b", "/c"})
	if got := len(source.viewCalls) - baseViews; got != 1 {
		t.Fatalf("View payload calls = %d, want 1", got)
	}
	if got := len(source.readAtCalls) - baseReads; got != 1 {
		t.Fatalf("ReadAt payload calls = %d, want 1", got)
	}
	assertReadAtCall(t, source.viewCalls[baseViews], pathMeta.Payload)
	assertReadAtCall(t, source.readAtCalls[baseReads], pathMeta.Payload)
}

func TestZeroRowSegmentRoundTrip(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "id", Vector: vector.NewInt64(nil)},
		vector.Column{Name: "name", Vector: vector.NewString(nil)},
	)

	var buf bytes.Buffer
	stats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	if stats.Rows != 0 {
		t.Fatalf("stats.Rows = %d, want 0", stats.Rows)
	}

	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	got, _, err := reader.ReadBatch(nil)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	if got.Count != 0 {
		t.Fatalf("batch.Count = %d, want 0", got.Count)
	}
	if mustColumn(t, got, "id").Vector.Len() != 0 {
		t.Fatalf("id len = %d, want 0", mustColumn(t, got, "id").Vector.Len())
	}
	if mustColumn(t, got, "name").Vector.Len() != 0 {
		t.Fatalf("name len = %d, want 0", mustColumn(t, got, "name").Vector.Len())
	}
}

func TestOpenSegmentRejectsFooterCorruption(t *testing.T) {
	batch := mustBatch(t, vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})})
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	tests := []struct {
		name    string
		corrupt func([]byte)
	}{
		{
			name: "footer_body",
			corrupt: func(data []byte) {
				data[len(data)-footerTailLen-1] ^= 0xff
			},
		},
		{
			name: "trailer_checksum",
			corrupt: func(data []byte) {
				data[len(data)-footerTailLen+8] ^= 0xff
			},
		},
		{
			name: "trailer_magic",
			corrupt: func(data []byte) {
				data[len(data)-1] ^= 0xff
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte(nil), buf.Bytes()...)
			tt.corrupt(data)
			if _, err := OpenSegmentBytes(data); err == nil {
				t.Fatalf("OpenSegmentBytes() error = nil, want corruption error")
			}
		})
	}
}

func TestOpenSegmentRejectsHeaderAndSizeCorruption(t *testing.T) {
	batch := mustBatch(t, vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})})
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	tests := []struct {
		name    string
		data    func() []byte
		openLen int
	}{
		{
			name: "segment_magic",
			data: func() []byte {
				data := append([]byte(nil), buf.Bytes()...)
				data[0] ^= 0xff
				return data
			},
		},
		{
			name: "segment_version",
			data: func() []byte {
				data := append([]byte(nil), buf.Bytes()...)
				binary.LittleEndian.PutUint16(data[len(segmentMagic):], formatVersion+1)
				return data
			},
		},
		{
			name: "header_segment_length",
			data: func() []byte {
				data := append([]byte(nil), buf.Bytes()...)
				segmentLenOffset := len(segmentMagic) + 2 + 8 + 4
				binary.LittleEndian.PutUint64(data[segmentLenOffset:], uint64(len(data)+1))
				return data
			},
		},
		{
			name: "truncated_segment",
			data: func() []byte {
				return append([]byte(nil), buf.Bytes()...)
			},
			openLen: buf.Len() - 1,
		},
		{
			name: "footer_length_too_large",
			data: func() []byte {
				data := append([]byte(nil), buf.Bytes()...)
				binary.LittleEndian.PutUint64(data[len(data)-footerTailLen:], uint64(len(data)))
				return data
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.data()
			openLen := tt.openLen
			if openLen == 0 {
				openLen = len(data)
			}
			if _, _, err := OpenSegment(bytes.NewReader(data), 0, int64(openLen), nil); err == nil {
				t.Fatalf("OpenSegment() error = nil, want corruption error")
			}
		})
	}
}

func TestReadColumnRejectsMissingColumn(t *testing.T) {
	batch := mustBatch(t, vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1})})
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	reader, err := OpenSegmentBytes(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	if _, _, err := reader.ReadColumn("missing", nil); err == nil {
		t.Fatalf("ReadColumn() error = nil, want missing column error")
	}
}

func mustBatch(t *testing.T, columns ...vector.Column) vector.Batch {
	t.Helper()
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatalf("NewBatch() error = %v", err)
	}
	return batch
}

func mustColumn(t *testing.T, batch vector.Batch, name string) vector.Column {
	t.Helper()
	col, ok := batch.Column(name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	return col
}

func mustReaderColumn(t *testing.T, reader Reader, name string) Column {
	t.Helper()
	col, ok := reader.Column(name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	return col
}

func assertStrings(t *testing.T, got vector.String, want []string) {
	t.Helper()
	if got.Len() != len(want) {
		gotValues := stringValues(got)
		t.Fatalf("string len = %d, want %d; got %v", got.Len(), len(want), gotValues)
	}
	for i, wantValue := range want {
		if gotValue := got.Value(i); gotValue != wantValue {
			t.Fatalf("string[%d] = %q, want %q", i, gotValue, wantValue)
		}
	}
}

func stringValues(values vector.String) []string {
	out := make([]string, values.Len())
	for i := range out {
		out[i] = values.Value(i)
	}
	return out
}

func assertReadAtCall(t *testing.T, call readAtCall, r Range) {
	t.Helper()
	if call.off != r.Offset || call.n != int(r.Bytes) {
		t.Fatalf("range call = %+v, want offset %d length %d", call, r.Offset, r.Bytes)
	}
}

type readAtCall struct {
	off int64
	n   int
}

type recordingReaderAt struct {
	data  []byte
	calls []readAtCall
}

func (r *recordingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.calls = append(r.calls, readAtCall{off: off, n: len(p)})
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

type recordingByteViewer struct {
	data        []byte
	readAtCalls []readAtCall
	viewCalls   []readAtCall
}

func (r *recordingByteViewer) ReadAt(p []byte, off int64) (int, error) {
	r.readAtCalls = append(r.readAtCalls, readAtCall{off: off, n: len(p)})
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *recordingByteViewer) View(off int64, n int) ([]byte, bool) {
	r.viewCalls = append(r.viewCalls, readAtCall{off: off, n: n})
	if off < 0 || n < 0 {
		return nil, false
	}
	if off > int64(len(r.data)) || int64(n) > int64(len(r.data))-off {
		return nil, false
	}
	return r.data[off : off+int64(n)], true
}

type fallbackByteViewer struct {
	data        []byte
	readAtCalls []readAtCall
	viewCalls   []readAtCall
}

func (r *fallbackByteViewer) ReadAt(p []byte, off int64) (int, error) {
	r.readAtCalls = append(r.readAtCalls, readAtCall{off: off, n: len(p)})
	if off < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *fallbackByteViewer) View(off int64, n int) ([]byte, bool) {
	r.viewCalls = append(r.viewCalls, readAtCall{off: off, n: n})
	return nil, false
}
