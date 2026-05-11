package storage

import (
	"bufio"
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const segmentMagic = "DRIPV3S1"

const segmentWriteBufferBytes = 4 << 20

const (
	segmentFooterPlainMagic = "DRIPFTR0"
	segmentFooterFlateMagic = "DRIPFTZ1"
)

// Minimum encoded bytes for a PageMeta before optional stats payloads:
// rowStart(4), rows(4), nullCount(4), offset(8), length(8), kind(1),
// encoding(1), allValid/allNull(2), and seven stats presence flags.
const segmentPageMetaFooterBytes = 4 + 4 + 4 + 8 + 8 + 1 + 1 + 2 + 7

type SegmentID uint64

type PageMeta struct {
	RowStart    uint32
	Rows        uint32
	NullCount   uint32
	Offset      uint64
	Length      uint64
	Kind        types.VecKind
	Encoding    types.Encoding
	AllValid    bool
	AllNull     bool
	Bool        *BoolStats
	Int32       *Int32Stats
	Int64       *Int64Stats
	Int32Values *Int32ValueStats
	Int64Values *Int64ValueStats
	UUID        *UUIDStats
	Text        *TextStats
}

type ColumnMeta struct {
	Name       string
	Type       types.Type
	EnumLabels []string
	Rows       uint32
	NullCount  uint32
	AllValid   bool
	AllNull    bool
	Bool       *BoolStats
	Int32      *Int32Stats
	Int64      *Int64Stats
	UUID       *UUIDStats
	Text       *TextStats
	Pages      []PageMeta
}

type SegmentMeta struct {
	ID       SegmentID
	Rows     uint32
	PageRows uint32
	Columns  []ColumnMeta
}

func WriteSegment(path string, id SegmentID, batches []types.Batch) (SegmentMeta, error) {
	if len(batches) == 0 {
		return SegmentMeta{}, fmt.Errorf("segment write requires at least one batch")
	}
	if err := validateSegmentBatches(batches); err != nil {
		return SegmentMeta{}, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return SegmentMeta{}, err
	}
	committed := false
	defer func() {
		_ = file.Close()
		if !committed {
			_ = os.Remove(path)
		}
	}()
	writer := bufio.NewWriterSize(file, segmentWriteBufferBytes)
	if _, err := writer.WriteString(segmentMagic); err != nil {
		return SegmentMeta{}, err
	}
	offset := uint64(len(segmentMagic))
	meta := SegmentMeta{ID: id, Rows: uint32(totalBatchRows(batches)), PageRows: types.StandardBatchRows, Columns: make([]ColumnMeta, len(batches[0].Columns))}
	for colIndex, firstCol := range batches[0].Columns {
		meta.Columns[colIndex] = ColumnMeta{Name: firstCol.Name, Type: firstCol.Type, EnumLabels: append([]string(nil), firstCol.EnumLabels...), Rows: meta.Rows}
	}
	rowStart := 0
	scratch := make([][]byte, len(meta.Columns))
	for _, batch := range batches {
		pages, err := encodeSegmentBatchPages(batch, rowStart, scratch)
		if err != nil {
			return SegmentMeta{}, err
		}
		for colIndex, page := range pages {
			page.meta.Offset = offset
			if _, err := writer.Write(page.payload); err != nil {
				return SegmentMeta{}, err
			}
			offset += uint64(len(page.payload))
			scratch[colIndex] = page.scratch

			colMeta := &meta.Columns[colIndex]
			colMeta.NullCount += page.meta.NullCount
			colMeta.Bool = mergeBoolStats(colMeta.Bool, page.meta.Bool)
			colMeta.Int32 = mergeInt32Stats(colMeta.Int32, page.meta.Int32)
			colMeta.Int64 = mergeInt64Stats(colMeta.Int64, page.meta.Int64)
			colMeta.UUID = mergeUUIDStats(colMeta.UUID, page.meta.UUID)
			colMeta.Text = mergeTextStats(colMeta.Text, page.meta.Text)
			colMeta.Pages = append(colMeta.Pages, page.meta)
		}
		rowStart += batch.Len
	}
	for i := range meta.Columns {
		colMeta := &meta.Columns[i]
		colMeta.AllValid = colMeta.NullCount == 0
		colMeta.AllNull = colMeta.NullCount == colMeta.Rows
		finalizeColumnTextBlooms(colMeta)
		finalizeColumnUUIDBlooms(colMeta)
	}
	footer, err := marshalSegmentMeta(meta)
	if err != nil {
		return SegmentMeta{}, err
	}
	if _, err := writer.Write(footer); err != nil {
		return SegmentMeta{}, err
	}
	var tail [8]byte
	binary.LittleEndian.PutUint64(tail[:], uint64(len(footer)))
	if _, err := writer.Write(tail[:]); err != nil {
		return SegmentMeta{}, err
	}
	if _, err := writer.WriteString(segmentMagic); err != nil {
		return SegmentMeta{}, err
	}
	if err := writer.Flush(); err != nil {
		return SegmentMeta{}, err
	}
	if err := file.Sync(); err != nil {
		return SegmentMeta{}, err
	}
	committed = true
	return meta, nil
}

type encodedSegmentPage struct {
	payload []byte
	meta    PageMeta
	scratch []byte
	err     error
}

func encodeSegmentBatchPages(batch types.Batch, rowStart int, scratch [][]byte) ([]encodedSegmentPage, error) {
	pages := make([]encodedSegmentPage, len(batch.Columns))
	if len(batch.Columns) == 1 {
		encodeSegmentBatchPage(batch.Columns[0], rowStart, scratch[0], &pages[0])
	} else {
		var wg sync.WaitGroup
		for colIndex := range batch.Columns {
			wg.Add(1)
			go func(colIndex int) {
				defer wg.Done()
				encodeSegmentBatchPage(batch.Columns[colIndex], rowStart, scratch[colIndex], &pages[colIndex])
			}(colIndex)
		}
		wg.Wait()
	}
	for _, page := range pages {
		if page.err != nil {
			return nil, page.err
		}
	}
	return pages, nil
}

func encodeSegmentBatchPage(col types.Column, rowStart int, scratch []byte, out *encodedSegmentPage) {
	page, nextScratch, err := encodeSegmentPageInto(col.V, scratch)
	if err != nil {
		out.err = fmt.Errorf("column %q: %w", col.Name, err)
		return
	}
	pageMeta := PageMeta{RowStart: uint32(rowStart), Rows: uint32(page.Rows), NullCount: uint32(page.NullCount), Length: uint64(len(page.Payload)), Kind: page.Kind, Encoding: page.Encoding}
	applyPageStats(&pageMeta, col.V)
	out.payload = page.Payload
	out.meta = pageMeta
	out.scratch = nextScratch
}

func ReadSegmentFooter(path string) (SegmentMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return SegmentMeta{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return SegmentMeta{}, err
	}
	minSize := int64(len(segmentMagic)*2 + 8)
	if info.Size() < minSize {
		return SegmentMeta{}, fmt.Errorf("segment %q is too small", path)
	}
	header := make([]byte, len(segmentMagic))
	if _, err := file.ReadAt(header, 0); err != nil {
		return SegmentMeta{}, err
	}
	if string(header) != segmentMagic {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid header", path)
	}
	tail := make([]byte, len(segmentMagic)+8)
	if _, err := file.ReadAt(tail, info.Size()-int64(len(tail))); err != nil {
		return SegmentMeta{}, err
	}
	if string(tail[8:]) != segmentMagic {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid footer magic", path)
	}
	footerLen := binary.LittleEndian.Uint64(tail[:8])
	dataEnd := info.Size() - int64(len(tail))
	footerCapacity := dataEnd - int64(len(segmentMagic))
	if footerCapacity < 0 || footerLen > uint64(footerCapacity) || footerLen > uint64(math.MaxInt) {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid footer length", path)
	}
	footerStart := dataEnd - int64(footerLen)
	footer := make([]byte, int(footerLen))
	if _, err := file.ReadAt(footer, footerStart); err != nil {
		return SegmentMeta{}, err
	}
	return unmarshalSegmentMeta(footer)
}

func encodeSegmentPageInto(v types.Vec, scratch []byte) (codec.Page, []byte, error) {
	if v.Kind == types.VecText {
		if best, ok := codec.TextCandidates().Pick(v); ok {
			page, err := best.EncodeInto(scratch)
			return page, nextPageScratch(scratch, page.Payload), err
		}
	}
	if best, ok := codec.FixedCandidates().Pick(v); ok {
		page, err := best.EncodeInto(scratch)
		return page, nextPageScratch(scratch, page.Payload), err
	}
	page, err := codec.Plain{}.Encode(v)
	return page, nextPageScratch(scratch, page.Payload), err
}

func nextPageScratch(scratch []byte, payload []byte) []byte {
	if cap(payload) > cap(scratch) {
		return payload[:0]
	}
	return scratch[:0]
}

func validateSegmentBatches(batches []types.Batch) error {
	if len(batches[0].Columns) == 0 {
		return fmt.Errorf("segment batch has no columns")
	}
	for i, batch := range batches {
		if batch.Sel != nil {
			return fmt.Errorf("batch %d has selection", i)
		}
		if len(batch.Columns) != len(batches[0].Columns) {
			return fmt.Errorf("batch %d column count mismatch", i)
		}
		for colIndex, col := range batch.Columns {
			want := batches[0].Columns[colIndex]
			if col.Name != want.Name || col.Type != want.Type {
				return fmt.Errorf("batch %d column %d mismatch", i, colIndex)
			}
		}
	}
	return nil
}

func totalBatchRows(batches []types.Batch) int {
	rows := 0
	for _, batch := range batches {
		rows += batch.Len
	}
	return rows
}

func marshalSegmentMeta(meta SegmentMeta) ([]byte, error) {
	raw, err := marshalSegmentMetaRaw(meta)
	if err != nil {
		return nil, err
	}
	return encodeSegmentFooter(raw)
}

func marshalSegmentMetaRaw(meta SegmentMeta) ([]byte, error) {
	w := segmentMetaWriter{}
	writeU64(&w, uint64(meta.ID))
	writeU32(&w, meta.Rows)
	writeU32(&w, meta.PageRows)
	writeU32(&w, uint32(len(meta.Columns)))
	for _, col := range meta.Columns {
		writeString(&w, col.Name)
		writeType(&w, col.Type)
		writeU32(&w, uint32(len(col.EnumLabels)))
		for _, label := range col.EnumLabels {
			writeString(&w, label)
		}
		writeU32(&w, col.Rows)
		writeU32(&w, col.NullCount)
		writeBool(&w, col.AllValid)
		writeBool(&w, col.AllNull)
		writeBoolStats(&w, col.Bool)
		writeInt32Stats(&w, col.Int32)
		writeInt64Stats(&w, col.Int64)
		writeUUIDStats(&w, col.UUID)
		writeTextStats(&w, col.Text)
		writeU32(&w, uint32(len(col.Pages)))
		for _, page := range col.Pages {
			writeU32(&w, page.RowStart)
			writeU32(&w, page.Rows)
			writeU32(&w, page.NullCount)
			writeU64(&w, page.Offset)
			writeU64(&w, page.Length)
			w.writeByte(byte(page.Kind))
			w.writeByte(byte(page.Encoding))
			writeBool(&w, page.AllValid)
			writeBool(&w, page.AllNull)
			writeBoolStats(&w, page.Bool)
			writeInt32Stats(&w, page.Int32)
			writeInt64Stats(&w, page.Int64)
			writeInt32ValueStats(&w, page.Int32Values)
			writeInt64ValueStats(&w, page.Int64Values)
			writeUUIDStats(&w, page.UUID)
			writeTextStats(&w, page.Text)
		}
	}
	return w.bytes()
}

func encodeSegmentFooter(raw []byte) ([]byte, error) {
	compressed, err := compressSegmentFooter(raw)
	if err == nil && len(compressed) < len(raw) {
		return segmentFooterEnvelope(segmentFooterFlateMagic, uint64(len(raw)), compressed), nil
	}
	return segmentFooterEnvelope(segmentFooterPlainMagic, uint64(len(raw)), raw), nil
}

func decodeSegmentFooter(data []byte) ([]byte, error) {
	if len(data) < len(segmentFooterPlainMagic)+8 {
		return nil, fmt.Errorf("segment footer missing encoding header")
	}
	magic := string(data[:len(segmentFooterPlainMagic)])
	rawLen := binary.LittleEndian.Uint64(data[len(segmentFooterPlainMagic) : len(segmentFooterPlainMagic)+8])
	body := data[len(segmentFooterPlainMagic)+8:]
	if rawLen > uint64(math.MaxInt) {
		return nil, fmt.Errorf("segment footer raw length exceeds int capacity")
	}
	switch magic {
	case segmentFooterPlainMagic:
		if uint64(len(body)) != rawLen {
			return nil, fmt.Errorf("segment footer raw length %d does not match body length %d", rawLen, len(body))
		}
		return body, nil
	case segmentFooterFlateMagic:
		raw, err := decompressSegmentFooter(body)
		if err != nil {
			return nil, err
		}
		if len(raw) != int(rawLen) {
			return nil, fmt.Errorf("segment footer expanded to %d bytes, want %d", len(raw), rawLen)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("segment footer has unknown encoding %q", magic)
	}
}

func segmentFooterEnvelope(magic string, rawLen uint64, body []byte) []byte {
	out := make([]byte, 0, len(magic)+8+len(body))
	out = append(out, magic...)
	out = binary.LittleEndian.AppendUint64(out, rawLen)
	out = append(out, body...)
	return out
}

func compressSegmentFooter(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(raw); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressSegmentFooter(body []byte) ([]byte, error) {
	zr := flate.NewReader(bytes.NewReader(body))
	raw, err := io.ReadAll(zr)
	closeErr := zr.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return raw, nil
}

func unmarshalSegmentMeta(data []byte) (SegmentMeta, error) {
	data, err := decodeSegmentFooter(data)
	if err != nil {
		return SegmentMeta{}, err
	}
	r := segmentMetaReader{r: bytes.NewReader(data)}
	meta := SegmentMeta{ID: SegmentID(r.readU64()), Rows: r.readU32(), PageRows: r.readU32()}
	cols := r.readU32()
	if r.err != nil {
		return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
	}
	if uint64(cols) > uint64(r.r.Len()) {
		return SegmentMeta{}, fmt.Errorf("segment footer column count %d exceeds remaining metadata", cols)
	}
	meta.Columns = make([]ColumnMeta, 0, int(cols))
	for i := uint32(0); i < cols; i++ {
		col := ColumnMeta{Name: r.readString(), Type: r.readType()}
		labels := r.readU32()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		if uint64(labels) > uint64(r.r.Len()/4) {
			return SegmentMeta{}, fmt.Errorf("segment footer column %d label count %d exceeds remaining metadata", i, labels)
		}
		if labels != 0 {
			col.EnumLabels = make([]string, 0, int(labels))
		}
		for j := uint32(0); j < labels; j++ {
			col.EnumLabels = append(col.EnumLabels, r.readString())
		}
		col.Rows = r.readU32()
		col.NullCount = r.readU32()
		col.AllValid = r.readBool()
		col.AllNull = r.readBool()
		col.Bool = r.readBoolStats()
		col.Int32 = r.readInt32Stats()
		col.Int64 = r.readInt64Stats()
		col.UUID = r.readUUIDStats()
		col.Text = r.readTextStats()
		pages := r.readU32()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		if uint64(pages) > uint64(r.r.Len()/segmentPageMetaFooterBytes) {
			return SegmentMeta{}, fmt.Errorf("segment footer column %d page count %d exceeds remaining metadata", i, pages)
		}
		col.Pages = make([]PageMeta, 0, int(pages))
		for j := uint32(0); j < pages; j++ {
			page := PageMeta{RowStart: r.readU32(), Rows: r.readU32(), NullCount: r.readU32(), Offset: r.readU64(), Length: r.readU64()}
			kind := r.readByte()
			enc := r.readByte()
			if r.err != nil {
				return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
			}
			page.Kind = types.VecKind(kind)
			page.Encoding = types.Encoding(enc)
			page.AllValid = r.readBool()
			page.AllNull = r.readBool()
			page.Bool = r.readBoolStats()
			page.Int32 = r.readInt32Stats()
			page.Int64 = r.readInt64Stats()
			page.Int32Values = r.readInt32ValueStats()
			page.Int64Values = r.readInt64ValueStats()
			page.UUID = r.readUUIDStats()
			page.Text = r.readTextStats()
			if r.err != nil {
				return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
			}
			col.Pages = append(col.Pages, page)
		}
		meta.Columns = append(meta.Columns, col)
	}
	if r.r.Len() != 0 {
		return SegmentMeta{}, fmt.Errorf("segment footer has %d trailing bytes", r.r.Len())
	}
	return meta, nil
}

type segmentMetaReader struct {
	r   *bytes.Reader
	err error
}

func (r *segmentMetaReader) readType() types.Type {
	kind := r.readByte()
	name := r.readString()
	if r.err != nil {
		return types.Type{}
	}
	return types.Type{Kind: types.Kind(kind), Name: name}
}

func (r *segmentMetaReader) readBool() bool {
	return r.readByte() != 0
}

func (r *segmentMetaReader) readBoolStats() *BoolStats {
	if !r.readBool() {
		return nil
	}
	out := &BoolStats{HasTrue: r.readBool(), HasFalse: r.readBool()}
	if r.err != nil {
		return nil
	}
	return out
}

func (r *segmentMetaReader) readInt32Stats() *Int32Stats {
	if !r.readBool() {
		return nil
	}
	min := int32(r.readU32())
	max := int32(r.readU32())
	sum := int64(r.readU64())
	sumValid := r.readBool()
	if r.err != nil {
		return nil
	}
	return &Int32Stats{Min: min, Max: max, Sum: sum, SumValid: sumValid}
}

func (r *segmentMetaReader) readInt64Stats() *Int64Stats {
	if !r.readBool() {
		return nil
	}
	min := int64(r.readU64())
	max := int64(r.readU64())
	sum := int64(r.readU64())
	sumValid := r.readBool()
	if r.err != nil {
		return nil
	}
	return &Int64Stats{Min: min, Max: max, Sum: sum, SumValid: sumValid}
}

func (r *segmentMetaReader) readInt32ValueStats() *Int32ValueStats {
	if !r.readBool() {
		return nil
	}
	out := &Int32ValueStats{Truncated: r.readBool()}
	count := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(count) > uint64(r.r.Len()/4) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]int32, 0, int(count))
	for i := uint32(0); i < count; i++ {
		out.Values = append(out.Values, int32(r.readU32()))
	}
	if r.err != nil {
		return nil
	}
	return out
}

func (r *segmentMetaReader) readInt64ValueStats() *Int64ValueStats {
	if !r.readBool() {
		return nil
	}
	out := &Int64ValueStats{Truncated: r.readBool()}
	count := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(count) > uint64(r.r.Len()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]int64, 0, int(count))
	for i := uint32(0); i < count; i++ {
		out.Values = append(out.Values, int64(r.readU64()))
	}
	if r.err != nil {
		return nil
	}
	return out
}

func (r *segmentMetaReader) readUUIDStats() *UUIDStats {
	if !r.readBool() {
		return nil
	}
	bloomCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(bloomCount) > uint64(r.r.Len()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out := &UUIDStats{}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, 0, int(bloomCount))
		for i := uint32(0); i < bloomCount; i++ {
			out.HashBloom = append(out.HashBloom, r.readU64())
		}
	}
	if r.err != nil {
		return nil
	}
	return out
}

func (r *segmentMetaReader) readTextStats() *TextStats {
	if !r.readBool() {
		return nil
	}
	out := &TextStats{DataBytes: r.readU64(), Truncated: r.readBool()}
	count := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(count) > uint64(r.r.Len()/4) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]string, 0, int(count))
	out.Counts = make([]uint32, 0, int(count))
	for i := uint32(0); i < count; i++ {
		out.Values = append(out.Values, r.readString())
		out.Counts = append(out.Counts, r.readU32())
	}
	bloomCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(bloomCount) > uint64(r.r.Len()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, 0, int(bloomCount))
		for i := uint32(0); i < bloomCount; i++ {
			out.HashBloom = append(out.HashBloom, r.readU64())
		}
	}
	if r.err != nil {
		return nil
	}
	return out
}

func (r *segmentMetaReader) readString() string {
	n := r.readU32()
	if r.err != nil {
		return ""
	}
	if uint64(n) > uint64(r.r.Len()) {
		r.err = io.ErrUnexpectedEOF
		return ""
	}
	buf := make([]byte, int(n))
	if _, err := io.ReadFull(r.r, buf); err != nil {
		r.err = err
		return ""
	}
	return string(buf)
}

func (r *segmentMetaReader) readByte() byte {
	if r.err != nil {
		return 0
	}
	b, err := r.r.ReadByte()
	if err != nil {
		r.err = err
		return 0
	}
	return b
}

func (r *segmentMetaReader) readU32() uint32 {
	if r.err != nil {
		return 0
	}
	var buf [4]byte
	if _, err := io.ReadFull(r.r, buf[:]); err != nil {
		r.err = err
		return 0
	}
	return binary.LittleEndian.Uint32(buf[:])
}

func (r *segmentMetaReader) readU64() uint64 {
	if r.err != nil {
		return 0
	}
	var buf [8]byte
	if _, err := io.ReadFull(r.r, buf[:]); err != nil {
		r.err = err
		return 0
	}
	return binary.LittleEndian.Uint64(buf[:])
}

type segmentMetaWriter struct {
	buf     bytes.Buffer
	scratch []byte
	err     error
}

func (w *segmentMetaWriter) bytes() ([]byte, error) {
	if w.err != nil {
		return nil, w.err
	}
	return w.buf.Bytes(), nil
}

func (w *segmentMetaWriter) write(data []byte) {
	if w.err != nil {
		return
	}
	_, w.err = w.buf.Write(data)
}

func (w *segmentMetaWriter) writeByte(value byte) {
	if w.err != nil {
		return
	}
	w.err = w.buf.WriteByte(value)
}

func (w *segmentMetaWriter) writeString(value string) {
	if w.err != nil {
		return
	}
	_, w.err = w.buf.WriteString(value)
}

func writeType(w *segmentMetaWriter, typ types.Type) {
	w.writeByte(byte(typ.Kind))
	writeString(w, typ.Name)
}

func writeBool(w *segmentMetaWriter, value bool) {
	if value {
		w.writeByte(1)
		return
	}
	w.writeByte(0)
}

func writeBoolStats(w *segmentMetaWriter, stats *BoolStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeBool(w, stats.HasTrue)
	writeBool(w, stats.HasFalse)
}

func writeInt32Stats(w *segmentMetaWriter, stats *Int32Stats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU32(w, uint32(stats.Min))
	writeU32(w, uint32(stats.Max))
	writeU64(w, uint64(stats.Sum))
	writeBool(w, stats.SumValid)
}

func writeInt64Stats(w *segmentMetaWriter, stats *Int64Stats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU64(w, uint64(stats.Min))
	writeU64(w, uint64(stats.Max))
	writeU64(w, uint64(stats.Sum))
	writeBool(w, stats.SumValid)
}

func writeInt32ValueStats(w *segmentMetaWriter, stats *Int32ValueStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeBool(w, stats.Truncated)
	writeU32(w, uint32(len(stats.Values)))
	for _, value := range stats.Values {
		writeU32(w, uint32(value))
	}
}

func writeInt64ValueStats(w *segmentMetaWriter, stats *Int64ValueStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeBool(w, stats.Truncated)
	writeU32(w, uint32(len(stats.Values)))
	for _, value := range stats.Values {
		writeU64(w, uint64(value))
	}
}

func writeUUIDStats(w *segmentMetaWriter, stats *UUIDStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU32(w, uint32(len(stats.HashBloom)))
	for _, word := range stats.HashBloom {
		writeU64(w, word)
	}
}

func writeTextStats(w *segmentMetaWriter, stats *TextStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU64(w, stats.DataBytes)
	writeBool(w, stats.Truncated)
	writeU32(w, uint32(len(stats.Values)))
	for i, value := range stats.Values {
		writeString(w, value)
		count := uint32(0)
		if i < len(stats.Counts) {
			count = stats.Counts[i]
		}
		writeU32(w, count)
	}
	writeU32(w, uint32(len(stats.HashBloom)))
	for _, word := range stats.HashBloom {
		writeU64(w, word)
	}
}

func writeString(w *segmentMetaWriter, value string) {
	writeU32(w, uint32(len(value)))
	w.writeString(value)
}

func writeU32(w *segmentMetaWriter, value uint32) {
	w.scratch = binary.LittleEndian.AppendUint32(w.scratch[:0], value)
	w.write(w.scratch)
}

func writeU64(w *segmentMetaWriter, value uint64) {
	w.scratch = binary.LittleEndian.AppendUint64(w.scratch[:0], value)
	w.write(w.scratch)
}
