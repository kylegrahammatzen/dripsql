package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const segmentMagic = "DRIPV3S1"

const segmentPageMetaFooterBytes = 4 + 4 + 4 + 8 + 8 + 1 + 1 + 2 + 6

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
	if _, err := file.WriteString(segmentMagic); err != nil {
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
			if _, err := file.Write(page.payload); err != nil {
				return SegmentMeta{}, err
			}
			offset += uint64(len(page.payload))
			scratch[colIndex] = page.scratch

			colMeta := &meta.Columns[colIndex]
			colMeta.NullCount += page.meta.NullCount
			colMeta.Bool = mergeBoolStats(colMeta.Bool, page.meta.Bool)
			colMeta.Int32 = mergeInt32Stats(colMeta.Int32, page.meta.Int32)
			colMeta.Int64 = mergeInt64Stats(colMeta.Int64, page.meta.Int64)
			colMeta.Text = mergeTextStats(colMeta.Text, page.meta.Text)
			colMeta.Pages = append(colMeta.Pages, page.meta)
		}
		rowStart += batch.Len
	}
	for i := range meta.Columns {
		colMeta := &meta.Columns[i]
		colMeta.AllValid = colMeta.NullCount == 0
		colMeta.AllNull = colMeta.NullCount == colMeta.Rows
	}
	footer, err := marshalSegmentMeta(meta)
	if err != nil {
		return SegmentMeta{}, err
	}
	if _, err := file.Write(footer); err != nil {
		return SegmentMeta{}, err
	}
	var tail [8]byte
	binary.LittleEndian.PutUint64(tail[:], uint64(len(footer)))
	if _, err := file.Write(tail[:]); err != nil {
		return SegmentMeta{}, err
	}
	if _, err := file.WriteString(segmentMagic); err != nil {
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
	if footerCapacity < 0 || footerLen > uint64(footerCapacity) || footerLen > uint64(maxInt()) {
		return SegmentMeta{}, fmt.Errorf("segment %q has invalid footer length", path)
	}
	footerStart := dataEnd - int64(footerLen)
	footer := make([]byte, int(footerLen))
	if _, err := file.ReadAt(footer, footerStart); err != nil {
		return SegmentMeta{}, err
	}
	return unmarshalSegmentMeta(footer)
}

func encodeSegmentPage(v types.Vec) (codec.Page, error) {
	page, _, err := encodeSegmentPageInto(v, nil)
	return page, err
}

func encodeSegmentPageInto(v types.Vec, scratch []byte) (codec.Page, []byte, error) {
	if v.Kind == types.VecText {
		if best, ok := codec.PickSmallestPrepared(v, codec.Plain{}, codec.Dictionary{}, codec.Constant{}); ok {
			page, err := best.EncodeInto(scratch)
			return page, nextPageScratch(scratch, page.Payload), err
		}
	}
	if best, ok := codec.PickSmallestPrepared(v, codec.Plain{}, codec.Constant{}, codec.FORBitPack{}); ok {
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
	var buf bytes.Buffer
	writeU64(&buf, uint64(meta.ID))
	writeU32(&buf, meta.Rows)
	writeU32(&buf, meta.PageRows)
	writeU32(&buf, uint32(len(meta.Columns)))
	for _, col := range meta.Columns {
		writeString(&buf, col.Name)
		writeType(&buf, col.Type)
		writeU32(&buf, uint32(len(col.EnumLabels)))
		for _, label := range col.EnumLabels {
			writeString(&buf, label)
		}
		writeU32(&buf, col.Rows)
		writeU32(&buf, col.NullCount)
		writeBool(&buf, col.AllValid)
		writeBool(&buf, col.AllNull)
		writeBoolStats(&buf, col.Bool)
		writeInt32Stats(&buf, col.Int32)
		writeInt64Stats(&buf, col.Int64)
		writeTextStats(&buf, col.Text)
		writeU32(&buf, uint32(len(col.Pages)))
		for _, page := range col.Pages {
			writeU32(&buf, page.RowStart)
			writeU32(&buf, page.Rows)
			writeU32(&buf, page.NullCount)
			writeU64(&buf, page.Offset)
			writeU64(&buf, page.Length)
			buf.WriteByte(byte(page.Kind))
			buf.WriteByte(byte(page.Encoding))
			writeBool(&buf, page.AllValid)
			writeBool(&buf, page.AllNull)
			writeBoolStats(&buf, page.Bool)
			writeInt32Stats(&buf, page.Int32)
			writeInt64Stats(&buf, page.Int64)
			writeInt32ValueStats(&buf, page.Int32Values)
			writeInt64ValueStats(&buf, page.Int64Values)
			writeTextStats(&buf, page.Text)
		}
	}
	return buf.Bytes(), nil
}

func unmarshalSegmentMeta(data []byte) (SegmentMeta, error) {
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

func (r *segmentMetaReader) readTextStats() *TextStats {
	if !r.readBool() {
		return nil
	}
	out := &TextStats{Truncated: r.readBool()}
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
	hashCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(hashCount) > uint64(r.r.Len()/2) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	if hashCount == 0 {
		return out
	}
	out.Hashes = make([]uint16, 0, int(hashCount))
	for i := uint32(0); i < hashCount; i++ {
		out.Hashes = append(out.Hashes, r.readU16())
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

func (r *segmentMetaReader) readU16() uint16 {
	if r.err != nil {
		return 0
	}
	var buf [2]byte
	if _, err := io.ReadFull(r.r, buf[:]); err != nil {
		r.err = err
		return 0
	}
	return binary.LittleEndian.Uint16(buf[:])
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

func writeType(w io.Writer, typ types.Type) {
	_, _ = w.Write([]byte{byte(typ.Kind)})
	writeString(w, typ.Name)
}

func writeBool(w io.Writer, value bool) {
	if value {
		_, _ = w.Write([]byte{1})
		return
	}
	_, _ = w.Write([]byte{0})
}

func writeBoolStats(w io.Writer, stats *BoolStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeBool(w, stats.HasTrue)
	writeBool(w, stats.HasFalse)
}

func writeInt32Stats(w io.Writer, stats *Int32Stats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU32(w, uint32(stats.Min))
	writeU32(w, uint32(stats.Max))
	writeU64(w, uint64(stats.Sum))
	writeBool(w, stats.SumValid)
}

func writeInt64Stats(w io.Writer, stats *Int64Stats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
	writeU64(w, uint64(stats.Min))
	writeU64(w, uint64(stats.Max))
	writeU64(w, uint64(stats.Sum))
	writeBool(w, stats.SumValid)
}

func writeInt32ValueStats(w io.Writer, stats *Int32ValueStats) {
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

func writeInt64ValueStats(w io.Writer, stats *Int64ValueStats) {
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

func writeTextStats(w io.Writer, stats *TextStats) {
	writeBool(w, stats != nil)
	if stats == nil {
		return
	}
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
	writeU32(w, uint32(len(stats.Hashes)))
	for _, hash := range stats.Hashes {
		writeU16(w, hash)
	}
}

func writeString(w io.Writer, value string) {
	writeU32(w, uint32(len(value)))
	_, _ = w.Write([]byte(value))
}

func writeU32(w io.Writer, value uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, _ = w.Write(buf[:])
}

func writeU16(w io.Writer, value uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, _ = w.Write(buf[:])
}

func writeU64(w io.Writer, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = w.Write(buf[:])
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
