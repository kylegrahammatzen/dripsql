package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/klauspost/compress/flate"
	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const segmentMagic = "DRIPV4S1"

const segmentWriteBufferBytes = 4 << 20

const (
	segmentFooterPlainMagicV4 = "DRIPFTR6"
	segmentFooterFlateMagicV4 = "DRIPFTZ7"
)

// v4 wire format: the page directory is a fixed-size table of 32-byte
// entries indexed by pageIndex, so cold open skims it with pointer
// arithmetic instead of parsing variable-length per-page records. Per-page
// stats live in a length-prefixed heavy section after the directory, lazy
// decoded on first LoadColumn.
const segmentPageEntryBytes = 4 + 4 + 4 + 8 + 8 + 1 + 1 + 1 + 1

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

// ColumnStats bundles every per-column stats variant so column metadata
// carries one sidecar instead of seven parallel pointer fields.
type ColumnStats struct {
	Bool        *BoolStats
	Int32       *Int32Stats
	Int64       *Int64Stats
	Int32Values *Int32ValueStats
	Int64Values *Int64ValueStats
	UUID        *UUIDStats
	Text        *TextStats
}

// ColumnMeta embeds types.Column for Name, Type, and EnumLabels so storage
// reuses the engine-wide column descriptor instead of carrying parallel
// fields. The embedded Vec field stays zero in segment metadata since
// payloads live on disk.
type ColumnMeta struct {
	types.Column
	Rows      uint32
	NullCount uint32
	AllValid  bool
	AllNull   bool
	Stats     ColumnStats
	Pages     []PageMeta
}

type SegmentMeta struct {
	ID       SegmentID
	Rows     uint32
	PageRows uint32
	Columns  []ColumnMeta

	// PageRowCounts is the per-page row count, kept eager so cold-open can
	// size SegmentPageInfo without loading col.Pages from the heavy section.
	PageRowCounts []uint32

	// Lazy heavy-field decoding state. See segment_lazy.go.
	lazyBody   []byte
	lazyRanges []byteRange
	lazyOnce   []sync.Once
	lazyErrs   []error
}

func WriteSegment(path string, id SegmentID, batches []types.Batch) (SegmentMeta, error) {
	return WriteSegmentWith(path, id, types.CompressionDefault, batches)
}

func WriteSegmentWith(path string, id SegmentID, compression types.CompressionPolicy, batches []types.Batch) (SegmentMeta, error) {
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
	meta := SegmentMeta{ID: id, Rows: uint32(totalBatchRows(batches)), PageRows: types.StandardBatchRows, Columns: make([]ColumnMeta, len(batches[0].Columns)), PageRowCounts: make([]uint32, 0, len(batches))}
	for colIndex, firstCol := range batches[0].Columns {
		meta.Columns[colIndex] = ColumnMeta{
			Column: types.Column{Name: firstCol.Name, Type: firstCol.Type, EnumLabels: append([]string(nil), firstCol.EnumLabels...)},
			Rows:   meta.Rows,
		}
	}
	rowStart := 0
	scratch := make([][]byte, len(meta.Columns))
	var sma *segmentSMAAccumulator
	if !smaDisabled {
		sma = newSegmentSMAAccumulator(batches[0].Columns)
	}
	for _, batch := range batches {
		pages, err := encodeSegmentBatchPages(batch, rowStart, scratch, compression)
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
			colMeta.Stats.Bool = mergeBoolStats(colMeta.Stats.Bool, page.meta.Bool)
			colMeta.Stats.Int32 = mergeNumericStats(colMeta.Stats.Int32, page.meta.Int32)
			colMeta.Stats.Int64 = mergeNumericStats(colMeta.Stats.Int64, page.meta.Int64)
			colMeta.Stats.UUID = mergeUUIDStats(colMeta.Stats.UUID, page.meta.UUID)
			colMeta.Stats.Text = mergeTextStats(colMeta.Stats.Text, page.meta.Text)
			colMeta.Pages = append(colMeta.Pages, page.meta)
		}
		if sma != nil {
			sma.observeBatch(batch)
		}
		meta.PageRowCounts = append(meta.PageRowCounts, uint32(batch.Len))
		rowStart += batch.Len
	}
	for i := range meta.Columns {
		colMeta := &meta.Columns[i]
		colMeta.AllValid = colMeta.NullCount == 0
		colMeta.AllNull = colMeta.NullCount == colMeta.Rows
		finalizeColumnTextBlooms(colMeta)
		finalizeColumnUUIDBlooms(colMeta)
		finalizeColumnInt64Blooms(colMeta)
		finalizeColumnInt32Blooms(colMeta)
	}
	if sma != nil {
		sma.finalize(meta.Columns)
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

func encodeSegmentBatchPages(batch types.Batch, rowStart int, scratch [][]byte, compression types.CompressionPolicy) ([]encodedSegmentPage, error) {
	pages := make([]encodedSegmentPage, len(batch.Columns))
	if len(batch.Columns) == 1 {
		encodeSegmentBatchPage(batch.Columns[0], rowStart, scratch[0], &pages[0], compression)
	} else {
		var wg sync.WaitGroup
		for colIndex := range batch.Columns {
			wg.Add(1)
			go func(colIndex int) {
				defer wg.Done()
				encodeSegmentBatchPage(batch.Columns[colIndex], rowStart, scratch[colIndex], &pages[colIndex], compression)
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

func encodeSegmentBatchPage(col types.Column, rowStart int, scratch []byte, out *encodedSegmentPage, compression types.CompressionPolicy) {
	page, nextScratch, err := encodeSegmentPageInto(col.V, scratch, compression)
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

func ReadSegmentFooter(path string) (SegmentMeta, int64, error) {
	file, err := openSegmentForRead(path)
	if err != nil {
		return SegmentMeta{}, 0, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return SegmentMeta{}, 0, err
	}
	size := info.Size()
	minSize := int64(len(segmentMagic)*2 + 8)
	if size < minSize {
		return SegmentMeta{}, 0, fmt.Errorf("segment %q is too small", path)
	}
	header := make([]byte, len(segmentMagic))
	if _, err := readAt(file, header, 0); err != nil {
		return SegmentMeta{}, 0, err
	}
	if string(header) != segmentMagic {
		return SegmentMeta{}, 0, fmt.Errorf("segment %q has invalid header", path)
	}
	tail := make([]byte, len(segmentMagic)+8)
	if _, err := readAt(file, tail, size-int64(len(tail))); err != nil {
		return SegmentMeta{}, 0, err
	}
	if string(tail[8:]) != segmentMagic {
		return SegmentMeta{}, 0, fmt.Errorf("segment %q has invalid footer magic", path)
	}
	footerLen := binary.LittleEndian.Uint64(tail[:8])
	dataEnd := size - int64(len(tail))
	footerCapacity := dataEnd - int64(len(segmentMagic))
	if footerCapacity < 0 || footerLen > uint64(footerCapacity) || footerLen > uint64(math.MaxInt) {
		return SegmentMeta{}, 0, fmt.Errorf("segment %q has invalid footer length", path)
	}
	footerStart := dataEnd - int64(footerLen)
	footer := make([]byte, int(footerLen))
	if _, err := readAt(file, footer, footerStart); err != nil {
		return SegmentMeta{}, 0, err
	}
	meta, err := unmarshalSegmentMeta(footer)
	if err != nil {
		return SegmentMeta{}, 0, err
	}
	return meta, size, nil
}

func encodeSegmentPageInto(v types.Vec, scratch []byte, compression types.CompressionPolicy) (codec.Page, []byte, error) {
	if v.Kind == types.VecText {
		if best, ok := codec.TextCandidatesFor(compression).Pick(v); ok {
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
		writeBoolStats(&w, col.Stats.Bool)
		writeInt32Stats(&w, col.Stats.Int32)
		writeInt64Stats(&w, col.Stats.Int64)
		writeU32(&w, uint32(len(col.Pages)))
		for _, page := range col.Pages {
			writePageEntry(&w, page)
		}
		heavy := segmentMetaWriter{}
		writeUUIDStats(&heavy, col.Stats.UUID)
		writeTextStats(&heavy, col.Stats.Text)
		writeInt32ValueStats(&heavy, col.Stats.Int32Values)
		writeInt64ValueStats(&heavy, col.Stats.Int64Values)
		for _, page := range col.Pages {
			writeBool(&heavy, page.AllValid)
			writeBool(&heavy, page.AllNull)
			writeBoolStats(&heavy, page.Bool)
			writeInt32Stats(&heavy, page.Int32)
			writeInt64Stats(&heavy, page.Int64)
			writeInt32ValueStats(&heavy, page.Int32Values)
			writeInt64ValueStats(&heavy, page.Int64Values)
			writeUUIDStats(&heavy, page.UUID)
			writeTextStats(&heavy, page.Text)
		}
		heavyBytes, err := heavy.bytes()
		if err != nil {
			return nil, err
		}
		writeU32(&w, uint32(len(heavyBytes)))
		w.write(heavyBytes)
	}
	return w.bytes()
}

// writePageEntry writes the 32-byte fixed-size page directory entry. Keep
// the field order in lockstep with readPageEntry below; cold open relies on
// the layout staying stable so the page directory is a contiguous table.
func writePageEntry(w *segmentMetaWriter, page PageMeta) {
	writeU32(w, page.RowStart)
	writeU32(w, page.Rows)
	writeU32(w, page.NullCount)
	writeU64(w, page.Offset)
	writeU64(w, page.Length)
	w.writeByte(byte(page.Kind))
	w.writeByte(byte(page.Encoding))
	w.writeByte(0)
	w.writeByte(0)
}

// encodeSegmentFooter only invokes flate when the compressed payload is at
// least 4x smaller than the raw payload. Bloom-heavy footers compress poorly
// and the decode cost was 60% of cold-ish CPU at 100M scale before this
// gate. Footers dominated by dictionaries still hit the ratio and stay
// flate'd.
func encodeSegmentFooter(raw []byte) ([]byte, error) {
	compressed, err := compressSegmentFooter(raw)
	if err == nil && len(compressed)*4 < len(raw) {
		return segmentFooterEnvelope(segmentFooterFlateMagicV4, uint64(len(raw)), compressed), nil
	}
	return segmentFooterEnvelope(segmentFooterPlainMagicV4, uint64(len(raw)), raw), nil
}

// decodeSegmentFooter returns the raw v4 footer body. v1/v2/v3 are not read.
func decodeSegmentFooter(data []byte) ([]byte, error) {
	if len(data) < len(segmentFooterPlainMagicV4)+8 {
		return nil, fmt.Errorf("segment footer missing encoding header")
	}
	magic := string(data[:len(segmentFooterPlainMagicV4)])
	rawLen := binary.LittleEndian.Uint64(data[len(segmentFooterPlainMagicV4) : len(segmentFooterPlainMagicV4)+8])
	body := data[len(segmentFooterPlainMagicV4)+8:]
	if rawLen > uint64(math.MaxInt) {
		return nil, fmt.Errorf("segment footer raw length exceeds int capacity")
	}
	switch magic {
	case segmentFooterPlainMagicV4:
		if uint64(len(body)) != rawLen {
			return nil, fmt.Errorf("segment footer raw length %d does not match body length %d", rawLen, len(body))
		}
		return body, nil
	case segmentFooterFlateMagicV4:
		raw, err := decompressSegmentFooter(body, int(rawLen))
		if err != nil {
			return nil, err
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

// flateReaderPool reuses flate decoders so the per-segment Reset path
// replaces what otherwise would be a NewReader allocation per segment during
// parallel cold open.
var flateReaderPool = sync.Pool{
	New: func() any {
		return flate.NewReader(bytes.NewReader(nil))
	},
}

// decompressSegmentFooter decompresses body into a buffer pre-sized to rawLen,
// which is taken from the footer envelope. Pre-sizing eliminates the doubling
// growth io.ReadAll would otherwise do, which showed up as ~60% of cold-open
// CPU at 100M-row scale.
func decompressSegmentFooter(body []byte, rawLen int) ([]byte, error) {
	if rawLen < 0 {
		return nil, fmt.Errorf("segment footer raw length %d is negative", rawLen)
	}
	zr := flateReaderPool.Get().(io.ReadCloser)
	defer flateReaderPool.Put(zr)
	if err := zr.(flate.Resetter).Reset(bytes.NewReader(body), nil); err != nil {
		return nil, err
	}
	raw := make([]byte, rawLen)
	if _, err := io.ReadFull(zr, raw); err != nil {
		return nil, err
	}
	// Confirm the stream ended exactly at rawLen by trying to read one more byte.
	var tail [1]byte
	if n, err := zr.Read(tail[:]); err == nil && n != 0 {
		return nil, fmt.Errorf("segment footer expanded past declared raw length %d", rawLen)
	} else if err != nil && err != io.EOF {
		return nil, err
	}
	return raw, nil
}

func unmarshalSegmentMeta(data []byte) (SegmentMeta, error) {
	body, err := decodeSegmentFooter(data)
	if err != nil {
		return SegmentMeta{}, err
	}
	r := segmentMetaReader{data: body}
	meta := SegmentMeta{ID: SegmentID(r.readU64()), Rows: r.readU32(), PageRows: r.readU32()}
	cols := r.readU32()
	if r.err != nil {
		return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
	}
	if uint64(cols) > uint64(r.remaining()) {
		return SegmentMeta{}, fmt.Errorf("segment footer column count %d exceeds remaining metadata", cols)
	}
	meta.Columns = make([]ColumnMeta, int(cols))
	meta.lazyBody = body
	meta.lazyRanges = make([]byteRange, int(cols))
	meta.lazyOnce = make([]sync.Once, int(cols))
	meta.lazyErrs = make([]error, int(cols))
	for i := range cols {
		col := &meta.Columns[i]
		col.Name = r.readString()
		col.Type = r.readType()
		labels := r.readU32()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		if uint64(labels) > uint64(r.remaining()/4) {
			return SegmentMeta{}, fmt.Errorf("segment footer column %d label count %d exceeds remaining metadata", i, labels)
		}
		if labels != 0 {
			col.EnumLabels = make([]string, int(labels))
			for j := range col.EnumLabels {
				col.EnumLabels[j] = r.readString()
			}
		}
		col.Rows = r.readU32()
		col.NullCount = r.readU32()
		col.AllValid = r.readBool()
		col.AllNull = r.readBool()
		col.Stats.Bool = r.readBoolStats()
		col.Stats.Int32 = r.readInt32Stats()
		col.Stats.Int64 = r.readInt64Stats()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		pages := r.readU32()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		if uint64(pages) > uint64(r.remaining()/segmentPageEntryBytes) {
			return SegmentMeta{}, fmt.Errorf("segment footer column %d page count %d exceeds remaining metadata", i, pages)
		}
		if i == 0 {
			meta.PageRowCounts = make([]uint32, int(pages))
		}
		dirStart := r.pos
		if i == 0 {
			for j := range pages {
				meta.PageRowCounts[j] = binary.LittleEndian.Uint32(body[dirStart+int(j)*segmentPageEntryBytes+4:])
			}
		}
		r.skip(int(pages) * segmentPageEntryBytes)
		heavyLen := r.readU32()
		if r.err != nil {
			return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
		}
		if uint64(heavyLen) > uint64(r.remaining()) {
			return SegmentMeta{}, fmt.Errorf("segment footer column %d heavy length %d exceeds remaining metadata", i, heavyLen)
		}
		meta.lazyRanges[i] = byteRange{start: r.pos, end: r.pos + int(heavyLen), pageDir: dirStart, pageCount: int(pages)}
		r.skip(int(heavyLen))
	}
	if r.err != nil {
		return SegmentMeta{}, fmt.Errorf("segment footer truncated: %w", r.err)
	}
	if r.remaining() != 0 {
		return SegmentMeta{}, fmt.Errorf("segment footer has %d trailing bytes", r.remaining())
	}
	return meta, nil
}

// segmentMetaReader is a byte-slice cursor over the decompressed footer body.
// One bounds check per read is meaningfully faster than bytes.Reader at
// cold-open scale.
type segmentMetaReader struct {
	data []byte
	pos  int
	err  error
}

func (r *segmentMetaReader) remaining() int {
	return len(r.data) - r.pos
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
	if uint64(count) > uint64(r.remaining()/4) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]int32, int(count))
	for i := range out.Values {
		out.Values[i] = int32(r.readU32())
	}
	if r.err != nil {
		return nil
	}
	bloomCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(bloomCount) > uint64(r.remaining()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, int(bloomCount))
		for i := range out.HashBloom {
			out.HashBloom[i] = r.readU64()
		}
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
	if uint64(count) > uint64(r.remaining()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]int64, int(count))
	for i := range out.Values {
		out.Values[i] = int64(r.readU64())
	}
	if r.err != nil {
		return nil
	}
	bloomCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(bloomCount) > uint64(r.remaining()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, int(bloomCount))
		for i := range out.HashBloom {
			out.HashBloom[i] = r.readU64()
		}
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
	if uint64(bloomCount) > uint64(r.remaining()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out := &UUIDStats{}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, int(bloomCount))
		for i := range out.HashBloom {
			out.HashBloom[i] = r.readU64()
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
	if uint64(count) > uint64(r.remaining()/4) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	out.Values = make([]string, int(count))
	out.Counts = make([]uint32, int(count))
	for i := range out.Values {
		out.Values[i] = r.readString()
		out.Counts[i] = r.readU32()
	}
	bloomCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if uint64(bloomCount) > uint64(r.remaining()/8) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	if bloomCount != 0 {
		out.HashBloom = make([]uint64, int(bloomCount))
		for i := range out.HashBloom {
			out.HashBloom[i] = r.readU64()
		}
	}
	if r.err != nil {
		return nil
	}
	groupCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if groupCount != 0 {
		out.GroupSums = make(map[string][]int64, int(groupCount))
		for range groupCount {
			key := r.readString()
			n := r.readU32()
			if r.err != nil {
				return nil
			}
			if uint64(n) > uint64(r.remaining()/8) {
				r.err = io.ErrUnexpectedEOF
				return nil
			}
			sums := make([]int64, int(n))
			for i := range sums {
				sums[i] = int64(r.readU64())
			}
			out.GroupSums[key] = sums
		}
	}
	siblingCount := r.readU32()
	if r.err != nil {
		return nil
	}
	if siblingCount != 0 {
		out.GroupCounts = make(map[string]map[string][]int64, int(siblingCount))
		for range siblingCount {
			col := r.readString()
			valueCount := r.readU32()
			if r.err != nil {
				return nil
			}
			bySibling := make(map[string][]int64, int(valueCount))
			for range valueCount {
				value := r.readString()
				n := r.readU32()
				if r.err != nil {
					return nil
				}
				if uint64(n) > uint64(r.remaining()/8) {
					r.err = io.ErrUnexpectedEOF
					return nil
				}
				counts := make([]int64, int(n))
				for i := range counts {
					counts[i] = int64(r.readU64())
				}
				bySibling[value] = counts
			}
			out.GroupCounts[col] = bySibling
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
	if r.pos+int(n) > len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return ""
	}
	s := string(r.data[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s
}

func (r *segmentMetaReader) readByte() byte {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	b := r.data[r.pos]
	r.pos++
	return b
}

func (r *segmentMetaReader) readU32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.pos+4 > len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v
}

func (r *segmentMetaReader) readU64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.pos+8 > len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v
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
	writeU32(w, uint32(len(stats.HashBloom)))
	for _, word := range stats.HashBloom {
		writeU64(w, word)
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
	writeU32(w, uint32(len(stats.HashBloom)))
	for _, word := range stats.HashBloom {
		writeU64(w, word)
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
	// v2 GroupSums: deterministic key order so the on-disk bytes are stable.
	if stats.Truncated {
		writeU32(w, 0)
		writeU32(w, 0)
		return
	}
	keys := make([]string, 0, len(stats.GroupSums))
	for key := range stats.GroupSums {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeU32(w, uint32(len(keys)))
	for _, key := range keys {
		sums := stats.GroupSums[key]
		writeString(w, key)
		writeU32(w, uint32(len(sums)))
		for _, sum := range sums {
			writeU64(w, uint64(sum))
		}
	}
	// GroupCounts: outer keys (sibling col names) sorted, inner keys (sibling
	// values) sorted, values parallel to this column's Values.
	if len(stats.GroupCounts) == 0 {
		writeU32(w, 0)
		return
	}
	siblings := make([]string, 0, len(stats.GroupCounts))
	for col := range stats.GroupCounts {
		siblings = append(siblings, col)
	}
	sort.Strings(siblings)
	writeU32(w, uint32(len(siblings)))
	for _, col := range siblings {
		writeString(w, col)
		bySibling := stats.GroupCounts[col]
		values := make([]string, 0, len(bySibling))
		for v := range bySibling {
			values = append(values, v)
		}
		sort.Strings(values)
		writeU32(w, uint32(len(values)))
		for _, value := range values {
			writeString(w, value)
			counts := bySibling[value]
			writeU32(w, uint32(len(counts)))
			for _, count := range counts {
				writeU64(w, uint64(count))
			}
		}
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
