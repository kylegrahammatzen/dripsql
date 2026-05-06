package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type byteViewer interface {
	View(off int64, n int) ([]byte, bool)
}

// Reader reads one immutable segment from an io.ReaderAt source.
type Reader struct {
	r      io.ReaderAt
	view   byteViewer
	offset int64
	size   int64
	dir    Directory
}

// OpenSegment opens a segment from source using a segment-relative offset and size.
func OpenSegment(r io.ReaderAt, offset int64, size int64, scratch []byte) (Reader, []byte, error) {
	if r == nil {
		return Reader{}, scratch, fmt.Errorf("nil segment source")
	}
	if offset < 0 {
		return Reader{}, scratch, fmt.Errorf("negative segment offset %d", offset)
	}
	if size < minSegmentSize {
		return Reader{}, scratch, fmt.Errorf("segment size %d is too small", size)
	}
	segmentEnd, err := checkedAddInt64("segment end", offset, size)
	if err != nil {
		return Reader{}, scratch, err
	}
	view, _ := r.(byteViewer)

	var header [headerLen]byte
	if err := readAtFull(r, header[:], offset); err != nil {
		return Reader{}, scratch, err
	}
	rows, columnCount, segmentLen, err := decodeHeader(header[:])
	if err != nil {
		return Reader{}, scratch, err
	}
	if segmentLen != size {
		return Reader{}, scratch, fmt.Errorf("segment length %d does not match open size %d", segmentLen, size)
	}

	var trailer [footerTailLen]byte
	trailerOffset := segmentEnd - footerTailLen64
	if err := readAtFull(r, trailer[:], trailerOffset); err != nil {
		return Reader{}, scratch, err
	}
	footerLen, footerChecksum, err := decodeFooterTrailer(trailer[:])
	if err != nil {
		return Reader{}, scratch, err
	}
	if footerLen > uint64(size-minSegmentSize) {
		return Reader{}, scratch, fmt.Errorf("footer length %d exceeds segment data area", footerLen)
	}
	footerBytes, err := checkedInt("footer length", footerLen)
	if err != nil {
		return Reader{}, scratch, err
	}
	footerStart := size - footerTailLen64 - int64(footerBytes)
	if footerStart < headerLen64 {
		return Reader{}, scratch, fmt.Errorf("footer starts before payload area")
	}

	footer, nextScratch, _, err := readRange(r, view, offset+footerStart, footerBytes, scratch)
	if err != nil {
		return Reader{}, scratch, err
	}
	scratch = nextScratch
	if got := crc32.Checksum(footer, crc32cTable); got != footerChecksum {
		return Reader{}, scratch, fmt.Errorf("footer checksum mismatch: got %08x want %08x", got, footerChecksum)
	}
	dir, err := decodeFooter(footer, columnCount, rows, footerStart)
	if err != nil {
		return Reader{}, scratch, err
	}

	return Reader{r: r, view: view, offset: offset, size: size, dir: dir}, scratch, nil
}

// OpenSegmentBytes opens a segment backed by an immutable byte slice.
func OpenSegmentBytes(data []byte) (Reader, error) {
	reader, _, err := OpenSegment(byteSource(data), 0, int64(len(data)), nil)
	return reader, err
}

// Directory returns the parsed segment directory.
func (r Reader) Directory() Directory {
	return Directory{Rows: r.dir.Rows, Columns: cloneColumns(r.dir.Columns)}
}

// Stats returns segment metadata in the same shape as WriteSegment.
func (r Reader) Stats() SegmentStats {
	return SegmentStats{Rows: r.dir.Rows, Columns: cloneColumns(r.dir.Columns)}
}

// Column returns metadata for one column.
func (r Reader) Column(name string) (Column, bool) {
	return r.dir.Column(name)
}

// ReadColumn decodes one column by name.
func (r Reader) ReadColumn(name string, scratch []byte) (vector.Column, []byte, error) {
	col, ok := r.dir.Column(name)
	if !ok {
		return vector.Column{}, scratch, fmt.Errorf("missing column %q", name)
	}
	return r.readColumnByMeta(col, scratch)
}

// ReadPages decodes the page directory for one column, if present.
func (r Reader) ReadPages(name string, scratch []byte) ([]Page, []byte, error) {
	col, ok := r.dir.Column(name)
	if !ok {
		return nil, scratch, fmt.Errorf("missing column %q", name)
	}
	if col.Pages.Offset == 0 && col.Pages.Bytes == 0 {
		return nil, scratch, nil
	}
	pageBytes, err := checkedInt("page directory length", uint64(col.Pages.Bytes))
	if err != nil {
		return nil, scratch, err
	}
	payload, nextScratch, _, err := readRange(r.r, r.view, r.offset+col.Pages.Offset, pageBytes, scratch)
	if err != nil {
		return nil, scratch, err
	}
	pages, err := decodePageDirectory(payload, col)
	if err != nil {
		return nil, scratch, fmt.Errorf("decode pages for column %q: %w", name, err)
	}
	return pages, nextScratch, nil
}

func (r Reader) readColumnByMeta(col Column, scratch []byte) (vector.Column, []byte, error) {
	payload, nextScratch, stable, err := r.readPayload(col, scratch)
	if err != nil {
		return vector.Column{}, scratch, err
	}
	scratch = nextScratch
	decoded, err := decodeColumn(col, payload, stable)
	if err != nil {
		return vector.Column{}, scratch, fmt.Errorf("decode column %q: %w", col.Name, err)
	}
	return vector.Column{Name: col.Name, Vector: decoded}, scratch, nil
}

// ReadBatch decodes all columns in directory order.
func (r Reader) ReadBatch(scratch []byte) (vector.Batch, []byte, error) {
	columns := make([]vector.Column, 0, len(r.dir.Columns))
	for _, meta := range r.dir.Columns {
		col, nextScratch, err := r.readColumnByMeta(meta, scratch)
		if err != nil {
			return vector.Batch{}, scratch, err
		}
		scratch = nextScratch
		columns = append(columns, col)
	}
	batch, err := vector.NewBatch(columns...)
	if err != nil {
		return vector.Batch{}, scratch, err
	}
	if batch.Count != r.dir.Rows {
		panic(fmt.Sprintf("storage: decoded batch row count %d != directory rows %d", batch.Count, r.dir.Rows))
	}
	return batch, scratch, nil
}

func (r Reader) readPayload(col Column, scratch []byte) ([]byte, []byte, bool, error) {
	// Safe by decodeFooter validation and OpenSegment's checked segment-end arithmetic.
	payloadBytes := int(col.Payload.Bytes)
	absOffset := r.offset + col.Payload.Offset
	return readRange(r.r, r.view, absOffset, payloadBytes, scratch)
}

func readRange(r io.ReaderAt, view byteViewer, offset int64, n int, scratch []byte) ([]byte, []byte, bool, error) {
	if n == 0 {
		return nil, scratch, true, nil
	}
	if view != nil {
		if data, ok := view.View(offset, n); ok {
			return data, scratch, true, nil
		}
	}
	if cap(scratch) < n {
		scratch = make([]byte, n)
	}
	buf := scratch[:n]
	if err := readAtFull(r, buf, offset); err != nil {
		return nil, scratch, false, err
	}
	// buf aliases scratch. Callers must not reuse scratch until decode is complete.
	// stable=false tells decoders to copy any returned vector data that would otherwise alias it.
	return buf, scratch, false, nil
}

func decodeHeader(buf []byte) (rows int, columns int, segmentLen int64, err error) {
	if !equalMagic(buf, segmentMagic) {
		return 0, 0, 0, fmt.Errorf("invalid segment magic %x", buf[:len(segmentMagic)])
	}
	off := len(segmentMagic)
	version := binary.LittleEndian.Uint16(buf[off:])
	off += 2
	if version != formatVersion {
		return 0, 0, 0, fmt.Errorf("unsupported segment version %d", version)
	}
	rowCount := binary.LittleEndian.Uint64(buf[off:])
	off += 8
	rows, err = checkedInt("segment row count", rowCount)
	if err != nil {
		return 0, 0, 0, err
	}
	columnCount := binary.LittleEndian.Uint32(buf[off:])
	off += 4
	columns, err = checkedInt("segment column count", uint64(columnCount))
	if err != nil {
		return 0, 0, 0, err
	}
	segmentLen64 := binary.LittleEndian.Uint64(buf[off:])
	segmentLen, err = checkedInt64("segment length", segmentLen64)
	if err != nil {
		return 0, 0, 0, err
	}
	if segmentLen < minSegmentSize {
		return 0, 0, 0, fmt.Errorf("invalid segment length %d", segmentLen)
	}
	return rows, columns, segmentLen, nil
}

func decodeFooterTrailer(buf []byte) (footerLen uint64, checksum uint32, err error) {
	footerLen = binary.LittleEndian.Uint64(buf[:])
	checksum = binary.LittleEndian.Uint32(buf[8:])
	if !equalMagic(buf[12:], footerMagic) {
		return 0, 0, fmt.Errorf("invalid footer magic %x", buf[12:])
	}
	return footerLen, checksum, nil
}

func equalMagic(buf []byte, magic string) bool {
	if len(buf) < len(magic) {
		return false
	}
	for i := 0; i < len(magic); i++ {
		if buf[i] != magic[i] {
			return false
		}
	}
	return true
}

func decodeColumn(col Column, payload []byte, stable bool) (vector.Vector, error) {
	switch col.Kind {
	case vector.KindInt64:
		return decodeInt64Payload(col.Codec, payload, col.Count)
	case vector.KindString:
		switch col.Codec {
		case CodecPlain:
			return decodeStringPlain(payload, col.Count, stable)
		case CodecDictionary:
			return decodeStringDictionary(payload, col.Count, stable)
		case CodecStringPrefix:
			return decodeStringPrefix(payload, col.Count)
		case CodecStringTemplate:
			return decodeStringTemplate(payload, col.Count)
		default:
			return nil, fmt.Errorf("unsupported codec %s", col.Codec)
		}
	default:
		return nil, fmt.Errorf("unsupported kind %s", col.Kind)
	}
}

func readAtFull(r io.ReaderAt, buf []byte, off int64) error {
	if len(buf) == 0 {
		return nil
	}
	n, err := r.ReadAt(buf, off)
	if n == len(buf) {
		return nil
	}
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return io.ErrUnexpectedEOF
}

type byteSource []byte

func (s byteSource) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative read offset %d: %w", off, io.ErrUnexpectedEOF)
	}
	if off >= int64(len(s)) {
		return 0, io.EOF
	}
	n := copy(p, s[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s byteSource) View(off int64, n int) ([]byte, bool) {
	if off < 0 || n < 0 {
		return nil, false
	}
	if off > int64(len(s)) || int64(n) > int64(len(s))-off {
		return nil, false
	}
	return s[off : off+int64(n)], true
}
