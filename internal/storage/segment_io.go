package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	segmentHeaderLen     = len(segmentMagic) + 2 + 8 + 4 + 8
	columnFixedHeaderLen = 1 + 8 + 8 + 1 + 8 + 8
	segmentFooterMagic   = "DRIPFTR1"
	footerTrailerLen     = 8 + len(segmentFooterMagic)
)

type columnFooterEntry struct {
	Name   string
	Offset uint64
}

type countingWriter struct {
	w      io.Writer
	offset uint64
}

func (w *countingWriter) Write(buf []byte) (int, error) {
	n, err := w.w.Write(buf)
	w.offset += uint64(n)
	return n, err
}

type segmentWriter struct {
	w *countingWriter
}

func newSegmentWriter(w io.Writer) *segmentWriter {
	return &segmentWriter{w: &countingWriter{w: w}}
}

func (s *segmentWriter) WriteHeader(rows int, columns int, segmentLen uint64) error {
	if rows < 0 {
		return fmt.Errorf("negative row count %d", rows)
	}
	if columns < 0 {
		return fmt.Errorf("negative column count %d", columns)
	}
	if uint64(columns) > uint64(^uint32(0)) {
		return fmt.Errorf("column count %d overflows uint32", columns)
	}

	var buf [segmentHeaderLen]byte
	offset := copy(buf[:], segmentMagic)
	binary.LittleEndian.PutUint16(buf[offset:], segmentVersion)
	offset += 2
	binary.LittleEndian.PutUint64(buf[offset:], uint64(rows))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(columns))
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], segmentLen)
	return writeFull(s.w, buf[:])
}

func (s *segmentWriter) WriteColumn(stats ColumnStats, encoded []byte) error {
	if err := s.writeColumnHeader(stats, uint64(len(encoded))); err != nil {
		return err
	}
	return writeFull(s.w, encoded)
}

func (s *segmentWriter) WriteFooter(entries []columnFooterEntry) error {
	footer := binary.LittleEndian.AppendUint32(nil, uint32(len(entries)))
	for _, entry := range entries {
		var err error
		footer, err = appendString16(footer, entry.Name)
		if err != nil {
			return err
		}
		footer = binary.LittleEndian.AppendUint64(footer, entry.Offset)
	}
	if err := writeFull(s.w, footer); err != nil {
		return err
	}

	var trailer [footerTrailerLen]byte
	binary.LittleEndian.PutUint64(trailer[:], uint64(len(footer)))
	copy(trailer[8:], segmentFooterMagic)
	return writeFull(s.w, trailer[:])
}

func (s *segmentWriter) writeColumnHeader(stats ColumnStats, encodedLen uint64) error {
	if err := writeString16(s.w, stats.Name); err != nil {
		return err
	}

	var buf [columnFixedHeaderLen]byte
	offset := 0
	buf[offset] = uint8(stats.Kind)
	offset++
	binary.LittleEndian.PutUint64(buf[offset:], uint64(stats.Count))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], encodedLen)
	offset += 8
	if stats.HasMinMax {
		buf[offset] = 1
	}
	offset++
	binary.LittleEndian.PutUint64(buf[offset:], uint64(stats.MinInt64))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], uint64(stats.MaxInt64))
	return writeFull(s.w, buf[:])
}

type segmentReader struct {
	r          io.Reader
	start      int64
	end        int64
	hasBounds  bool
	footerRead bool
}

func newSegmentReader(r io.Reader) *segmentReader {
	return &segmentReader{r: r}
}

func (s *segmentReader) readHeaderChecked() (rows int, cols int, err error) {
	rowCount, columnCount, err := s.ReadHeader()
	if err != nil {
		return 0, 0, err
	}
	rows, err = checkedInt("segment row count", rowCount)
	if err != nil {
		return 0, 0, err
	}
	cols, err = checkedInt("segment column count", uint64(columnCount))
	if err != nil {
		return 0, 0, err
	}
	return rows, cols, nil
}

func (s *segmentReader) ReadHeader() (rows uint64, columns uint32, err error) {
	if seeker, ok := s.r.(io.Seeker); ok {
		start, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, 0, err
		}
		s.start = start
	}

	var buf [segmentHeaderLen]byte
	if err := readFull(s.r, buf[:]); err != nil {
		return 0, 0, err
	}
	if string(buf[:len(segmentMagic)]) != segmentMagic {
		return 0, 0, fmt.Errorf("invalid segment magic %q", string(buf[:len(segmentMagic)]))
	}

	offset := len(segmentMagic)
	version := binary.LittleEndian.Uint16(buf[offset:])
	offset += 2
	if version != segmentVersion {
		return 0, 0, fmt.Errorf("unsupported segment version %d", version)
	}

	rows = binary.LittleEndian.Uint64(buf[offset:])
	offset += 8
	columns = binary.LittleEndian.Uint32(buf[offset:])
	offset += 4
	segmentLen := binary.LittleEndian.Uint64(buf[offset:])
	if segmentLen < uint64(segmentHeaderLen+footerTrailerLen) {
		return 0, 0, fmt.Errorf("invalid segment length %d", segmentLen)
	}
	if _, ok := s.r.(io.Seeker); ok {
		s.end = s.start + int64(segmentLen)
		s.hasBounds = true
	}
	return rows, columns, nil
}

func (s *segmentReader) ReadColumnStats() (ColumnStats, error) {
	stats, err := s.readColumnHeader()
	if err != nil {
		return ColumnStats{}, err
	}
	if err := s.skip(stats.EncodedLen); err != nil {
		return ColumnStats{}, err
	}
	return stats, nil
}

func (s *segmentReader) skip(n int) error {
	if n < 0 {
		return fmt.Errorf("negative skip length %d", n)
	}
	if n == 0 {
		return nil
	}
	if seeker, ok := s.r.(io.Seeker); ok {
		// Seek past EOF succeeds for files and byte readers, so check bounds before advancing.
		current, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		end, err := seeker.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		if current+int64(n) > end {
			_, _ = seeker.Seek(current, io.SeekStart)
			return io.ErrUnexpectedEOF
		}
		_, err = seeker.Seek(current+int64(n), io.SeekStart)
		return err
	}
	_, err := io.CopyN(io.Discard, s.r, int64(n))
	return err
}

func (s *segmentReader) readColumnHeader() (ColumnStats, error) {
	name, err := readString16(s.r)
	if err != nil {
		return ColumnStats{}, err
	}

	var buf [columnFixedHeaderLen]byte
	if err := readFull(s.r, buf[:]); err != nil {
		return ColumnStats{}, err
	}
	return decodeColumnFixedHeader(name, buf[:])
}

func (s *segmentReader) readFooter(expectedColumns int) ([]columnFooterEntry, bool, error) {
	seeker, ok := s.r.(io.ReadSeeker)
	if !ok || !s.hasBounds {
		return nil, false, nil
	}
	if s.end-s.start < int64(segmentHeaderLen+footerTrailerLen) {
		return nil, true, io.ErrUnexpectedEOF
	}

	fileEnd, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, true, err
	}
	if s.end > fileEnd {
		return nil, true, io.ErrUnexpectedEOF
	}
	trailerStart := s.end - int64(footerTrailerLen)
	if _, err := seeker.Seek(trailerStart, io.SeekStart); err != nil {
		return nil, true, err
	}

	var trailer [footerTrailerLen]byte
	if err := readFull(s.r, trailer[:]); err != nil {
		return nil, true, err
	}
	if string(trailer[8:]) != segmentFooterMagic {
		return nil, true, fmt.Errorf("invalid segment footer magic %q", string(trailer[8:]))
	}
	footerLen := binary.LittleEndian.Uint64(trailer[:])
	footerStart := trailerStart - int64(footerLen)
	if footerStart < s.start+int64(segmentHeaderLen) {
		return nil, true, io.ErrUnexpectedEOF
	}
	if _, err := seeker.Seek(footerStart, io.SeekStart); err != nil {
		return nil, true, err
	}
	footerSize, err := checkedInt("segment footer length", footerLen)
	if err != nil {
		return nil, true, err
	}
	footer := make([]byte, footerSize)
	if err := readFull(s.r, footer); err != nil {
		return nil, true, err
	}
	entries, err := parseFooterPayload(footer, expectedColumns)
	if err != nil {
		return nil, true, err
	}
	s.footerRead = true
	return entries, true, nil
}

func (s *segmentReader) finishSegment(expectedColumns int) error {
	if _, ok := s.r.(io.ReadSeeker); ok && s.hasBounds {
		if !s.footerRead {
			if _, _, err := s.readFooter(expectedColumns); err != nil {
				return err
			}
		}
		return s.seekToSegmentEnd()
	}
	return s.readFooterSequential(expectedColumns)
}

func (s *segmentReader) seekToSegmentOffset(offset uint64) error {
	seeker, ok := s.r.(io.Seeker)
	if !ok || !s.hasBounds {
		return fmt.Errorf("segment reader does not support seeking")
	}
	if offset > uint64(s.end-s.start) {
		return io.ErrUnexpectedEOF
	}
	_, err := seeker.Seek(s.start+int64(offset), io.SeekStart)
	return err
}

func (s *segmentReader) seekToSegmentEnd() error {
	seeker, ok := s.r.(io.Seeker)
	if !ok || !s.hasBounds {
		return nil
	}
	fileEnd, err := seeker.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if s.end > fileEnd {
		return io.ErrUnexpectedEOF
	}
	_, err = seeker.Seek(s.end, io.SeekStart)
	return err
}

func (s *segmentReader) readFooterSequential(expectedColumns int) error {
	var countBuf [4]byte
	if err := readFull(s.r, countBuf[:]); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(countBuf[:])
	if int(count) != expectedColumns {
		return fmt.Errorf("segment footer column count %d does not match header count %d", count, expectedColumns)
	}
	footerLen := uint64(4)
	for range count {
		name, err := readString16(s.r)
		if err != nil {
			return err
		}
		footerLen += uint64(2 + len(name))
		var offsetBuf [8]byte
		if err := readFull(s.r, offsetBuf[:]); err != nil {
			return err
		}
		footerLen += 8
	}

	var trailer [footerTrailerLen]byte
	if err := readFull(s.r, trailer[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint64(trailer[:]) != footerLen {
		return fmt.Errorf("segment footer length mismatch")
	}
	if string(trailer[8:]) != segmentFooterMagic {
		return fmt.Errorf("invalid segment footer magic %q", string(trailer[8:]))
	}
	return nil
}

func parseFooterPayload(footer []byte, expectedColumns int) ([]columnFooterEntry, error) {
	offset := 0
	countBytes, err := readBytes(footer, &offset, 4)
	if err != nil {
		return nil, err
	}
	count := binary.LittleEndian.Uint32(countBytes)
	if int(count) != expectedColumns {
		return nil, fmt.Errorf("segment footer column count %d does not match header count %d", count, expectedColumns)
	}
	entries := make([]columnFooterEntry, 0, count)
	for range count {
		nameBytes, err := readString16Bytes(footer, &offset)
		if err != nil {
			return nil, err
		}
		offsetBytes, err := readBytes(footer, &offset, 8)
		if err != nil {
			return nil, err
		}
		entries = append(entries, columnFooterEntry{Name: string(nameBytes), Offset: binary.LittleEndian.Uint64(offsetBytes)})
	}
	if offset != len(footer) {
		return nil, fmt.Errorf("trailing segment footer bytes: %d", len(footer)-offset)
	}
	return entries, nil
}

func decodeColumnFixedHeader(name string, fixed []byte) (ColumnStats, error) {
	if len(fixed) != columnFixedHeaderLen {
		return ColumnStats{}, io.ErrUnexpectedEOF
	}
	offset := 0
	kindByte := fixed[offset]
	offset++
	count := binary.LittleEndian.Uint64(fixed[offset:])
	offset += 8
	encodedLen := binary.LittleEndian.Uint64(fixed[offset:])
	offset += 8
	hasMinMax := fixed[offset] != 0
	offset++
	min := int64(binary.LittleEndian.Uint64(fixed[offset:]))
	offset += 8
	max := int64(binary.LittleEndian.Uint64(fixed[offset:]))

	countLabel := "column count"
	encodedLenLabel := "column encoded length"
	if name != "" {
		countLabel = fmt.Sprintf("column %q count", name)
		encodedLenLabel = fmt.Sprintf("column %q encoded length", name)
	}

	columnCount, err := checkedInt(countLabel, count)
	if err != nil {
		return ColumnStats{}, err
	}
	if encodedLen > maxEncodedColumnLen {
		if name == "" {
			return ColumnStats{}, fmt.Errorf("encoded column is too large: %d", encodedLen)
		}
		return ColumnStats{}, fmt.Errorf("encoded column %q is too large: %d", name, encodedLen)
	}
	columnEncodedLen, err := checkedInt(encodedLenLabel, encodedLen)
	if err != nil {
		return ColumnStats{}, err
	}

	return ColumnStats{
		Name:       name,
		Kind:       vector.Kind(kindByte),
		Count:      columnCount,
		HasMinMax:  hasMinMax,
		MinInt64:   min,
		MaxInt64:   max,
		EncodedLen: columnEncodedLen,
	}, nil
}

func writeString16(w io.Writer, value string) error {
	if len(value) > 1<<16-1 {
		return fmt.Errorf("string %q is too long", value)
	}

	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], uint16(len(value)))
	if err := writeFull(w, buf[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, value)
	return err
}

func appendString16(buf []byte, value string) ([]byte, error) {
	if len(value) > 1<<16-1 {
		return nil, fmt.Errorf("string %q is too long", value)
	}
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(value)))
	buf = append(buf, value...)
	return buf, nil
}

func footerEntryLen(name string) (uint64, error) {
	if len(name) > 1<<16-1 {
		return 0, fmt.Errorf("string %q is too long", name)
	}
	return uint64(2 + len(name) + 8), nil
}

func readString16(r io.Reader) (string, error) {
	var lengthBuf [2]byte
	if err := readFull(r, lengthBuf[:]); err != nil {
		return "", err
	}
	length := binary.LittleEndian.Uint16(lengthBuf[:])
	buf := make([]byte, length)
	if err := readFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeFull(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		buf = buf[n:]
	}
	return nil
}

func readFull(r io.Reader, buf []byte) error {
	_, err := io.ReadFull(r, buf)
	return err
}
