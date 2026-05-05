package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	segmentHeaderLen     = len(segmentMagic) + 2 + 8 + 4
	columnFixedHeaderLen = 1 + 8 + 8 + 1 + 8 + 8
)

type segmentWriter struct {
	w io.Writer
}

func newSegmentWriter(w io.Writer) *segmentWriter {
	return &segmentWriter{w: w}
}

func (s *segmentWriter) WriteHeader(rows int, columns int) error {
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
	return writeFull(s.w, buf[:])
}

func (s *segmentWriter) WriteColumn(stats ColumnStats, encoded []byte) error {
	if err := s.writeColumnHeader(stats, uint64(len(encoded))); err != nil {
		return err
	}
	return writeFull(s.w, encoded)
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
	r io.Reader
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
