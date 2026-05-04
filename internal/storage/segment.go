// Package storage contains DripSQL's durable columnar storage primitives.
package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var segmentMagic = []byte("DRIPSEG1")

const (
	segmentVersion      uint16 = 1
	maxEncodedColumnLen uint64 = 1 << 30
)

// ColumnStats describes one encoded column in a segment.
type ColumnStats struct {
	Name       string
	Kind       vector.Kind
	Count      int
	HasMinMax  bool
	MinInt64   int64
	MaxInt64   int64
	EncodedLen int
}

// SegmentStats describes a written segment.
type SegmentStats struct {
	Rows    int
	Columns []ColumnStats
}

// Column returns segment column stats by name.
func (s SegmentStats) Column(name string) (ColumnStats, bool) {
	for _, col := range s.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnStats{}, false
}

// WriteSegment writes a small immutable columnar segment.
func WriteSegment(w io.Writer, batch vector.Batch) (SegmentStats, error) {
	sw := newSegmentWriter(w)
	if err := sw.WriteHeader(batch.Count, len(batch.Columns)); err != nil {
		return SegmentStats{}, err
	}

	stats := SegmentStats{Rows: batch.Count, Columns: make([]ColumnStats, 0, len(batch.Columns))}
	for _, col := range batch.Columns {
		encoded, colStats, err := EncodeColumn(col)
		if err != nil {
			return SegmentStats{}, err
		}

		if err := sw.WriteColumn(colStats, encoded); err != nil {
			return SegmentStats{}, err
		}

		stats.Columns = append(stats.Columns, colStats)
	}

	return stats, nil
}

// ReadSegment reads a segment written by WriteSegment.
func ReadSegment(r io.Reader) (vector.Batch, SegmentStats, error) {
	sr := newSegmentReader(r)
	rowCount, columnCount, err := sr.ReadHeader()
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}
	rows, err := checkedInt("segment row count", rowCount)
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}

	columns := make([]vector.Column, 0, columnCount)
	stats := SegmentStats{Rows: rows, Columns: make([]ColumnStats, 0, columnCount)}
	for range columnCount {
		col, colStats, err := sr.ReadColumn()
		if err != nil {
			return vector.Batch{}, SegmentStats{}, err
		}
		columns = append(columns, col)
		stats.Columns = append(stats.Columns, colStats)
	}

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}
	if batch.Count != rows {
		return vector.Batch{}, SegmentStats{}, fmt.Errorf("segment row count %d does not match decoded batch count %d", rowCount, batch.Count)
	}

	return batch, stats, nil
}

// ReadSegmentStats reads segment metadata and skips encoded column payloads without decoding vectors.
func ReadSegmentStats(r io.Reader) (SegmentStats, error) {
	sr := newSegmentReader(r)
	rowCount, columnCount, err := sr.ReadHeader()
	if err != nil {
		return SegmentStats{}, err
	}
	rows, err := checkedInt("segment row count", rowCount)
	if err != nil {
		return SegmentStats{}, err
	}

	stats := SegmentStats{Rows: rows, Columns: make([]ColumnStats, 0, columnCount)}
	for range columnCount {
		colStats, err := sr.ReadColumnStats()
		if err != nil {
			return SegmentStats{}, err
		}
		stats.Columns = append(stats.Columns, colStats)
	}

	return stats, nil
}

// ReadSegmentColumnStats reads metadata for one column without decoding vector payloads.
func ReadSegmentColumnStats(r io.Reader, column string) (ColumnStats, bool, error) {
	sr := newSegmentReader(r)
	rowCount, columnCount, err := sr.ReadHeader()
	if err != nil {
		return ColumnStats{}, false, err
	}
	if _, err := checkedInt("segment row count", rowCount); err != nil {
		return ColumnStats{}, false, err
	}

	for range columnCount {
		stats, err := sr.readColumnHeader()
		if err != nil {
			return ColumnStats{}, false, err
		}
		if stats.Name == column {
			return stats, true, nil
		}
		if err := sr.skip(stats.EncodedLen); err != nil {
			return ColumnStats{}, false, err
		}
	}

	return ColumnStats{}, false, nil
}

// CanSkipInt64Equal reports whether an equality predicate cannot match a column by min/max stats.
func CanSkipInt64Equal(stats ColumnStats, value int64) bool {
	return stats.Kind == vector.KindInt64 &&
		stats.HasMinMax &&
		(value < stats.MinInt64 || value > stats.MaxInt64)
}

// EncodeColumn encodes one vector column into DripSQL's current segment payload format.
func EncodeColumn(col vector.Column) ([]byte, ColumnStats, error) {
	stats := ColumnStats{Name: col.Name, Kind: col.Vector.Kind(), Count: col.Vector.Len()}

	switch values := col.Vector.(type) {
	case vector.Int64:
		min, max, ok := values.MinMax()
		stats.HasMinMax = ok
		stats.MinInt64 = min
		stats.MaxInt64 = max
		encoded := encodeInt64(values.Values)
		stats.EncodedLen = len(encoded)
		return encoded, stats, nil
	case vector.String:
		encoded, err := encodeString(values.Values)
		if err != nil {
			return nil, ColumnStats{}, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		stats.EncodedLen = len(encoded)
		return encoded, stats, nil
	default:
		return nil, ColumnStats{}, fmt.Errorf("segment encoding is not implemented for %s vectors", col.Vector.Kind())
	}
}

type segmentWriter struct {
	w io.Writer
}

func newSegmentWriter(w io.Writer) *segmentWriter {
	return &segmentWriter{w: w}
}

func (s *segmentWriter) WriteHeader(rows int, columns int) error {
	if _, err := s.w.Write(segmentMagic); err != nil {
		return err
	}
	if err := s.u16(segmentVersion); err != nil {
		return err
	}
	if err := s.u64(uint64(rows)); err != nil {
		return err
	}
	return s.u32(uint32(columns))
}

func (s *segmentWriter) WriteColumn(stats ColumnStats, encoded []byte) error {
	if err := s.string16(stats.Name); err != nil {
		return err
	}
	if err := s.u8(uint8(stats.Kind)); err != nil {
		return err
	}
	if err := s.u64(uint64(stats.Count)); err != nil {
		return err
	}
	if err := s.u64(uint64(len(encoded))); err != nil {
		return err
	}
	if err := s.writeColumnStats(stats); err != nil {
		return err
	}
	_, err := s.w.Write(encoded)
	return err
}

func (s *segmentWriter) writeColumnStats(stats ColumnStats) error {
	var minMax byte
	if stats.HasMinMax {
		minMax = 1
	}
	if err := s.u8(minMax); err != nil {
		return err
	}
	if err := s.u64(uint64(stats.MinInt64)); err != nil {
		return err
	}
	return s.u64(uint64(stats.MaxInt64))
}

func (s *segmentWriter) string16(value string) error {
	if len(value) > 1<<16-1 {
		return fmt.Errorf("string %q is too long", value)
	}
	if err := s.u16(uint16(len(value))); err != nil {
		return err
	}
	_, err := io.WriteString(s.w, value)
	return err
}

func (s *segmentWriter) u8(value uint8) error {
	var buf [1]byte
	buf[0] = value
	_, err := s.w.Write(buf[:])
	return err
}

func (s *segmentWriter) u16(value uint16) error {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], value)
	_, err := s.w.Write(buf[:])
	return err
}

func (s *segmentWriter) u32(value uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := s.w.Write(buf[:])
	return err
}

func (s *segmentWriter) u64(value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := s.w.Write(buf[:])
	return err
}

type segmentReader struct {
	r io.Reader
}

func newSegmentReader(r io.Reader) *segmentReader {
	return &segmentReader{r: r}
}

func (s *segmentReader) ReadHeader() (rows uint64, columns uint32, err error) {
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(s.r, magic); err != nil {
		return 0, 0, err
	}
	if !bytes.Equal(magic, segmentMagic) {
		return 0, 0, fmt.Errorf("invalid segment magic %q", string(magic))
	}

	version, err := s.u16()
	if err != nil {
		return 0, 0, err
	}
	if version != segmentVersion {
		return 0, 0, fmt.Errorf("unsupported segment version %d", version)
	}

	rows, err = s.u64()
	if err != nil {
		return 0, 0, err
	}
	columns, err = s.u32()
	if err != nil {
		return 0, 0, err
	}
	return rows, columns, nil
}

func (s *segmentReader) ReadColumn() (vector.Column, ColumnStats, error) {
	stats, err := s.readColumnHeader()
	if err != nil {
		return vector.Column{}, ColumnStats{}, err
	}

	encoded := make([]byte, stats.EncodedLen)
	if _, err := io.ReadFull(s.r, encoded); err != nil {
		return vector.Column{}, ColumnStats{}, err
	}

	values, err := decodeValues(stats.Kind, encoded, stats.Count)
	if err != nil {
		return vector.Column{}, ColumnStats{}, err
	}

	return vector.Column{Name: stats.Name, Vector: values}, stats, nil
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
	if n == 0 {
		return nil
	}
	if seeker, ok := s.r.(io.Seeker); ok {
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
	name, err := s.string16()
	if err != nil {
		return ColumnStats{}, err
	}
	kindByte, err := s.u8()
	if err != nil {
		return ColumnStats{}, err
	}
	count, err := s.u64()
	if err != nil {
		return ColumnStats{}, err
	}
	encodedLen, err := s.u64()
	if err != nil {
		return ColumnStats{}, err
	}
	columnCount, err := checkedInt(fmt.Sprintf("column %q count", name), count)
	if err != nil {
		return ColumnStats{}, err
	}
	if encodedLen > maxEncodedColumnLen {
		return ColumnStats{}, fmt.Errorf("encoded column %q is too large: %d", name, encodedLen)
	}
	columnEncodedLen, err := checkedInt(fmt.Sprintf("column %q encoded length", name), encodedLen)
	if err != nil {
		return ColumnStats{}, err
	}

	stats := ColumnStats{Name: name, Kind: vector.Kind(kindByte), Count: columnCount, EncodedLen: columnEncodedLen}
	if err := s.readColumnStats(&stats); err != nil {
		return ColumnStats{}, err
	}
	return stats, nil
}

func (s *segmentReader) readColumnStats(stats *ColumnStats) error {
	minMax, err := s.u8()
	if err != nil {
		return err
	}
	stats.HasMinMax = minMax != 0

	min, err := s.u64()
	if err != nil {
		return err
	}
	max, err := s.u64()
	if err != nil {
		return err
	}
	stats.MinInt64 = int64(min)
	stats.MaxInt64 = int64(max)
	return nil
}

func (s *segmentReader) string16() (string, error) {
	length, err := s.u16()
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(s.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (s *segmentReader) u8() (uint8, error) {
	var buf [1]byte
	_, err := io.ReadFull(s.r, buf[:])
	return buf[0], err
}

func (s *segmentReader) u16() (uint16, error) {
	var buf [2]byte
	_, err := io.ReadFull(s.r, buf[:])
	return binary.LittleEndian.Uint16(buf[:]), err
}

func (s *segmentReader) u32() (uint32, error) {
	var buf [4]byte
	_, err := io.ReadFull(s.r, buf[:])
	return binary.LittleEndian.Uint32(buf[:]), err
}

func (s *segmentReader) u64() (uint64, error) {
	var buf [8]byte
	_, err := io.ReadFull(s.r, buf[:])
	return binary.LittleEndian.Uint64(buf[:]), err
}

func encodeInt64(values []int64) []byte {
	out := make([]byte, 0, len(values)*8)
	for _, value := range values {
		out = binary.LittleEndian.AppendUint64(out, uint64(value))
	}
	return out
}

func encodeString(values []string) ([]byte, error) {
	size := 0
	for _, value := range values {
		if len(value) > 1<<32-1 {
			return nil, fmt.Errorf("string value is too long")
		}
		size += 4 + len(value)
	}

	out := make([]byte, 0, size)
	for _, value := range values {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	return out, nil
}

func decodeValues(kind vector.Kind, encoded []byte, count int) (vector.Vector, error) {
	switch kind {
	case vector.KindInt64:
		values, err := decodeInt64(encoded, count)
		if err != nil {
			return nil, err
		}
		return vector.FromInt64(values), nil
	case vector.KindString:
		values, err := decodeString(encoded, count)
		if err != nil {
			return nil, err
		}
		return vector.FromString(values), nil
	default:
		return nil, fmt.Errorf("unsupported vector kind %s", kind)
	}
}

func decodeInt64(encoded []byte, count int) ([]int64, error) {
	if len(encoded) != count*8 {
		return nil, fmt.Errorf("invalid int64 encoded length %d for count %d", len(encoded), count)
	}

	values := make([]int64, count)
	for i := range values {
		values[i] = int64(binary.LittleEndian.Uint64(encoded[i*8:]))
	}
	return values, nil
}

func decodeString(encoded []byte, count int) ([]string, error) {
	values := make([]string, count)
	offset := 0
	for i := range values {
		if len(encoded)-offset < 4 {
			return nil, fmt.Errorf("short string length at row %d", i)
		}
		length := int(binary.LittleEndian.Uint32(encoded[offset:]))
		offset += 4
		if len(encoded)-offset < length {
			return nil, fmt.Errorf("short string data at row %d", i)
		}
		values[i] = string(encoded[offset : offset+length])
		offset += length
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("trailing string bytes: %d", len(encoded)-offset)
	}
	return values, nil
}

func checkedInt(name string, value uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, fmt.Errorf("%s overflows int: %d", name, value)
	}
	return int(value), nil
}
