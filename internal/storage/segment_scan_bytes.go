package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// SelectSegmentInt64EqualBytes appends matching row indexes from one encoded segment byte slice.
func SelectSegmentInt64EqualBytes(data []byte, column string, value int64, scratch []uint32) ([]uint32, bool, error) {
	stats, payload, found, err := findColumnPayloadBytes(data, column)
	if err != nil {
		return scratch, found, err
	}
	if !found {
		return scratch, false, nil
	}
	if stats.Kind != vector.KindInt64 {
		return scratch, true, fmt.Errorf("column %q is %s, want int64", column, stats.Kind)
	}
	if canSkipInt64Equal(stats, value) {
		return scratch[:0], true, nil
	}

	selected, err := selectInt64EqualPayload(payload, stats, value, scratch)
	if err != nil {
		return scratch, true, err
	}
	return selected, found, nil
}

// CountSegmentInt64EqualBytes counts matching rows from one encoded segment byte slice.
func CountSegmentInt64EqualBytes(data []byte, column string, value int64) (int, bool, error) {
	stats, payload, found, err := findColumnPayloadBytes(data, column)
	if err != nil || !found {
		return 0, found, err
	}
	if stats.Kind != vector.KindInt64 {
		return 0, true, fmt.Errorf("column %q is %s, want int64", column, stats.Kind)
	}
	if canSkipInt64Equal(stats, value) {
		return 0, true, nil
	}

	count, err := countInt64EqualPayload(payload, stats, value)
	if err != nil {
		return 0, true, err
	}
	return count, true, nil
}

// SelectSegmentStringEqualBytes appends matching row indexes from one encoded string column byte slice.
func SelectSegmentStringEqualBytes(data []byte, column string, value string, scratch []uint32) ([]uint32, bool, error) {
	stats, payload, found, err := findColumnPayloadBytes(data, column)
	if err != nil {
		return scratch, found, err
	}
	if !found {
		return scratch, false, nil
	}
	if stats.Kind != vector.KindString {
		return scratch, true, fmt.Errorf("column %q is %s, want string", column, stats.Kind)
	}

	selected, err := selectStringEqualPayload(payload, stats, value, scratch)
	if err != nil {
		return scratch, true, err
	}
	return selected, true, nil
}

// CountSegmentStringEqualBytes counts matching rows from one encoded string column byte slice.
func CountSegmentStringEqualBytes(data []byte, column string, value string) (int, bool, error) {
	stats, payload, found, err := findColumnPayloadBytes(data, column)
	if err != nil || !found {
		return 0, found, err
	}
	if stats.Kind != vector.KindString {
		return 0, true, fmt.Errorf("column %q is %s, want string", column, stats.Kind)
	}

	count, err := countStringEqualPayload(payload, stats, value)
	if err != nil {
		return 0, true, err
	}
	return count, true, nil
}

// GroupSegmentStringCountsBytes adds per-value row counts from one encoded string column into counts.
func GroupSegmentStringCountsBytes(data []byte, column string, counts map[string]int) (bool, error) {
	stats, payload, found, err := findColumnPayloadBytes(data, column)
	if err != nil || !found {
		return found, err
	}
	if stats.Kind != vector.KindString {
		return true, fmt.Errorf("column %q is %s, want string", column, stats.Kind)
	}
	if err := groupStringCountsPayload(payload, stats, counts); err != nil {
		return true, err
	}
	return true, nil
}

// CountSegmentInt64EqualAt reads only the requested int64 column from one encoded segment and counts matches.
func CountSegmentInt64EqualAt(r io.ReaderAt, segmentOffset int64, segmentBytes int64, column string, value int64, scratch []byte) (count int, found bool, outScratch []byte, bytesRead int64, err error) {
	loc, scratch, found, bytesRead, err := locateColumnPayloadAt(r, segmentOffset, segmentBytes, column, scratch)
	if err != nil || !found {
		return 0, found, scratch, bytesRead, err
	}
	if loc.stats.Kind != vector.KindInt64 {
		return 0, true, scratch, bytesRead, fmt.Errorf("column %q is %s, want int64", column, loc.stats.Kind)
	}
	if canSkipInt64Equal(loc.stats, value) {
		return 0, true, scratch, bytesRead, nil
	}

	payload, scratch, payloadBytesRead, err := readColumnPayloadAt(r, loc, scratch)
	bytesRead += payloadBytesRead
	if err != nil {
		return 0, true, scratch, bytesRead, err
	}
	count, err = countInt64EqualPayload(payload, loc.stats, value)
	if err != nil {
		return 0, true, scratch, bytesRead, err
	}
	return count, true, scratch, bytesRead, nil
}

// CountSegmentStringEqualAt reads only the requested string column from one encoded segment and counts matches.
func CountSegmentStringEqualAt(r io.ReaderAt, segmentOffset int64, segmentBytes int64, column string, value string, scratch []byte) (count int, found bool, outScratch []byte, bytesRead int64, err error) {
	loc, scratch, found, bytesRead, err := locateColumnPayloadAt(r, segmentOffset, segmentBytes, column, scratch)
	if err != nil || !found {
		return 0, found, scratch, bytesRead, err
	}
	if loc.stats.Kind != vector.KindString {
		return 0, true, scratch, bytesRead, fmt.Errorf("column %q is %s, want string", column, loc.stats.Kind)
	}

	payload, scratch, payloadBytesRead, err := readColumnPayloadAt(r, loc, scratch)
	bytesRead += payloadBytesRead
	if err != nil {
		return 0, true, scratch, bytesRead, err
	}
	count, err = countStringEqualPayload(payload, loc.stats, value)
	if err != nil {
		return 0, true, scratch, bytesRead, err
	}
	return count, true, scratch, bytesRead, nil
}

// GroupSegmentStringCountsAtCached reads only the requested string column and reuses stable map keys from keyScratch.
func GroupSegmentStringCountsAtCached(r io.ReaderAt, segmentOffset int64, segmentBytes int64, column string, counts map[string]int, scratch []byte, keyScratch []string) (found bool, outScratch []byte, outKeyScratch []string, bytesRead int64, err error) {
	return groupSegmentStringCountsAt(r, segmentOffset, segmentBytes, column, counts, scratch, keyScratch)
}

func groupSegmentStringCountsAt(r io.ReaderAt, segmentOffset int64, segmentBytes int64, column string, counts map[string]int, scratch []byte, keyScratch []string) (found bool, outScratch []byte, outKeyScratch []string, bytesRead int64, err error) {
	loc, scratch, found, bytesRead, err := locateColumnPayloadAt(r, segmentOffset, segmentBytes, column, scratch)
	if err != nil || !found {
		return found, scratch, keyScratch, bytesRead, err
	}
	if loc.stats.Kind != vector.KindString {
		return true, scratch, keyScratch, bytesRead, fmt.Errorf("column %q is %s, want string", column, loc.stats.Kind)
	}

	payload, scratch, payloadBytesRead, err := readColumnPayloadAt(r, loc, scratch)
	bytesRead += payloadBytesRead
	if err != nil {
		return true, scratch, keyScratch, bytesRead, err
	}
	keyScratch, err = groupStringCountsPayloadWithKeyCache(payload, loc.stats, counts, keyScratch, true)
	if err != nil {
		return true, scratch, keyScratch, bytesRead, err
	}
	return true, scratch, keyScratch, bytesRead, nil
}

type segmentColumnLocation struct {
	stats         ColumnStats
	payloadOffset int64
	payloadLen    int
}

// ColumnPayloadRange describes one column payload's byte range inside an encoded segment.
type ColumnPayloadRange struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
	Bytes  int64  `json:"bytes"`
}

// SegmentColumnPayloadRanges returns segment-relative payload byte ranges for every column.
func SegmentColumnPayloadRanges(data []byte) ([]ColumnPayloadRange, error) {
	_, cols, segmentLen, err := readSegmentHeader(data)
	if err != nil {
		return nil, err
	}
	segment := data[:segmentLen]
	trailer := segment[segmentLen-footerTrailerLen:]
	footerStart, _, err := footerPayloadRangeBytes(segmentLen, trailer)
	if err != nil {
		return nil, err
	}
	footer, err := footerPayloadBytes(segment)
	if err != nil {
		return nil, err
	}
	entries, err := parseFooterPayload(footer, cols)
	if err != nil {
		return nil, err
	}

	ranges := make([]ColumnPayloadRange, 0, len(entries))
	for _, entry := range entries {
		columnOffset, err := checkedInt("column offset", entry.Offset)
		if err != nil {
			return nil, err
		}
		if columnOffset < segmentHeaderLen || columnOffset >= footerStart {
			return nil, io.ErrUnexpectedEOF
		}
		offset := columnOffset
		name, stats, err := readColumnHeaderBytes(segment, &offset)
		if err != nil {
			return nil, err
		}
		if !bytesEqualString(name, entry.Name) {
			return nil, fmt.Errorf("segment footer offset for %q points to column %q", entry.Name, string(name))
		}
		if stats.EncodedLen > footerStart-offset {
			return nil, io.ErrUnexpectedEOF
		}
		ranges = append(ranges, ColumnPayloadRange{
			Name:   entry.Name,
			Offset: int64(offset),
			Bytes:  int64(stats.EncodedLen),
		})
	}
	return ranges, nil
}

// CountColumnInt64EqualAt reads one int64 column payload range and counts matches.
func CountColumnInt64EqualAt(r io.ReaderAt, payloadOffset int64, payloadBytes int64, stats ColumnStats, value int64, scratch []byte) (count int, outScratch []byte, bytesRead int64, err error) {
	if stats.Kind != vector.KindInt64 {
		return 0, scratch, 0, fmt.Errorf("column %q is %s, want int64", stats.Name, stats.Kind)
	}
	if canSkipInt64Equal(stats, value) {
		return 0, scratch, 0, nil
	}
	payload, scratch, bytesRead, err := readPayloadRangeAt(r, payloadOffset, payloadBytes, stats, scratch)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	count, err = countInt64EqualPayload(payload, stats, value)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	return count, scratch, bytesRead, nil
}

// CountColumnStringEqualAt reads one string column payload range and counts matches.
func CountColumnStringEqualAt(r io.ReaderAt, payloadOffset int64, payloadBytes int64, stats ColumnStats, value string, scratch []byte) (count int, outScratch []byte, bytesRead int64, err error) {
	if stats.Kind != vector.KindString {
		return 0, scratch, 0, fmt.Errorf("column %q is %s, want string", stats.Name, stats.Kind)
	}
	if CanSkipStringEqual(stats, value) {
		return 0, scratch, 0, nil
	}
	payload, scratch, bytesRead, err := readPayloadRangeAt(r, payloadOffset, payloadBytes, stats, scratch)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	count, err = countStringEqualPayload(payload, stats, value)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	return count, scratch, bytesRead, nil
}

// GroupColumnStringCountsAtCached reads one string column payload range and reuses stable map keys from keyScratch.
func GroupColumnStringCountsAtCached(r io.ReaderAt, payloadOffset int64, payloadBytes int64, stats ColumnStats, counts map[string]int, scratch []byte, keyScratch []string) (outScratch []byte, outKeyScratch []string, bytesRead int64, err error) {
	return groupColumnStringCountsAt(r, payloadOffset, payloadBytes, stats, counts, scratch, keyScratch)
}

func groupColumnStringCountsAt(r io.ReaderAt, payloadOffset int64, payloadBytes int64, stats ColumnStats, counts map[string]int, scratch []byte, keyScratch []string) (outScratch []byte, outKeyScratch []string, bytesRead int64, err error) {
	if stats.Kind != vector.KindString {
		return scratch, keyScratch, 0, fmt.Errorf("column %q is %s, want string", stats.Name, stats.Kind)
	}
	payload, scratch, bytesRead, err := readPayloadRangeAt(r, payloadOffset, payloadBytes, stats, scratch)
	if err != nil {
		return scratch, keyScratch, bytesRead, err
	}
	keyScratch, err = groupStringCountsPayloadWithKeyCache(payload, stats, counts, keyScratch, true)
	if err != nil {
		return scratch, keyScratch, bytesRead, err
	}
	return scratch, keyScratch, bytesRead, nil
}

func findColumnPayloadBytes(data []byte, column string) (ColumnStats, []byte, bool, error) {
	_, cols, segmentLen, err := readSegmentHeader(data)
	if err != nil {
		return ColumnStats{}, nil, false, err
	}
	columnOffset, found, err := findColumnOffsetBytes(data, cols, segmentLen, column)
	if err != nil || !found {
		return ColumnStats{}, nil, found, err
	}
	segment := data[:segmentLen]
	offset := columnOffset
	name, stats, payload, err := readColumnBytes(segment, &offset)
	if err != nil {
		return ColumnStats{}, nil, true, err
	}
	if !bytesEqualString(name, column) {
		return ColumnStats{}, nil, true, fmt.Errorf("segment footer offset for %q points to column %q", column, string(name))
	}
	stats.Name = column
	return stats, payload, true, nil
}

func locateColumnPayloadAt(r io.ReaderAt, segmentOffset int64, segmentBytes int64, column string, scratch []byte) (segmentColumnLocation, []byte, bool, int64, error) {
	if segmentOffset < 0 {
		return segmentColumnLocation{}, scratch, false, 0, fmt.Errorf("negative segment offset %d", segmentOffset)
	}
	if segmentBytes < int64(segmentHeaderLen+footerTrailerLen) {
		return segmentColumnLocation{}, scratch, false, 0, io.ErrUnexpectedEOF
	}
	segmentLen, err := checkedInt("segment length", uint64(segmentBytes))
	if err != nil {
		return segmentColumnLocation{}, scratch, false, 0, err
	}

	var bytesRead int64
	scratch = ensureScratch(scratch, segmentHeaderLen)
	header := scratch[:segmentHeaderLen]
	if err := readFullAt(r, header, segmentOffset); err != nil {
		return segmentColumnLocation{}, scratch, false, bytesRead, err
	}
	bytesRead += int64(segmentHeaderLen)
	_, cols, encodedSegmentLen, err := readSegmentHeaderPrefix(header)
	if err != nil {
		return segmentColumnLocation{}, scratch, false, bytesRead, err
	}
	if encodedSegmentLen != segmentLen {
		return segmentColumnLocation{}, scratch, false, bytesRead, fmt.Errorf("segment length %d does not match manifest length %d", encodedSegmentLen, segmentLen)
	}

	scratch = ensureScratch(scratch, footerTrailerLen)
	trailer := scratch[:footerTrailerLen]
	trailerOffset := segmentOffset + int64(segmentLen-footerTrailerLen)
	if err := readFullAt(r, trailer, trailerOffset); err != nil {
		return segmentColumnLocation{}, scratch, false, bytesRead, err
	}
	bytesRead += int64(footerTrailerLen)
	footerStart, footerLen, err := footerPayloadRangeBytes(segmentLen, trailer)
	if err != nil {
		return segmentColumnLocation{}, scratch, false, bytesRead, err
	}

	scratch = ensureScratch(scratch, footerLen)
	footer := scratch[:footerLen]
	if err := readFullAt(r, footer, segmentOffset+int64(footerStart)); err != nil {
		return segmentColumnLocation{}, scratch, false, bytesRead, err
	}
	bytesRead += int64(footerLen)
	columnOffset, found, err := findColumnOffsetInFooter(footer, cols, segmentLen, column)
	if err != nil || !found {
		return segmentColumnLocation{}, scratch, found, bytesRead, err
	}

	scratch = ensureScratch(scratch, 2)
	nameLenBuf := scratch[:2]
	if err := readFullAt(r, nameLenBuf, segmentOffset+int64(columnOffset)); err != nil {
		return segmentColumnLocation{}, scratch, true, bytesRead, err
	}
	bytesRead += 2
	nameLen := int(binary.LittleEndian.Uint16(nameLenBuf))
	nameAndFixedLen, err := checkedAddInt("column header length", nameLen, columnFixedHeaderLen)
	if err != nil {
		return segmentColumnLocation{}, scratch, true, bytesRead, err
	}
	columnHeaderLen, err := checkedAddInt("column header length", len(nameLenBuf), nameAndFixedLen)
	if err != nil {
		return segmentColumnLocation{}, scratch, true, bytesRead, err
	}
	if columnOffset+columnHeaderLen > footerStart {
		return segmentColumnLocation{}, scratch, true, bytesRead, io.ErrUnexpectedEOF
	}

	scratch = ensureScratch(scratch, nameAndFixedLen)
	nameAndFixed := scratch[:nameAndFixedLen]
	if err := readFullAt(r, nameAndFixed, segmentOffset+int64(columnOffset+len(nameLenBuf))); err != nil {
		return segmentColumnLocation{}, scratch, true, bytesRead, err
	}
	bytesRead += int64(nameAndFixedLen)
	name := nameAndFixed[:nameLen]
	if !bytesEqualString(name, column) {
		return segmentColumnLocation{}, scratch, true, bytesRead, fmt.Errorf("segment footer offset for %q points to column %q", column, string(name))
	}
	stats, err := decodeColumnFixedHeader(column, nameAndFixed[nameLen:])
	if err != nil {
		return segmentColumnLocation{}, scratch, true, bytesRead, err
	}
	payloadOffset := columnOffset + columnHeaderLen
	if stats.EncodedLen > footerStart-payloadOffset {
		return segmentColumnLocation{}, scratch, true, bytesRead, io.ErrUnexpectedEOF
	}
	return segmentColumnLocation{
		stats:         stats,
		payloadOffset: segmentOffset + int64(payloadOffset),
		payloadLen:    stats.EncodedLen,
	}, scratch, true, bytesRead, nil
}

func readColumnPayloadAt(r io.ReaderAt, loc segmentColumnLocation, scratch []byte) ([]byte, []byte, int64, error) {
	scratch = ensureScratch(scratch, loc.payloadLen)
	payload := scratch[:loc.payloadLen]
	if err := readFullAt(r, payload, loc.payloadOffset); err != nil {
		return nil, scratch, 0, err
	}
	return payload, scratch, int64(loc.payloadLen), nil
}

func readPayloadRangeAt(r io.ReaderAt, payloadOffset int64, payloadBytes int64, stats ColumnStats, scratch []byte) ([]byte, []byte, int64, error) {
	if payloadBytes < 0 {
		return nil, scratch, 0, fmt.Errorf("column %q has negative payload byte length %d", stats.Name, payloadBytes)
	}
	payloadLen, err := checkedInt("column payload length", uint64(payloadBytes))
	if err != nil {
		return nil, scratch, 0, err
	}
	if payloadLen != stats.EncodedLen {
		return nil, scratch, 0, fmt.Errorf("column %q payload length %d does not match stats length %d", stats.Name, payloadLen, stats.EncodedLen)
	}
	scratch = ensureScratch(scratch, payloadLen)
	payload := scratch[:payloadLen]
	if err := readFullAt(r, payload, payloadOffset); err != nil {
		return nil, scratch, 0, err
	}
	return payload, scratch, payloadBytes, nil
}

func readSegmentHeader(data []byte) (offset int, cols int, segmentLen int, err error) {
	offset, cols, segmentLen, err = readSegmentHeaderPrefix(data)
	if err != nil {
		return 0, 0, 0, err
	}
	if segmentLen > len(data) {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	return offset, cols, segmentLen, nil
}

func readSegmentHeaderPrefix(data []byte) (offset int, cols int, segmentLen int, err error) {
	if len(data) < segmentHeaderLen {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	if !bytesEqualString(data[:len(segmentMagic)], segmentMagic) {
		return 0, 0, 0, fmt.Errorf("invalid segment magic %q", string(data[:len(segmentMagic)]))
	}
	offset = len(segmentMagic)

	version := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if version != segmentVersion {
		return 0, 0, 0, fmt.Errorf("unsupported segment version %d", version)
	}
	rowCount := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if _, err := checkedInt("segment row count", rowCount); err != nil {
		return 0, 0, 0, err
	}
	columnCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	cols, err = checkedInt("segment column count", uint64(columnCount))
	if err != nil {
		return 0, 0, 0, err
	}
	encodedSegmentLen := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	segmentLen, err = checkedInt("segment length", encodedSegmentLen)
	if err != nil {
		return 0, 0, 0, err
	}
	if segmentLen < segmentHeaderLen+footerTrailerLen {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	return offset, cols, segmentLen, nil
}

func findColumnOffsetBytes(data []byte, cols int, segmentLen int, column string) (int, bool, error) {
	footer, err := footerPayloadBytes(data[:segmentLen])
	if err != nil {
		return 0, false, err
	}
	return findColumnOffsetInFooter(footer, cols, segmentLen, column)
}

func findColumnOffsetInFooter(footer []byte, cols int, segmentLen int, column string) (int, bool, error) {
	offset := 0
	countBytes, err := readBytes(footer, &offset, 4)
	if err != nil {
		return 0, false, err
	}
	count := binary.LittleEndian.Uint32(countBytes)
	if int(count) != cols {
		return 0, false, fmt.Errorf("segment footer column count %d does not match header count %d", count, cols)
	}
	for range count {
		name, err := readString16Bytes(footer, &offset)
		if err != nil {
			return 0, false, err
		}
		offsetBytes, err := readBytes(footer, &offset, 8)
		if err != nil {
			return 0, false, err
		}
		columnOffset, err := checkedInt("column offset", binary.LittleEndian.Uint64(offsetBytes))
		if err != nil {
			return 0, false, err
		}
		if bytesEqualString(name, column) {
			if columnOffset < segmentHeaderLen || columnOffset >= segmentLen-footerTrailerLen {
				return 0, true, io.ErrUnexpectedEOF
			}
			return columnOffset, true, nil
		}
	}
	if offset != len(footer) {
		return 0, false, fmt.Errorf("trailing segment footer bytes: %d", len(footer)-offset)
	}
	return 0, false, nil
}

func footerPayloadBytes(data []byte) ([]byte, error) {
	if len(data) < segmentHeaderLen+footerTrailerLen {
		return nil, io.ErrUnexpectedEOF
	}
	trailer := data[len(data)-footerTrailerLen:]
	footerStart, _, err := footerPayloadRangeBytes(len(data), trailer)
	if err != nil {
		return nil, err
	}
	return data[footerStart : len(data)-footerTrailerLen], nil
}

func footerPayloadRangeBytes(segmentLen int, trailer []byte) (footerStart int, footerLen int, err error) {
	if len(trailer) != footerTrailerLen {
		return 0, 0, io.ErrUnexpectedEOF
	}
	if !bytesEqualString(trailer[8:], segmentFooterMagic) {
		return 0, 0, fmt.Errorf("invalid segment footer magic %q", string(trailer[8:]))
	}
	footerLen, err = checkedInt("segment footer length", binary.LittleEndian.Uint64(trailer[:]))
	if err != nil {
		return 0, 0, err
	}
	footerStart = segmentLen - footerTrailerLen - footerLen
	if footerStart < segmentHeaderLen {
		return 0, 0, io.ErrUnexpectedEOF
	}
	return footerStart, footerLen, nil
}

func ensureScratch(scratch []byte, size int) []byte {
	if cap(scratch) < size {
		return make([]byte, size)
	}
	return scratch[:size]
}

func readFullAt(r io.ReaderAt, buf []byte, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative read offset %d", offset)
	}
	if len(buf) == 0 {
		return nil
	}
	n, err := r.ReadAt(buf, offset)
	if err != nil {
		if err == io.EOF && n == len(buf) {
			return nil
		}
		if n < len(buf) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func readColumnBytes(data []byte, offset *int) ([]byte, ColumnStats, []byte, error) {
	name, stats, err := readColumnHeaderBytes(data, offset)
	if err != nil {
		return nil, ColumnStats{}, nil, err
	}
	payload, err := readBytes(data, offset, stats.EncodedLen)
	if err != nil {
		return nil, ColumnStats{}, nil, err
	}
	return name, stats, payload, nil
}

func readColumnHeaderBytes(data []byte, offset *int) ([]byte, ColumnStats, error) {
	name, err := readString16Bytes(data, offset)
	if err != nil {
		return nil, ColumnStats{}, err
	}
	fixed, err := readBytes(data, offset, columnFixedHeaderLen)
	if err != nil {
		return nil, ColumnStats{}, err
	}

	stats, err := decodeColumnFixedHeader("", fixed)
	if err != nil {
		return nil, ColumnStats{}, err
	}
	return name, stats, nil
}

func readString16Bytes(data []byte, offset *int) ([]byte, error) {
	lengthBytes, err := readBytes(data, offset, 2)
	if err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint16(lengthBytes)
	return readBytes(data, offset, int(length))
}

func readBytes(data []byte, offset *int, n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative read length %d", n)
	}
	if len(data)-*offset < n {
		return nil, io.ErrUnexpectedEOF
	}
	buf := data[*offset : *offset+n]
	*offset += n
	return buf, nil
}

func selectStringEqualPayload(payload []byte, stats ColumnStats, value string, scratch []uint32) ([]uint32, error) {
	if uint64(stats.Count) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("column %q count %d exceeds selection vector capacity", stats.Name, stats.Count)
	}
	scratch = scratch[:0]
	return scanStringEqualPayload(payload, stats, value, scratch)
}

func countStringEqualPayload(payload []byte, stats ColumnStats, value string) (int, error) {
	if len(payload) == 0 {
		return 0, fmt.Errorf("missing string codec")
	}
	switch payload[0] {
	case stringCodecPlain:
		return countPlainStringEqualPayload(payload, stats, value)
	case stringCodecDictionary:
		return countDictionaryStringEqualPayload(payload, stats, value)
	default:
		return 0, fmt.Errorf("unsupported string codec %d", payload[0])
	}
}

func scanStringEqualPayload(payload []byte, stats ColumnStats, value string, scratch []uint32) ([]uint32, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("missing string codec")
	}
	switch payload[0] {
	case stringCodecPlain:
		return scanPlainStringEqualPayload(payload, stats, value, scratch)
	case stringCodecDictionary:
		return scanDictionaryStringEqualPayload(payload, stats, value, scratch)
	default:
		return nil, fmt.Errorf("unsupported string codec %d", payload[0])
	}
}

func groupStringCountsPayload(payload []byte, stats ColumnStats, counts map[string]int) error {
	_, err := groupStringCountsPayloadWithKeyCache(payload, stats, counts, nil, false)
	return err
}

func groupStringCountsPayloadWithKeyCache(payload []byte, stats ColumnStats, counts map[string]int, keyScratch []string, cacheKeys bool) ([]string, error) {
	if len(payload) == 0 {
		return keyScratch, fmt.Errorf("missing string codec")
	}
	switch payload[0] {
	case stringCodecPlain:
		return groupPlainStringCountsPayloadWithKeyCache(payload, stats, counts, keyScratch, cacheKeys)
	case stringCodecDictionary:
		return groupDictionaryStringCountsPayloadWithKeyCache(payload, stats, counts, keyScratch, cacheKeys)
	default:
		return keyScratch, fmt.Errorf("unsupported string codec %d", payload[0])
	}
}

func scanPlainStringEqualPayload(payload []byte, stats ColumnStats, value string, scratch []uint32) ([]uint32, error) {
	offset := 1
	for row := range stats.Count {
		if len(payload)-offset < 4 {
			return nil, fmt.Errorf("short string length at row %d", row)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return nil, fmt.Errorf("short string data at row %d", row)
		}
		if int(length) == len(value) && bytesEqualString(payload[offset:offset+int(length)], value) {
			scratch = append(scratch, uint32(row))
		}
		offset += int(length)
	}
	if offset != len(payload) {
		return nil, fmt.Errorf("trailing string bytes: %d", len(payload)-offset)
	}
	return scratch, nil
}

func countPlainStringEqualPayload(payload []byte, stats ColumnStats, value string) (int, error) {
	count := 0
	offset := 1
	for row := range stats.Count {
		if len(payload)-offset < 4 {
			return 0, fmt.Errorf("short string length at row %d", row)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return 0, fmt.Errorf("short string data at row %d", row)
		}
		if int(length) == len(value) && bytesEqualString(payload[offset:offset+int(length)], value) {
			count++
		}
		offset += int(length)
	}
	if offset != len(payload) {
		return 0, fmt.Errorf("trailing string bytes: %d", len(payload)-offset)
	}
	return count, nil
}

func groupPlainStringCountsPayloadWithKeyCache(payload []byte, stats ColumnStats, counts map[string]int, keyScratch []string, cacheKeys bool) ([]string, error) {
	offset := 1
	for row := range stats.Count {
		if len(payload)-offset < 4 {
			return keyScratch, fmt.Errorf("short string length at row %d", row)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return keyScratch, fmt.Errorf("short string data at row %d", row)
		}
		keyScratch = addStringCount(counts, payload[offset:offset+int(length)], 1, keyScratch, cacheKeys)
		offset += int(length)
	}
	if offset != len(payload) {
		return keyScratch, fmt.Errorf("trailing string bytes: %d", len(payload)-offset)
	}
	return keyScratch, nil
}

func scanDictionaryStringEqualPayload(payload []byte, stats ColumnStats, value string, scratch []uint32) ([]uint32, error) {
	targetID, ids, idEncoding, dictCount, found, err := findStringDictionaryID(payload, stats, value)
	if err != nil {
		return nil, err
	}
	if !found {
		return scratch, nil
	}
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		return scanPackedDictionaryStringEqualPayload(ids, stats, targetID, stringDictionaryPackedBitWidth(idEncoding), dictCount, scratch)
	}
	switch idEncoding {
	case 1:
		target := byte(targetID)
		for row := 0; row < stats.Count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return nil, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				scratch = append(scratch, uint32(row))
			}
		}
	case 2:
		target := uint16(targetID)
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint16(ids[row*2:])
			if int(id) >= dictCount {
				return nil, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				scratch = append(scratch, uint32(row))
			}
		}
	case 4:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return nil, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == targetID {
				scratch = append(scratch, uint32(row))
			}
		}
	}
	return scratch, nil
}

func countDictionaryStringEqualPayload(payload []byte, stats ColumnStats, value string) (int, error) {
	targetID, ids, idEncoding, dictCount, found, err := findStringDictionaryID(payload, stats, value)
	if err != nil || !found {
		return 0, err
	}
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		return countPackedDictionaryStringEqualPayload(ids, stats, targetID, stringDictionaryPackedBitWidth(idEncoding), dictCount)
	}
	count := 0
	switch idEncoding {
	case 1:
		target := byte(targetID)
		for row := 0; row < stats.Count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return 0, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				count++
			}
		}
	case 2:
		target := uint16(targetID)
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint16(ids[row*2:])
			if int(id) >= dictCount {
				return 0, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				count++
			}
		}
	case 4:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return 0, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			if id == targetID {
				count++
			}
		}
	}
	return count, nil
}

func groupDictionaryStringCountsPayloadWithKeyCache(payload []byte, stats ColumnStats, counts map[string]int, keyScratch []string, cacheKeys bool) ([]string, error) {
	dictOffset, ids, idEncoding, dictCount, err := parseStringDictionaryPayload(payload, stats)
	if err != nil {
		return keyScratch, err
	}
	var smallDictCounts [64]int
	dictCounts := smallDictCounts[:]
	if dictCount > len(dictCounts) {
		dictCounts = make([]int, dictCount)
	}
	dictCounts = dictCounts[:dictCount]
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		if err := groupPackedDictionaryStringCountsPayload(ids, stats, stringDictionaryPackedBitWidth(idEncoding), dictCount, dictCounts); err != nil {
			return keyScratch, err
		}
	} else {
		switch idEncoding {
		case 1:
			for row := 0; row < stats.Count; row++ {
				id := ids[row]
				if int(id) >= dictCount {
					return keyScratch, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
				}
				dictCounts[id]++
			}
		case 2:
			for row := 0; row < stats.Count; row++ {
				id := binary.LittleEndian.Uint16(ids[row*2:])
				if int(id) >= dictCount {
					return keyScratch, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
				}
				dictCounts[id]++
			}
		case 4:
			for row := 0; row < stats.Count; row++ {
				id := binary.LittleEndian.Uint32(ids[row*4:])
				if uint64(id) >= uint64(dictCount) {
					return keyScratch, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
				}
				dictCounts[id]++
			}
		}
	}
	return addStringDictionaryCounts(payload, dictOffset, dictCounts, counts, keyScratch, cacheKeys)
}

func addStringDictionaryCounts(payload []byte, offset int, dictCounts []int, counts map[string]int, keyScratch []string, cacheKeys bool) ([]string, error) {
	for id, count := range dictCounts {
		if len(payload)-offset < 4 {
			return keyScratch, fmt.Errorf("short string dictionary length at value %d", id)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return keyScratch, fmt.Errorf("short string dictionary data at value %d", id)
		}
		if count != 0 {
			keyScratch = addStringCount(counts, payload[offset:offset+int(length)], count, keyScratch, cacheKeys)
		}
		offset += int(length)
	}
	return keyScratch, nil
}

func addStringCount(counts map[string]int, value []byte, count int, keyScratch []string, cacheKeys bool) []string {
	for key := range counts {
		if bytesEqualString(value, key) {
			counts[key] += count
			return keyScratch
		}
	}
	if cacheKeys {
		for _, key := range keyScratch {
			if bytesEqualString(value, key) {
				counts[key] += count
				return keyScratch
			}
		}
	}
	key := string(value)
	counts[key] = count
	if cacheKeys {
		keyScratch = append(keyScratch, key)
	}
	return keyScratch
}

func scanPackedDictionaryStringEqualPayload(ids []byte, stats ColumnStats, targetID uint32, bitWidth int, dictCount int, scratch []uint32) ([]uint32, error) {
	if bitWidth == 0 {
		for row := 0; row < stats.Count; row++ {
			scratch = append(scratch, uint32(row))
		}
		return scratch, nil
	}
	if bitWidth == 2 {
		return scanPacked2DictionaryStringEqualPayload(ids, stats, targetID, dictCount, scratch)
	}
	checkRange := uint64(dictCount) < uint64(1)<<uint(bitWidth)
	for row := 0; row < stats.Count; row++ {
		id := packedDictionaryIDAt(ids, row, bitWidth)
		if checkRange && uint64(id) >= uint64(dictCount) {
			return nil, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		if id == targetID {
			scratch = append(scratch, uint32(row))
		}
	}
	return scratch, nil
}

func scanPacked2DictionaryStringEqualPayload(ids []byte, stats ColumnStats, targetID uint32, dictCount int, scratch []uint32) ([]uint32, error) {
	checkRange := dictCount < 1<<2
	row := 0
	target := byte(targetID)
	for _, packed := range ids[:stats.Count/4] {
		id0 := packed & 3
		id1 := (packed >> 2) & 3
		id2 := (packed >> 4) & 3
		id3 := (packed >> 6) & 3
		if checkRange && (int(id0) >= dictCount || int(id1) >= dictCount || int(id2) >= dictCount || int(id3) >= dictCount) {
			return nil, fmt.Errorf("string dictionary id out of range near row %d", row)
		}
		if id0 == target {
			scratch = append(scratch, uint32(row))
		}
		if id1 == target {
			scratch = append(scratch, uint32(row+1))
		}
		if id2 == target {
			scratch = append(scratch, uint32(row+2))
		}
		if id3 == target {
			scratch = append(scratch, uint32(row+3))
		}
		row += 4
	}
	remaining := stats.Count - row
	if remaining == 0 {
		return scratch, nil
	}
	packed := ids[row/4]
	for i := 0; i < remaining; i++ {
		id := (packed >> (i * 2)) & 3
		if checkRange && int(id) >= dictCount {
			return nil, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		if id == target {
			scratch = append(scratch, uint32(row))
		}
		row++
	}
	return scratch, nil
}

func countPackedDictionaryStringEqualPayload(ids []byte, stats ColumnStats, targetID uint32, bitWidth int, dictCount int) (int, error) {
	if bitWidth == 0 {
		return stats.Count, nil
	}
	if bitWidth == 2 {
		return countPacked2DictionaryStringEqualPayload(ids, stats, targetID, dictCount)
	}
	count := 0
	checkRange := uint64(dictCount) < uint64(1)<<uint(bitWidth)
	for row := 0; row < stats.Count; row++ {
		id := packedDictionaryIDAt(ids, row, bitWidth)
		if checkRange && uint64(id) >= uint64(dictCount) {
			return 0, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		if id == targetID {
			count++
		}
	}
	return count, nil
}

func countPacked2DictionaryStringEqualPayload(ids []byte, stats ColumnStats, targetID uint32, dictCount int) (int, error) {
	count := 0
	checkRange := dictCount < 1<<2
	row := 0
	target := byte(targetID)
	for _, packed := range ids[:stats.Count/4] {
		id0 := packed & 3
		id1 := (packed >> 2) & 3
		id2 := (packed >> 4) & 3
		id3 := (packed >> 6) & 3
		if checkRange && (int(id0) >= dictCount || int(id1) >= dictCount || int(id2) >= dictCount || int(id3) >= dictCount) {
			return 0, fmt.Errorf("string dictionary id out of range near row %d", row)
		}
		if id0 == target {
			count++
		}
		if id1 == target {
			count++
		}
		if id2 == target {
			count++
		}
		if id3 == target {
			count++
		}
		row += 4
	}
	remaining := stats.Count - row
	if remaining == 0 {
		return count, nil
	}
	packed := ids[row/4]
	for i := 0; i < remaining; i++ {
		id := (packed >> (i * 2)) & 3
		if checkRange && int(id) >= dictCount {
			return 0, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		if id == target {
			count++
		}
		row++
	}
	return count, nil
}

func groupPackedDictionaryStringCountsPayload(ids []byte, stats ColumnStats, bitWidth int, dictCount int, dictCounts []int) error {
	if bitWidth == 0 {
		if dictCount == 0 {
			if stats.Count == 0 {
				return nil
			}
			return fmt.Errorf("string dictionary id 0 out of range at row 0")
		}
		dictCounts[0] += stats.Count
		return nil
	}
	if bitWidth == 2 {
		return groupPacked2DictionaryStringCountsPayload(ids, stats, dictCount, dictCounts)
	}
	checkRange := uint64(dictCount) < uint64(1)<<uint(bitWidth)
	for row := 0; row < stats.Count; row++ {
		id := packedDictionaryIDAt(ids, row, bitWidth)
		if checkRange && uint64(id) >= uint64(dictCount) {
			return fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		dictCounts[id]++
	}
	return nil
}

func groupPacked2DictionaryStringCountsPayload(ids []byte, stats ColumnStats, dictCount int, dictCounts []int) error {
	checkRange := dictCount < 1<<2
	row := 0
	for _, packed := range ids[:stats.Count/4] {
		id0 := packed & 3
		id1 := (packed >> 2) & 3
		id2 := (packed >> 4) & 3
		id3 := (packed >> 6) & 3
		if checkRange && (int(id0) >= dictCount || int(id1) >= dictCount || int(id2) >= dictCount || int(id3) >= dictCount) {
			return fmt.Errorf("string dictionary id out of range near row %d", row)
		}
		dictCounts[id0]++
		dictCounts[id1]++
		dictCounts[id2]++
		dictCounts[id3]++
		row += 4
	}
	remaining := stats.Count - row
	if remaining == 0 {
		return nil
	}
	packed := ids[row/4]
	for i := 0; i < remaining; i++ {
		id := (packed >> (i * 2)) & 3
		if checkRange && int(id) >= dictCount {
			return fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		dictCounts[id]++
		row++
	}
	return nil
}

func findStringDictionaryID(payload []byte, stats ColumnStats, value string) (targetID uint32, ids []byte, idEncoding int, dictCount int, found bool, err error) {
	if len(payload) < 1+4+1 {
		return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary header")
	}
	offset := 1
	dictCount = int(binary.LittleEndian.Uint32(payload[offset:]))
	offset += 4
	idEncoding = int(payload[offset])
	offset++
	if err := validateStringDictionaryIDEncoding(stats, dictCount, idEncoding); err != nil {
		return 0, nil, 0, 0, false, err
	}

	for id := range dictCount {
		if len(payload)-offset < 4 {
			return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary length at value %d", id)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary data at value %d", id)
		}
		if int(length) == len(value) && bytesEqualString(payload[offset:offset+int(length)], value) {
			targetID = uint32(id)
			found = true
		}
		offset += int(length)
	}

	idsLen, err := stringDictionaryIDsLen(stats.Count, idEncoding)
	if err != nil {
		return 0, nil, 0, 0, false, err
	}
	if len(payload)-offset < idsLen {
		return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary ids")
	}
	if len(payload)-offset > idsLen {
		return 0, nil, 0, 0, false, fmt.Errorf("trailing string bytes: %d", len(payload)-offset-idsLen)
	}
	return targetID, payload[offset : offset+idsLen], idEncoding, dictCount, found, nil
}

func parseStringDictionaryPayload(payload []byte, stats ColumnStats) (dictOffset int, ids []byte, idEncoding int, dictCount int, err error) {
	if len(payload) < 1+4+1 {
		return 0, nil, 0, 0, fmt.Errorf("short string dictionary header")
	}
	offset := 1
	dictCount = int(binary.LittleEndian.Uint32(payload[offset:]))
	offset += 4
	idEncoding = int(payload[offset])
	offset++
	if err := validateStringDictionaryIDEncoding(stats, dictCount, idEncoding); err != nil {
		return 0, nil, 0, 0, err
	}

	dictOffset = offset
	for id := range dictCount {
		if len(payload)-offset < 4 {
			return 0, nil, 0, 0, fmt.Errorf("short string dictionary length at value %d", id)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return 0, nil, 0, 0, fmt.Errorf("short string dictionary data at value %d", id)
		}
		offset += int(length)
	}

	idsLen, err := stringDictionaryIDsLen(stats.Count, idEncoding)
	if err != nil {
		return 0, nil, 0, 0, err
	}
	if len(payload)-offset < idsLen {
		return 0, nil, 0, 0, fmt.Errorf("short string dictionary ids")
	}
	if len(payload)-offset > idsLen {
		return 0, nil, 0, 0, fmt.Errorf("trailing string bytes: %d", len(payload)-offset-idsLen)
	}
	return dictOffset, payload[offset : offset+idsLen], idEncoding, dictCount, nil
}

func bytesEqualString(buf []byte, value string) bool {
	if len(buf) != len(value) {
		return false
	}
	for i, b := range buf {
		if b != value[i] {
			return false
		}
	}
	return true
}
