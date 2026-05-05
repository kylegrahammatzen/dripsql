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

func readSegmentHeader(data []byte) (offset int, cols int, segmentLen int, err error) {
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
	if segmentLen < segmentHeaderLen+footerTrailerLen || segmentLen > len(data) {
		return 0, 0, 0, io.ErrUnexpectedEOF
	}
	return offset, cols, segmentLen, nil
}

func findColumnOffsetBytes(data []byte, cols int, segmentLen int, column string) (int, bool, error) {
	footer, err := footerPayloadBytes(data[:segmentLen])
	if err != nil {
		return 0, false, err
	}
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
	if !bytesEqualString(trailer[8:], segmentFooterMagic) {
		return nil, fmt.Errorf("invalid segment footer magic %q", string(trailer[8:]))
	}
	footerLen, err := checkedInt("segment footer length", binary.LittleEndian.Uint64(trailer[:]))
	if err != nil {
		return nil, err
	}
	footerStart := len(data) - footerTrailerLen - footerLen
	if footerStart < segmentHeaderLen {
		return nil, io.ErrUnexpectedEOF
	}
	return data[footerStart : len(data)-footerTrailerLen], nil
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
	if len(payload) == 0 {
		return fmt.Errorf("missing string codec")
	}
	switch payload[0] {
	case stringCodecPlain:
		return groupPlainStringCountsPayload(payload, stats, counts)
	case stringCodecDictionary:
		return groupDictionaryStringCountsPayload(payload, stats, counts)
	default:
		return fmt.Errorf("unsupported string codec %d", payload[0])
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

func groupPlainStringCountsPayload(payload []byte, stats ColumnStats, counts map[string]int) error {
	offset := 1
	for row := range stats.Count {
		if len(payload)-offset < 4 {
			return fmt.Errorf("short string length at row %d", row)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return fmt.Errorf("short string data at row %d", row)
		}
		counts[string(payload[offset:offset+int(length)])]++
		offset += int(length)
	}
	if offset != len(payload) {
		return fmt.Errorf("trailing string bytes: %d", len(payload)-offset)
	}
	return nil
}

func scanDictionaryStringEqualPayload(payload []byte, stats ColumnStats, value string, scratch []uint32) ([]uint32, error) {
	targetID, ids, idWidth, dictCount, found, err := findStringDictionaryID(payload, stats, value)
	if err != nil {
		return nil, err
	}
	if !found {
		return scratch, nil
	}
	switch idWidth {
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
	targetID, ids, idWidth, dictCount, found, err := findStringDictionaryID(payload, stats, value)
	if err != nil || !found {
		return 0, err
	}
	count := 0
	switch idWidth {
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

func groupDictionaryStringCountsPayload(payload []byte, stats ColumnStats, counts map[string]int) error {
	dictRanges, ids, idWidth, dictCount, err := parseStringDictionaryPayload(payload, stats)
	if err != nil {
		return err
	}
	dictCounts := make([]int, dictCount)
	switch idWidth {
	case 1:
		for row := 0; row < stats.Count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			dictCounts[id]++
		}
	case 2:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint16(ids[row*2:])
			if int(id) >= dictCount {
				return fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			dictCounts[id]++
		}
	case 4:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
			}
			dictCounts[id]++
		}
	}
	for id, count := range dictCounts {
		if count == 0 {
			continue
		}
		packed := dictRanges[id]
		start := int(packed >> 32)
		length := int(uint32(packed))
		counts[string(payload[start:start+length])] += count
	}
	return nil
}

func findStringDictionaryID(payload []byte, stats ColumnStats, value string) (targetID uint32, ids []byte, idWidth int, dictCount int, found bool, err error) {
	if len(payload) < 1+4+1 {
		return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary header")
	}
	offset := 1
	dictCount = int(binary.LittleEndian.Uint32(payload[offset:]))
	offset += 4
	idWidth = int(payload[offset])
	offset++
	if idWidth != 1 && idWidth != 2 && idWidth != 4 {
		return 0, nil, 0, 0, false, fmt.Errorf("unsupported string dictionary id width %d", idWidth)
	}
	if idWidth == 1 && dictCount > 1<<8 {
		return 0, nil, 0, 0, false, fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if idWidth == 2 && dictCount > 1<<16 {
		return 0, nil, 0, 0, false, fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if dictCount > stats.Count {
		return 0, nil, 0, 0, false, fmt.Errorf("string dictionary value count %d exceeds row count %d", dictCount, stats.Count)
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

	idsLen, err := checkedMulInt("string dictionary ids length", stats.Count, idWidth)
	if err != nil {
		return 0, nil, 0, 0, false, err
	}
	if len(payload)-offset < idsLen {
		return 0, nil, 0, 0, false, fmt.Errorf("short string dictionary ids")
	}
	if len(payload)-offset > idsLen {
		return 0, nil, 0, 0, false, fmt.Errorf("trailing string bytes: %d", len(payload)-offset-idsLen)
	}
	return targetID, payload[offset : offset+idsLen], idWidth, dictCount, found, nil
}

func parseStringDictionaryPayload(payload []byte, stats ColumnStats) (dictRanges []uint64, ids []byte, idWidth int, dictCount int, err error) {
	if len(payload) < 1+4+1 {
		return nil, nil, 0, 0, fmt.Errorf("short string dictionary header")
	}
	offset := 1
	dictCount = int(binary.LittleEndian.Uint32(payload[offset:]))
	offset += 4
	idWidth = int(payload[offset])
	offset++
	if idWidth != 1 && idWidth != 2 && idWidth != 4 {
		return nil, nil, 0, 0, fmt.Errorf("unsupported string dictionary id width %d", idWidth)
	}
	if idWidth == 1 && dictCount > 1<<8 {
		return nil, nil, 0, 0, fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if idWidth == 2 && dictCount > 1<<16 {
		return nil, nil, 0, 0, fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if dictCount > stats.Count {
		return nil, nil, 0, 0, fmt.Errorf("string dictionary value count %d exceeds row count %d", dictCount, stats.Count)
	}

	dictRanges = make([]uint64, dictCount)
	for id := range dictCount {
		if len(payload)-offset < 4 {
			return nil, nil, 0, 0, fmt.Errorf("short string dictionary length at value %d", id)
		}
		length := binary.LittleEndian.Uint32(payload[offset:])
		offset += 4
		if uint64(length) > uint64(len(payload)-offset) {
			return nil, nil, 0, 0, fmt.Errorf("short string dictionary data at value %d", id)
		}
		dictRanges[id] = uint64(offset)<<32 | uint64(length)
		offset += int(length)
	}

	idsLen, err := checkedMulInt("string dictionary ids length", stats.Count, idWidth)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if len(payload)-offset < idsLen {
		return nil, nil, 0, 0, fmt.Errorf("short string dictionary ids")
	}
	if len(payload)-offset > idsLen {
		return nil, nil, 0, 0, fmt.Errorf("trailing string bytes: %d", len(payload)-offset-idsLen)
	}
	return dictRanges, payload[offset : offset+idsLen], idWidth, dictCount, nil
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
