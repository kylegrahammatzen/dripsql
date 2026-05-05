package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// SelectSegmentInt64Equal appends matching row indexes for one int64 segment column into scratch.
func SelectSegmentInt64Equal(r io.Reader, column string, value int64, scratch []uint32) ([]uint32, bool, error) {
	sr := newSegmentReader(r)
	_, cols, err := sr.readHeaderChecked()
	if err != nil {
		return scratch, false, err
	}

	selected := scratch
	found := false
	for range cols {
		stats, err := sr.readColumnHeader()
		if err != nil {
			return scratch, false, err
		}
		if stats.Name != column || found {
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, false, err
			}
			continue
		}

		found = true
		if stats.Kind != vector.KindInt64 {
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, true, err
			}
			return scratch, true, fmt.Errorf("column %q is %s, want int64", column, stats.Kind)
		}
		if canSkipInt64Equal(stats, value) {
			selected = scratch[:0]
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, true, err
			}
			continue
		}

		selected, err = sr.selectInt64EqualPayload(stats, value, scratch)
		if err != nil {
			return scratch, true, err
		}
	}
	if err := sr.finishSegmentSequential(cols); err != nil {
		return scratch, found, err
	}

	return selected, found, nil
}

func canSkipInt64Equal(stats ColumnStats, value int64) bool {
	return stats.Kind == vector.KindInt64 &&
		stats.HasMinMax &&
		(value < stats.MinInt64 || value > stats.MaxInt64)
}

func (s *segmentReader) selectInt64EqualPayload(stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validateInt64SelectionStats(stats); err != nil {
		return nil, err
	}
	if stats.EncodedLen < 1 {
		return nil, fmt.Errorf("missing int64 codec")
	}

	var codec [1]byte
	if err := readFull(s.r, codec[:]); err != nil {
		return nil, err
	}
	scratch = scratch[:0]
	switch codec[0] {
	case int64CodecPlain:
		return s.selectPlainInt64EqualPayload(stats, value, scratch)
	case int64CodecDictionary:
		return s.selectDictionaryInt64EqualPayload(stats, value, scratch)
	default:
		return nil, fmt.Errorf("unsupported int64 codec %d", codec[0])
	}
}

func (s *segmentReader) selectPlainInt64EqualPayload(stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validatePlainInt64Payload(stats, stats.EncodedLen); err != nil {
		return nil, err
	}
	scratch = scratch[:0]
	var buf [32 * 1024]byte
	row := 0
	remaining := stats.EncodedLen - 1
	for remaining > 0 {
		chunk := min(remaining, len(buf))
		if err := readFull(s.r, buf[:chunk]); err != nil {
			return nil, err
		}
		for offset := 0; offset < chunk; offset += 8 {
			if int64(binary.LittleEndian.Uint64(buf[offset:])) == value {
				scratch = append(scratch, uint32(row))
			}
			row++
		}
		remaining -= chunk
	}
	return scratch, nil
}

func (s *segmentReader) selectDictionaryInt64EqualPayload(stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	var header [5]byte
	if err := readFull(s.r, header[:]); err != nil {
		return nil, err
	}
	dictCount := int(binary.LittleEndian.Uint32(header[:]))
	idEncoding := int(header[4])
	if err := validateInt64DictionaryIDEncoding(stats, dictCount, idEncoding); err != nil {
		return nil, err
	}
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, 8)
	if err != nil {
		return nil, err
	}
	idsLen, err := int64DictionaryIDsLen(stats.Count, idEncoding)
	if err != nil {
		return nil, err
	}
	encodedLen, err := checkedAddInt("int64 dictionary encoded length", 1+4+1, dictLen)
	if err != nil {
		return nil, err
	}
	encodedLen, err = checkedAddInt("int64 dictionary encoded length", encodedLen, idsLen)
	if err != nil {
		return nil, err
	}
	if stats.EncodedLen != encodedLen {
		return nil, fmt.Errorf("invalid int64 dictionary encoded length %d for count %d", stats.EncodedLen, stats.Count)
	}

	targetID := uint32(0)
	found := false
	var valueBuf [8]byte
	for id := 0; id < dictCount; id++ {
		if err := readFull(s.r, valueBuf[:]); err != nil {
			return nil, err
		}
		if int64(binary.LittleEndian.Uint64(valueBuf[:])) == value {
			targetID = uint32(id)
			found = true
		}
	}

	return s.selectInt64DictionaryIDs(stats, targetID, idEncoding, dictCount, found, scratch)
}

func (s *segmentReader) selectInt64DictionaryIDs(stats ColumnStats, targetID uint32, idEncoding int, dictCount int, found bool, scratch []uint32) ([]uint32, error) {
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return s.selectPackedInt64DictionaryIDs(stats, targetID, int64DictionaryPackedBitWidth(idEncoding), dictCount, found, scratch)
	}
	switch idEncoding {
	case 1:
		target := byte(targetID)
		var idBuf [1]byte
		for row := 0; row < stats.Count; row++ {
			if err := readFull(s.r, idBuf[:]); err != nil {
				return nil, err
			}
			id := idBuf[0]
			if int(id) >= dictCount {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if found && id == target {
				scratch = append(scratch, uint32(row))
			}
		}
	case 2:
		target := uint16(targetID)
		var idBuf [2]byte
		for row := 0; row < stats.Count; row++ {
			if err := readFull(s.r, idBuf[:]); err != nil {
				return nil, err
			}
			id := binary.LittleEndian.Uint16(idBuf[:])
			if int(id) >= dictCount {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if found && id == target {
				scratch = append(scratch, uint32(row))
			}
		}
	case 4:
		var idBuf [4]byte
		for row := 0; row < stats.Count; row++ {
			if err := readFull(s.r, idBuf[:]); err != nil {
				return nil, err
			}
			id := binary.LittleEndian.Uint32(idBuf[:])
			if uint64(id) >= uint64(dictCount) {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if found && id == targetID {
				scratch = append(scratch, uint32(row))
			}
		}
	}
	return scratch, nil
}

func (s *segmentReader) selectPackedInt64DictionaryIDs(stats ColumnStats, targetID uint32, bitWidth int, dictCount int, found bool, scratch []uint32) ([]uint32, error) {
	if bitWidth == 0 {
		if found {
			for row := 0; row < stats.Count; row++ {
				scratch = append(scratch, uint32(row))
			}
		}
		return scratch, nil
	}
	idsLen, err := packedInt64DictionaryIDsLen(stats.Count, bitWidth)
	if err != nil {
		return nil, err
	}
	var buf [32 * 1024]byte
	chunk := buf[:0]
	chunkOffset := 0
	remaining := idsLen
	bitBuffer := uint64(0)
	bitsInBuffer := 0
	mask := (uint64(1) << uint(bitWidth)) - 1
	if bitWidth == 32 {
		mask = uint64(^uint32(0))
	}
	for row := 0; row < stats.Count; row++ {
		for bitsInBuffer < bitWidth {
			if chunkOffset == len(chunk) {
				if remaining == 0 {
					return nil, io.ErrUnexpectedEOF
				}
				chunkLen := min(remaining, len(buf))
				if err := readFull(s.r, buf[:chunkLen]); err != nil {
					return nil, err
				}
				chunk = buf[:chunkLen]
				chunkOffset = 0
				remaining -= chunkLen
			}
			bitBuffer |= uint64(chunk[chunkOffset]) << bitsInBuffer
			bitsInBuffer += 8
			chunkOffset++
		}
		id := uint32(bitBuffer & mask)
		bitBuffer >>= bitWidth
		bitsInBuffer -= bitWidth
		if uint64(id) >= uint64(dictCount) {
			return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		if found && id == targetID {
			scratch = append(scratch, uint32(row))
		}
	}
	return scratch, nil
}

func selectInt64EqualPayload(payload []byte, stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validateInt64SelectionStats(stats); err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("missing int64 codec")
	}
	scratch = scratch[:0]
	switch payload[0] {
	case int64CodecPlain:
		return selectPlainInt64EqualPayload(payload, stats, value, scratch)
	case int64CodecDictionary:
		return selectDictionaryInt64EqualPayload(payload, stats, value, scratch)
	default:
		return nil, fmt.Errorf("unsupported int64 codec %d", payload[0])
	}
}

func selectPlainInt64EqualPayload(payload []byte, stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validatePlainInt64Payload(stats, len(payload)); err != nil {
		return nil, err
	}
	for row := range stats.Count {
		if int64(binary.LittleEndian.Uint64(payload[1+row*8:])) == value {
			scratch = append(scratch, uint32(row))
		}
	}
	return scratch, nil
}

func selectDictionaryInt64EqualPayload(payload []byte, stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	targetID, ids, idEncoding, dictCount, found, err := findInt64DictionaryID(payload, stats, value)
	if err != nil {
		return nil, err
	}
	if !found {
		return scratch, nil
	}
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return selectPackedDictionaryInt64EqualPayload(ids, stats, targetID, int64DictionaryPackedBitWidth(idEncoding), dictCount, scratch)
	}
	switch idEncoding {
	case 1:
		target := byte(targetID)
		for row := 0; row < stats.Count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
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
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				scratch = append(scratch, uint32(row))
			}
		}
	case 4:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if id == targetID {
				scratch = append(scratch, uint32(row))
			}
		}
	}
	return scratch, nil
}

func selectPackedDictionaryInt64EqualPayload(ids []byte, stats ColumnStats, targetID uint32, bitWidth int, dictCount int, scratch []uint32) ([]uint32, error) {
	if bitWidth == 0 {
		for row := 0; row < stats.Count; row++ {
			scratch = append(scratch, uint32(row))
		}
		return scratch, nil
	}
	if bitWidth == 10 {
		return selectPacked10DictionaryInt64EqualPayload(ids, stats, targetID, dictCount, scratch)
	}
	mask := (uint64(1) << uint(bitWidth)) - 1
	if bitWidth == 32 {
		mask = uint64(^uint32(0))
	}
	checkRange := uint64(dictCount) < uint64(1)<<uint(bitWidth)
	bitBuffer := uint64(0)
	bitsInBuffer := 0
	offset := 0
	for row := 0; row < stats.Count; row++ {
		for bitsInBuffer < bitWidth {
			bitBuffer |= uint64(ids[offset]) << bitsInBuffer
			bitsInBuffer += 8
			offset++
		}
		id := uint32(bitBuffer & mask)
		bitBuffer >>= bitWidth
		bitsInBuffer -= bitWidth
		if checkRange && uint64(id) >= uint64(dictCount) {
			return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		if id == targetID {
			scratch = append(scratch, uint32(row))
		}
	}
	return scratch, nil
}

func selectPacked10DictionaryInt64EqualPayload(ids []byte, stats ColumnStats, targetID uint32, dictCount int, scratch []uint32) ([]uint32, error) {
	checkRange := dictCount < 1<<10
	row := 0
	offset := 0
	groups := stats.Count / 4
	target := targetID
	for range groups {
		packed := uint64(ids[offset]) |
			uint64(ids[offset+1])<<8 |
			uint64(ids[offset+2])<<16 |
			uint64(ids[offset+3])<<24 |
			uint64(ids[offset+4])<<32
		id0 := uint32(packed & 0x3ff)
		id1 := uint32((packed >> 10) & 0x3ff)
		id2 := uint32((packed >> 20) & 0x3ff)
		id3 := uint32((packed >> 30) & 0x3ff)
		if checkRange && (int(id0) >= dictCount || int(id1) >= dictCount || int(id2) >= dictCount || int(id3) >= dictCount) {
			return nil, fmt.Errorf("int64 dictionary id out of range near row %d", row)
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
		offset += 5
	}

	remainingRows := stats.Count - row
	if remainingRows == 0 {
		return scratch, nil
	}
	bitBuffer := uint64(0)
	bitsInBuffer := 0
	for remaining := 0; remaining < remainingRows; remaining++ {
		for bitsInBuffer < 10 {
			bitBuffer |= uint64(ids[offset]) << bitsInBuffer
			bitsInBuffer += 8
			offset++
		}
		id := uint32(bitBuffer & 0x3ff)
		bitBuffer >>= 10
		bitsInBuffer -= 10
		if checkRange && int(id) >= dictCount {
			return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		if id == target {
			scratch = append(scratch, uint32(row))
		}
		row++
	}
	return scratch, nil
}

func countInt64EqualPayload(payload []byte, stats ColumnStats, value int64) (int, error) {
	if len(payload) == 0 {
		return 0, fmt.Errorf("missing int64 codec")
	}
	switch payload[0] {
	case int64CodecPlain:
		return countPlainInt64EqualPayload(payload, stats, value)
	case int64CodecDictionary:
		return countDictionaryInt64EqualPayload(payload, stats, value)
	default:
		return 0, fmt.Errorf("unsupported int64 codec %d", payload[0])
	}
}

func countPlainInt64EqualPayload(payload []byte, stats ColumnStats, value int64) (int, error) {
	if err := validatePlainInt64Payload(stats, len(payload)); err != nil {
		return 0, err
	}
	count := 0
	for row := range stats.Count {
		if int64(binary.LittleEndian.Uint64(payload[1+row*8:])) == value {
			count++
		}
	}
	return count, nil
}

func countDictionaryInt64EqualPayload(payload []byte, stats ColumnStats, value int64) (int, error) {
	targetID, ids, idEncoding, dictCount, found, err := findInt64DictionaryID(payload, stats, value)
	if err != nil || !found {
		return 0, err
	}
	count := 0
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return countPackedDictionaryInt64EqualPayload(ids, stats, targetID, int64DictionaryPackedBitWidth(idEncoding), dictCount)
	}
	switch idEncoding {
	case 1:
		target := byte(targetID)
		for row := 0; row < stats.Count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return 0, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
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
				return 0, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if id == target {
				count++
			}
		}
	case 4:
		for row := 0; row < stats.Count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return 0, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			if id == targetID {
				count++
			}
		}
	}
	return count, nil
}

func countPackedDictionaryInt64EqualPayload(ids []byte, stats ColumnStats, targetID uint32, bitWidth int, dictCount int) (int, error) {
	if bitWidth == 0 {
		return stats.Count, nil
	}
	if bitWidth == 10 {
		return countPacked10DictionaryInt64EqualPayload(ids, stats, targetID, dictCount)
	}
	count := 0
	mask := (uint64(1) << uint(bitWidth)) - 1
	if bitWidth == 32 {
		mask = uint64(^uint32(0))
	}
	checkRange := uint64(dictCount) < uint64(1)<<uint(bitWidth)
	bitBuffer := uint64(0)
	bitsInBuffer := 0
	offset := 0
	for row := 0; row < stats.Count; row++ {
		for bitsInBuffer < bitWidth {
			bitBuffer |= uint64(ids[offset]) << bitsInBuffer
			bitsInBuffer += 8
			offset++
		}
		id := uint32(bitBuffer & mask)
		bitBuffer >>= bitWidth
		bitsInBuffer -= bitWidth
		if checkRange && uint64(id) >= uint64(dictCount) {
			return 0, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		if id == targetID {
			count++
		}
	}
	return count, nil
}

func countPacked10DictionaryInt64EqualPayload(ids []byte, stats ColumnStats, targetID uint32, dictCount int) (int, error) {
	count := 0
	checkRange := dictCount < 1<<10
	row := 0
	offset := 0
	groups := stats.Count / 4
	target := targetID
	for range groups {
		packed := uint64(ids[offset]) |
			uint64(ids[offset+1])<<8 |
			uint64(ids[offset+2])<<16 |
			uint64(ids[offset+3])<<24 |
			uint64(ids[offset+4])<<32
		id0 := uint32(packed & 0x3ff)
		id1 := uint32((packed >> 10) & 0x3ff)
		id2 := uint32((packed >> 20) & 0x3ff)
		id3 := uint32((packed >> 30) & 0x3ff)
		if checkRange && (int(id0) >= dictCount || int(id1) >= dictCount || int(id2) >= dictCount || int(id3) >= dictCount) {
			return 0, fmt.Errorf("int64 dictionary id out of range near row %d", row)
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
		offset += 5
	}

	remainingRows := stats.Count - row
	if remainingRows == 0 {
		return count, nil
	}
	bitBuffer := uint64(0)
	bitsInBuffer := 0
	for remaining := 0; remaining < remainingRows; remaining++ {
		for bitsInBuffer < 10 {
			bitBuffer |= uint64(ids[offset]) << bitsInBuffer
			bitsInBuffer += 8
			offset++
		}
		id := uint32(bitBuffer & 0x3ff)
		bitBuffer >>= 10
		bitsInBuffer -= 10
		if checkRange && int(id) >= dictCount {
			return 0, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		if id == target {
			count++
		}
		row++
	}
	return count, nil
}

func findInt64DictionaryID(payload []byte, stats ColumnStats, value int64) (targetID uint32, ids []byte, idEncoding int, dictCount int, found bool, err error) {
	dict, ids, idEncoding, dictCount, err := parseInt64DictionaryPayload(payload, stats)
	if err != nil {
		return 0, nil, 0, 0, false, err
	}
	for id := 0; id < dictCount; id++ {
		if int64(binary.LittleEndian.Uint64(dict[id*8:])) == value {
			targetID = uint32(id)
			found = true
		}
	}
	return targetID, ids, idEncoding, dictCount, found, nil
}

func validateInt64SelectionStats(stats ColumnStats) error {
	if stats.Count < 0 {
		return fmt.Errorf("negative int64 count %d", stats.Count)
	}
	if uint64(stats.Count) > uint64(^uint32(0)) {
		return fmt.Errorf("column %q count %d exceeds selection vector capacity", stats.Name, stats.Count)
	}
	return nil
}
