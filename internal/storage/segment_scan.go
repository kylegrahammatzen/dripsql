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
	idWidth := int(header[4])
	if err := validateInt64DictionaryHeader(stats, dictCount, idWidth); err != nil {
		return nil, err
	}
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, 8)
	if err != nil {
		return nil, err
	}
	idsLen, err := checkedMulInt("int64 dictionary ids length", stats.Count, idWidth)
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

	return s.selectInt64DictionaryIDs(stats, targetID, idWidth, dictCount, found, scratch)
}

func (s *segmentReader) selectInt64DictionaryIDs(stats ColumnStats, targetID uint32, idWidth int, dictCount int, found bool, scratch []uint32) ([]uint32, error) {
	switch idWidth {
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
	targetID, ids, idWidth, dictCount, found, err := findInt64DictionaryID(payload, stats, value)
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
	targetID, ids, idWidth, dictCount, found, err := findInt64DictionaryID(payload, stats, value)
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

func findInt64DictionaryID(payload []byte, stats ColumnStats, value int64) (targetID uint32, ids []byte, idWidth int, dictCount int, found bool, err error) {
	dict, ids, idWidth, dictCount, err := parseInt64DictionaryPayload(payload, stats)
	if err != nil {
		return 0, nil, 0, 0, false, err
	}
	for id := 0; id < dictCount; id++ {
		if int64(binary.LittleEndian.Uint64(dict[id*8:])) == value {
			targetID = uint32(id)
			found = true
		}
	}
	return targetID, ids, idWidth, dictCount, found, nil
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

func validateInt64DictionaryHeader(stats ColumnStats, dictCount int, idWidth int) error {
	if idWidth != 1 && idWidth != 2 && idWidth != 4 {
		return fmt.Errorf("unsupported int64 dictionary id width %d", idWidth)
	}
	if idWidth == 1 && dictCount > 1<<8 {
		return fmt.Errorf("int64 dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if idWidth == 2 && dictCount > 1<<16 {
		return fmt.Errorf("int64 dictionary value count %d exceeds id width %d", dictCount, idWidth)
	}
	if dictCount > stats.Count {
		return fmt.Errorf("int64 dictionary value count %d exceeds row count %d", dictCount, stats.Count)
	}
	return nil
}
