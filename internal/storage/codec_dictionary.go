package storage

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

const (
	dictionaryPackedIDFlag             = 1 << 7
	dictionaryCountWidth               = 8
	maxLinearDictionaryValues          = 16
	maxDictionaryValues                = 1 << 16
	maxStringDictionaryDataLen         = 1 << 20
	dictionarySampleRows               = 4096
	dictionarySampleMinRows            = 1024
	dictionarySampleMaxDistinctPercent = 80
)

func dictionaryIDEncoding(dictCount int, rows int) int {
	fixedWidth := dictionaryIDWidth(dictCount)
	packedWidth := dictionaryPackedBitWidthForCount(dictCount)
	if packedWidth >= fixedWidth*8 {
		return fixedWidth
	}
	fixedLen, fixedErr := checkedMulInt("dictionary fixed ids length", rows, fixedWidth)
	packedLen, packedErr := packedDictionaryIDsLen(rows, packedWidth)
	if fixedErr == nil && packedErr == nil && packedLen < fixedLen {
		return dictionaryPackedIDFlag | packedWidth
	}
	return fixedWidth
}

func dictionaryIDWidth(dictCount int) int {
	if dictCount <= 1<<8 {
		return 1
	}
	if dictCount <= 1<<16 {
		return 2
	}
	return 4
}

func dictionaryPackedBitWidthForCount(dictCount int) int {
	if dictCount <= 1 {
		return 0
	}
	return bits.Len(uint(dictCount - 1))
}

func dictionaryIDsLen(rows int, idEncoding int) (int, error) {
	if idEncoding&dictionaryPackedIDFlag != 0 {
		return packedDictionaryIDsLen(rows, idEncoding&^dictionaryPackedIDFlag)
	}
	return checkedMulInt("dictionary ids length", rows, idEncoding)
}

func packedDictionaryIDsLen(rows int, bitWidth int) (int, error) {
	bitsLen, err := checkedMulInt("dictionary packed id bits", rows, bitWidth)
	if err != nil {
		return 0, err
	}
	bitsLen, err = checkedAddInt("dictionary packed id bits", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return bitsLen / 8, nil
}

func dictionaryCountsLen(dictCount int) (int, error) {
	return checkedMulInt("dictionary counts length", dictCount, dictionaryCountWidth)
}

func dictionaryCountAt(counts []byte, id int) uint64 {
	return binary.LittleEndian.Uint64(counts[id*dictionaryCountWidth:])
}

func validateDictionaryCounts(label string, counts []byte, dictCount int, rows int) error {
	if rows < 0 {
		return fmt.Errorf("negative %s dictionary row count %d", label, rows)
	}
	expected, err := dictionaryCountsLen(dictCount)
	if err != nil {
		return err
	}
	if len(counts) != expected {
		return fmt.Errorf("%s dictionary counts have %d bytes, want %d", label, len(counts), expected)
	}
	var total uint64
	for id := 0; id < dictCount; id++ {
		count := dictionaryCountAt(counts, id)
		if ^uint64(0)-total < count {
			return fmt.Errorf("%s dictionary counts overflow", label)
		}
		total += count
	}
	if total != uint64(rows) {
		return fmt.Errorf("%s dictionary counts sum %d does not match row count %d", label, total, rows)
	}
	return nil
}

func validateDictionaryIDEncoding(label string, rows int, dictCount int, idEncoding int) error {
	if dictCount <= 0 {
		return fmt.Errorf("%s dictionary value count %d must be positive", label, dictCount)
	}
	if dictCount > rows {
		return fmt.Errorf("%s dictionary value count %d exceeds row count %d", label, dictCount, rows)
	}
	if idEncoding&dictionaryPackedIDFlag != 0 {
		bitWidth := idEncoding &^ dictionaryPackedIDFlag
		if bitWidth < 0 || bitWidth > 32 {
			return fmt.Errorf("unsupported %s dictionary packed bit width %d", label, bitWidth)
		}
		if bitWidth == 0 {
			if dictCount > 1 {
				return fmt.Errorf("%s dictionary value count %d exceeds packed bit width %d", label, dictCount, bitWidth)
			}
			return nil
		}
		if uint64(dictCount) > uint64(1)<<uint(bitWidth) {
			return fmt.Errorf("%s dictionary value count %d exceeds packed bit width %d", label, dictCount, bitWidth)
		}
		return nil
	}
	if idEncoding != 1 && idEncoding != 2 && idEncoding != 4 {
		return fmt.Errorf("unsupported %s dictionary id width %d", label, idEncoding)
	}
	if idEncoding == 1 && dictCount > 1<<8 {
		return fmt.Errorf("%s dictionary value count %d exceeds id width %d", label, dictCount, idEncoding)
	}
	if idEncoding == 2 && dictCount > 1<<16 {
		return fmt.Errorf("%s dictionary value count %d exceeds id width %d", label, dictCount, idEncoding)
	}
	return nil
}

func appendDictionaryID(out []byte, id uint32, idWidth int) []byte {
	switch idWidth {
	case 1:
		out = append(out, byte(id))
	case 2:
		out = binary.LittleEndian.AppendUint16(out, uint16(id))
	case 4:
		out = binary.LittleEndian.AppendUint32(out, id)
	}
	return out
}

func dictionaryIDAt(ids []byte, row int, idEncoding int, dictCount int, label string) (uint32, error) {
	if idEncoding&dictionaryPackedIDFlag != 0 {
		id := packedDictionaryIDAt(ids, row, idEncoding&^dictionaryPackedIDFlag)
		if uint64(id) >= uint64(dictCount) {
			return 0, fmt.Errorf("%s dictionary id %d out of range at row %d", label, id, row)
		}
		return id, nil
	}
	return fixedDictionaryIDAt(ids, row, idEncoding, dictCount, label)
}

func fixedDictionaryIDAt(ids []byte, row int, idWidth int, dictCount int, label string) (uint32, error) {
	var id uint32
	switch idWidth {
	case 1:
		id = uint32(ids[row])
	case 2:
		id = uint32(binary.LittleEndian.Uint16(ids[row*2:]))
	case 4:
		id = binary.LittleEndian.Uint32(ids[row*4:])
	default:
		return 0, fmt.Errorf("unsupported %s dictionary id width %d", label, idWidth)
	}
	if uint64(id) >= uint64(dictCount) {
		return 0, fmt.Errorf("%s dictionary id %d out of range at row %d", label, id, row)
	}
	return id, nil
}

func setPackedDictionaryID(ids []byte, row int, bitWidth int, id uint32) {
	if bitWidth == 0 {
		return
	}
	bitOffset := row * bitWidth
	byteOffset := bitOffset / 8
	shift := uint(bitOffset % 8)
	value := uint64(id) << shift
	bytes := (int(shift) + bitWidth + 7) / 8
	switch bytes {
	case 1:
		ids[byteOffset] |= byte(value)
	case 2:
		v := binary.LittleEndian.Uint16(ids[byteOffset:]) | uint16(value)
		binary.LittleEndian.PutUint16(ids[byteOffset:], v)
	case 3:
		v := binary.LittleEndian.Uint16(ids[byteOffset:]) | uint16(value)
		binary.LittleEndian.PutUint16(ids[byteOffset:], v)
		ids[byteOffset+2] |= byte(value >> 16)
	case 4:
		v := binary.LittleEndian.Uint32(ids[byteOffset:]) | uint32(value)
		binary.LittleEndian.PutUint32(ids[byteOffset:], v)
	case 5:
		v := binary.LittleEndian.Uint32(ids[byteOffset:]) | uint32(value)
		binary.LittleEndian.PutUint32(ids[byteOffset:], v)
		ids[byteOffset+4] |= byte(value >> 32)
	}
}

func packedDictionaryIDAt(ids []byte, row int, bitWidth int) uint32 {
	if bitWidth == 0 {
		return 0
	}
	bitOffset := row * bitWidth
	byteOffset := bitOffset / 8
	shift := uint(bitOffset % 8)
	bytes := (int(shift) + bitWidth + 7) / 8
	var value uint64
	switch bytes {
	case 1:
		value = uint64(ids[byteOffset])
	case 2:
		value = uint64(binary.LittleEndian.Uint16(ids[byteOffset:]))
	case 3:
		value = uint64(binary.LittleEndian.Uint16(ids[byteOffset:])) | uint64(ids[byteOffset+2])<<16
	case 4:
		value = uint64(binary.LittleEndian.Uint32(ids[byteOffset:]))
	case 5:
		value = uint64(binary.LittleEndian.Uint32(ids[byteOffset:])) | uint64(ids[byteOffset+4])<<32
	}
	value >>= shift
	if bitWidth == 32 {
		return uint32(value)
	}
	mask := (uint64(1) << uint(bitWidth)) - 1
	return uint32(value & mask)
}
