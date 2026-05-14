package types

import "encoding/binary"

// FORValue returns the row's frame-of-reference value (base plus packed
// offset). Callers must ensure e is the encoded state from an EncodingFORBitPack
// vector and that row is in [0, FORData length / width).
func (e *EncodedState) FORValue(row int) int64 {
	return e.FORBase + int64(e.forOffset(row))
}

// FORTargetOffset converts an absolute target value into its packed offset and
// reports whether that offset fits in the page's width — a quick reject for
// equality predicates against values outside the page's range.
func (e *EncodedState) FORTargetOffset(want int64) (uint64, bool) {
	offset := uint64(want) - uint64(e.FORBase)
	if e.FORWidth < 64 && offset >= (uint64(1)<<uint(e.FORWidth)) {
		return 0, false
	}
	return offset, true
}

// FOROffsetMatches reports whether the packed offset at row equals offset
// without converting the row back to its absolute value. Used in the equality
// hot path so the caller can compare against a precomputed target offset.
func (e *EncodedState) FOROffsetMatches(row int, offset uint64) bool {
	return e.forOffset(row) == offset
}

// forOffset extracts the row's packed offset from FORData. Aligned widths take
// direct typed reads; unaligned widths fall back to a single uint64 window
// when in-bounds, and bit-by-bit at the tail.
func (e *EncodedState) forOffset(row int) uint64 {
	switch e.FORWidth {
	case 8:
		return uint64(e.FORData[row])
	case 16:
		return uint64(binary.LittleEndian.Uint16(e.FORData[row*2 : row*2+2]))
	case 32:
		return uint64(binary.LittleEndian.Uint32(e.FORData[row*4 : row*4+4]))
	case 64:
		return binary.LittleEndian.Uint64(e.FORData[row*8 : row*8+8])
	}
	return forOffsetBits(e.FORData, row, e.FORWidth)
}

func forOffsetBits(data []byte, row int, width int) uint64 {
	bitOffset := row * width
	byteOffset := bitOffset >> 3
	if width <= 56 && byteOffset+8 <= len(data) {
		shift := uint(bitOffset & 7)
		mask := (uint64(1) << uint(width)) - 1
		return (binary.LittleEndian.Uint64(data[byteOffset:byteOffset+8]) >> shift) & mask
	}
	var value uint64
	for bit := range width {
		absoluteBit := bitOffset + bit
		if data[absoluteBit>>3]&(byte(1)<<uint(absoluteBit&7)) != 0 {
			value |= uint64(1) << uint(bit)
		}
	}
	return value
}
