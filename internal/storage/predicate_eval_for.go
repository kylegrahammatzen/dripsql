package storage

import (
	"encoding/binary"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func evalFORBitPackLeafBound(v types.Vec, pred boundPredicate, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalFORBitPackEq(v, pred.int64Value, false, input, out)
	case PredicateOpNotEq:
		return evalFORBitPackEq(v, pred.int64Value, true, input, out)
	case PredicateOpLess:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value < want }, input, out)
	case PredicateOpLessEqual:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value <= want }, input, out)
	case PredicateOpGreater:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value > want }, input, out)
	case PredicateOpGreaterEqual:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value >= want }, input, out)
	case PredicateOpBetween:
		return evalFORBitPackBetween(v, pred.lo, pred.hi, input, out)
	case PredicateOpIn:
		return evalFORBitPackIn(v, pred.intSet, false, input, out)
	case PredicateOpNotIn:
		return evalFORBitPackIn(v, pred.intSet, true, input, out)
	default:
		return 0
	}
}

func evalFORBitPackEq(v types.Vec, want int64, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	wantOffset, ok := forBitPackTargetOffset(v, want)
	if !ok {
		if invert {
			return selectValidRows(v.Valid, v.Len, input, out)
		}
		return 0
	}
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && (forBitPackOffset(v, row) == wantOffset) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && (forBitPackOffset(v, row) == wantOffset) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackCompare(v types.Vec, want int64, match func(value int64, want int64) bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && match(forBitPackValue(v, row), want) {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && match(forBitPackValue(v, row), want) {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackBetween(v types.Vec, lo int64, hi int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			value := forBitPackValue(v, row)
			if types.IsValid(v.Valid, row) && lo <= value && value <= hi {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		value := forBitPackValue(v, row)
		if types.IsValid(v.Valid, row) && lo <= value && value <= hi {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackIn(v types.Vec, matcher int64Matcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && matcher.Has(forBitPackValue(v, row)) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && matcher.Has(forBitPackValue(v, row)) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func forBitPackTargetOffset(v types.Vec, want int64) (uint64, bool) {
	offset := uint64(want) - uint64(v.FORBase)
	if v.FORWidth < 64 && offset >= (uint64(1)<<uint(v.FORWidth)) {
		return 0, false
	}
	return offset, true
}

func forBitPackValue(v types.Vec, row int) int64 {
	return v.FORBase + int64(forBitPackOffset(v, row))
}

func forBitPackOffset(v types.Vec, row int) uint64 {
	width := v.FORWidth
	switch width {
	case 8:
		return uint64(v.FORData[row])
	case 16:
		return uint64(binary.LittleEndian.Uint16(v.FORData[row*2 : row*2+2]))
	case 32:
		return uint64(binary.LittleEndian.Uint32(v.FORData[row*4 : row*4+4]))
	case 64:
		return binary.LittleEndian.Uint64(v.FORData[row*8 : row*8+8])
	default:
		return forBitPackOffsetBits(v.FORData, row, width)
	}
}

func forBitPackOffsetBits(data []byte, row int, width int) uint64 {
	bitOffset := row * width
	if width <= 56 && (bitOffset>>3)+8 <= len(data) {
		byteOffset := bitOffset >> 3
		shift := uint(bitOffset & 7)
		mask := (uint64(1) << uint(width)) - 1
		return (binary.LittleEndian.Uint64(data[byteOffset:byteOffset+8]) >> shift) & mask
	}
	var value uint64
	for bit := 0; bit < width; bit++ {
		absoluteBit := bitOffset + bit
		if data[absoluteBit>>3]&(byte(1)<<uint(absoluteBit&7)) != 0 {
			value |= uint64(1) << uint(bit)
		}
	}
	return value
}
