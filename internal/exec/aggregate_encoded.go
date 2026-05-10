package exec

import (
	"encoding/binary"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func sumInt64VectorSelected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return sumFORBitPackInt64Selected(v, sel, base)
	case types.EncodingConstant:
		return sumConstantInt64Selected(v, sel, base)
	default:
		return sumInt64Selected(v.I64, v.Valid, sel, base)
	}
}

func sumInt32VectorSelected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return sumFORBitPackInt32Selected(v, sel)
	case types.EncodingConstant:
		return sumConstantInt32Selected(v, sel)
	default:
		return sumInt32Selected(v.I32, v.Valid, sel)
	}
}

func minMaxInt64VectorSelected(v types.Vec, sel types.SelectionMask, min bool) (int64, bool) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return minMaxFORBitPackInt64Selected(v, sel, min)
	case types.EncodingConstant:
		if !v.ConstantValid || countValidSelected(v.Valid, sel) == 0 {
			return 0, false
		}
		return v.ConstantI64, true
	default:
		return minMaxInt64Selected(v.I64, v.Valid, sel, min)
	}
}

func sumConstantInt64Selected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	if !v.ConstantValid {
		return base, 0, false
	}
	count := countValidSelected(v.Valid, sel)
	sum := base
	for i := int64(0); i < count; i++ {
		next, ok := AddInt64(sum, v.ConstantI64)
		if !ok {
			return 0, 0, true
		}
		sum = next
	}
	return sum, count, false
}

func sumConstantInt32Selected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	if !v.ConstantValid {
		return 0, 0
	}
	count := countValidSelected(v.Valid, sel)
	return v.ConstantI64 * count, count
}

func sumFORBitPackInt64Selected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	sum := base
	var count int64
	var overflow bool
	forEachFORBitPackSelected(v, sel, func(value int64) {
		if overflow {
			return
		}
		next, ok := AddInt64(sum, value)
		if !ok {
			overflow = true
			return
		}
		sum = next
		count++
	})
	if overflow {
		return 0, 0, true
	}
	return sum, count, false
}

func sumFORBitPackInt32Selected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	var sum int64
	var count int64
	forEachFORBitPackSelected(v, sel, func(value int64) {
		sum += value
		count++
	})
	return sum, count
}

func minMaxFORBitPackInt64Selected(v types.Vec, sel types.SelectionMask, min bool) (int64, bool) {
	var out int64
	set := false
	forEachFORBitPackSelected(v, sel, func(value int64) {
		if !set || (min && value < out) || (!min && value > out) {
			out = value
			set = true
		}
	})
	return out, set
}

func forEachFORBitPackSelected(v types.Vec, sel types.SelectionMask, visit func(int64)) {
	if selectionAll(sel) {
		for row := 0; row < sel.Rows; row++ {
			if types.IsValid(v.Valid, row) {
				visit(forBitPackEncodedValue(v, row))
			}
		}
		return
	}
	sel.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) {
			visit(forBitPackEncodedValue(v, row))
		}
	})
}

func forBitPackEncodedValue(v types.Vec, row int) int64 {
	return v.FORBase + int64(forBitPackEncodedOffset(v, row))
}

func forBitPackEncodedOffset(v types.Vec, row int) uint64 {
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
		return forBitPackEncodedOffsetBits(v.FORData, row, width)
	}
}

func forBitPackEncodedOffsetBits(data []byte, row int, width int) uint64 {
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
