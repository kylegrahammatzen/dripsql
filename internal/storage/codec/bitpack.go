package codec

import (
	"encoding/binary"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// bitpackSet writes value into payload at the given row using width bits each.
// The payload is expected to start zeroed so the per-bit OR establishes the
// bits without read-back. Only used for bit-by-bit fallback paths since aligned
// widths take direct typed writes in bitpackPack.
func bitpackSet(payload []byte, row int, width int, value uint64) {
	bitOffset := row * width
	for bit := range width {
		if value&(uint64(1)<<uint(bit)) == 0 {
			continue
		}
		absoluteBit := bitOffset + bit
		payload[absoluteBit>>3] |= byte(1 << uint(absoluteBit&7))
	}
}

// bitpackGetNaive reads a width-bit value from payload at the given row using
// the bit-by-bit path. The width-specialized unpack helpers below are used by
// FOR; DeltaBitPack uses this naive form because the inner loop carries a
// running sum that the typed unpack does not produce.
func bitpackGetNaive(payload []byte, row int, width int) uint64 {
	bitOffset := row * width
	var value uint64
	for bit := range width {
		absoluteBit := bitOffset + bit
		if payload[absoluteBit>>3]&(byte(1)<<uint(absoluteBit&7)) != 0 {
			value |= uint64(1) << uint(bit)
		}
	}
	return value
}

// bitpackPack packs values into dst using width bits each, after subtracting
// base. Aligned widths (8/16/32/64) take direct typed writes; widths 1-56 use
// a single-word window with read-OR-write; widths 57-63 fall back to bit-by-bit.
// Null rows are skipped (the payload is pre-zeroed by make).
func bitpackPack[T ~int16 | ~int32 | ~int64](dst []byte, values []T, valid types.Validity, base int64, width int) {
	n := len(values)
	switch width {
	case 8:
		for i := range n {
			if !types.IsValid(valid, i) {
				continue
			}
			dst[i] = byte(uint64(int64(values[i])) - uint64(base))
		}
	case 16:
		for i := range n {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint16(dst[i*2:i*2+2], uint16(uint64(int64(values[i]))-uint64(base)))
		}
	case 32:
		for i := range n {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint32(dst[i*4:i*4+4], uint32(uint64(int64(values[i]))-uint64(base)))
		}
	case 64:
		for i := range n {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint64(dst[i*8:i*8+8], uint64(int64(values[i]))-uint64(base))
		}
	default:
		if width <= 56 {
			mask := (uint64(1) << uint(width)) - 1
			safe := n
			for safe > 0 && ((safe-1)*width>>3)+8 > len(dst) {
				safe--
			}
			for i := 0; i < safe; i++ {
				if !types.IsValid(valid, i) {
					continue
				}
				v := (uint64(int64(values[i])) - uint64(base)) & mask
				bitOff := i * width
				byteOff := bitOff >> 3
				shift := uint(bitOff & 7)
				word := binary.LittleEndian.Uint64(dst[byteOff : byteOff+8])
				word |= v << shift
				binary.LittleEndian.PutUint64(dst[byteOff:byteOff+8], word)
			}
			for i := safe; i < n; i++ {
				if !types.IsValid(valid, i) {
					continue
				}
				bitpackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
			}
		} else {
			for i := range n {
				if !types.IsValid(valid, i) {
					continue
				}
				bitpackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
			}
		}
	}
}

// bitpackUnpack decodes a bitpacked payload into dst, adding base. Width is
// hoisted out of the row loop. Aligned widths (8/16/32/64) and the common
// 10/12/24 widths take direct read paths; other widths 1-56 use a single-word
// window (one uint64 read per value); widths 57-63 fall back to bit-by-bit.
// Tail rows whose 8-byte read would overrun the payload also use the
// bit-by-bit path.
func bitpackUnpack[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64, width int) {
	n := len(dst)
	switch width {
	case 8:
		for i := range n {
			dst[i] = T(base + int64(data[i]))
		}
	case 10:
		bitpackUnpackWidth10(data, dst, base)
	case 12:
		bitpackUnpackWidth12(data, dst, base)
	case 16:
		for i := range n {
			dst[i] = T(base + int64(binary.LittleEndian.Uint16(data[i*2:i*2+2])))
		}
	case 24:
		bitpackUnpackWidth24(data, dst, base)
	case 32:
		for i := range n {
			dst[i] = T(base + int64(binary.LittleEndian.Uint32(data[i*4:i*4+4])))
		}
	case 64:
		for i := range n {
			dst[i] = T(base + int64(binary.LittleEndian.Uint64(data[i*8:i*8+8])))
		}
	default:
		if width <= 56 {
			mask := (uint64(1) << uint(width)) - 1
			safe := n
			for safe > 0 && ((safe-1)*width>>3)+8 > len(data) {
				safe--
			}
			for i := 0; i < safe; i++ {
				bitOff := i * width
				byteOff := bitOff >> 3
				shift := uint(bitOff & 7)
				word := binary.LittleEndian.Uint64(data[byteOff : byteOff+8])
				dst[i] = T(base + int64((word>>shift)&mask))
			}
			for i := safe; i < n; i++ {
				dst[i] = T(base + int64(bitpackGetNaive(data, i, width)))
			}
		} else {
			for i := range n {
				dst[i] = T(base + int64(bitpackGetNaive(data, i, width)))
			}
		}
	}
}

func bitpackUnpackWidth10[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
	i := 0
	in := 0
	for i+4 <= len(dst) && in+5 <= len(data) {
		word := uint64(data[in]) |
			uint64(data[in+1])<<8 |
			uint64(data[in+2])<<16 |
			uint64(data[in+3])<<24 |
			uint64(data[in+4])<<32
		dst[i] = T(base + int64(word&0x3ff))
		dst[i+1] = T(base + int64((word>>10)&0x3ff))
		dst[i+2] = T(base + int64((word>>20)&0x3ff))
		dst[i+3] = T(base + int64((word>>30)&0x3ff))
		i += 4
		in += 5
	}
	for ; i < len(dst); i++ {
		dst[i] = T(base + int64(bitpackGetNaive(data, i, 10)))
	}
}

func bitpackUnpackWidth12[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
	i := 0
	in := 0
	for i+2 <= len(dst) && in+3 <= len(data) {
		word := uint64(data[in]) |
			uint64(data[in+1])<<8 |
			uint64(data[in+2])<<16
		dst[i] = T(base + int64(word&0xfff))
		dst[i+1] = T(base + int64((word>>12)&0xfff))
		i += 2
		in += 3
	}
	for ; i < len(dst); i++ {
		dst[i] = T(base + int64(bitpackGetNaive(data, i, 12)))
	}
}

func bitpackUnpackWidth24[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
	for i := range dst {
		in := i * 3
		value := uint64(data[in]) |
			uint64(data[in+1])<<8 |
			uint64(data[in+2])<<16
		dst[i] = T(base + int64(value))
	}
}
