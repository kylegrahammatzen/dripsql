package codec

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// FORBitPack encodes integer-shaped vectors as a frame-of-reference base plus
// bit-packed offsets. Best for clustered numeric ranges: a page of int64 values
// in a 1024-wide window encodes at ~10 bits/row, ~6× smaller than Plain.
type FORBitPack struct{}

func (FORBitPack) Encoding() types.Encoding { return types.EncodingFORBitPack }

func (FORBitPack) Encode(v types.Vec) (Page, error) {
	prepared, ok := (FORBitPack{}).Prepare(v)
	if !ok {
		return Page{}, fmt.Errorf("for+bitpack cannot encode %s/%s", v.Kind, v.Encoding)
	}
	return prepared.EncodeInto(nil)
}

func (FORBitPack) Prepare(v types.Vec) (PreparedEncoding, bool) {
	base, maxValue, ok := forBitPackRange(v)
	if !ok {
		return nil, false
	}
	width := bits.Len64(uint64(maxValue) - uint64(base))
	if width == 0 {
		return nil, false
	}
	return preparedFORBitPack{vec: v, base: base, width: width, size: forBitPackPayloadSize(v.Len, v.Valid, width)}, true
}

type preparedFORBitPack struct {
	vec   types.Vec
	base  int64
	width int
	size  int
}

func (p preparedFORBitPack) Encoding() types.Encoding { return types.EncodingFORBitPack }

func (p preparedFORBitPack) Size() int { return p.size }

func (p preparedFORBitPack) EncodeInto(scratch []byte) (Page, error) {
	payload := preparedPayload(scratch, p.size)
	pos := writeValidity(payload, p.vec.Valid)
	binary.LittleEndian.PutUint64(payload[pos:pos+8], uint64(p.base))
	pos += 8
	payload[pos] = byte(p.width)
	pos++
	switch p.vec.Kind {
	case types.VecInt16:
		forBitPackPackInt16(payload[pos:], p.vec.I16[:p.vec.Len], p.vec.Valid, p.base, p.width)
	case types.VecInt32, types.VecDate:
		forBitPackPackInt32(payload[pos:], p.vec.I32[:p.vec.Len], p.vec.Valid, p.base, p.width)
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		forBitPackPackInt64(payload[pos:], p.vec.I64[:p.vec.Len], p.vec.Valid, p.base, p.width)
	}
	return Page{Kind: p.vec.Kind, Encoding: types.EncodingFORBitPack, Rows: p.vec.Len, NullCount: types.NullCount(p.vec.Valid, p.vec.Len), Payload: payload}, nil
}

func (FORBitPack) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (FORBitPack{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (FORBitPack) DecodeEncoded(page Page) (types.Vec, error) {
	if page.Encoding != types.EncodingFORBitPack {
		return types.Vec{}, fmt.Errorf("for+bitpack cannot decode encoded %s", page.Encoding)
	}
	if !forBitPackSupportedKind(page.Kind) {
		return types.Vec{}, fmt.Errorf("for+bitpack unsupported kind %s", page.Kind)
	}
	valid, pos, err := readValidity(page.Payload, page.Rows, page.NullCount)
	if err != nil {
		return types.Vec{}, err
	}
	if len(page.Payload)-pos < 9 {
		return types.Vec{}, fmt.Errorf("for+bitpack header truncated")
	}
	base := int64(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
	pos += 8
	width := int(page.Payload[pos])
	pos++
	if width <= 0 || width > 64 {
		return types.Vec{}, fmt.Errorf("for+bitpack invalid width %d", width)
	}
	packedBytes := (page.Rows*width + 7) / 8
	if len(page.Payload)-pos < packedBytes {
		return types.Vec{}, fmt.Errorf("for+bitpack values truncated")
	}
	data := make([]byte, packedBytes)
	copy(data, page.Payload[pos:pos+packedBytes])
	return types.Vec{Kind: page.Kind, Encoding: types.EncodingFORBitPack, Len: page.Rows, Valid: valid, FORBase: base, FORWidth: width, FORData: data}, nil
}

func (FORBitPack) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("for+bitpack decode destination is nil")
	}
	if page.Encoding != types.EncodingFORBitPack {
		return fmt.Errorf("for+bitpack cannot decode %s", page.Encoding)
	}
	if !forBitPackSupportedKind(page.Kind) {
		return fmt.Errorf("for+bitpack unsupported kind %s", page.Kind)
	}
	valid, pos, err := readValidityInto(page.Payload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	if len(page.Payload)-pos < 9 {
		return fmt.Errorf("for+bitpack header truncated")
	}
	base := int64(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
	pos += 8
	width := int(page.Payload[pos])
	pos++
	if width <= 0 || width > 64 {
		return fmt.Errorf("for+bitpack invalid width %d", width)
	}
	packedBytes := (page.Rows*width + 7) / 8
	if len(page.Payload)-pos < packedBytes {
		return fmt.Errorf("for+bitpack values truncated")
	}
	preparePlainDecodeVec(dst, page.Kind)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	dst.Valid = valid
	data := page.Payload[pos : pos+packedBytes]
	switch page.Kind {
	case types.VecInt16:
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		forBitPackUnpackInt16(data, dst.I16, base, width)
	case types.VecInt32, types.VecDate:
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		forBitPackUnpackInt32(data, dst.I32, base, width)
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		forBitPackUnpackInt64(data, dst.I64, base, width)
	}
	return nil
}

func (FORBitPack) Estimate(v types.Vec) (int, bool) {
	base, maxValue, ok := forBitPackRange(v)
	if !ok {
		return 0, false
	}
	width := bits.Len64(uint64(maxValue) - uint64(base))
	if width == 0 {
		return 0, false
	}
	return forBitPackPayloadSize(v.Len, v.Valid, width), true
}

func forBitPackSupportedKind(kind types.VecKind) bool {
	switch kind {
	case types.VecInt16, types.VecInt32, types.VecDate, types.VecInt64, types.VecTimestamp, types.VecTime:
		return true
	default:
		return false
	}
}

func forBitPackValue(v types.Vec, row int) int64 {
	switch v.Kind {
	case types.VecInt16:
		return int64(v.I16[row])
	case types.VecInt32, types.VecDate:
		return int64(v.I32[row])
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return v.I64[row]
	default:
		panic("for+bitpack unsupported kind")
	}
}

func forBitPackRange(v types.Vec) (int64, int64, bool) {
	if v.Encoding != types.EncodingFlat || v.Len <= 0 || !forBitPackSupportedKind(v.Kind) {
		return 0, 0, false
	}
	found := false
	minValue := int64(0)
	maxValue := int64(0)
	for row := 0; row < v.Len; row++ {
		if !types.IsValid(v.Valid, row) {
			continue
		}
		value := forBitPackValue(v, row)
		if !found || value < minValue {
			minValue = value
		}
		if !found || value > maxValue {
			maxValue = value
		}
		found = true
	}
	return minValue, maxValue, found
}

func forBitPackPayloadSize(rows int, valid types.Validity, width int) int {
	return validityBytes(valid) + 8 + 1 + (rows*width+7)/8
}

func forBitPackSet(payload []byte, row int, width int, value uint64) {
	bitOffset := row * width
	for bit := 0; bit < width; bit++ {
		if value&(uint64(1)<<uint(bit)) == 0 {
			continue
		}
		absoluteBit := bitOffset + bit
		payload[absoluteBit>>3] |= byte(1 << uint(absoluteBit&7))
	}
}

func forBitPackGetNaive(payload []byte, row int, width int) uint64 {
	bitOffset := row * width
	var value uint64
	for bit := 0; bit < width; bit++ {
		absoluteBit := bitOffset + bit
		if payload[absoluteBit>>3]&(byte(1)<<uint(absoluteBit&7)) != 0 {
			value |= uint64(1) << uint(bit)
		}
	}
	return value
}

// forBitPackPackInt64 packs values into dst using width bits each, after
// subtracting base. Mirrors the unpacker structure: aligned widths take
// direct typed writes; widths 1-56 use a single-word window with read-OR-
// write; widths 57-63 fall back to bit-by-bit. Null rows are skipped (the
// payload is pre-zeroed by make).
func forBitPackPackInt64(dst []byte, values []int64, valid types.Validity, base int64, width int) {
	n := len(values)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			dst[i] = byte(uint64(values[i]) - uint64(base))
		}
	case 16:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint16(dst[i*2:i*2+2], uint16(uint64(values[i])-uint64(base)))
		}
	case 32:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint32(dst[i*4:i*4+4], uint32(uint64(values[i])-uint64(base)))
		}
	case 64:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint64(dst[i*8:i*8+8], uint64(values[i])-uint64(base))
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
				v := (uint64(values[i]) - uint64(base)) & mask
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
				forBitPackSet(dst, i, width, uint64(values[i])-uint64(base))
			}
		} else {
			for i := 0; i < n; i++ {
				if !types.IsValid(valid, i) {
					continue
				}
				forBitPackSet(dst, i, width, uint64(values[i])-uint64(base))
			}
		}
	}
}

func forBitPackPackInt32(dst []byte, values []int32, valid types.Validity, base int64, width int) {
	n := len(values)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			dst[i] = byte(uint64(int64(values[i])) - uint64(base))
		}
	case 16:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint16(dst[i*2:i*2+2], uint16(uint64(int64(values[i]))-uint64(base)))
		}
	case 32:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint32(dst[i*4:i*4+4], uint32(uint64(int64(values[i]))-uint64(base)))
		}
	default:
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
			forBitPackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
		}
	}
}

func forBitPackPackInt16(dst []byte, values []int16, valid types.Validity, base int64, width int) {
	n := len(values)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			dst[i] = byte(uint64(int64(values[i])) - uint64(base))
		}
	case 16:
		for i := 0; i < n; i++ {
			if !types.IsValid(valid, i) {
				continue
			}
			binary.LittleEndian.PutUint16(dst[i*2:i*2+2], uint16(uint64(int64(values[i]))-uint64(base)))
		}
	default:
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
			forBitPackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
		}
	}
}

// forBitPackUnpackInt64 decodes a bitpacked payload into dst, adding base.
// Width is hoisted out of the row loop. Aligned widths (8/16/32/64) take
// direct read paths; widths 1-56 use a single-word window (one uint64 read per
// value); widths 57-63 fall back to bit-by-bit. Tail rows whose 8-byte read
// would overrun the payload also use the bit-by-bit path.
func forBitPackUnpackInt64(data []byte, dst []int64, base int64, width int) {
	n := len(dst)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			dst[i] = base + int64(data[i])
		}
	case 10:
		forBitPackUnpackWidth10(data, dst, base)
	case 12:
		forBitPackUnpackWidth12(data, dst, base)
	case 16:
		for i := 0; i < n; i++ {
			dst[i] = base + int64(binary.LittleEndian.Uint16(data[i*2:i*2+2]))
		}
	case 24:
		forBitPackUnpackWidth24(data, dst, base)
	case 32:
		for i := 0; i < n; i++ {
			dst[i] = base + int64(binary.LittleEndian.Uint32(data[i*4:i*4+4]))
		}
	case 64:
		for i := 0; i < n; i++ {
			dst[i] = base + int64(binary.LittleEndian.Uint64(data[i*8:i*8+8]))
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
				dst[i] = base + int64((word>>shift)&mask)
			}
			for i := safe; i < n; i++ {
				dst[i] = base + int64(forBitPackGetNaive(data, i, width))
			}
		} else {
			for i := 0; i < n; i++ {
				dst[i] = base + int64(forBitPackGetNaive(data, i, width))
			}
		}
	}
}

func forBitPackUnpackInt32(data []byte, dst []int32, base int64, width int) {
	n := len(dst)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			dst[i] = int32(base + int64(data[i]))
		}
	case 10:
		forBitPackUnpackWidth10(data, dst, base)
	case 12:
		forBitPackUnpackWidth12(data, dst, base)
	case 16:
		for i := 0; i < n; i++ {
			dst[i] = int32(base + int64(binary.LittleEndian.Uint16(data[i*2:i*2+2])))
		}
	case 24:
		forBitPackUnpackWidth24(data, dst, base)
	case 32:
		for i := 0; i < n; i++ {
			dst[i] = int32(base + int64(binary.LittleEndian.Uint32(data[i*4:i*4+4])))
		}
	default:
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
			dst[i] = int32(base + int64((word>>shift)&mask))
		}
		for i := safe; i < n; i++ {
			dst[i] = int32(base + int64(forBitPackGetNaive(data, i, width)))
		}
	}
}

func forBitPackUnpackInt16(data []byte, dst []int16, base int64, width int) {
	n := len(dst)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			dst[i] = int16(base + int64(data[i]))
		}
	case 10:
		forBitPackUnpackWidth10(data, dst, base)
	case 12:
		forBitPackUnpackWidth12(data, dst, base)
	case 16:
		for i := 0; i < n; i++ {
			dst[i] = int16(base + int64(binary.LittleEndian.Uint16(data[i*2:i*2+2])))
		}
	default:
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
			dst[i] = int16(base + int64((word>>shift)&mask))
		}
		for i := safe; i < n; i++ {
			dst[i] = int16(base + int64(forBitPackGetNaive(data, i, width)))
		}
	}
}

func forBitPackUnpackWidth10[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
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
		dst[i] = T(base + int64(forBitPackGetNaive(data, i, 10)))
	}
}

func forBitPackUnpackWidth12[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
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
		dst[i] = T(base + int64(forBitPackGetNaive(data, i, 12)))
	}
}

func forBitPackUnpackWidth24[T ~int32 | ~int64](data []byte, dst []T, base int64) {
	for i := 0; i < len(dst); i++ {
		in := i * 3
		value := uint64(data[in]) |
			uint64(data[in+1])<<8 |
			uint64(data[in+2])<<16
		dst[i] = T(base + int64(value))
	}
}
