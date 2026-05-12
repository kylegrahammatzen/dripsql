package codec

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"unsafe"

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
		forBitPackPack(payload[pos:], p.vec.I16[:p.vec.Len], p.vec.Valid, p.base, p.width)
	case types.VecInt32, types.VecDate:
		forBitPackPack(payload[pos:], p.vec.I32[:p.vec.Len], p.vec.Valid, p.base, p.width)
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		forBitPackPack(payload[pos:], p.vec.I64[:p.vec.Len], p.vec.Valid, p.base, p.width)
	case types.VecEnum32:
		// Enum codes are stored as uint32; reinterpret as int32 since the
		// FOR pack writes (value - base) as a width-bit unsigned offset and
		// the sign of the input doesn't affect that arithmetic.
		if p.vec.Len > 0 {
			i32 := unsafe.Slice((*int32)(unsafe.Pointer(&p.vec.U32[0])), p.vec.Len)
			forBitPackPack(payload[pos:], i32, p.vec.Valid, p.base, p.width)
		}
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
	if !page.Kind.IsFORPackable() {
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
	return types.Vec{Kind: page.Kind, Encoding: types.EncodingFORBitPack, Len: page.Rows, Valid: valid, Encoded: &types.EncodedState{FORBase: base, FORWidth: width, FORData: data}}, nil
}

func (FORBitPack) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("for+bitpack decode destination is nil")
	}
	if page.Encoding != types.EncodingFORBitPack {
		return fmt.Errorf("for+bitpack cannot decode %s", page.Encoding)
	}
	if !page.Kind.IsFORPackable() {
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
	resetVecForDecode(dst, page.Kind, false)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	dst.Valid = valid
	data := page.Payload[pos : pos+packedBytes]
	switch page.Kind {
	case types.VecInt16:
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		forBitPackUnpack(data, dst.I16, base, width)
	case types.VecInt32, types.VecDate:
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		forBitPackUnpack(data, dst.I32, base, width)
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		forBitPackUnpack(data, dst.I64, base, width)
	case types.VecEnum32:
		dst.U32 = resizeSlice(dst.U32, page.Rows)
		if page.Rows > 0 {
			i32 := unsafe.Slice((*int32)(unsafe.Pointer(&dst.U32[0])), page.Rows)
			forBitPackUnpack(data, i32, base, width)
		}
	}
	return nil
}

// DecodeSelected bulk-decodes the page and ignores sel because the per-row offset-extraction path benchmarked 3-11× slower than DecodeInto at every density we measured.
func (FORBitPack) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	if page.Encoding != types.EncodingFORBitPack {
		return types.Vec{}, fmt.Errorf("for+bitpack cannot decode selected %s", page.Encoding)
	}
	if !page.Kind.IsFORPackable() {
		return types.Vec{}, fmt.Errorf("for+bitpack unsupported kind %s", page.Kind)
	}
	if sel.Rows != page.Rows {
		return types.Vec{}, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, page.Rows)
	}
	return (FORBitPack{}).Decode(page)
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

func forBitPackRange(v types.Vec) (int64, int64, bool) {
	if v.Encoding != types.EncodingFlat || v.Len <= 0 || !v.Kind.IsFORPackable() {
		return 0, 0, false
	}
	switch v.Kind {
	case types.VecInt16:
		return rangeOver(v.I16[:v.Len], v.Valid)
	case types.VecInt32, types.VecDate:
		return rangeOver(v.I32[:v.Len], v.Valid)
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return rangeOver(v.I64[:v.Len], v.Valid)
	case types.VecEnum32:
		return rangeOver(v.U32[:v.Len], v.Valid)
	}
	return 0, 0, false
}

// rangeOver returns the min/max valid value in values, or (0,0,false) if no
// row is valid. Hoists the kind switch out of the per-row loop so the inner
// loop is a tight typed comparison.
func rangeOver[T int16 | int32 | int64 | uint32](values []T, valid types.Validity) (int64, int64, bool) {
	found := false
	var minValue, maxValue T
	for i, v := range values {
		if !types.IsValid(valid, i) {
			continue
		}
		if !found || v < minValue {
			minValue = v
		}
		if !found || v > maxValue {
			maxValue = v
		}
		found = true
	}
	return int64(minValue), int64(maxValue), found
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

// forBitPackPack packs values into dst using width bits each, after
// subtracting base. Aligned widths take direct typed writes; widths 1-56 use a
// single-word window with read-OR-write; widths 57-63 fall back to bit-by-bit.
// Null rows are skipped (the payload is pre-zeroed by make).
func forBitPackPack[T ~int16 | ~int32 | ~int64](dst []byte, values []T, valid types.Validity, base int64, width int) {
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
	case 64:
		for i := 0; i < n; i++ {
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
				forBitPackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
			}
		} else {
			for i := 0; i < n; i++ {
				if !types.IsValid(valid, i) {
					continue
				}
				forBitPackSet(dst, i, width, uint64(int64(values[i]))-uint64(base))
			}
		}
	}
}

// forBitPackUnpack decodes a bitpacked payload into dst, adding base. Width is
// hoisted out of the row loop. Aligned widths (8/16/32/64) and the common
// 10/12/24 widths take direct read paths; other widths 1-56 use a single-word
// window (one uint64 read per value); widths 57-63 fall back to bit-by-bit.
// Tail rows whose 8-byte read would overrun the payload also use the
// bit-by-bit path.
func forBitPackUnpack[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64, width int) {
	n := len(dst)
	switch width {
	case 8:
		for i := 0; i < n; i++ {
			dst[i] = T(base + int64(data[i]))
		}
	case 10:
		forBitPackUnpackWidth10(data, dst, base)
	case 12:
		forBitPackUnpackWidth12(data, dst, base)
	case 16:
		for i := 0; i < n; i++ {
			dst[i] = T(base + int64(binary.LittleEndian.Uint16(data[i*2:i*2+2])))
		}
	case 24:
		forBitPackUnpackWidth24(data, dst, base)
	case 32:
		for i := 0; i < n; i++ {
			dst[i] = T(base + int64(binary.LittleEndian.Uint32(data[i*4:i*4+4])))
		}
	case 64:
		for i := 0; i < n; i++ {
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
				dst[i] = T(base + int64(forBitPackGetNaive(data, i, width)))
			}
		} else {
			for i := 0; i < n; i++ {
				dst[i] = T(base + int64(forBitPackGetNaive(data, i, width)))
			}
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

func forBitPackUnpackWidth24[T ~int16 | ~int32 | ~int64](data []byte, dst []T, base int64) {
	for i := 0; i < len(dst); i++ {
		in := i * 3
		value := uint64(data[in]) |
			uint64(data[in+1])<<8 |
			uint64(data[in+2])<<16
		dst[i] = T(base + int64(value))
	}
}
