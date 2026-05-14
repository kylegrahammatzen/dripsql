// Plain encode/decode of fixed-width slices uses unsafe.Slice to memcpy the
// underlying bytes directly. The on-disk segment format is little-endian, and
// every callsite in this package uses binary.LittleEndian (no BigEndian or
// runtime.GOARCH branches anywhere in the codebase), so the host byte order is
// expected to match the format. The fast paths below assume a little-endian
// host; running on a big-endian build would require swapping bytes per element.
package codec

import (
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Plain struct{}

func (Plain) Encoding() types.Encoding { return types.EncodingFlat }

func (Plain) Encode(v types.Vec) (Page, error) {
	prepared, ok := (Plain{}).Prepare(v)
	if !ok {
		if v.Encoding != types.EncodingFlat {
			return Page{}, fmt.Errorf("plain codec requires flat vector, got %s", v.Encoding)
		}
		return Page{}, fmt.Errorf("plain codec unsupported kind %s", v.Kind)
	}
	return prepared.EncodeInto(nil)
}

func (Plain) Prepare(v types.Vec) (PreparedEncoding, bool) {
	if v.Encoding != types.EncodingFlat {
		return nil, false
	}
	valuesSize, ok := plainValuesSize(v)
	if !ok {
		return nil, false
	}
	return preparedPlain{vec: v, size: validityBytes(v.Valid) + valuesSize}, true
}

type preparedPlain struct {
	vec  types.Vec
	size int
}

func (p preparedPlain) Encoding() types.Encoding { return types.EncodingFlat }

func (p preparedPlain) Size() int { return p.size }

func (p preparedPlain) EncodeInto(scratch []byte) (Page, error) {
	payload := preparedPayload(scratch, p.size)
	pos := writeValidity(payload, p.vec.Valid)
	v := p.vec
	switch v.Kind {
	case types.VecBool:
		pos += encodeFixedSlice(payload[pos:], v.BoolBits[:types.ValidityWords(v.Len)])
	case types.VecInt16:
		pos += encodeFixedSlice(payload[pos:], v.I16[:v.Len])
	case types.VecInt32, types.VecDate:
		pos += encodeFixedSlice(payload[pos:], v.I32[:v.Len])
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		pos += encodeFixedSlice(payload[pos:], v.I64[:v.Len])
	case types.VecFloat32:
		pos += encodeFixedSlice(payload[pos:], v.F32[:v.Len])
	case types.VecFloat64:
		pos += encodeFixedSlice(payload[pos:], v.F64[:v.Len])
	case types.VecUUID:
		pos += encodeFixedSlice(payload[pos:], v.UUID[:v.Len])
	case types.VecEnum32:
		pos += encodeFixedSlice(payload[pos:], v.U32[:v.Len])
	case types.VecText, types.VecBytes, types.VecJSON:
		pos += encodeFixedSlice(payload[pos:], v.Var.Offsets[:v.Len+1])
		copy(payload[pos:], v.Var.Data)
	default:
		return Page{}, fmt.Errorf("plain codec unsupported kind %s", v.Kind)
	}
	return Page{Kind: v.Kind, Encoding: types.EncodingFlat, Rows: v.Len, NullCount: types.NullCount(v.Valid, v.Len), Payload: payload}, nil
}

func (Plain) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (Plain{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

// DecodeSelected bulk-decodes fixed-width pages and ignores sel because the per-row IterSet path benchmarked 36-56x slower than DecodeInto at every density we measured; var-bytes kinds keep the compact path since only copying selected bytes is a real memory win.
func (Plain) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	if page.Encoding != types.EncodingFlat {
		return types.Vec{}, fmt.Errorf("plain codec cannot decode selected %s", page.Encoding)
	}
	if sel.Rows != page.Rows {
		return types.Vec{}, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, page.Rows)
	}
	if !page.Kind.IsVarBytes() {
		return (Plain{}).Decode(page)
	}
	valid, pos, err := readValidity(page.Payload, page.Rows, page.NullCount)
	if err != nil {
		return types.Vec{}, err
	}
	varBytes, err := plainDecodeSelectedVarBytes(page.Payload[pos:], page.Rows, valid, sel)
	if err != nil {
		return types.Vec{}, err
	}
	return types.Vec{Kind: page.Kind, Encoding: types.EncodingFlat, Len: page.Rows, Valid: valid, Var: varBytes}, nil
}

func (Plain) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("plain decode destination is nil")
	}
	if page.Encoding != types.EncodingFlat {
		return fmt.Errorf("plain codec cannot decode %s", page.Encoding)
	}
	valid, pos, err := readValidityInto(page.Payload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	resetVecForDecode(dst, page.Kind, false)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	dst.Valid = valid
	switch page.Kind {
	case types.VecBool:
		words := types.ValidityWords(page.Rows)
		if len(page.Payload)-pos < words*8 {
			return fmt.Errorf("plain bool payload truncated")
		}
		dst.BoolBits = resizeSlice(dst.BoolBits, words)
		decodeFixedSlice(dst.BoolBits, page.Payload[pos:])
	case types.VecInt16:
		if len(page.Payload)-pos < page.Rows*2 {
			return fmt.Errorf("plain int16 payload truncated")
		}
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		decodeFixedSlice(dst.I16, page.Payload[pos:])
	case types.VecInt32, types.VecDate:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain int32 payload truncated")
		}
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		decodeFixedSlice(dst.I32, page.Payload[pos:])
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		if len(page.Payload)-pos < page.Rows*8 {
			return fmt.Errorf("plain int64 payload truncated")
		}
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		decodeFixedSlice(dst.I64, page.Payload[pos:])
	case types.VecFloat32:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain float32 payload truncated")
		}
		dst.F32 = resizeSlice(dst.F32, page.Rows)
		decodeFixedSlice(dst.F32, page.Payload[pos:])
	case types.VecFloat64:
		if len(page.Payload)-pos < page.Rows*8 {
			return fmt.Errorf("plain float64 payload truncated")
		}
		dst.F64 = resizeSlice(dst.F64, page.Rows)
		decodeFixedSlice(dst.F64, page.Payload[pos:])
	case types.VecUUID:
		if len(page.Payload)-pos < page.Rows*16 {
			return fmt.Errorf("plain uuid payload truncated")
		}
		dst.UUID = resizeSlice(dst.UUID, page.Rows)
		decodeFixedSlice(dst.UUID, page.Payload[pos:])
	case types.VecEnum32:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain enum payload truncated")
		}
		dst.U32 = resizeSlice(dst.U32, page.Rows)
		decodeFixedSlice(dst.U32, page.Payload[pos:])
	case types.VecText, types.VecBytes, types.VecJSON:
		offsetBytes := (page.Rows + 1) * 4
		if len(page.Payload)-pos < offsetBytes {
			return fmt.Errorf("plain varbytes offsets truncated")
		}
		offsets := resizeSlice(dst.Var.Offsets, page.Rows+1)
		decodeFixedSlice(offsets, page.Payload[pos:])
		pos += offsetBytes
		data := resizeSlice(dst.Var.Data, len(page.Payload)-pos)
		copy(data, page.Payload[pos:])
		dst.Var = types.VarBytes{Offsets: offsets, Data: data, Prefixes: resizeSlice(dst.Var.Prefixes, page.Rows)}
		dst.Var.RebuildPrefixes()
	default:
		return fmt.Errorf("plain codec unsupported kind %s", page.Kind)
	}
	return nil
}

// encodeFixedSlice memcpys the underlying bytes of values into dst and returns
// the number of bytes written. Caller must ensure dst has room for
// len(values)*sizeof(T) bytes. Little-endian host required.
func encodeFixedSlice[T any](dst []byte, values []T) int {
	if len(values) == 0 {
		return 0
	}
	n := len(values) * int(unsafe.Sizeof(values[0]))
	src := unsafe.Slice((*byte)(unsafe.Pointer(&values[0])), n)
	return copy(dst, src)
}

// decodeFixedSlice memcpys src into the underlying bytes of dst. Caller must
// ensure len(src) >= len(dst)*sizeof(T). Little-endian host required.
func decodeFixedSlice[T any](dst []T, src []byte) {
	if len(dst) == 0 {
		return
	}
	n := len(dst) * int(unsafe.Sizeof(dst[0]))
	view := unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), n)
	copy(view, src[:n])
}

func (Plain) Estimate(v types.Vec) (int, bool) {
	if v.Encoding != types.EncodingFlat {
		return 0, false
	}
	valuesSize, ok := plainValuesSize(v)
	if !ok {
		return 0, false
	}
	return validityBytes(v.Valid) + valuesSize, true
}

func plainValuesSize(v types.Vec) (int, bool) {
	if w := v.Kind.FixedWidth(); w > 0 {
		return v.Len * int(w), true
	}
	switch v.Kind {
	case types.VecBool:
		return types.ValidityWords(v.Len) * 8, true
	case types.VecText, types.VecBytes, types.VecJSON:
		return (v.Len+1)*4 + len(v.Var.Data), true
	}
	return 0, false
}

func plainDecodeSelectedVarBytes(payload []byte, rows int, valid types.Validity, sel types.SelectionMask) (types.VarBytes, error) {
	offsetBytes := (rows + 1) * 4
	if len(payload) < offsetBytes {
		return types.VarBytes{}, fmt.Errorf("plain varbytes offsets truncated")
	}
	sourceOffsets := make([]uint32, rows+1)
	for i := range sourceOffsets {
		sourceOffsets[i] = binary.LittleEndian.Uint32(payload[i*4 : i*4+4])
	}
	sourceData := payload[offsetBytes:]
	if err := validateSourceOffsets(sourceOffsets, len(sourceData)); err != nil {
		return types.VarBytes{}, err
	}
	dataBytes := 0
	sel.IterSet(func(row int) {
		if types.IsValid(valid, row) {
			dataBytes += int(sourceOffsets[row+1] - sourceOffsets[row])
		}
	})
	out := types.NewVarBytes(rows, dataBytes)
	for row := range rows {
		if !sel.IsSet(row) || !types.IsValid(valid, row) {
			out.AppendBytes(row, nil)
			continue
		}
		start := sourceOffsets[row]
		end := sourceOffsets[row+1]
		out.AppendBytes(row, sourceData[start:end])
	}
	return out, nil
}

func validateSourceOffsets(offsets []uint32, dataLen int) error {
	if len(offsets) == 0 {
		return fmt.Errorf("varbytes offsets missing")
	}
	if offsets[0] != 0 {
		return fmt.Errorf("varbytes first offset %d must be 0", offsets[0])
	}
	prev := uint32(0)
	for i := 1; i < len(offsets); i++ {
		offset := offsets[i]
		if offset < prev {
			return fmt.Errorf("varbytes offset %d is less than previous offset", i)
		}
		prev = offset
	}
	last := offsets[len(offsets)-1]
	if uint64(last) != uint64(dataLen) {
		return fmt.Errorf("varbytes last offset %d does not match data length %d", last, dataLen)
	}
	return nil
}

func validityBytes(valid types.Validity) int {
	return len(valid) * 8
}

func writeValidity(payload []byte, valid types.Validity) int {
	pos := 0
	for _, word := range valid {
		binary.LittleEndian.PutUint64(payload[pos:pos+8], word)
		pos += 8
	}
	return pos
}

func readValidity(payload []byte, rows int, nullCount int) (types.Validity, int, error) {
	return readValidityInto(payload, rows, nullCount, nil)
}

func readValidityInto(payload []byte, rows int, nullCount int, dst types.Validity) (types.Validity, int, error) {
	if nullCount == 0 {
		return nil, 0, nil
	}
	words := types.ValidityWords(rows)
	bytes := words * 8
	if len(payload) < bytes {
		return nil, 0, fmt.Errorf("validity payload truncated")
	}
	valid := resizeSlice(dst, words)
	for i := range valid {
		valid[i] = binary.LittleEndian.Uint64(payload[i*8 : i*8+8])
	}
	return valid, bytes, nil
}
