package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Constant struct{}

func (Constant) Encoding() types.Encoding { return types.EncodingConstant }

func (Constant) Encode(v types.Vec) (Page, error) {
	if v.Encoding != types.EncodingFlat {
		return Page{}, fmt.Errorf("constant codec requires flat vector, got %s", v.Encoding)
	}
	if !constantEncodable(v) {
		return Page{}, fmt.Errorf("constant codec requires an all-null or all-valid constant vector")
	}
	nullCount := types.NullCount(v.Valid, v.Len)
	if nullCount == v.Len {
		return Page{Kind: v.Kind, Encoding: types.EncodingConstant, Rows: v.Len, NullCount: nullCount}, nil
	}
	payload, err := constantPayload(v)
	if err != nil {
		return Page{}, err
	}
	return Page{Kind: v.Kind, Encoding: types.EncodingConstant, Rows: v.Len, NullCount: nullCount, Payload: payload}, nil
}

func (Constant) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (Constant{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

// DecodeEncoded materializes a Constant page in non-flat form so consumers
// (today: aggregate sinks like Sum/Min/Max over integer kinds) can read the
// repeated value once instead of expanding it across rows. Only integer-shaped
// kinds are supported because that's the only encoded surface any consumer
// reads; non-int kinds are routed through plain decode by the segment reader.
func (Constant) DecodeEncoded(page Page) (types.Vec, error) {
	if page.Encoding != types.EncodingConstant {
		return types.Vec{}, fmt.Errorf("constant codec cannot decode encoded %s", page.Encoding)
	}
	out := types.Vec{Kind: page.Kind, Encoding: types.EncodingConstant, Len: page.Rows, Encoded: &types.EncodedState{ConstantValid: true}}
	if page.NullCount == page.Rows {
		out.Encoded.ConstantValid = false
		out.Valid = make(types.Validity, types.ValidityWords(page.Rows))
		return out, nil
	}
	if page.NullCount != 0 {
		return types.Vec{}, fmt.Errorf("constant codec cannot expose partial-null page")
	}
	if err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
		return types.Vec{}, err
	}
	switch page.Kind {
	case types.VecInt16:
		out.Encoded.ConstantI64 = int64(int16(binary.LittleEndian.Uint16(page.Payload)))
	case types.VecInt32, types.VecDate:
		out.Encoded.ConstantI64 = int64(int32(binary.LittleEndian.Uint32(page.Payload)))
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		out.Encoded.ConstantI64 = int64(binary.LittleEndian.Uint64(page.Payload))
	default:
		return types.Vec{}, fmt.Errorf("constant codec encoded form unsupported for kind %s", page.Kind)
	}
	return out, nil
}

func (Constant) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	if page.Encoding != types.EncodingConstant {
		return types.Vec{}, fmt.Errorf("constant codec cannot decode selected %s", page.Encoding)
	}
	if sel.Rows != page.Rows {
		return types.Vec{}, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, page.Rows)
	}
	out := types.Vec{Kind: page.Kind, Encoding: types.EncodingFlat, Len: page.Rows}
	if page.NullCount == page.Rows {
		out.Valid = make(types.Validity, types.ValidityWords(page.Rows))
		if err := fillConstantZero(&out, page.Kind, page.Rows); err != nil {
			return types.Vec{}, err
		}
		return out, nil
	}
	switch page.Kind {
	case types.VecBool:
		if len(page.Payload) != 1 {
			return types.Vec{}, fmt.Errorf("constant bool payload length %d", len(page.Payload))
		}
		out.BoolBits = make([]uint64, types.ValidityWords(page.Rows))
		if page.Payload[0] != 0 {
			sel.IterSet(func(row int) {
				out.BoolBits[row>>6] |= uint64(1) << uint(row&63)
			})
		}
	case types.VecText, types.VecBytes, types.VecJSON:
		varBytes := types.NewVarBytes(page.Rows, len(page.Payload)*sel.PopCount())
		for row := 0; row < page.Rows; row++ {
			if sel.IsSet(row) {
				varBytes.AppendBytes(row, page.Payload)
			} else {
				varBytes.AppendBytes(row, nil)
			}
		}
		out.Var = varBytes
	default:
		if err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
			return types.Vec{}, err
		}
		switch page.Kind {
		case types.VecInt16:
			out.I16 = fillConstantSelectedSlice(page.Rows, int16(binary.LittleEndian.Uint16(page.Payload)), sel)
		case types.VecInt32, types.VecDate:
			out.I32 = fillConstantSelectedSlice(page.Rows, int32(binary.LittleEndian.Uint32(page.Payload)), sel)
		case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
			out.I64 = fillConstantSelectedSlice(page.Rows, int64(binary.LittleEndian.Uint64(page.Payload)), sel)
		case types.VecFloat32:
			out.F32 = fillConstantSelectedSlice(page.Rows, math.Float32frombits(binary.LittleEndian.Uint32(page.Payload)), sel)
		case types.VecFloat64:
			out.F64 = fillConstantSelectedSlice(page.Rows, math.Float64frombits(binary.LittleEndian.Uint64(page.Payload)), sel)
		case types.VecUUID:
			var value types.UUID16
			copy(value[:], page.Payload)
			out.UUID = fillConstantSelectedSlice(page.Rows, value, sel)
		case types.VecEnum32:
			out.U32 = fillConstantSelectedSlice(page.Rows, binary.LittleEndian.Uint32(page.Payload), sel)
		}
	}
	return out, nil
}

func (Constant) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("constant decode destination is nil")
	}
	if page.Encoding != types.EncodingConstant {
		return fmt.Errorf("constant codec cannot decode %s", page.Encoding)
	}
	resetVecForDecode(dst, page.Kind, false)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	if page.NullCount == page.Rows {
		dst.Valid = make(types.Validity, types.ValidityWords(page.Rows))
		return fillConstantZero(dst, page.Kind, page.Rows)
	}
	dst.Valid = nil
	switch page.Kind {
	case types.VecBool:
		if len(page.Payload) != 1 {
			return fmt.Errorf("constant bool payload length %d", len(page.Payload))
		}
		dst.BoolBits = resizeSlice(dst.BoolBits, types.ValidityWords(page.Rows))
		clear(dst.BoolBits)
		if page.Payload[0] != 0 {
			for row := 0; row < page.Rows; row++ {
				dst.BoolBits[row>>6] |= uint64(1) << uint(row&63)
			}
		}
		return nil
	case types.VecText, types.VecBytes, types.VecJSON:
		return fillConstantVarBytes(dst, page.Payload, page.Rows)
	}
	if err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
		return err
	}
	switch page.Kind {
	case types.VecInt16:
		value := int16(binary.LittleEndian.Uint16(page.Payload))
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		for row := range dst.I16 {
			dst.I16[row] = value
		}
	case types.VecInt32, types.VecDate:
		value := int32(binary.LittleEndian.Uint32(page.Payload))
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		for row := range dst.I32 {
			dst.I32[row] = value
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		value := int64(binary.LittleEndian.Uint64(page.Payload))
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		for row := range dst.I64 {
			dst.I64[row] = value
		}
	case types.VecFloat32:
		value := math.Float32frombits(binary.LittleEndian.Uint32(page.Payload))
		dst.F32 = resizeSlice(dst.F32, page.Rows)
		for row := range dst.F32 {
			dst.F32[row] = value
		}
	case types.VecFloat64:
		value := math.Float64frombits(binary.LittleEndian.Uint64(page.Payload))
		dst.F64 = resizeSlice(dst.F64, page.Rows)
		for row := range dst.F64 {
			dst.F64[row] = value
		}
	case types.VecUUID:
		dst.UUID = resizeSlice(dst.UUID, page.Rows)
		var value types.UUID16
		copy(value[:], page.Payload)
		for row := range dst.UUID {
			dst.UUID[row] = value
		}
	case types.VecEnum32:
		value := binary.LittleEndian.Uint32(page.Payload)
		dst.U32 = resizeSlice(dst.U32, page.Rows)
		for row := range dst.U32 {
			dst.U32[row] = value
		}
	default:
		return fmt.Errorf("constant codec unsupported kind %s", page.Kind)
	}
	return nil
}

func (Constant) Estimate(v types.Vec) (int, bool) {
	if !constantEncodable(v) {
		return 0, false
	}
	if types.NullCount(v.Valid, v.Len) == v.Len {
		return 0, true
	}
	size, ok := constantValueSize(v)
	return size, ok
}

func constantEncodable(v types.Vec) bool {
	if v.Encoding != types.EncodingFlat || v.Len == 0 {
		return false
	}
	nullCount := types.NullCount(v.Valid, v.Len)
	if nullCount == v.Len {
		return true
	}
	if nullCount != 0 {
		return false
	}
	return constantAllEqual(v)
}

func constantAllEqual(v types.Vec) bool {
	switch v.Kind {
	case types.VecBool:
		first := v.BoolBits[0]&1 != 0
		for row := 1; row < v.Len; row++ {
			if (v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0) != first {
				return false
			}
		}
		return true
	case types.VecInt16:
		for _, value := range v.I16[1:v.Len] {
			if value != v.I16[0] {
				return false
			}
		}
		return true
	case types.VecInt32, types.VecDate:
		for _, value := range v.I32[1:v.Len] {
			if value != v.I32[0] {
				return false
			}
		}
		return true
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		for _, value := range v.I64[1:v.Len] {
			if value != v.I64[0] {
				return false
			}
		}
		return true
	case types.VecFloat32:
		first := math.Float32bits(v.F32[0])
		for _, value := range v.F32[1:v.Len] {
			if math.Float32bits(value) != first {
				return false
			}
		}
		return true
	case types.VecFloat64:
		first := math.Float64bits(v.F64[0])
		for _, value := range v.F64[1:v.Len] {
			if math.Float64bits(value) != first {
				return false
			}
		}
		return true
	case types.VecUUID:
		for _, value := range v.UUID[1:v.Len] {
			if value != v.UUID[0] {
				return false
			}
		}
		return true
	case types.VecEnum32:
		for _, value := range v.U32[1:v.Len] {
			if value != v.U32[0] {
				return false
			}
		}
		return true
	case types.VecText, types.VecBytes, types.VecJSON:
		first := v.Var.Bytes(0)
		for row := 1; row < v.Len; row++ {
			if !bytes.Equal(v.Var.Bytes(row), first) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func constantValueSize(v types.Vec) (int, bool) {
	if size, ok := fixedKindBytes(v.Kind); ok {
		return size, true
	}
	switch v.Kind {
	case types.VecBool:
		return 1, true
	case types.VecText, types.VecBytes, types.VecJSON:
		return int(v.Var.Offsets[1]), true
	}
	return 0, false
}

func constantPayload(v types.Vec) ([]byte, error) {
	size, ok := constantValueSize(v)
	if !ok {
		return nil, fmt.Errorf("constant codec unsupported kind %s", v.Kind)
	}
	payload := make([]byte, size)
	switch v.Kind {
	case types.VecBool:
		if v.BoolBits[0]&1 != 0 {
			payload[0] = 1
		}
	case types.VecInt16:
		binary.LittleEndian.PutUint16(payload, uint16(v.I16[0]))
	case types.VecInt32, types.VecDate:
		binary.LittleEndian.PutUint32(payload, uint32(v.I32[0]))
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		binary.LittleEndian.PutUint64(payload, uint64(v.I64[0]))
	case types.VecFloat32:
		binary.LittleEndian.PutUint32(payload, math.Float32bits(v.F32[0]))
	case types.VecFloat64:
		binary.LittleEndian.PutUint64(payload, math.Float64bits(v.F64[0]))
	case types.VecUUID:
		copy(payload, v.UUID[0][:])
	case types.VecEnum32:
		binary.LittleEndian.PutUint32(payload, v.U32[0])
	case types.VecText, types.VecBytes, types.VecJSON:
		copy(payload, v.Var.Bytes(0))
	}
	return payload, nil
}

func fillConstantZero(dst *types.Vec, kind types.VecKind, rows int) error {
	switch kind {
	case types.VecBool:
		dst.BoolBits = resizeAndZero(dst.BoolBits, types.ValidityWords(rows))
	case types.VecInt16:
		dst.I16 = resizeAndZero(dst.I16, rows)
	case types.VecInt32, types.VecDate:
		dst.I32 = resizeAndZero(dst.I32, rows)
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		dst.I64 = resizeAndZero(dst.I64, rows)
	case types.VecFloat32:
		dst.F32 = resizeAndZero(dst.F32, rows)
	case types.VecFloat64:
		dst.F64 = resizeAndZero(dst.F64, rows)
	case types.VecUUID:
		dst.UUID = resizeAndZero(dst.UUID, rows)
	case types.VecEnum32:
		dst.U32 = resizeAndZero(dst.U32, rows)
	case types.VecText, types.VecBytes, types.VecJSON:
		dst.Var = types.NewVarBytes(rows, 0)
	default:
		return fmt.Errorf("constant codec unsupported kind %s", kind)
	}
	return nil
}

// resizeAndZero returns a slice of length n that reuses dst's backing array
// when possible, with all elements zeroed. Used by Constant.Decode* paths to
// rebuild typed slices without leaking stale data through reused capacity.
func resizeAndZero[S ~[]E, E any](dst S, n int) S {
	out := resizeSlice(dst, n)
	clear(out)
	return out
}

func fillConstantVarBytes(dst *types.Vec, value []byte, rows int) error {
	if rows < 0 || len(value) > int(^uint(0)>>1)/max(rows, 1) {
		return fmt.Errorf("constant varbytes decoded size exceeds int capacity")
	}
	varBytes := types.NewVarBytes(rows, len(value)*rows)
	for row := 0; row < rows; row++ {
		varBytes.AppendBytes(row, value)
	}
	dst.Var = varBytes
	return nil
}

// fillConstantSelectedSlice scatters value into the selected rows of a fresh
// length-rows slice, leaving unselected rows zero-valued.
func fillConstantSelectedSlice[T any](rows int, value T, sel types.SelectionMask) []T {
	out := make([]T, rows)
	sel.IterSet(func(row int) { out[row] = value })
	return out
}
