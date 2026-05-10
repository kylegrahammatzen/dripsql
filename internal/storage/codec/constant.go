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

func (Constant) DecodeEncoded(page Page) (types.Vec, error) {
	if page.Encoding != types.EncodingConstant {
		return types.Vec{}, fmt.Errorf("constant codec cannot decode encoded %s", page.Encoding)
	}
	out := types.Vec{Kind: page.Kind, Encoding: types.EncodingConstant, Len: page.Rows, ConstantValid: true}
	if page.NullCount == page.Rows {
		out.ConstantValid = false
		out.Valid = types.NewValidity(page.Rows)
		for row := 0; row < page.Rows; row++ {
			types.SetInvalid(out.Valid, row)
		}
		return out, nil
	}
	if page.NullCount != 0 {
		return types.Vec{}, fmt.Errorf("constant codec cannot expose partial-null page")
	}
	switch page.Kind {
	case types.VecBool:
		if len(page.Payload) != 1 {
			return types.Vec{}, fmt.Errorf("constant bool payload length %d", len(page.Payload))
		}
		out.ConstantBool = page.Payload[0] != 0
	case types.VecText, types.VecBytes, types.VecJSON:
		out.ConstantBytes = append([]byte(nil), page.Payload...)
	default:
		if _, err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
			return types.Vec{}, err
		}
		switch page.Kind {
		case types.VecInt16:
			out.ConstantI64 = int64(int16(binary.LittleEndian.Uint16(page.Payload)))
		case types.VecInt32, types.VecDate:
			out.ConstantI64 = int64(int32(binary.LittleEndian.Uint32(page.Payload)))
		case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
			out.ConstantI64 = int64(binary.LittleEndian.Uint64(page.Payload))
		case types.VecFloat32:
			out.ConstantF64 = float64(math.Float32frombits(binary.LittleEndian.Uint32(page.Payload)))
		case types.VecFloat64:
			out.ConstantF64 = math.Float64frombits(binary.LittleEndian.Uint64(page.Payload))
		case types.VecUUID:
			copy(out.ConstantUUID[:], page.Payload)
		case types.VecEnum32:
			out.ConstantU32 = binary.LittleEndian.Uint32(page.Payload)
		}
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
		out.Valid = types.NewValidity(page.Rows)
		for row := 0; row < page.Rows; row++ {
			types.SetInvalid(out.Valid, row)
		}
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
		if _, err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
			return types.Vec{}, err
		}
		switch page.Kind {
		case types.VecInt16:
			value := int16(binary.LittleEndian.Uint16(page.Payload))
			out.I16 = make([]int16, page.Rows)
			sel.IterSet(func(row int) { out.I16[row] = value })
		case types.VecInt32, types.VecDate:
			value := int32(binary.LittleEndian.Uint32(page.Payload))
			out.I32 = make([]int32, page.Rows)
			sel.IterSet(func(row int) { out.I32[row] = value })
		case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
			value := int64(binary.LittleEndian.Uint64(page.Payload))
			out.I64 = make([]int64, page.Rows)
			sel.IterSet(func(row int) { out.I64[row] = value })
		case types.VecFloat32:
			value := math.Float32frombits(binary.LittleEndian.Uint32(page.Payload))
			out.F32 = make([]float32, page.Rows)
			sel.IterSet(func(row int) { out.F32[row] = value })
		case types.VecFloat64:
			value := math.Float64frombits(binary.LittleEndian.Uint64(page.Payload))
			out.F64 = make([]float64, page.Rows)
			sel.IterSet(func(row int) { out.F64[row] = value })
		case types.VecUUID:
			var value types.UUID16
			copy(value[:], page.Payload)
			out.UUID = make([]types.UUID16, page.Rows)
			sel.IterSet(func(row int) { out.UUID[row] = value })
		case types.VecEnum32:
			value := binary.LittleEndian.Uint32(page.Payload)
			out.U32 = make([]uint32, page.Rows)
			sel.IterSet(func(row int) { out.U32[row] = value })
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
	preparePlainDecodeVec(dst, page.Kind)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	if page.NullCount == page.Rows {
		dst.Valid = types.NewValidity(page.Rows)
		for row := 0; row < page.Rows; row++ {
			types.SetInvalid(dst.Valid, row)
		}
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
	if _, err := expectFixedPayload(page.Payload, page.Kind, "constant"); err != nil {
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
		dst.BoolBits = resizeSlice(dst.BoolBits, types.ValidityWords(rows))
		clear(dst.BoolBits)
	case types.VecInt16:
		dst.I16 = resizeSlice(dst.I16, rows)
		clear(dst.I16)
	case types.VecInt32, types.VecDate:
		dst.I32 = resizeSlice(dst.I32, rows)
		clear(dst.I32)
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		dst.I64 = resizeSlice(dst.I64, rows)
		clear(dst.I64)
	case types.VecFloat32:
		dst.F32 = resizeSlice(dst.F32, rows)
		clear(dst.F32)
	case types.VecFloat64:
		dst.F64 = resizeSlice(dst.F64, rows)
		clear(dst.F64)
	case types.VecUUID:
		dst.UUID = resizeSlice(dst.UUID, rows)
		clear(dst.UUID)
	case types.VecEnum32:
		dst.U32 = resizeSlice(dst.U32, rows)
		clear(dst.U32)
	case types.VecText, types.VecBytes, types.VecJSON:
		dst.Var = types.NewVarBytes(rows, 0)
	default:
		return fmt.Errorf("constant codec unsupported kind %s", kind)
	}
	return nil
}

func fillConstantVarBytes(dst *types.Vec, value []byte, rows int) error {
	if rows < 0 || len(value) > codecMaxInt()/max(rows, 1) {
		return fmt.Errorf("constant varbytes decoded size exceeds int capacity")
	}
	varBytes := types.NewVarBytes(rows, len(value)*rows)
	for row := 0; row < rows; row++ {
		varBytes.AppendBytes(row, value)
	}
	dst.Var = varBytes
	return nil
}

func codecMaxInt() int {
	return int(^uint(0) >> 1)
}
