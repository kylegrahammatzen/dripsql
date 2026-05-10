package codec

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Plain struct{}

func (Plain) Encoding() types.Encoding { return types.EncodingFlat }

func (Plain) Encode(v types.Vec) (Page, error) {
	if v.Encoding != types.EncodingFlat {
		return Page{}, fmt.Errorf("plain codec requires flat vector, got %s", v.Encoding)
	}
	payload := make([]byte, validityBytes(v.Valid)+plainValuesSize(v))
	pos := writeValidity(payload, v.Valid)
	switch v.Kind {
	case types.VecBool:
		for i := 0; i < types.ValidityWords(v.Len); i++ {
			binary.LittleEndian.PutUint64(payload[pos:pos+8], v.BoolBits[i])
			pos += 8
		}
	case types.VecInt16:
		for _, value := range v.I16[:v.Len] {
			binary.LittleEndian.PutUint16(payload[pos:pos+2], uint16(value))
			pos += 2
		}
	case types.VecInt32, types.VecDate:
		for _, value := range v.I32[:v.Len] {
			binary.LittleEndian.PutUint32(payload[pos:pos+4], uint32(value))
			pos += 4
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		for _, value := range v.I64[:v.Len] {
			binary.LittleEndian.PutUint64(payload[pos:pos+8], uint64(value))
			pos += 8
		}
	case types.VecFloat32:
		for _, value := range v.F32[:v.Len] {
			binary.LittleEndian.PutUint32(payload[pos:pos+4], math.Float32bits(value))
			pos += 4
		}
	case types.VecFloat64:
		for _, value := range v.F64[:v.Len] {
			binary.LittleEndian.PutUint64(payload[pos:pos+8], math.Float64bits(value))
			pos += 8
		}
	case types.VecUUID:
		for _, value := range v.UUID[:v.Len] {
			copy(payload[pos:pos+16], value[:])
			pos += 16
		}
	case types.VecEnum32:
		for _, value := range v.U32[:v.Len] {
			binary.LittleEndian.PutUint32(payload[pos:pos+4], value)
			pos += 4
		}
	case types.VecText, types.VecBytes, types.VecJSON:
		for _, offset := range v.Var.Offsets[:v.Len+1] {
			binary.LittleEndian.PutUint32(payload[pos:pos+4], offset)
			pos += 4
		}
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

func (Plain) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	if page.Encoding != types.EncodingFlat {
		return types.Vec{}, fmt.Errorf("plain codec cannot decode selected %s", page.Encoding)
	}
	if sel.Rows != page.Rows {
		return types.Vec{}, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, page.Rows)
	}
	valid, pos, err := readValidity(page.Payload, page.Rows, page.NullCount)
	if err != nil {
		return types.Vec{}, err
	}
	out := types.Vec{Kind: page.Kind, Encoding: types.EncodingFlat, Len: page.Rows, Valid: valid}
	switch page.Kind {
	case types.VecBool:
		words := types.ValidityWords(page.Rows)
		if len(page.Payload)-pos < words*8 {
			return types.Vec{}, fmt.Errorf("plain bool payload truncated")
		}
		out.BoolBits = make([]uint64, words)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) && page.Payload[pos+(row>>6)*8+(row&63)/8]&(byte(1)<<uint(row&7)) != 0 {
				out.BoolBits[row>>6] |= uint64(1) << uint(row&63)
			}
		})
	case types.VecInt16:
		if len(page.Payload)-pos < page.Rows*2 {
			return types.Vec{}, fmt.Errorf("plain int16 payload truncated")
		}
		out.I16 = make([]int16, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*2
				out.I16[row] = int16(binary.LittleEndian.Uint16(page.Payload[off : off+2]))
			}
		})
	case types.VecInt32, types.VecDate:
		if len(page.Payload)-pos < page.Rows*4 {
			return types.Vec{}, fmt.Errorf("plain int32 payload truncated")
		}
		out.I32 = make([]int32, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*4
				out.I32[row] = int32(binary.LittleEndian.Uint32(page.Payload[off : off+4]))
			}
		})
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		if len(page.Payload)-pos < page.Rows*8 {
			return types.Vec{}, fmt.Errorf("plain int64 payload truncated")
		}
		out.I64 = make([]int64, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*8
				out.I64[row] = int64(binary.LittleEndian.Uint64(page.Payload[off : off+8]))
			}
		})
	case types.VecFloat32:
		if len(page.Payload)-pos < page.Rows*4 {
			return types.Vec{}, fmt.Errorf("plain float32 payload truncated")
		}
		out.F32 = make([]float32, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*4
				out.F32[row] = math.Float32frombits(binary.LittleEndian.Uint32(page.Payload[off : off+4]))
			}
		})
	case types.VecFloat64:
		if len(page.Payload)-pos < page.Rows*8 {
			return types.Vec{}, fmt.Errorf("plain float64 payload truncated")
		}
		out.F64 = make([]float64, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*8
				out.F64[row] = math.Float64frombits(binary.LittleEndian.Uint64(page.Payload[off : off+8]))
			}
		})
	case types.VecUUID:
		if len(page.Payload)-pos < page.Rows*16 {
			return types.Vec{}, fmt.Errorf("plain uuid payload truncated")
		}
		out.UUID = make([]types.UUID16, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*16
				copy(out.UUID[row][:], page.Payload[off:off+16])
			}
		})
	case types.VecEnum32:
		if len(page.Payload)-pos < page.Rows*4 {
			return types.Vec{}, fmt.Errorf("plain enum payload truncated")
		}
		out.U32 = make([]uint32, page.Rows)
		sel.IterSet(func(row int) {
			if types.IsValid(valid, row) {
				off := pos + row*4
				out.U32[row] = binary.LittleEndian.Uint32(page.Payload[off : off+4])
			}
		})
	case types.VecText, types.VecBytes, types.VecJSON:
		varBytes, err := plainDecodeSelectedVarBytes(page.Payload[pos:], page.Rows, valid, sel)
		if err != nil {
			return types.Vec{}, err
		}
		out.Var = varBytes
	default:
		return types.Vec{}, fmt.Errorf("plain codec unsupported kind %s", page.Kind)
	}
	return out, nil
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
	preparePlainDecodeVec(dst, page.Kind)
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
		for i := range dst.BoolBits {
			dst.BoolBits[i] = binary.LittleEndian.Uint64(page.Payload[pos : pos+8])
			pos += 8
		}
	case types.VecInt16:
		if len(page.Payload)-pos < page.Rows*2 {
			return fmt.Errorf("plain int16 payload truncated")
		}
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		for i := range dst.I16 {
			dst.I16[i] = int16(binary.LittleEndian.Uint16(page.Payload[pos : pos+2]))
			pos += 2
		}
	case types.VecInt32, types.VecDate:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain int32 payload truncated")
		}
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		for i := range dst.I32 {
			dst.I32[i] = int32(binary.LittleEndian.Uint32(page.Payload[pos : pos+4]))
			pos += 4
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		if len(page.Payload)-pos < page.Rows*8 {
			return fmt.Errorf("plain int64 payload truncated")
		}
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		for i := range dst.I64 {
			dst.I64[i] = int64(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
			pos += 8
		}
	case types.VecFloat32:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain float32 payload truncated")
		}
		dst.F32 = resizeSlice(dst.F32, page.Rows)
		for i := range dst.F32 {
			dst.F32[i] = math.Float32frombits(binary.LittleEndian.Uint32(page.Payload[pos : pos+4]))
			pos += 4
		}
	case types.VecFloat64:
		if len(page.Payload)-pos < page.Rows*8 {
			return fmt.Errorf("plain float64 payload truncated")
		}
		dst.F64 = resizeSlice(dst.F64, page.Rows)
		for i := range dst.F64 {
			dst.F64[i] = math.Float64frombits(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
			pos += 8
		}
	case types.VecUUID:
		if len(page.Payload)-pos < page.Rows*16 {
			return fmt.Errorf("plain uuid payload truncated")
		}
		dst.UUID = resizeSlice(dst.UUID, page.Rows)
		for i := range dst.UUID {
			copy(dst.UUID[i][:], page.Payload[pos:pos+16])
			pos += 16
		}
	case types.VecEnum32:
		if len(page.Payload)-pos < page.Rows*4 {
			return fmt.Errorf("plain enum payload truncated")
		}
		dst.U32 = resizeSlice(dst.U32, page.Rows)
		for i := range dst.U32 {
			dst.U32[i] = binary.LittleEndian.Uint32(page.Payload[pos : pos+4])
			pos += 4
		}
	case types.VecText, types.VecBytes, types.VecJSON:
		offsetBytes := (page.Rows + 1) * 4
		if len(page.Payload)-pos < offsetBytes {
			return fmt.Errorf("plain varbytes offsets truncated")
		}
		offsets := resizeSlice(dst.Var.Offsets, page.Rows+1)
		for i := range offsets {
			offsets[i] = binary.LittleEndian.Uint32(page.Payload[pos : pos+4])
			pos += 4
		}
		data := resizeSlice(dst.Var.Data, len(page.Payload)-pos)
		copy(data, page.Payload[pos:])
		dst.Var = types.VarBytes{Offsets: offsets, Data: data}
	default:
		return fmt.Errorf("plain codec unsupported kind %s", page.Kind)
	}
	return nil
}

func (Plain) Estimate(v types.Vec) (int, bool) {
	if v.Encoding != types.EncodingFlat {
		return 0, false
	}
	return validityBytes(v.Valid) + plainValuesSize(v), true
}

func plainValuesSize(v types.Vec) int {
	if size, ok := fixedKindBytes(v.Kind); ok {
		return v.Len * size
	}
	switch v.Kind {
	case types.VecBool:
		return types.ValidityWords(v.Len) * 8
	case types.VecText, types.VecBytes, types.VecJSON:
		return (v.Len+1)*4 + len(v.Var.Data)
	}
	return 0
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
	for row := 0; row < rows; row++ {
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

func preparePlainDecodeVec(v *types.Vec, kind types.VecKind) {
	clearVecCodecFields(v, kind, false)
}

func prepareDictionaryDecodeVec(v *types.Vec) {
	clearVecCodecFields(v, 0, true)
}

func resizeSlice[S ~[]E, E any](dst S, n int) S {
	if cap(dst) < n {
		return make(S, n)
	}
	return dst[:n]
}
