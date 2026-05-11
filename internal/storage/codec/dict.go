package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const DictMaxValues = 256

type Dictionary struct{}

func (Dictionary) Encoding() types.Encoding { return types.EncodingDictionary }

func (Dictionary) Encode(v types.Vec) (Page, error) {
	prepared, ok := (Dictionary{}).Prepare(v)
	if !ok {
		return Page{}, fmt.Errorf("dictionary codec requires flat text vector with <= %d distinct values", DictMaxValues)
	}
	return prepared.EncodeInto(nil)
}

func (Dictionary) Prepare(v types.Vec) (PreparedEncoding, bool) {
	if v.Encoding != types.EncodingFlat || v.Kind != types.VecText {
		return nil, false
	}
	values, ids, err := buildDictionary(v)
	if err != nil {
		return nil, false
	}
	size := validityBytes(v.Valid) + 2 + len(values.Offsets)*4 + len(values.Data) + v.Len
	return preparedDictionary{vec: v, values: values, ids: ids, size: size}, true
}

type preparedDictionary struct {
	vec    types.Vec
	values types.VarBytes
	ids    []uint8
	size   int
}

func (p preparedDictionary) Encoding() types.Encoding { return types.EncodingDictionary }

func (p preparedDictionary) Size() int { return p.size }

func (p preparedDictionary) EncodeInto(scratch []byte) (Page, error) {
	dataLen := len(p.values.Data)
	payload := preparedPayload(scratch, p.size)
	pos := writeValidity(payload, p.vec.Valid)
	binary.LittleEndian.PutUint16(payload[pos:pos+2], uint16(len(p.values.Offsets)-1))
	pos += 2
	for _, offset := range p.values.Offsets {
		binary.LittleEndian.PutUint32(payload[pos:pos+4], offset)
		pos += 4
	}
	copy(payload[pos:], p.values.Data)
	pos += dataLen
	copy(payload[pos:], p.ids)
	return Page{Kind: types.VecText, Encoding: types.EncodingDictionary, Rows: p.vec.Len, NullCount: types.NullCount(p.vec.Valid, p.vec.Len), Payload: payload}, nil
}

func (Dictionary) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (Dictionary{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (Dictionary) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	if page.Encoding != types.EncodingDictionary || page.Kind != types.VecText {
		return types.Vec{}, fmt.Errorf("dictionary codec cannot decode selected %s/%s", page.Encoding, page.Kind)
	}
	if sel.Rows != page.Rows {
		return types.Vec{}, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, page.Rows)
	}
	valid, pos, err := readValidity(page.Payload, page.Rows, page.NullCount)
	if err != nil {
		return types.Vec{}, err
	}
	if len(page.Payload)-pos < 2 {
		return types.Vec{}, fmt.Errorf("dictionary payload missing count")
	}
	count := int(binary.LittleEndian.Uint16(page.Payload[pos : pos+2]))
	pos += 2
	if count > DictMaxValues {
		return types.Vec{}, fmt.Errorf("dictionary count %d exceeds %d", count, DictMaxValues)
	}
	offsetBytes := (count + 1) * 4
	if len(page.Payload)-pos < offsetBytes {
		return types.Vec{}, fmt.Errorf("dictionary offsets truncated")
	}
	offsets := make([]uint32, count+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(page.Payload[pos : pos+4])
		pos += 4
	}
	dataLen := int(offsets[count])
	if err := validateSourceOffsets(offsets, dataLen); err != nil {
		return types.Vec{}, err
	}
	if len(page.Payload)-pos < dataLen+page.Rows {
		return types.Vec{}, fmt.Errorf("dictionary payload truncated")
	}
	data := make([]byte, dataLen)
	copy(data, page.Payload[pos:pos+dataLen])
	pos += dataLen
	encodedIDs := page.Payload[pos : pos+page.Rows]
	ids := make([]uint8, page.Rows)
	sel.IterSet(func(row int) {
		if types.IsValid(valid, row) {
			ids[row] = encodedIDs[row]
		}
	})
	return types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: page.Rows, Valid: valid, DictIDs: ids, DictValues: types.VarBytes{Offsets: offsets, Data: data}}, nil
}

func (Dictionary) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("dictionary decode destination is nil")
	}
	if page.Encoding != types.EncodingDictionary || page.Kind != types.VecText {
		return fmt.Errorf("dictionary codec cannot decode %s/%s", page.Encoding, page.Kind)
	}
	valid, pos, err := readValidityInto(page.Payload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	prepareDictionaryDecodeVec(dst)
	dst.Kind = types.VecText
	dst.Encoding = types.EncodingDictionary
	dst.Len = page.Rows
	dst.Valid = valid
	if len(page.Payload)-pos < 2 {
		return fmt.Errorf("dictionary payload missing count")
	}
	count := int(binary.LittleEndian.Uint16(page.Payload[pos : pos+2]))
	pos += 2
	if count > DictMaxValues {
		return fmt.Errorf("dictionary count %d exceeds %d", count, DictMaxValues)
	}
	offsetBytes := (count + 1) * 4
	if len(page.Payload)-pos < offsetBytes {
		return fmt.Errorf("dictionary offsets truncated")
	}
	offsets := resizeSlice(dst.DictValues.Offsets, count+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(page.Payload[pos : pos+4])
		pos += 4
	}
	dataLen := int(offsets[count])
	if len(page.Payload)-pos < dataLen+page.Rows {
		return fmt.Errorf("dictionary payload truncated")
	}
	data := resizeSlice(dst.DictValues.Data, dataLen)
	copy(data, page.Payload[pos:pos+dataLen])
	pos += dataLen
	ids := resizeSlice(dst.DictIDs, page.Rows)
	copy(ids, page.Payload[pos:pos+page.Rows])
	dst.DictIDs = ids
	dst.DictValues = types.VarBytes{Offsets: offsets, Data: data}
	return nil
}

func (Dictionary) Estimate(v types.Vec) (int, bool) {
	if v.Encoding != types.EncodingFlat || v.Kind != types.VecText {
		return 0, false
	}
	values, _, err := buildDictionary(v)
	if err != nil {
		return 0, false
	}
	return validityBytes(v.Valid) + 2 + len(values.Offsets)*4 + len(values.Data) + v.Len, true
}

func buildDictionary(v types.Vec) (types.VarBytes, []uint8, error) {
	ids := make([]uint8, v.Len)
	index := make(map[string]uint8)
	values := types.NewVarBytes(DictMaxValues, 0)
	rows := 0
	for row := 0; row < v.Len; row++ {
		if !types.IsValid(v.Valid, row) {
			continue
		}
		value := v.Var.Bytes(row)
		key := v.Var.String(row)
		id, ok := index[key]
		if !ok {
			if len(index) >= DictMaxValues {
				return types.VarBytes{}, nil, fmt.Errorf("dictionary exceeds %d distinct values", DictMaxValues)
			}
			id = uint8(len(index))
			index[key] = id
			values.AppendBytes(rows, value)
			rows++
		}
		ids[row] = id
	}
	if len(index) == 0 {
		return types.VarBytes{}, nil, fmt.Errorf("dictionary requires at least one non-null value")
	}
	values.Offsets = values.Offsets[:rows+1]
	return values, ids, nil
}
