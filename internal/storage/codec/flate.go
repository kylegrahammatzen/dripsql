package codec

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	flateHeaderBytes       = 4
	flateMinSavingsPercent = 5
)

type Flate struct{}

func (Flate) Encoding() types.Encoding { return types.EncodingFlate }

func (Flate) Encode(v types.Vec) (Page, error) {
	prepared, ok := (Flate{}).Prepare(v)
	if !ok {
		if v.Encoding != types.EncodingFlat {
			return Page{}, fmt.Errorf("flate codec requires flat vector, got %s", v.Encoding)
		}
		return Page{}, fmt.Errorf("flate codec requires compressible varbytes vector")
	}
	return prepared.EncodeInto(nil)
}

func (Flate) Prepare(v types.Vec) (PreparedEncoding, bool) {
	if v.Encoding != types.EncodingFlat || !flateVarBytesKind(v.Kind) {
		return nil, false
	}
	plain, ok := (Plain{}).Prepare(v)
	if !ok {
		return nil, false
	}
	plainPage, err := plain.EncodeInto(nil)
	if err != nil {
		return nil, false
	}
	payload, ok := flatePayload(plainPage.Payload)
	if !ok {
		return nil, false
	}
	return preparedFlate{kind: v.Kind, rows: v.Len, nullCount: types.NullCount(v.Valid, v.Len), payload: payload}, true
}

type preparedFlate struct {
	kind      types.VecKind
	rows      int
	nullCount int
	payload   []byte
}

func (p preparedFlate) Encoding() types.Encoding { return types.EncodingFlate }

func (p preparedFlate) Size() int { return len(p.payload) }

func (p preparedFlate) EncodeInto(_ []byte) (Page, error) {
	return Page{Kind: p.kind, Encoding: types.EncodingFlate, Rows: p.rows, NullCount: p.nullCount, Payload: p.payload}, nil
}

func (Flate) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (Flate{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (Flate) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("flate decode destination is nil")
	}
	plainPayload, err := inflatePayload(page)
	if err != nil {
		return err
	}
	return decodeFlateVarBytesInto(page, plainPayload, dst)
}

func (Flate) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	plainPayload, err := inflatePayload(page)
	if err != nil {
		return types.Vec{}, err
	}
	plainPage := page
	plainPage.Encoding = types.EncodingFlat
	plainPage.Payload = plainPayload
	return (Plain{}).DecodeSelected(plainPage, sel)
}

func (Flate) Estimate(v types.Vec) (int, bool) {
	prepared, ok := (Flate{}).Prepare(v)
	if !ok {
		return 0, false
	}
	return prepared.Size(), true
}

func flatePayload(plain []byte) ([]byte, bool) {
	if uint64(len(plain)) > uint64(^uint32(0)) {
		return nil, false
	}
	var compressed bytes.Buffer
	zw, err := flate.NewWriter(&compressed, flate.BestSpeed)
	if err != nil {
		return nil, false
	}
	if _, err := zw.Write(plain); err != nil {
		_ = zw.Close()
		return nil, false
	}
	if err := zw.Close(); err != nil {
		return nil, false
	}
	payloadSize := flateHeaderBytes + compressed.Len()
	if payloadSize*100 >= len(plain)*(100-flateMinSavingsPercent) {
		return nil, false
	}
	payload := make([]byte, payloadSize)
	binary.LittleEndian.PutUint32(payload[:flateHeaderBytes], uint32(len(plain)))
	copy(payload[flateHeaderBytes:], compressed.Bytes())
	return payload, true
}

func inflatePayload(page Page) ([]byte, error) {
	if page.Encoding != types.EncodingFlate {
		return nil, fmt.Errorf("flate codec cannot decode %s", page.Encoding)
	}
	if !flateVarBytesKind(page.Kind) {
		return nil, fmt.Errorf("flate codec unsupported kind %s", page.Kind)
	}
	if len(page.Payload) < flateHeaderBytes {
		return nil, fmt.Errorf("flate payload missing uncompressed length")
	}
	want := int(binary.LittleEndian.Uint32(page.Payload[:flateHeaderBytes]))
	zr := flate.NewReader(bytes.NewReader(page.Payload[flateHeaderBytes:]))
	plain, err := io.ReadAll(zr)
	closeErr := zr.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(plain) != want {
		return nil, fmt.Errorf("flate payload expanded to %d bytes, want %d", len(plain), want)
	}
	return plain, nil
}

func decodeFlateVarBytesInto(page Page, plainPayload []byte, dst *types.Vec) error {
	valid, pos, err := readValidityInto(plainPayload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	offsetBytes := (page.Rows + 1) * 4
	if len(plainPayload)-pos < offsetBytes {
		return fmt.Errorf("flate varbytes offsets truncated")
	}
	preparePlainDecodeVec(dst, page.Kind)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	dst.Valid = valid
	offsets := resizeSlice(dst.Var.Offsets, page.Rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(plainPayload[pos : pos+4])
		pos += 4
	}
	data := plainPayload[pos:]
	if err := validateSourceOffsets(offsets, len(data)); err != nil {
		return err
	}
	dst.Var = types.VarBytes{Offsets: offsets, Data: data}
	return nil
}

func flateVarBytesKind(kind types.VecKind) bool {
	switch kind {
	case types.VecText, types.VecBytes, types.VecJSON:
		return true
	default:
		return false
	}
}
