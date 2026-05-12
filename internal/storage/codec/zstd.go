package codec

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	zstdHeaderBytes       = 4
	zstdMinSavingsPercent = 5
)

type Zstd struct{}

func (Zstd) Encoding() types.Encoding { return types.EncodingZstd }

func (Zstd) Encode(v types.Vec) (Page, error) {
	prepared, ok := (Zstd{}).Prepare(v)
	if !ok {
		if v.Encoding != types.EncodingFlat {
			return Page{}, fmt.Errorf("zstd codec requires flat vector, got %s", v.Encoding)
		}
		return Page{}, fmt.Errorf("zstd codec requires compressible varbytes vector")
	}
	return prepared.EncodeInto(nil)
}

func (Zstd) Prepare(v types.Vec) (PreparedEncoding, bool) {
	if v.Encoding != types.EncodingFlat || !v.Kind.IsVarBytes() {
		return nil, false
	}
	plainSize := validityBytes(v.Valid) + (v.Len+1)*4 + len(v.Var.Data)
	plainPayload := make([]byte, plainSize)
	pos := writeValidity(plainPayload, v.Valid)
	pos += encodeFixedSlice(plainPayload[pos:], v.Var.Offsets[:v.Len+1])
	copy(plainPayload[pos:], v.Var.Data)
	payload, ok := zstdPayload(plainPayload)
	if !ok {
		return nil, false
	}
	return preparedZstd{kind: v.Kind, rows: v.Len, nullCount: types.NullCount(v.Valid, v.Len), payload: payload}, true
}

type preparedZstd struct {
	kind      types.VecKind
	rows      int
	nullCount int
	payload   []byte
}

func (p preparedZstd) Encoding() types.Encoding { return types.EncodingZstd }

func (p preparedZstd) Size() int { return len(p.payload) }

func (p preparedZstd) EncodeInto(_ []byte) (Page, error) {
	return Page{Kind: p.kind, Encoding: types.EncodingZstd, Rows: p.rows, NullCount: p.nullCount, Payload: p.payload}, nil
}

func (Zstd) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (Zstd{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (Zstd) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("zstd decode destination is nil")
	}
	plainPayload, err := decompressZstd(page)
	if err != nil {
		return err
	}
	return decodeCompressedVarBytesInto(page, plainPayload, dst)
}

func (Zstd) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	plainPayload, err := decompressZstd(page)
	if err != nil {
		return types.Vec{}, err
	}
	plainPage := page
	plainPage.Encoding = types.EncodingFlat
	plainPage.Payload = plainPayload
	return (Plain{}).DecodeSelected(plainPage, sel)
}

func (Zstd) Estimate(v types.Vec) (int, bool) {
	prepared, ok := (Zstd{}).Prepare(v)
	if !ok {
		return 0, false
	}
	return prepared.Size(), true
}

// Encoder/decoder allocation is expensive enough to dwarf per-page work, so
// pool one each for the lifetime of the process.
var (
	zstdEncoder     *zstd.Encoder
	zstdEncoderOnce sync.Once
	zstdDecoder     *zstd.Decoder
	zstdDecoderOnce sync.Once
)

func getZstdEncoder() *zstd.Encoder {
	zstdEncoderOnce.Do(func() {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic(fmt.Sprintf("zstd: NewWriter: %v", err))
		}
		zstdEncoder = enc
	})
	return zstdEncoder
}

func getZstdDecoder() *zstd.Decoder {
	zstdDecoderOnce.Do(func() {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(fmt.Sprintf("zstd: NewReader: %v", err))
		}
		zstdDecoder = dec
	})
	return zstdDecoder
}

func zstdPayload(plain []byte) ([]byte, bool) {
	if uint64(len(plain)) > uint64(^uint32(0)) {
		return nil, false
	}
	compressed := getZstdEncoder().EncodeAll(plain, make([]byte, 0, len(plain)/2))
	payloadSize := zstdHeaderBytes + len(compressed)
	if payloadSize*100 >= len(plain)*(100-zstdMinSavingsPercent) {
		return nil, false
	}
	payload := make([]byte, payloadSize)
	binary.LittleEndian.PutUint32(payload[:zstdHeaderBytes], uint32(len(plain)))
	copy(payload[zstdHeaderBytes:], compressed)
	return payload, true
}

func decompressZstd(page Page) ([]byte, error) {
	if page.Encoding != types.EncodingZstd {
		return nil, fmt.Errorf("zstd codec cannot decode %s", page.Encoding)
	}
	if !page.Kind.IsVarBytes() {
		return nil, fmt.Errorf("zstd codec unsupported kind %s", page.Kind)
	}
	if len(page.Payload) < zstdHeaderBytes {
		return nil, fmt.Errorf("zstd payload missing uncompressed length")
	}
	want := int(binary.LittleEndian.Uint32(page.Payload[:zstdHeaderBytes]))
	plain, err := getZstdDecoder().DecodeAll(page.Payload[zstdHeaderBytes:], make([]byte, 0, want))
	if err != nil {
		return nil, err
	}
	if len(plain) != want {
		return nil, fmt.Errorf("zstd payload expanded to %d bytes, want %d", len(plain), want)
	}
	return plain, nil
}
