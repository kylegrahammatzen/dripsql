package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// compressedVarBytesBase is the shared base for codecs that compress
// Plain's varbytes payload as a single opaque blob. Flate and Zstd plug
// their own compress and decompress functions in; everything else, from
// the payload framing and savings threshold to the decode-into path and
// the Plain-driven DecodeSelected, is shared.
type compressedVarBytesBase struct {
	encoding      types.Encoding
	headerBytes   int
	minSavingsPct int
	compress      func(plain []byte) ([]byte, error)
	decompress    func(compressed []byte, hint int) ([]byte, error)
}

func (c compressedVarBytesBase) Encoding() types.Encoding { return c.encoding }

func (c compressedVarBytesBase) Encode(v types.Vec) (Page, error) {
	prepared, ok := c.Prepare(v)
	if !ok {
		if v.Encoding != types.EncodingFlat {
			return Page{}, fmt.Errorf("%s codec requires flat vector, got %s", c.encoding, v.Encoding)
		}
		return Page{}, fmt.Errorf("%s codec requires compressible varbytes vector", c.encoding)
	}
	return prepared.EncodeInto(nil)
}

func (c compressedVarBytesBase) Prepare(v types.Vec) (PreparedEncoding, bool) {
	if v.Encoding != types.EncodingFlat || !v.Kind.IsVarBytes() {
		return nil, false
	}
	plainSize := validityBytes(v.Valid) + (v.Len+1)*4 + len(v.Var.Data)
	if uint64(plainSize) > uint64(^uint32(0)) {
		return nil, false
	}
	plain := make([]byte, plainSize)
	pos := writeValidity(plain, v.Valid)
	pos += encodeFixedSlice(plain[pos:], v.Var.Offsets[:v.Len+1])
	copy(plain[pos:], v.Var.Data)
	compressed, err := c.compress(plain)
	if err != nil {
		return nil, false
	}
	payloadSize := c.headerBytes + len(compressed)
	if payloadSize*100 >= len(plain)*(100-c.minSavingsPct) {
		return nil, false
	}
	payload := make([]byte, payloadSize)
	binary.LittleEndian.PutUint32(payload[:c.headerBytes], uint32(len(plain)))
	copy(payload[c.headerBytes:], compressed)
	return preparedCompressedVarBytes{encoding: c.encoding, kind: v.Kind, rows: v.Len, nullCount: types.NullCount(v.Valid, v.Len), payload: payload}, true
}

func (c compressedVarBytesBase) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := c.DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (c compressedVarBytesBase) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("%s decode destination is nil", c.encoding)
	}
	plain, err := c.inflate(page)
	if err != nil {
		return err
	}
	return decodeCompressedVarBytesInto(page, plain, dst)
}

func (c compressedVarBytesBase) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	plain, err := c.inflate(page)
	if err != nil {
		return types.Vec{}, err
	}
	plainPage := page
	plainPage.Encoding = types.EncodingFlat
	plainPage.Payload = plain
	return (Plain{}).DecodeSelected(plainPage, sel)
}

func (c compressedVarBytesBase) Estimate(v types.Vec) (int, bool) {
	prepared, ok := c.Prepare(v)
	if !ok {
		return 0, false
	}
	return prepared.Size(), true
}

func (c compressedVarBytesBase) inflate(page Page) ([]byte, error) {
	if page.Encoding != c.encoding {
		return nil, fmt.Errorf("%s codec cannot decode %s", c.encoding, page.Encoding)
	}
	if !page.Kind.IsVarBytes() {
		return nil, fmt.Errorf("%s codec unsupported kind %s", c.encoding, page.Kind)
	}
	if len(page.Payload) < c.headerBytes {
		return nil, fmt.Errorf("%s payload missing uncompressed length", c.encoding)
	}
	want := int(binary.LittleEndian.Uint32(page.Payload[:c.headerBytes]))
	plain, err := c.decompress(page.Payload[c.headerBytes:], want)
	if err != nil {
		return nil, err
	}
	if len(plain) != want {
		return nil, fmt.Errorf("%s payload expanded to %d bytes, want %d", c.encoding, len(plain), want)
	}
	return plain, nil
}

type preparedCompressedVarBytes struct {
	encoding  types.Encoding
	kind      types.VecKind
	rows      int
	nullCount int
	payload   []byte
}

func (p preparedCompressedVarBytes) Encoding() types.Encoding { return p.encoding }
func (p preparedCompressedVarBytes) Size() int                { return len(p.payload) }
func (p preparedCompressedVarBytes) EncodeInto(_ []byte) (Page, error) {
	return Page{Kind: p.kind, Encoding: p.encoding, Rows: p.rows, NullCount: p.nullCount, Payload: p.payload}, nil
}

// decodeCompressedVarBytesInto materializes the decompressed plain varbytes
// layout into dst, shared by every codec that compresses Plain's varbytes
// payload as a single opaque blob.
func decodeCompressedVarBytesInto(page Page, plainPayload []byte, dst *types.Vec) error {
	valid, pos, err := readValidityInto(plainPayload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	offsetBytes := (page.Rows + 1) * 4
	if len(plainPayload)-pos < offsetBytes {
		return fmt.Errorf("varbytes offsets truncated")
	}
	resetVecForDecode(dst, page.Kind, false)
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
	dst.Var = types.VarBytes{Offsets: offsets, Data: data, Prefixes: resizeSlice(dst.Var.Prefixes, page.Rows)}
	dst.Var.RebuildPrefixes()
	return nil
}
