// compressed_text unifies Flate and Zstd as one struct parameterized by compress and decompress functions.
// Both serialize the varbytes plain wire, compress it, and prepend an uncompressed-length header.
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type compressedTextCodec struct {
	enc        schema.Encoding
	compress   func(plain []byte) ([]byte, error)
	decompress func(src []byte, uncompLen int) ([]byte, error)
}

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	zstdEncoder, _ = zstd.NewWriter(nil)
	zstdDecoder, _ = zstd.NewReader(nil)
	Register(&compressedTextCodec{
		enc:        schema.EncFlate,
		compress:   flateCompress,
		decompress: flateDecompress,
	})
	Register(&compressedTextCodec{
		enc:        schema.EncZstd,
		compress:   zstdCompress,
		decompress: zstdDecompress,
	})
}

func (c *compressedTextCodec) Encoding() schema.Encoding { return c.enc }

func (c *compressedTextCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	if !v.Kind.IsVarBytes() {
		return nil, ErrSkip
	}
	plainSize := varbytesWireSize(v)
	plain := ctxTrial(ctx)
	if cap(plain) < plainSize {
		plain = make([]byte, plainSize)
	} else {
		plain = plain[:plainSize]
	}
	writeVarbytesWire(plain, v)
	comp, err := c.compress(plain)
	if err != nil {
		return nil, fmt.Errorf("%v encode: %w", c.enc, err)
	}
	out := make([]byte, 4+len(comp))
	binary.LittleEndian.PutUint32(out[0:4], uint32(plainSize))
	copy(out[4:], comp)
	return out, nil
}

func (c *compressedTextCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("%v decode: %w", c.enc, err)
	}
	if !kind.IsVarBytes() {
		return fmt.Errorf("%v decode: kind %v not varbytes", c.enc, kind)
	}
	if len(payload) < 4 {
		return fmt.Errorf("%v decode: header truncated", c.enc)
	}
	uncompLen := int(binary.LittleEndian.Uint32(payload[0:4]))
	plain, err := c.decompress(payload[4:], uncompLen)
	if err != nil {
		return fmt.Errorf("%v decode: %w", c.enc, err)
	}
	if len(plain) != uncompLen {
		return fmt.Errorf("%v decode: uncompressed length mismatch: got %d want %d", c.enc, len(plain), uncompLen)
	}
	if err := readVarbytesWire(plain, kind, rows, dst); err != nil {
		return err
	}
	// The decoded layout is flat, so report EncPlain rather than the wire choice whose Enc contract implies codec-owned state.
	dst.Enc = schema.EncPlain
	dst.Valid = nil
	return nil
}

func flateCompress(plain []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(plain); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func flateDecompress(src []byte, uncompLen int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	out := bytes.NewBuffer(make([]byte, 0, uncompLen))
	if _, err := out.ReadFrom(r); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func zstdCompress(plain []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(plain, nil), nil
}

func zstdDecompress(src []byte, uncompLen int) ([]byte, error) {
	return zstdDecoder.DecodeAll(src, make([]byte, 0, uncompLen))
}
