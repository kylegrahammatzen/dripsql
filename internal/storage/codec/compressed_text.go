// compressed_text unifies Flate and Zstd: serialize varbytes plain wire, compress, prepend uncompressed-length header.
// One struct parameterized by compress/decompress functions. Two registered instances replace the prior flate.go + zstd.go.
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type compressedTextCodec struct {
	enc        types.Encoding
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
		enc:        types.EncodingFlate,
		compress:   flateCompress,
		decompress: flateDecompress,
	})
	Register(&compressedTextCodec{
		enc:        types.EncodingZstd,
		compress:   zstdCompress,
		decompress: zstdDecompress,
	})
}

func (c *compressedTextCodec) Encoding() types.Encoding { return c.enc }

func (c *compressedTextCodec) Estimate(v types.Vec) (int, bool) {
	if !v.Kind.IsVarBytes() {
		return 0, false
	}
	return 4 + varbytesWireSize(v), true
}

func (c *compressedTextCodec) Encode(v types.Vec, scratch []byte) ([]byte, error) {
	if !v.Kind.IsVarBytes() {
		return nil, fmt.Errorf("%v encode: kind %v not varbytes", c.enc, v.Kind)
	}
	plainSize := varbytesWireSize(v)
	plain := scratch
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

func (c *compressedTextCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
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
	// Decoded layout is flat (StringView + data buffer); doc invariant says
	// Enc != Flat means data points at codec-specific encoded state, which
	// is not the case here. Surface the runtime shape, not the wire choice.
	dst.Enc = types.EncodingFlat
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
