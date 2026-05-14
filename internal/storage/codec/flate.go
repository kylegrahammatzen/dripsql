package codec

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/flate"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Flate struct{}

var flateImpl = compressedVarBytesBase{
	encoding:      types.EncodingFlate,
	headerBytes:   4,
	minSavingsPct: 5,
	compress:      flateCompress,
	decompress:    flateDecompress,
}

func (Flate) Encoding() types.Encoding                  { return flateImpl.Encoding() }
func (Flate) Encode(v types.Vec) (Page, error)          { return flateImpl.Encode(v) }
func (Flate) Prepare(v types.Vec) (PreparedEncoding, bool) {
	return flateImpl.Prepare(v)
}
func (Flate) Decode(page Page) (types.Vec, error)       { return flateImpl.Decode(page) }
func (Flate) DecodeInto(page Page, dst *types.Vec) error {
	return flateImpl.DecodeInto(page, dst)
}
func (Flate) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	return flateImpl.DecodeSelected(page, sel)
}
func (Flate) Estimate(v types.Vec) (int, bool) { return flateImpl.Estimate(v) }

func flateCompress(plain []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(plain); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func flateDecompress(compressed []byte, _ int) ([]byte, error) {
	zr := flate.NewReader(bytes.NewReader(compressed))
	plain, err := io.ReadAll(zr)
	closeErr := zr.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return plain, nil
}
