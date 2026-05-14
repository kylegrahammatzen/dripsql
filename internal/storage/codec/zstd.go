package codec

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Zstd struct{}

var zstdImpl = compressedVarBytesBase{
	encoding:      types.EncodingZstd,
	headerBytes:   4,
	minSavingsPct: 5,
	compress:      zstdCompress,
	decompress:    zstdDecompress,
}

func (Zstd) Encoding() types.Encoding                  { return zstdImpl.Encoding() }
func (Zstd) Encode(v types.Vec) (Page, error)          { return zstdImpl.Encode(v) }
func (Zstd) Prepare(v types.Vec) (PreparedEncoding, bool) {
	return zstdImpl.Prepare(v)
}
func (Zstd) Decode(page Page) (types.Vec, error)       { return zstdImpl.Decode(page) }
func (Zstd) DecodeInto(page Page, dst *types.Vec) error {
	return zstdImpl.DecodeInto(page, dst)
}
func (Zstd) DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error) {
	return zstdImpl.DecodeSelected(page, sel)
}
func (Zstd) Estimate(v types.Vec) (int, bool) { return zstdImpl.Estimate(v) }

// Encoder and decoder allocation dwarfs per-page work, so pool one of each
// for the lifetime of the process.
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

func zstdCompress(plain []byte) ([]byte, error) {
	return getZstdEncoder().EncodeAll(plain, make([]byte, 0, len(plain)/2)), nil
}

func zstdDecompress(compressed []byte, hint int) ([]byte, error) {
	return getZstdDecoder().DecodeAll(compressed, make([]byte, 0, hint))
}
