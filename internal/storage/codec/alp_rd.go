// ALP-RD codec for float64 that decimal-ALP rejects.
// Round-trip is bitwise-exact, so NaN, Inf, and -0 are preserved.
package codec

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	alpRDHeaderSize  = 5
	alpRDHeadBits    = 16
	alpRDTailBits    = 64 - alpRDHeadBits
	alpRDMaxDictSize = 256
)

type alpRDCodec struct{}

func init() {
	Register(alpRDCodec{})
}

func (alpRDCodec) Encoding() types.Encoding { return types.EncodingALPRD }

func (c alpRDCodec) Encode(v types.Vec, ctx *EncodeContext) ([]byte, error) {
	if v.Kind != types.VecFloat64 {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	if rows == 0 {
		return nil, ErrSkip
	}
	src := v.F64()
	dict := make(map[uint16]uint16, 16)
	dictEntries := make([]uint16, 0, 16)
	indices := make([]uint64, rows)
	tails := make([]uint64, rows)
	for i, x := range src {
		bits := math.Float64bits(x)
		head := uint16(bits >> alpRDTailBits)
		tail := bits & ((uint64(1) << alpRDTailBits) - 1)
		idx, ok := dict[head]
		if !ok {
			if len(dictEntries) >= alpRDMaxDictSize {
				return nil, ErrSkip
			}
			idx = uint16(len(dictEntries))
			dict[head] = idx
			dictEntries = append(dictEntries, head)
		}
		indices[i] = uint64(idx)
		tails[i] = tail
	}
	dictWidth := bitsForN(len(dictEntries))
	if dictWidth == 0 {
		dictWidth = 1
	}
	tailWidth := alpRDTailBits
	n := alpRDHeaderSize + len(dictEntries)*2 + PackedSize(rows, dictWidth) + PackedSize(rows, tailWidth)
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	scratch[0] = byte(len(dictEntries))
	scratch[1] = byte(dictWidth)
	scratch[2] = byte(tailWidth)
	binary.LittleEndian.PutUint16(scratch[3:5], uint16(rows))
	pos := alpRDHeaderSize
	for _, h := range dictEntries {
		binary.LittleEndian.PutUint16(scratch[pos:pos+2], h)
		pos += 2
	}
	idxBytes := PackedSize(rows, dictWidth)
	Pack(dictWidth, indices, scratch[pos:pos+idxBytes])
	pos += idxBytes
	tailBytes := PackedSize(rows, tailWidth)
	Pack(tailWidth, tails, scratch[pos:pos+tailBytes])
	return scratch, nil
}

func (alpRDCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("alp-rd decode: %w", err)
	}
	if kind != types.VecFloat64 {
		return fmt.Errorf("alp-rd decode: kind %v not float64", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("alp-rd decode: zero rows but %d-byte payload", len(payload))
		}
		dst.ResetForDecode(kind)
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	if len(payload) < alpRDHeaderSize {
		return fmt.Errorf("alp-rd decode: header truncated")
	}
	dictCount := int(payload[0])
	dictWidth := int(payload[1])
	tailWidth := int(payload[2])
	wireRows := int(binary.LittleEndian.Uint16(payload[3:5]))
	if wireRows != rows {
		return fmt.Errorf("alp-rd decode: rows %d != wire rows %d", rows, wireRows)
	}
	if dictCount == 0 || dictCount > alpRDMaxDictSize {
		return fmt.Errorf("alp-rd decode: dict count %d out of range", dictCount)
	}
	if dictWidth < 1 || dictWidth > 16 {
		return fmt.Errorf("alp-rd decode: dict width %d out of range", dictWidth)
	}
	if tailWidth != alpRDTailBits {
		return fmt.Errorf("alp-rd decode: tail width %d != %d", tailWidth, alpRDTailBits)
	}
	pos := alpRDHeaderSize
	if pos+dictCount*2 > len(payload) {
		return fmt.Errorf("alp-rd decode: dict body truncated")
	}
	dictEntries := make([]uint16, dictCount)
	for i := range dictCount {
		dictEntries[i] = binary.LittleEndian.Uint16(payload[pos+i*2 : pos+i*2+2])
	}
	pos += dictCount * 2
	idxBytes := PackedSize(rows, dictWidth)
	if pos+idxBytes > len(payload) {
		return fmt.Errorf("alp-rd decode: indices truncated")
	}
	indices := make([]uint64, rows)
	Unpack(dictWidth, payload[pos:pos+idxBytes], rows, indices)
	pos += idxBytes
	tailBytes := PackedSize(rows, tailWidth)
	if pos+tailBytes != len(payload) {
		return fmt.Errorf("alp-rd decode: tail %d != expected %d", len(payload)-pos, tailBytes)
	}
	tails := make([]uint64, rows)
	Unpack(tailWidth, payload[pos:pos+tailBytes], rows, tails)
	dst.ResetForDecode(kind)
	dst.EnsureFixedBytes(rows)
	out := dst.F64()
	for i := range rows {
		idx := int(indices[i])
		if idx >= dictCount {
			return fmt.Errorf("alp-rd decode: row %d dict idx %d >= %d", i, idx, dictCount)
		}
		bits := (uint64(dictEntries[idx]) << alpRDTailBits) | tails[i]
		out[i] = math.Float64frombits(bits)
	}
	return nil
}

func bitsForN(n int) int {
	if n <= 1 {
		return 0
	}
	w := 0
	v := uint64(n - 1)
	for v > 0 {
		w++
		v >>= 1
	}
	return w
}
