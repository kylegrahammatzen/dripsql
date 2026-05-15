// Sequence codec: arithmetic progression start + step*i in 16 bytes regardless of row count.
// Scope is width-8 FOR-packable kinds (Int64, Timestamp, Time, Decimal64). Int64 wrap semantics on overflow are tolerated.
package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type sequenceCodec struct{}

func init() {
	Register(sequenceCodec{})
}

func (sequenceCodec) Encoding() types.Encoding { return types.EncodingSequence }

func (sequenceCodec) Estimate(v types.Vec) (int, bool) {
	if v.Kind.FixedWidth() != 8 || !v.Kind.IsFORPackable() {
		return 0, false
	}
	rows := int(v.Len)
	if rows < 2 {
		return 0, false
	}
	vals := v.I64()
	step := vals[1] - vals[0]
	for i := 2; i < rows; i++ {
		if vals[i]-vals[i-1] != step {
			return 0, false
		}
	}
	return 16, true
}

func (c sequenceCodec) Encode(v types.Vec, scratch []byte) ([]byte, error) {
	_, ok := c.Estimate(v)
	if !ok {
		return nil, fmt.Errorf("sequence encode: rows do not form an arithmetic progression")
	}
	if cap(scratch) < 16 {
		scratch = make([]byte, 16)
	} else {
		scratch = scratch[:16]
	}
	vals := v.I64()
	binary.LittleEndian.PutUint64(scratch[0:8], uint64(vals[0]))
	binary.LittleEndian.PutUint64(scratch[8:16], uint64(vals[1]-vals[0]))
	return scratch, nil
}

func (sequenceCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("sequence decode: %w", err)
	}
	if kind.FixedWidth() != 8 || !kind.IsFORPackable() {
		return fmt.Errorf("sequence decode: unsupported kind %v", kind)
	}
	if rows == 1 {
		return fmt.Errorf("sequence decode: rows=1 not a canonical sequence; encoder rejects rows<2")
	}
	expected := 0
	if rows > 0 {
		expected = 16
	}
	if len(payload) != expected {
		return fmt.Errorf("sequence decode: payload %d != expected %d", len(payload), expected)
	}
	dst.ResetForDecode(kind)
	if rows == 0 {
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	start := int64(binary.LittleEndian.Uint64(payload[0:8]))
	step := int64(binary.LittleEndian.Uint64(payload[8:16]))
	buf := dst.EnsureFixedBytes(rows)
	for i := range rows {
		binary.LittleEndian.PutUint64(buf[i*8:i*8+8], uint64(start+step*int64(i)))
	}
	return nil
}
