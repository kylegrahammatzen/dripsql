// Delta + bitpack codec: stores PackFirst (vals[0]), bitpacks (vals[i]-vals[i-1]) - min(deltas).
// Wire: [u64 LE PackFirst][u64 LE PackBase][u8 PackWidth][bitpack payload of rows-1 residuals].
package codec

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const deltaHeaderSize = 17

type deltaBitpackCodec struct{}

func init() {
	Register(deltaBitpackCodec{})
}

func (deltaBitpackCodec) Encoding() types.Encoding { return types.EncodingDeltaBitPack }

func (c deltaBitpackCodec) Estimate(v types.Vec) (int, bool) {
	if !v.Kind.IsFORPackable() {
		return 0, false
	}
	rows := int(v.Len)
	if rows < 2 {
		return 0, false
	}
	_, _, width, ok := deltaParams(v)
	if !ok {
		return 0, false
	}
	storageBits := int(v.Kind.FixedWidth()) * 8
	if width >= storageBits {
		return 0, false
	}
	return deltaHeaderSize + PackedSize(rows-1, width), true
}

func (c deltaBitpackCodec) Encode(v types.Vec, scratch []byte) ([]byte, error) {
	if !v.Kind.IsFORPackable() {
		return nil, fmt.Errorf("delta+bitpack encode: kind %v not FOR-packable", v.Kind)
	}
	rows := int(v.Len)
	if rows < 2 {
		return nil, fmt.Errorf("delta+bitpack encode: rows %d < 2", rows)
	}
	first, base, width, ok := deltaParams(v)
	if !ok {
		return nil, fmt.Errorf("delta+bitpack encode: width 0 or out of range")
	}
	n := deltaHeaderSize + PackedSize(rows-1, width)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	binary.LittleEndian.PutUint64(scratch[0:8], uint64(first))
	binary.LittleEndian.PutUint64(scratch[8:16], uint64(base))
	scratch[16] = byte(width)
	vals := make([]uint64, rows)
	readFORValues(v, vals)
	residuals := make([]uint64, rows-1)
	for i := range residuals {
		residuals[i] = vals[i+1] - vals[i] - uint64(base)
	}
	Pack(width, residuals, scratch[deltaHeaderSize:])
	return scratch, nil
}

func (deltaBitpackCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("delta+bitpack decode: %w", err)
	}
	if !kind.IsFORPackable() {
		return fmt.Errorf("delta+bitpack decode: kind %v not FOR-packable", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("delta+bitpack decode: zero rows but %d-byte payload", len(payload))
		}
		dst.ResetForDecode(kind)
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	if rows < 2 {
		return fmt.Errorf("delta+bitpack decode: rows %d < 2", rows)
	}
	if len(payload) < deltaHeaderSize {
		return fmt.Errorf("delta+bitpack decode: header truncated, have %d", len(payload))
	}
	first := int64(binary.LittleEndian.Uint64(payload[0:8]))
	base := int64(binary.LittleEndian.Uint64(payload[8:16]))
	width := int(payload[16])
	if width < 1 || width > 64 {
		return fmt.Errorf("delta+bitpack decode: width %d out of range", width)
	}
	expected := deltaHeaderSize + PackedSize(rows-1, width)
	if len(payload) != expected {
		return fmt.Errorf("delta+bitpack decode: payload %d != expected %d", len(payload), expected)
	}
	residuals := make([]uint64, rows-1)
	Unpack(width, payload[deltaHeaderSize:], rows-1, residuals)
	vals := make([]uint64, rows)
	vals[0] = uint64(first)
	for i := 1; i < rows; i++ {
		vals[i] = vals[i-1] + residuals[i-1] + uint64(base)
	}
	dst.ResetForDecode(kind)
	dst.EnsureFixedBytes(rows)
	if err := writeFORValues(dst, vals, 0); err != nil {
		return fmt.Errorf("delta+bitpack decode: %w", err)
	}
	return nil
}

func deltaParams(v types.Vec) (first, base int64, width int, ok bool) {
	rows := int(v.Len)
	vals := make([]uint64, rows)
	readFORValues(v, vals)
	first = int64(vals[0])
	d0 := int64(vals[1]) - int64(vals[0])
	minD := d0
	maxD := d0
	for i := 2; i < rows; i++ {
		d := int64(vals[i]) - int64(vals[i-1])
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	span := uint64(maxD) - uint64(minD)
	if span == 0 {
		return 0, 0, 0, false
	}
	w := 64 - bits.LeadingZeros64(span)
	if w < 1 || w > 64 {
		return 0, 0, 0, false
	}
	return first, minD, w, true
}
