// Delta + bitpack codec: stores PackFirst (vals[0]), bitpacks (vals[i]-vals[i-1]) - min(deltas).
// Wire: [u64 LE PackFirst][u64 LE PackBase][u8 PackWidth][bitpack payload of rows-1 residuals].
package codec

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const deltaHeaderSize = 17

type deltaBitpackCodec struct{}

func init() {
	Register(deltaBitpackCodec{})
}

func (deltaBitpackCodec) Encoding() schema.Encoding { return schema.EncDelta }

func (c deltaBitpackCodec) deltaFits(v vector.Vec, ctx *EncodeContext) (first, base int64, width int, ok bool) {
	if !v.Kind.IsFORPackable() {
		return 0, 0, 0, false
	}
	rows := int(v.Len)
	if rows < 2 {
		return 0, 0, 0, false
	}
	if ctx != nil && ctx.Facts != nil && ctx.Facts.Int != nil {
		f := ctx.Facts.Int
		if !f.DeltaOK {
			return 0, 0, 0, false
		}
		storageBits := int(v.Kind.FixedWidth()) * 8
		if f.DeltaWidth >= storageBits {
			return 0, 0, 0, false
		}
		return f.First, f.DeltaBase, f.DeltaWidth, true
	}
	first, base, width, ok = deltaParams(v)
	if !ok {
		return 0, 0, 0, false
	}
	storageBits := int(v.Kind.FixedWidth()) * 8
	if width >= storageBits {
		return 0, 0, 0, false
	}
	return first, base, width, true
}

func (c deltaBitpackCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	first, base, width, ok := c.deltaFits(v, ctx)
	if !ok {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	n := deltaHeaderSize + PackedSize(rows-1, width)
	if maxLen, ok := ctxMaxEncodedLen(ctx); ok && n > maxLen {
		return nil, ErrSkip
	}
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	binary.LittleEndian.PutUint64(scratch[0:8], uint64(first))
	binary.LittleEndian.PutUint64(scratch[8:16], uint64(base))
	scratch[16] = byte(width)
	vals := ctxU64s(ctx, rows)
	readFORValues(v, vals)
	residuals := make([]uint64, rows-1)
	for i := range residuals {
		residuals[i] = vals[i+1] - vals[i] - uint64(base)
	}
	Pack(width, residuals, scratch[deltaHeaderSize:])
	return scratch, nil
}

func (deltaBitpackCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
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

func deltaParams(v vector.Vec) (first, base int64, width int, ok bool) {
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
