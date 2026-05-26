// FOR + bitpack codec for FOR-packable kinds (Int16/32/64, Date, Timestamp, Time, Decimal64, Enum32).
// Wire [u64 LE base][u8 width][bitpack payload]. Residuals = (val - base) bit-packed via FastLanes core.
package codec

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const forHeaderSize = 9

type forBitpackCodec struct{}

func init() {
	Register(forBitpackCodec{})
}

func (forBitpackCodec) Encoding() schema.Encoding { return schema.EncFOR }

func (c forBitpackCodec) forFits(v vector.Vec, ctx *EncodeContext) (base int64, width int, ok bool) {
	if !v.Kind.IsFORPackable() {
		return 0, 0, false
	}
	rows := int(v.Len)
	if rows == 0 {
		return 0, 0, false
	}
	if ctx != nil && ctx.Facts != nil && ctx.Facts.Int != nil {
		f := ctx.Facts.Int
		if f.ForWidth == 0 {
			return 0, 0, false
		}
		storageBits := int(v.Kind.FixedWidth()) * 8
		if f.ForWidth >= storageBits {
			return 0, 0, false
		}
		return f.ForBase, f.ForWidth, true
	}
	base, width, ok = forParams(v)
	if !ok {
		return 0, 0, false
	}
	storageBits := int(v.Kind.FixedWidth()) * 8
	if width >= storageBits {
		return 0, 0, false
	}
	return base, width, true
}

func (c forBitpackCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	base, width, ok := c.forFits(v, ctx)
	if !ok {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	n := forHeaderSize + PackedSize(rows, width)
	if maxLen, ok := ctxMaxEncodedLen(ctx); ok && n > maxLen {
		return nil, ErrSkip
	}
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	binary.LittleEndian.PutUint64(scratch[0:8], uint64(base))
	scratch[8] = byte(width)
	residuals := ctxU64s(ctx, rows)
	readFORValues(v, residuals)
	for i := range residuals {
		residuals[i] -= uint64(base)
	}
	Pack(width, residuals, scratch[forHeaderSize:])
	return scratch, nil
}

func (forBitpackCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("for+bitpack decode: %w", err)
	}
	if !kind.IsFORPackable() {
		return fmt.Errorf("for+bitpack decode: kind %v not FOR-packable", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("for+bitpack decode: zero rows but %d-byte payload", len(payload))
		}
		dst.ResetForDecode(kind)
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	if len(payload) < forHeaderSize {
		return fmt.Errorf("for+bitpack decode: header truncated, have %d", len(payload))
	}
	base := int64(binary.LittleEndian.Uint64(payload[0:8]))
	width := int(payload[8])
	if width < 1 || width > 64 {
		return fmt.Errorf("for+bitpack decode: width %d out of range", width)
	}
	expected := forHeaderSize + PackedSize(rows, width)
	if len(payload) != expected {
		return fmt.Errorf("for+bitpack decode: payload %d != expected %d", len(payload), expected)
	}
	residuals := make([]uint64, rows)
	Unpack(width, payload[forHeaderSize:], rows, residuals)
	dst.ResetForDecode(kind)
	dst.EnsureFixedBytes(rows)
	if err := writeFORValues(dst, residuals, base); err != nil {
		return fmt.Errorf("for+bitpack decode: %w", err)
	}
	return nil
}

func forParams(v vector.Vec) (base int64, width int, ok bool) {
	rows := int(v.Len)
	tmp := make([]uint64, rows)
	readFORValues(v, tmp)
	minVal := int64(tmp[0])
	maxVal := minVal
	for i := 1; i < rows; i++ {
		val := int64(tmp[i])
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}
	span := uint64(maxVal) - uint64(minVal)
	if span == 0 {
		return 0, 0, false
	}
	w := 64 - bits.LeadingZeros64(span)
	if w < 1 || w > 64 {
		return 0, 0, false
	}
	return minVal, w, true
}

func readFORValues(v vector.Vec, dst []uint64) {
	switch v.Kind.FixedWidth() {
	case 2:
		for i, x := range v.I16() {
			dst[i] = uint64(int64(x))
		}
	case 4:
		switch v.Kind {
		case vector.VecInt32, vector.VecDate:
			for i, x := range v.I32() {
				dst[i] = uint64(int64(x))
			}
		case vector.VecEnum32:
			for i, x := range v.U32() {
				dst[i] = uint64(x)
			}
		}
	case 8:
		for i, x := range v.I64() {
			dst[i] = uint64(x)
		}
	}
}

func writeFORValues(dst *vector.Vec, residuals []uint64, base int64) error {
	switch dst.Kind.FixedWidth() {
	case 2:
		out := dst.I16()
		for i, r := range residuals {
			val := base + int64(r)
			if val < math.MinInt16 || val > math.MaxInt16 {
				return fmt.Errorf("decoded value %d at row %d overflows int16", val, i)
			}
			out[i] = int16(val)
		}
	case 4:
		switch dst.Kind {
		case vector.VecInt32, vector.VecDate:
			out := dst.I32()
			for i, r := range residuals {
				val := base + int64(r)
				if val < math.MinInt32 || val > math.MaxInt32 {
					return fmt.Errorf("decoded value %d at row %d overflows int32", val, i)
				}
				out[i] = int32(val)
			}
		case vector.VecEnum32:
			out := dst.U32()
			for i, r := range residuals {
				val := base + int64(r)
				if val < 0 || val > math.MaxUint32 {
					return fmt.Errorf("decoded value %d at row %d overflows uint32", val, i)
				}
				out[i] = uint32(val)
			}
		}
	case 8:
		out := dst.I64()
		for i, r := range residuals {
			out[i] = base + int64(r)
		}
	}
	return nil
}
