// ALP float codec encodes float32 and float64 as mantissa = round(x * 10^e) + FOR + bitpack.
// Lossless only when every value round-trips exactly. NaN, Inf, -0, or non-decimal floats trigger ErrSkip.
package codec

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	alpHeaderSize = 10
	alpMaxExp     = 18
)

var alpPow10 = [alpMaxExp + 1]float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
	1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18,
}

type alpCodec struct{}

func init() {
	Register(alpCodec{})
}

func (alpCodec) Encoding() schema.Encoding { return schema.EncALP }

func (c alpCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	if v.Kind != vector.VecFloat32 && v.Kind != vector.VecFloat64 {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	if rows == 0 {
		return nil, ErrSkip
	}
	mantissas := ctxU64s(ctx, rows)
	exp, ok := alpFitExponent(v, mantissas)
	if !ok {
		return nil, ErrSkip
	}
	minVal, maxVal := int64(mantissas[0]), int64(mantissas[0])
	for i := 1; i < rows; i++ {
		m := int64(mantissas[i])
		if m < minVal {
			minVal = m
		}
		if m > maxVal {
			maxVal = m
		}
	}
	span := uint64(maxVal) - uint64(minVal)
	if span == 0 {
		return nil, ErrSkip
	}
	width := 64 - bits.LeadingZeros64(span)
	storageBits := int(v.Kind.FixedWidth()) * 8
	if width >= storageBits {
		return nil, ErrSkip
	}
	for i := range mantissas {
		mantissas[i] -= uint64(minVal)
	}
	n := alpHeaderSize + PackedSize(rows, width)
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	scratch[0] = exp
	binary.LittleEndian.PutUint64(scratch[1:9], uint64(minVal))
	scratch[9] = byte(width)
	if width > 0 {
		Pack(width, mantissas, scratch[alpHeaderSize:])
	}
	return scratch, nil
}

func (alpCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("alp decode: %w", err)
	}
	if kind != vector.VecFloat32 && kind != vector.VecFloat64 {
		return fmt.Errorf("alp decode: kind %v not float", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("alp decode: zero rows but %d-byte payload", len(payload))
		}
		dst.ResetForDecode(kind)
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	if len(payload) < alpHeaderSize {
		return fmt.Errorf("alp decode: header truncated, have %d", len(payload))
	}
	exp := payload[0]
	if exp > alpMaxExp {
		return fmt.Errorf("alp decode: exp %d out of range", exp)
	}
	minVal := int64(binary.LittleEndian.Uint64(payload[1:9]))
	width := int(payload[9])
	if width < 0 || width > 64 {
		return fmt.Errorf("alp decode: width %d out of range", width)
	}
	expected := alpHeaderSize + PackedSize(rows, width)
	if len(payload) != expected {
		return fmt.Errorf("alp decode: payload %d != expected %d", len(payload), expected)
	}
	mantissas := make([]uint64, rows)
	if width > 0 {
		Unpack(width, payload[alpHeaderSize:], rows, mantissas)
	}
	scale := alpPow10[exp]
	dst.ResetForDecode(kind)
	dst.EnsureFixedBytes(rows)
	if kind == vector.VecFloat64 {
		out := dst.F64()
		for i, r := range mantissas {
			m := minVal + int64(r)
			out[i] = float64(m) / scale
		}
		return nil
	}
	out := dst.F32()
	for i, r := range mantissas {
		m := minVal + int64(r)
		out[i] = float32(float64(m) / scale)
	}
	return nil
}

func alpFitExponent(v vector.Vec, mantissas []uint64) (byte, bool) {
	if v.Kind == vector.VecFloat64 {
		src := v.F64()
		for e := byte(0); e <= alpMaxExp; e++ {
			if alpFitF64(src, alpPow10[e], mantissas) {
				return e, true
			}
		}
		return 0, false
	}
	src := v.F32()
	for e := byte(0); e <= alpMaxExp; e++ {
		if alpFitF32(src, alpPow10[e], mantissas) {
			return e, true
		}
	}
	return 0, false
}

func alpFitF64(src []float64, scale float64, dst []uint64) bool {
	for i, x := range src {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return false
		}
		if x == 0 && math.Signbit(x) {
			return false
		}
		scaled := x * scale
		if scaled > math.MaxInt64 || scaled < math.MinInt64 {
			return false
		}
		m := int64(math.Round(scaled))
		if float64(m)/scale != x {
			return false
		}
		dst[i] = uint64(m)
	}
	return true
}

func alpFitF32(src []float32, scale float64, dst []uint64) bool {
	for i, x := range src {
		xf := float64(x)
		if math.IsNaN(xf) || math.IsInf(xf, 0) {
			return false
		}
		if xf == 0 && math.Signbit(xf) {
			return false
		}
		scaled := xf * scale
		if scaled > math.MaxInt64 || scaled < math.MinInt64 {
			return false
		}
		m := int64(math.Round(scaled))
		if float32(float64(m)/scale) != x {
			return false
		}
		dst[i] = uint64(m)
	}
	return true
}
