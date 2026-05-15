// Plain codec: zero-transform byte layout per Vec kind.
// Wire is native little-endian for fixed widths. Project targets are amd64/arm64.
package codec

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type plainCodec struct{}

func init() {
	Register(plainCodec{})
}

func (plainCodec) Encoding() types.Encoding { return types.EncodingFlat }

func (plainCodec) Estimate(v types.Vec) (int, bool) {
	w := v.Kind.FixedWidth()
	if w > 0 {
		return int(v.Len) * int(w), true
	}
	switch w {
	case types.WidthBool:
		return (int(v.Len) + 7) / 8, true
	case types.WidthVarBytes:
		return varbytesWireSize(v), true
	}
	return 0, false
}

func (c plainCodec) Encode(v types.Vec, scratch []byte) ([]byte, error) {
	n, ok := c.Estimate(v)
	if !ok {
		return nil, fmt.Errorf("plain encode: unsupported kind %v", v.Kind)
	}
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	w := v.Kind.FixedWidth()
	if w > 0 {
		copy(scratch, v.FixedBytes())
		return scratch, nil
	}
	switch w {
	case types.WidthBool:
		copy(scratch, v.BoolBits())
		if n > 0 {
			if rem := int(v.Len) & 7; rem != 0 {
				scratch[n-1] &= byte((1 << uint(rem)) - 1)
			}
		}
		return scratch, nil
	case types.WidthVarBytes:
		writeVarbytesWire(scratch, v)
		return scratch, nil
	}
	return nil, fmt.Errorf("plain encode: unreachable kind %v", v.Kind)
}

func (plainCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("plain decode: %w", err)
	}
	w := kind.FixedWidth()
	if w > 0 {
		need := rows * int(w)
		if len(payload) != need {
			return fmt.Errorf("plain decode: payload %d != expected %d", len(payload), need)
		}
		dst.ResetForDecode(kind)
		copy(dst.EnsureFixedBytes(rows), payload)
		return nil
	}
	switch w {
	case types.WidthBool:
		need := (rows + 7) / 8
		if len(payload) != need {
			return fmt.Errorf("plain decode bool: payload %d != expected %d", len(payload), need)
		}
		*dst = types.NewVec(kind, rows)
		bits := dst.BoolBits()
		copy(bits, payload)
		if rem := rows & 7; rem != 0 && len(bits) > 0 {
			bits[len(bits)-1] &= byte(1<<rem - 1)
		}
		return nil
	case types.WidthVarBytes:
		if err := readVarbytesWire(payload, kind, rows, dst); err != nil {
			return err
		}
		dst.Enc = types.EncodingFlat
		dst.Valid = nil
		return nil
	}
	return fmt.Errorf("plain decode: unsupported kind %v", kind)
}

func validateDecodeArgs(rows, nullCount int) error {
	if rows < 0 {
		return fmt.Errorf("rows %d negative", rows)
	}
	if rows > math.MaxInt32 {
		return fmt.Errorf("rows %d exceeds int32 range; types.Vec cannot represent it", rows)
	}
	if nullCount < 0 || nullCount > rows {
		return fmt.Errorf("nullCount %d out of range [0, %d]", nullCount, rows)
	}
	return nil
}

