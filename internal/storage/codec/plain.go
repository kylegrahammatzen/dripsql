// Plain codec: zero-transform byte layout per Vec kind.
// Wire is native little-endian for fixed widths. Project targets are amd64/arm64.
package codec

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type plainCodec struct{}

func init() {
	Register(plainCodec{})
}

func (plainCodec) Encoding() schema.Encoding { return schema.EncPlain }

func (c plainCodec) plainSize(v vector.Vec) (int, bool) {
	w := v.Kind.FixedWidth()
	if w > 0 {
		return int(v.Len) * int(w), true
	}
	switch w {
	case vector.WidthBool:
		return (int(v.Len) + 7) / 8, true
	case vector.WidthVarBytes:
		return varbytesWireSize(v), true
	}
	return 0, false
}

func (c plainCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	n, ok := c.plainSize(v)
	if !ok {
		return nil, ErrSkip
	}
	scratch := ctxTrial(ctx)
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
	case vector.WidthBool:
		copy(scratch, v.BoolBits())
		if n > 0 {
			if rem := int(v.Len) & 7; rem != 0 {
				scratch[n-1] &= byte((1 << uint(rem)) - 1)
			}
		}
		return scratch, nil
	case vector.WidthVarBytes:
		writeVarbytesWire(scratch, v)
		return scratch, nil
	}
	return nil, fmt.Errorf("plain encode: unreachable kind %v", v.Kind)
}

func (plainCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
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
	case vector.WidthBool:
		need := (rows + 7) / 8
		if len(payload) != need {
			return fmt.Errorf("plain decode bool: payload %d != expected %d", len(payload), need)
		}
		*dst = vector.NewVec(kind, rows)
		bits := dst.BoolBits()
		copy(bits, payload)
		if rem := rows & 7; rem != 0 && len(bits) > 0 {
			bits[len(bits)-1] &= byte(1<<rem - 1)
		}
		return nil
	case vector.WidthVarBytes:
		if err := readVarbytesWire(payload, kind, rows, dst); err != nil {
			return err
		}
		dst.Enc = schema.EncPlain
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
		return fmt.Errorf("rows %d exceeds int32 range; vector.Vec cannot represent it", rows)
	}
	if nullCount < 0 || nullCount > rows {
		return fmt.Errorf("nullCount %d out of range [0, %d]", nullCount, rows)
	}
	return nil
}
