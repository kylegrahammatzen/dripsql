// Constant codec stores one value on disk regardless of row count.
// Returns ErrSkip when rows differ so cascade selection naturally skips it.
package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type constantCodec struct{}

func init() {
	Register(constantCodec{})
}

func (constantCodec) Encoding() schema.Encoding { return schema.EncConstant }

func (constantCodec) constantSize(v vector.Vec, ctx *EncodeContext) (int, bool) {
	w := v.Kind.FixedWidth()
	if w == 0 {
		return 0, false
	}
	if int(v.Len) == 0 {
		return 0, true
	}
	if ctx != nil && ctx.Facts != nil && ctx.Facts.Int != nil {
		if !ctx.Facts.Int.ConstantOK {
			return 0, false
		}
		return constantValueSize(v), true
	}
	if !constantAllEqual(v) {
		return 0, false
	}
	return constantValueSize(v), true
}

func constantAllEqual(v vector.Vec) bool {
	rows := int(v.Len)
	w := v.Kind.FixedWidth()
	if w > 0 {
		bs := v.FixedBytes()
		first := bs[:w]
		for i := 1; i < rows; i++ {
			if !bytes.Equal(bs[i*int(w):i*int(w)+int(w)], first) {
				return false
			}
		}
		return true
	}
	switch w {
	case vector.WidthBool:
		bits := v.BoolBits()
		first := (bits[0] & 1) != 0
		for i := 1; i < rows; i++ {
			bit := (bits[i>>3]>>uint(i&7))&1 != 0
			if bit != first {
				return false
			}
		}
		return true
	case vector.WidthVarBytes:
		vb := v.Var()
		first := vb.Bytes(0)
		for i := 1; i < rows; i++ {
			if !bytes.Equal(vb.Bytes(i), first) {
				return false
			}
		}
		return true
	}
	return false
}

func constantValueSize(v vector.Vec) int {
	w := v.Kind.FixedWidth()
	if w > 0 {
		return int(w)
	}
	switch w {
	case vector.WidthBool:
		return 1
	case vector.WidthVarBytes:
		return 4 + v.Var().Len(0)
	}
	return 0
}

func (c constantCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	n, ok := c.constantSize(v, ctx)
	if !ok {
		return nil, ErrSkip
	}
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	rows := int(v.Len)
	if rows == 0 {
		return scratch[:0], nil
	}
	w := v.Kind.FixedWidth()
	if w > 0 {
		copy(scratch, v.FixedBytes()[:w])
		return scratch, nil
	}
	switch w {
	case vector.WidthBool:
		if v.BoolBits()[0]&1 != 0 {
			scratch[0] = 1
		} else {
			scratch[0] = 0
		}
		return scratch, nil
	case vector.WidthVarBytes:
		first := v.Var().Bytes(0)
		binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(first)))
		copy(scratch[4:], first)
		return scratch, nil
	}
	return nil, fmt.Errorf("constant encode: unsupported kind %v", v.Kind)
}

func (constantCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("constant decode: %w", err)
	}
	w := kind.FixedWidth()
	if w > 0 {
		expected := 0
		if rows > 0 {
			expected = int(w)
		}
		if len(payload) != expected {
			return fmt.Errorf("constant decode: payload %d != expected %d", len(payload), expected)
		}
		dst.ResetForDecode(kind)
		buf := dst.EnsureFixedBytes(rows)
		for i := range rows {
			copy(buf[i*int(w):], payload)
		}
		return nil
	}
	switch w {
	case vector.WidthBool:
		expected := 0
		if rows > 0 {
			expected = 1
		}
		if len(payload) != expected {
			return fmt.Errorf("constant decode bool: payload %d != expected %d", len(payload), expected)
		}
		*dst = vector.NewVec(kind, rows)
		if rows == 0 {
			return nil
		}
		if payload[0] != 0 && payload[0] != 1 {
			return fmt.Errorf("constant decode bool: payload byte %d not canonical (0 or 1)", payload[0])
		}
		if payload[0] == 1 {
			bits := dst.BoolBits()
			for i := range bits {
				bits[i] = 0xFF
			}
			if rem := rows & 7; rem != 0 {
				bits[len(bits)-1] = byte((1 << uint(rem)) - 1)
			}
		}
		return nil
	case vector.WidthVarBytes:
		var value []byte
		if rows > 0 {
			if len(payload) < 4 {
				return fmt.Errorf("constant decode varbytes: header truncated")
			}
			length := int(binary.LittleEndian.Uint32(payload[0:4]))
			if len(payload) != 4+length {
				return fmt.Errorf("constant decode varbytes: payload %d != expected %d", len(payload), 4+length)
			}
			value = payload[4 : 4+length]
		} else if len(payload) != 0 {
			return fmt.Errorf("constant decode varbytes: zero rows but %d-byte payload", len(payload))
		}
		*dst = vector.NewVarVec(kind, rows, len(value))
		if rows > 0 {
			dst.Var().Broadcast(value)
		}
		return nil
	}
	return fmt.Errorf("constant decode: unsupported kind %v", kind)
}
