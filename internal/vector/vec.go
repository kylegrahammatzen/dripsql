// Vec is the slim runtime vector: one unsafe.Pointer replaces nine typed slice headers.
// Per doc "Encoded state lives in `data`, not as a sidecar": when Enc != Flat the
// data pointer addresses a codec-specific struct (dictState, packState, ...) instead
// of raw bytes. No separate sidecar slot needed.
package vector

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type Vec struct {
	Kind  VecKind
	Enc   schema.Encoding
	Len   int32
	Cap   int32
	data  unsafe.Pointer
	Valid Validity
}

func NewVec(kind VecKind, rows int) Vec {
	if rows < 0 || rows > math.MaxInt32 {
		panic(fmt.Sprintf("NewVec: rows %d out of int32 range", rows))
	}
	v := Vec{Kind: kind, Enc: schema.EncPlain, Cap: int32(rows), Len: int32(rows)}
	v.allocBacking(rows)
	return v
}

func NewVarVec(kind VecKind, rows int, dataBytes int) Vec {
	if !kind.IsVarBytes() {
		panic(fmt.Sprintf("NewVarVec called with non-varbytes kind %v", kind))
	}
	if rows < 0 || rows > math.MaxInt32 {
		panic(fmt.Sprintf("NewVarVec: rows %d out of int32 range", rows))
	}
	vb := NewVarBytes(rows, dataBytes)
	return Vec{
		Kind: kind,
		Enc:  schema.EncPlain,
		Len:  int32(rows),
		Cap:  int32(rows),
		data: unsafe.Pointer(&vb),
	}
}

func (v *Vec) allocBacking(rows int) {
	if rows == 0 {
		v.data = nil
		return
	}
	w := v.Kind.FixedWidth()
	switch w {
	case 0:
		panic(fmt.Sprintf("allocBacking: kind %v has no width", v.Kind))
	case WidthVarBytes:
		vb := NewVarBytes(rows, 0)
		v.data = unsafe.Pointer(&vb)
	case WidthBool:
		buf := make([]byte, (rows+7)/8)
		v.data = unsafe.Pointer(unsafe.SliceData(buf))
	default:
		buf := make([]byte, rows*int(w))
		v.data = unsafe.Pointer(unsafe.SliceData(buf))
	}
}

func (v *Vec) EnsureFixedBytes(rows int) []byte {
	if rows < 0 || rows > math.MaxInt32 {
		panic(fmt.Sprintf("EnsureFixedBytes: rows %d out of int32 range", rows))
	}
	w := v.Kind.FixedWidth()
	if w <= 0 {
		panic(fmt.Sprintf("EnsureFixedBytes on non-fixed kind %v", v.Kind))
	}
	if rows == 0 {
		v.Len = 0
		return nil
	}
	if v.data == nil || int(v.Cap) < rows {
		v.allocBacking(rows)
		v.Cap = int32(rows)
	}
	v.Len = int32(rows)
	return vecBytes(v, rows*int(w))
}

func (v Vec) Clone() Vec {
	out := Vec{Kind: v.Kind, Enc: v.Enc, Len: v.Len, Cap: v.Len}
	if v.Valid != nil {
		out.Valid = v.Valid.Clone()
	}
	if v.data == nil {
		return out
	}
	switch v.Kind.FixedWidth() {
	case WidthVarBytes:
		vb := v.Var().Clone()
		out.data = unsafe.Pointer(&vb)
	case WidthBool:
		buf := append([]byte(nil), v.BoolBits()...)
		out.data = unsafe.Pointer(unsafe.SliceData(buf))
	default:
		buf := append([]byte(nil), v.FixedBytes()...)
		out.data = unsafe.Pointer(unsafe.SliceData(buf))
	}
	return out
}

// Drops the existing buffer when kind changes so a wider element type cannot reuse
// an undersized allocation (Cap counts rows, not bytes).
func (v *Vec) ResetForDecode(kind VecKind) {
	if v.Kind != kind {
		v.data = nil
		v.Cap = 0
	}
	v.Kind = kind
	v.Enc = schema.EncPlain
	v.Len = 0
	v.Valid = nil
}

func vecSlice[T any](v *Vec) []T {
	return unsafe.Slice((*T)(v.data), int(v.Len))
}

func vecBytes(v *Vec, n int) []byte {
	return unsafe.Slice((*byte)(v.data), n)
}

func (v *Vec) I16() []int16     { return vecSlice[int16](v) }
func (v *Vec) I32() []int32     { return vecSlice[int32](v) }
func (v *Vec) I64() []int64     { return vecSlice[int64](v) }
func (v *Vec) F32() []float32   { return vecSlice[float32](v) }
func (v *Vec) F64() []float64   { return vecSlice[float64](v) }
func (v *Vec) U32() []uint32    { return vecSlice[uint32](v) }
func (v *Vec) UUID() []UUID16   { return vecSlice[UUID16](v) }
func (v *Vec) BoolBits() []byte { return vecBytes(v, (int(v.Len)+7)/8) }
func (v *Vec) Var() *VarBytes   { return (*VarBytes)(v.data) }

// Truncate shrinks the logical row count of v to rows. Fixed-width vectors just lower Len;
// varbytes additionally slice the views slice so VarBytes.Rows() agrees with Vec.Len. Caller
// is responsible for ensuring rows <= current Len.
func (v *Vec) Truncate(rows int) {
	if rows < 0 || rows > int(v.Len) {
		return
	}
	v.Len = int32(rows)
	if v.Kind.FixedWidth() == WidthVarBytes && v.data != nil {
		vb := (*VarBytes)(v.data)
		vb.views = vb.views[:rows]
	}
}

func (v Vec) FixedBytes() []byte {
	w := v.Kind.FixedWidth()
	if w <= 0 {
		return nil
	}
	return unsafe.Slice((*byte)(v.data), int(v.Len)*int(w))
}

func (v *Vec) LoadFixedBytes(src []byte, rows int) error {
	w := v.Kind.FixedWidth()
	if w <= 0 {
		return fmt.Errorf("LoadFixedBytes on non-fixed kind %v", v.Kind)
	}
	need := rows * int(w)
	if len(src) < need {
		return fmt.Errorf("payload truncated: have %d, need %d", len(src), need)
	}
	copy(v.EnsureFixedBytes(rows), src[:need])
	return nil
}

func (v Vec) Validate() error {
	w := v.Kind.FixedWidth()
	if w == 0 {
		return fmt.Errorf("Vec.Kind %v is invalid or unknown", v.Kind)
	}
	if v.Len < 0 || v.Cap < 0 {
		return fmt.Errorf("Vec has negative Len=%d Cap=%d", v.Len, v.Cap)
	}
	if v.Len > v.Cap {
		return fmt.Errorf("Vec.Len %d exceeds Cap %d", v.Len, v.Cap)
	}
	if v.Len > 0 && v.data == nil {
		return fmt.Errorf("Vec.Len=%d but data is nil", v.Len)
	}
	if w == WidthVarBytes && v.data != nil {
		vb := (*VarBytes)(v.data)
		if vb.Rows() != int(v.Len) {
			return fmt.Errorf("varbytes rows %d does not match Vec.Len %d", vb.Rows(), v.Len)
		}
	}
	if v.Valid != nil && len(v.Valid) < ValidityWords(int(v.Len)) {
		return fmt.Errorf("Vec.Valid has %d words, need %d for Len=%d", len(v.Valid), ValidityWords(int(v.Len)), v.Len)
	}
	return nil
}

func NewVecForKind(kind VecKind, rows int) (Vec, error) {
	if kind == 0 {
		return Vec{}, fmt.Errorf("vec: invalid VecKind")
	}
	if kind.IsVarBytes() {
		return NewVarVec(kind, rows, 0), nil
	}
	w := kind.FixedWidth()
	if w == 0 {
		return Vec{}, fmt.Errorf("vec: unsupported VecKind %v", kind)
	}
	return NewVec(kind, rows), nil
}

func NewAllValid(rows int) Validity {
	v := make(Validity, ValidityWords(rows))
	for r := range rows {
		v.SetValid(r)
	}
	return v
}

func (c Column) ValueAt(row int) (any, error) {
	if c.V.Valid != nil && !c.V.Valid.IsValid(row) {
		return nil, nil
	}
	switch c.V.Kind {
	case VecBool:
		return c.V.BoolBits()[row>>3]&(1<<(row&7)) != 0, nil
	case VecInt16:
		return int64(c.V.I16()[row]), nil
	case VecInt32, VecDate:
		return int64(c.V.I32()[row]), nil
	case VecInt64, VecTimestamp, VecTime, VecDecimal64:
		return c.V.I64()[row], nil
	case VecFloat32:
		return float64(c.V.F32()[row]), nil
	case VecFloat64:
		return c.V.F64()[row], nil
	case VecText, VecBytes, VecJSON:
		return string(c.V.Var().Bytes(row)), nil
	case VecUUID:
		u := c.V.UUID()[row]
		return FormatUUID(u), nil
	case VecEnum32:
		code := c.V.U32()[row]
		if code == 0 || int(code-1) >= len(c.EnumLabels) {
			return nil, fmt.Errorf("column %q enum code %d out of range", c.Name, code)
		}
		return c.EnumLabels[code-1], nil
	}
	return nil, fmt.Errorf("column %q unsupported VecKind %v", c.Name, c.V.Kind)
}

func CopyVecRow(src Vec, srcRow int, dst *Vec, dstRow int) error {
	if src.Kind != dst.Kind {
		return fmt.Errorf("vec: row copy kind mismatch %v vs %v", src.Kind, dst.Kind)
	}
	if src.Valid != nil && !src.Valid.IsValid(srcRow) {
		if dst.Valid == nil {
			dst.Valid = NewAllValid(int(dst.Len))
		}
		dst.Valid.SetInvalid(dstRow)
		return nil
	}
	if dst.Valid != nil {
		dst.Valid.SetValid(dstRow)
	}
	w := src.Kind.FixedWidth()
	switch w {
	case WidthVarBytes:
		dst.Var().AppendBytes(dstRow, src.Var().Bytes(srcRow))
	case WidthBool:
		if src.BoolBits()[srcRow>>3]&(1<<(srcRow&7)) != 0 {
			dst.BoolBits()[dstRow>>3] |= 1 << (dstRow & 7)
		}
	default:
		sz := int(w)
		srcBytes := src.FixedBytes()
		dstBytes := dst.FixedBytes()
		copy(dstBytes[dstRow*sz:(dstRow+1)*sz], srcBytes[srcRow*sz:(srcRow+1)*sz])
	}
	return nil
}
