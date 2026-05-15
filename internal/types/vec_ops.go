// Shared Vec helpers used by exec and engine: kind-dispatched constructor, row copy with
// validity tracking, all-valid Validity allocation, and per-row any extraction.
package types

import "fmt"

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
