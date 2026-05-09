package kernel

import (
	"bytes"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func EqBytes(v vector.VarBytes, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs []byte) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i := 0; i < len(v.Offsets)-1; i++ {
				if bytes.Equal(v.Bytes(i), rhs) {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i := 0; i < len(v.Offsets)-1; i++ {
			if validBit(valid, i) && bytes.Equal(v.Bytes(i), rhs) {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if bytes.Equal(v.Bytes(int(row)), rhs) {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && bytes.Equal(v.Bytes(i), rhs) {
			out = append(out, row)
		}
	}
	return out
}

// PrefixBytes follows bytes.HasPrefix semantics; an empty prefix matches every valid row.
func PrefixBytes(v vector.VarBytes, valid vector.Validity, sel vector.Sel, out vector.Sel, prefix []byte) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i := 0; i < len(v.Offsets)-1; i++ {
				if bytes.HasPrefix(v.Bytes(i), prefix) {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i := 0; i < len(v.Offsets)-1; i++ {
			if validBit(valid, i) && bytes.HasPrefix(v.Bytes(i), prefix) {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if bytes.HasPrefix(v.Bytes(int(row)), prefix) {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && bytes.HasPrefix(v.Bytes(i), prefix) {
			out = append(out, row)
		}
	}
	return out
}
