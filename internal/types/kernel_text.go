package types

import (
	"bytes"
	"unicode/utf8"
)

func EqBytes(v VarBytes, valid Validity, sel Sel, out Sel, rhs []byte) Sel {
	rows := v.Rows()
	if sel == nil {
		out = ensureSelLen(out, rows)
		n := 0
		if valid == nil {
			for i := 0; i < rows; i++ {
				if bytes.Equal(v.Bytes(i), rhs) {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i := 0; i < rows; i++ {
			if validBit(valid, i) && bytes.Equal(v.Bytes(i), rhs) {
				out[n] = Row(i)
				n++
			}
		}
		return out[:n]
	}
	out = ensureSelLen(out, len(sel))
	n := 0
	if valid == nil {
		for _, row := range sel {
			if bytes.Equal(v.Bytes(int(row)), rhs) {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && bytes.Equal(v.Bytes(i), rhs) {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

// PrefixBytes follows bytes.HasPrefix semantics; an empty prefix matches
// every valid row.
func PrefixBytes(v VarBytes, valid Validity, sel Sel, out Sel, prefix []byte) Sel {
	rows := v.Rows()
	if sel == nil {
		out = ensureSelLen(out, rows)
		n := 0
		if valid == nil {
			for i := 0; i < rows; i++ {
				if bytes.HasPrefix(v.Bytes(i), prefix) {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i := 0; i < rows; i++ {
			if validBit(valid, i) && bytes.HasPrefix(v.Bytes(i), prefix) {
				out[n] = Row(i)
				n++
			}
		}
		return out[:n]
	}
	out = ensureSelLen(out, len(sel))
	n := 0
	if valid == nil {
		for _, row := range sel {
			if bytes.HasPrefix(v.Bytes(int(row)), prefix) {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && bytes.HasPrefix(v.Bytes(i), prefix) {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func CompareBytes(v VarBytes, row int, rhs []byte) int {
	return bytes.Compare(v.Bytes(row), rhs)
}

func LengthBytes(v VarBytes, row int) int {
	return len(v.Bytes(row))
}

func LengthRunes(v VarBytes, row int) int {
	return utf8.RuneCount(v.Bytes(row))
}
