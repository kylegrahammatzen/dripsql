package types

import "math"

func TakeInt64(dst []int64, values []int64, sel Sel) []int64 {
	if cap(dst) < len(sel) {
		dst = make([]int64, len(sel))
	} else {
		dst = dst[:len(sel)]
	}
	for i, row := range sel {
		dst[i] = values[int(row)]
	}
	return dst
}

func TakeFloat64(dst []float64, values []float64, sel Sel) []float64 {
	if cap(dst) < len(sel) {
		dst = make([]float64, len(sel))
	} else {
		dst = dst[:len(sel)]
	}
	for i, row := range sel {
		dst[i] = values[int(row)]
	}
	return dst
}

// TakeVarBytes copies selected rows into dst. dst must not alias values.
func TakeVarBytes(dst VarBytes, values VarBytes, sel Sel) VarBytes {
	rows := len(sel)
	if cap(dst.Offsets) < rows+1 {
		dst.Offsets = make([]uint32, rows+1)
	} else {
		dst.Offsets = dst.Offsets[:rows+1]
	}
	dst.Offsets[0] = 0
	total := 0
	for _, row := range sel {
		total += len(values.Bytes(int(row)))
		if total > math.MaxUint32 {
			panic("VarBytes data exceeds uint32 offset capacity")
		}
	}
	if cap(dst.Data) < total {
		dst.Data = make([]byte, total)
	} else {
		dst.Data = dst.Data[:total]
	}
	pos := 0
	for i, row := range sel {
		pos += copy(dst.Data[pos:], values.Bytes(int(row)))
		dst.Offsets[i+1] = uint32(pos)
	}
	return dst
}
