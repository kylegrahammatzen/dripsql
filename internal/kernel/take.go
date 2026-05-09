package kernel

import "github.com/kylegrahammatzen/dripsql/internal/vector"

func TakeInt64(dst []int64, values []int64, sel vector.Sel) []int64 {
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

func TakeFloat64(dst []float64, values []float64, sel vector.Sel) []float64 {
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

func TakeVarBytes(dst vector.VarBytes, values vector.VarBytes, sel vector.Sel) vector.VarBytes {
	rows := len(sel)
	if cap(dst.Offsets) < rows+1 {
		dst.Offsets = make([]uint32, rows+1)
	} else {
		dst.Offsets = dst.Offsets[:rows+1]
	}
	dst.Offsets[0] = 0
	dst.Data = dst.Data[:0]
	for i, row := range sel {
		dst.Data = append(dst.Data, values.Bytes(int(row))...)
		dst.Offsets[i+1] = uint32(len(dst.Data))
	}
	return dst
}
