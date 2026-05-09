package kernel

import "github.com/kylegrahammatzen/dripsql/internal/vector"

// Float64 kernels mirror int64.go's dispatch shape; keep behavior fixes in sync.

// EqFloat64 uses Go equality semantics, so NaN never matches, including a NaN rhs.
func EqFloat64(x []float64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs float64) vector.Sel {
	if rhs != rhs {
		return out[:0]
	}
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if v == rhs {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && v == rhs {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if x[int(row)] == rhs {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if !validBit(valid, i) {
			continue
		}
		if x[i] == rhs {
			out = append(out, row)
		}
	}
	return out
}

func BetweenFloat64(x []float64, valid vector.Validity, sel vector.Sel, out vector.Sel, lo float64, hi float64) vector.Sel {
	if lo != lo || hi != hi || lo > hi {
		return out[:0]
	}
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if lo <= v && v <= hi {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && lo <= v && v <= hi {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			v := x[int(row)]
			if lo <= v && v <= hi {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if !validBit(valid, i) {
			continue
		}
		v := x[i]
		if lo <= v && v <= hi {
			out = append(out, row)
		}
	}
	return out
}

// MinMaxFloat64 uses Go comparisons where a leading NaN remains the result and later NaNs do not replace established min/max values.
func MinMaxFloat64(x []float64, valid vector.Validity, sel vector.Sel) (min float64, max float64, ok bool) {
	if sel == nil {
		if valid == nil {
			if len(x) == 0 {
				return 0, 0, false
			}
			min, max = x[0], x[0]
			for _, v := range x[1:] {
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
			}
			return min, max, true
		}
		for i, v := range x {
			if !validBit(valid, i) {
				continue
			}
			if !ok {
				min, max, ok = v, v, true
				continue
			}
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		return min, max, ok
	}
	if valid == nil {
		for _, row := range sel {
			v := x[int(row)]
			if !ok {
				min, max, ok = v, v, true
				continue
			}
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		return min, max, ok
	}
	for _, row := range sel {
		i := int(row)
		if !validBit(valid, i) {
			continue
		}
		v := x[i]
		if !ok {
			min, max, ok = v, v, true
			continue
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max, ok
}

// SumFloat64 expects x to be sliced to the represented row count because nil validity returns len(x) as count.
func SumFloat64(x []float64, valid vector.Validity, sel vector.Sel) (sum float64, count int) {
	if sel == nil {
		if valid == nil {
			for _, v := range x {
				sum += v
			}
			return sum, len(x)
		}
		for i, v := range x {
			if validBit(valid, i) {
				sum += v
				count++
			}
		}
		return sum, count
	}
	if valid == nil {
		for _, row := range sel {
			sum += x[int(row)]
		}
		return sum, len(sel)
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) {
			sum += x[i]
			count++
		}
	}
	return sum, count
}
