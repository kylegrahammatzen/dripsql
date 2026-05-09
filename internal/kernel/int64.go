package kernel

import "github.com/kylegrahammatzen/dripsql/internal/vector"

func EqInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	if sel == nil {
		if valid == nil {
			return eqInt64All(x, out, rhs)
		}
		return eqInt64ValidAll(x, valid, out, rhs)
	}
	if valid == nil {
		return eqInt64Selected(x, sel, out, rhs)
	}
	return eqInt64SelectedValid(x, valid, sel, out, rhs)
}

func eqInt64All(x []int64, out vector.Sel, rhs int64) vector.Sel {
	out = ensureSelLen(out, len(x))
	n := 0
	for i, v := range x {
		if v == rhs {
			out[n] = vector.Row(i)
			n++
		}
	}
	return out[:n]
}

func eqInt64ValidAll(x []int64, valid vector.Validity, out vector.Sel, rhs int64) vector.Sel {
	out = ensureSelLen(out, len(x))
	n := 0
	for i, v := range x {
		if validBit(valid, i) && v == rhs {
			out[n] = vector.Row(i)
			n++
		}
	}
	return out[:n]
}

func eqInt64Selected(x []int64, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		if x[int(row)] == rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func eqInt64SelectedValid(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] == rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func LtInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if v < rhs {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && v < rhs {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if x[int(row)] < rhs {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] < rhs {
			out = append(out, row)
		}
	}
	return out
}

func LteInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if v <= rhs {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && v <= rhs {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if x[int(row)] <= rhs {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] <= rhs {
			out = append(out, row)
		}
	}
	return out
}

func GtInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if v > rhs {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && v > rhs {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if x[int(row)] > rhs {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] > rhs {
			out = append(out, row)
		}
	}
	return out
}

func GteInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, rhs int64) vector.Sel {
	out = out[:0]
	if sel == nil {
		if valid == nil {
			for i, v := range x {
				if v >= rhs {
					out = append(out, vector.Row(i))
				}
			}
			return out
		}
		for i, v := range x {
			if validBit(valid, i) && v >= rhs {
				out = append(out, vector.Row(i))
			}
		}
		return out
	}
	if valid == nil {
		for _, row := range sel {
			if x[int(row)] >= rhs {
				out = append(out, row)
			}
		}
		return out
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] >= rhs {
			out = append(out, row)
		}
	}
	return out
}

func BetweenInt64(x []int64, valid vector.Validity, sel vector.Sel, out vector.Sel, lo int64, hi int64) vector.Sel {
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

func MinMaxInt64(x []int64, valid vector.Validity, sel vector.Sel) (min int64, max int64, ok bool) {
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
