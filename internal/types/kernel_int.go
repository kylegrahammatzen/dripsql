package types

import "math/bits"

func EqInt64(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
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

func eqInt64All(x []int64, out Sel, rhs int64) Sel {
	out = ensureSelLen(out, len(x))
	n := 0
	for i, v := range x {
		if v == rhs {
			out[n] = Row(i)
			n++
		}
	}
	return out[:n]
}

func eqInt64ValidAll(x []int64, valid Validity, out Sel, rhs int64) Sel {
	out = ensureSelLen(out, len(x))
	n := 0
	for i, v := range x {
		if validBit(valid, i) && v == rhs {
			out[n] = Row(i)
			n++
		}
	}
	return out[:n]
}

func eqInt64Selected(x []int64, sel Sel, out Sel, rhs int64) Sel {
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

func eqInt64SelectedValid(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
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

func LtInt64(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		if valid == nil {
			for i, v := range x {
				if v < rhs {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i, v := range x {
			if validBit(valid, i) && v < rhs {
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
			if x[int(row)] < rhs {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] < rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func LteInt64(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		if valid == nil {
			for i, v := range x {
				if v <= rhs {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i, v := range x {
			if validBit(valid, i) && v <= rhs {
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
			if x[int(row)] <= rhs {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] <= rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func GtInt64(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		if valid == nil {
			for i, v := range x {
				if v > rhs {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i, v := range x {
			if validBit(valid, i) && v > rhs {
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
			if x[int(row)] > rhs {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] > rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func GteInt64(x []int64, valid Validity, sel Sel, out Sel, rhs int64) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		if valid == nil {
			for i, v := range x {
				if v >= rhs {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i, v := range x {
			if validBit(valid, i) && v >= rhs {
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
			if x[int(row)] >= rhs {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if validBit(valid, i) && x[i] >= rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func BetweenInt64(x []int64, valid Validity, sel Sel, out Sel, lo int64, hi int64) Sel {
	if lo > hi {
		return out[:0]
	}
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		if valid == nil {
			for i, v := range x {
				if lo <= v && v <= hi {
					out[n] = Row(i)
					n++
				}
			}
			return out[:n]
		}
		for i, v := range x {
			if validBit(valid, i) && lo <= v && v <= hi {
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
			v := x[int(row)]
			if lo <= v && v <= hi {
				out[n] = row
				n++
			}
		}
		return out[:n]
	}
	for _, row := range sel {
		i := int(row)
		if !validBit(valid, i) {
			continue
		}
		v := x[i]
		if lo <= v && v <= hi {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func MinMaxInt64(x []int64, valid Validity, sel Sel) (min int64, max int64, ok bool) {
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

func SumInt64(x []int64, valid Validity, sel Sel) (sum int64, count int, overflow bool) {
	if sel == nil {
		if valid == nil {
			for _, v := range x {
				next, overflow := addInt64Checked(sum, v)
				if overflow {
					return 0, count, true
				}
				sum = next
				count++
			}
			return sum, count, false
		}
		for i, v := range x {
			if !validBit(valid, i) {
				continue
			}
			next, overflow := addInt64Checked(sum, v)
			if overflow {
				return 0, count, true
			}
			sum = next
			count++
		}
		return sum, count, false
	}
	if valid == nil {
		for _, row := range sel {
			next, overflow := addInt64Checked(sum, x[int(row)])
			if overflow {
				return 0, count, true
			}
			sum = next
			count++
		}
		return sum, count, false
	}
	for _, row := range sel {
		i := int(row)
		if !validBit(valid, i) {
			continue
		}
		next, overflow := addInt64Checked(sum, x[i])
		if overflow {
			return 0, count, true
		}
		sum = next
		count++
	}
	return sum, count, false
}

func addInt64Checked(sum int64, v int64) (int64, bool) {
	next := sum + v
	if (v > 0 && next < sum) || (v < 0 && next > sum) {
		return 0, true
	}
	return next, false
}

func SumInt64Selected(x []int64, mask SelectionMask) (sum int64, count int, overflow bool) {
	limit := mask.Rows
	if len(x) < limit {
		limit = len(x)
	}
	totalWords := ValidityWords(limit)
	words := totalWords
	if len(mask.Words) < words {
		words = len(mask.Words)
	}
	for wordIdx := 0; wordIdx < words; wordIdx++ {
		word := mask.Words[wordIdx]
		if wordIdx == totalWords-1 {
			if rem := limit & 63; rem != 0 {
				word &= (uint64(1) << uint(rem)) - 1
			}
		}
		base := wordIdx * 64
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			v := x[base+bit]
			next, overflow := addInt64Checked(sum, v)
			if overflow {
				return 0, count, true
			}
			sum = next
			count++
			word &^= uint64(1) << uint(bit)
		}
	}
	return sum, count, false
}

func EqInt32(x []int32, valid Validity, sel Sel, out Sel, rhs int32) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		for i, v := range x {
			if (valid == nil || validBit(valid, i)) && v == rhs {
				out[n] = Row(i)
				n++
			}
		}
		return out[:n]
	}
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		if (valid == nil || validBit(valid, i)) && x[i] == rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func BetweenInt32(x []int32, valid Validity, sel Sel, out Sel, lo int32, hi int32) Sel {
	if lo > hi {
		return out[:0]
	}
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		for i, v := range x {
			if (valid == nil || validBit(valid, i)) && lo <= v && v <= hi {
				out[n] = Row(i)
				n++
			}
		}
		return out[:n]
	}
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		v := x[i]
		if (valid == nil || validBit(valid, i)) && lo <= v && v <= hi {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func EqInt16(x []int16, valid Validity, sel Sel, out Sel, rhs int16) Sel {
	if sel == nil {
		out = ensureSelLen(out, len(x))
		n := 0
		for i, v := range x {
			if (valid == nil || validBit(valid, i)) && v == rhs {
				out[n] = Row(i)
				n++
			}
		}
		return out[:n]
	}
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		if (valid == nil || validBit(valid, i)) && x[i] == rhs {
			out[n] = row
			n++
		}
	}
	return out[:n]
}
