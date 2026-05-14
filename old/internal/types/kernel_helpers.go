package types

func ensureSelLen(out Sel, n int) Sel {
	if cap(out) < n {
		return make(Sel, n)
	}
	return out[:n]
}

func validBit(valid Validity, row int) bool {
	return valid[row>>6]&(uint64(1)<<uint(row&63)) != 0
}
