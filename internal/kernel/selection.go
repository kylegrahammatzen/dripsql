package kernel

import "github.com/kylegrahammatzen/dripsql/internal/vector"

// ensureSelLen reserves room for up to n outputs while callers return the populated prefix.
func ensureSelLen(out vector.Sel, n int) vector.Sel {
	if cap(out) < n {
		return make(vector.Sel, n)
	}
	return out[:n]
}

func validBit(valid vector.Validity, row int) bool {
	return valid[row>>6]&(uint64(1)<<uint(row&63)) != 0
}
