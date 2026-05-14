package types

import "math/bits"

// Validity is a row-validity bitmap where nil means every row is valid.
type Validity []uint64

// ValidityWords returns the number of bitmap words needed for n rows and
// returns zero for non-positive n.
func ValidityWords(n int) int {
	if n <= 0 {
		return 0
	}
	return (n + 63) >> 6
}

func NewValidity(n int) Validity {
	valid := make(Validity, ValidityWords(n))
	FillValid(valid, n)
	return valid
}

// FillValid marks n rows valid and expects valid to have exactly
// ValidityWords(n) words.
func FillValid(valid Validity, n int) {
	for i := range valid {
		valid[i] = ^uint64(0)
	}
	if rem := n & 63; rem != 0 && len(valid) != 0 {
		valid[len(valid)-1] = (uint64(1) << uint(rem)) - 1
	}
}

func (valid Validity) IsAllValid() bool {
	return valid == nil
}

func IsValid(valid Validity, row int) bool {
	return valid == nil || valid[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func SetValid(valid Validity, row int) {
	valid[row>>6] |= uint64(1) << uint(row&63)
}

func SetInvalid(valid Validity, row int) {
	valid[row>>6] &^= uint64(1) << uint(row&63)
}

func ValidCount(valid Validity, n int) int {
	return n - NullCount(valid, n)
}

func NullCount(valid Validity, n int) int {
	if valid == nil || n <= 0 {
		return 0
	}
	ones := 0
	full := n >> 6
	for i := range full {
		ones += bits.OnesCount64(valid[i])
	}
	if rem := n & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		ones += bits.OnesCount64(valid[full] & mask)
	}
	return n - ones
}
