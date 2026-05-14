package types

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Validity is a packed bitmap where a set bit means the row is non-null; nil means every row is valid.
type Validity []uint64

// ValidityWords returns how many uint64 words a packed bitmap for rows rows needs.
func ValidityWords(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + 63) / 64
}

// NewValidity allocates a bitmap with every row marked valid.
func NewValidity(rows int) Validity {
	words := ValidityWords(rows)
	v := make(Validity, words)
	for i := range v {
		v[i] = ^uint64(0)
	}
	if rem := rows & 63; rem != 0 {
		v[words-1] = (uint64(1) << uint(rem)) - 1
	}
	return v
}

// IsValid reports whether row is valid, treating nil Validity as all-valid.
func IsValid(v Validity, row int) bool {
	if v == nil {
		return true
	}
	return v[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

// SetValid marks row as valid.
func SetValid(v Validity, row int) {
	v[row>>6] |= uint64(1) << uint(row&63)
}

// SetInvalid marks row as null.
func SetInvalid(v Validity, row int) {
	v[row>>6] &^= uint64(1) << uint(row&63)
}

// NullCount returns the number of null rows in v.
func NullCount(v Validity, rows int) int {
	if v == nil || rows == 0 {
		return 0
	}
	words := ValidityWords(rows)
	set := 0
	for i := 0; i < words-1; i++ {
		set += bits.OnesCount64(v[i])
	}
	tail := v[words-1]
	if rem := rows & 63; rem != 0 {
		tail &= (uint64(1) << uint(rem)) - 1
	}
	set += bits.OnesCount64(tail)
	return rows - set
}

// MarshalLE writes v's packed words to dst in little-endian and returns the byte count.
func (v Validity) MarshalLE(dst []byte) int {
	pos := 0
	for _, word := range v {
		binary.LittleEndian.PutUint64(dst[pos:pos+8], word)
		pos += 8
	}
	return pos
}

// UnmarshalValidity reads a packed bitmap and returns nil when nullCount is zero.
func UnmarshalValidity(src []byte, rows int, nullCount int, dst Validity) (Validity, int, error) {
	if nullCount == 0 {
		return nil, 0, nil
	}
	words := ValidityWords(rows)
	want := words * 8
	if len(src) < want {
		return nil, 0, fmt.Errorf("validity payload truncated: have %d bytes, need %d", len(src), want)
	}
	out := dst
	if cap(out) < words {
		out = make(Validity, words)
	} else {
		out = out[:words]
	}
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(src[i*8 : i*8+8])
	}
	return out, want, nil
}
