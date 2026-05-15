// Validity is a packed bitmap where a set bit means the row is non-null.
// nil Validity means every row is valid. Allocate only when nullCount > 0.
package types

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

type Validity []uint64

func ValidityWords(rows int) int {
	if rows <= 0 {
		return 0
	}
	return (rows + 63) / 64
}

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

func (v Validity) IsValid(row int) bool {
	if v == nil {
		return true
	}
	return v[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func (v Validity) SetValid(row int) {
	v[row>>6] |= uint64(1) << uint(row&63)
}

func (v Validity) SetInvalid(row int) {
	v[row>>6] &^= uint64(1) << uint(row&63)
}

func (v Validity) NullCount(rows int) int {
	if v == nil || rows == 0 {
		return 0
	}
	words := ValidityWords(rows)
	set := 0
	for _, word := range v[:words-1] {
		set += bits.OnesCount64(word)
	}
	tail := v[words-1]
	if rem := rows & 63; rem != 0 {
		tail &= (uint64(1) << uint(rem)) - 1
	}
	set += bits.OnesCount64(tail)
	return rows - set
}

func (v Validity) MaskTail(rows int) {
	if len(v) == 0 {
		return
	}
	if rem := rows & 63; rem != 0 {
		v[len(v)-1] &= (uint64(1) << uint(rem)) - 1
	}
}

func (v Validity) Clone() Validity {
	if v == nil {
		return nil
	}
	out := make(Validity, len(v))
	copy(out, v)
	return out
}

func (v Validity) MarshalLE(dst []byte) int {
	pos := 0
	for _, word := range v {
		binary.LittleEndian.PutUint64(dst[pos:pos+8], word)
		pos += 8
	}
	return pos
}

func UnmarshalValidity(src []byte, rows int, nullCount int, dst Validity) (Validity, int, error) {
	if nullCount < 0 || nullCount > rows {
		return nil, 0, fmt.Errorf("validity null count %d out of range [0, %d]", nullCount, rows)
	}
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
	out.MaskTail(rows)
	if got := out.NullCount(rows); got != nullCount {
		return nil, 0, fmt.Errorf("validity null count mismatch: payload says %d, decoded %d", nullCount, got)
	}
	return out, want, nil
}
