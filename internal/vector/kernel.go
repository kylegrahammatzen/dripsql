// Typed kernels shared by predicate eval, sort comparators, aggregate accumulators, and mask-narrowing filters.
// Generic over cmp.Ordered so int16/int32/int64 share one implementation.
package vector

import (
	"bytes"
	"cmp"
	"fmt"
	"math/bits"
)

func CmpOrdered[T cmp.Ordered](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func CmpBytes(a, b []byte) int { return bytes.Compare(a, b) }

func CmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case a:
		return 1
	default:
		return -1
	}
}

type Integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

func AddInt[T Integer](a, b T) T { return a + b }
func SubInt[T Integer](a, b T) T { return a - b }
func MulInt[T Integer](a, b T) T { return a * b }

func DivInt[T Integer](a, b T) (T, error) {
	if b == 0 {
		return 0, fmt.Errorf("divide by zero")
	}
	return a / b, nil
}

func ModInt[T Integer](a, b T) (T, error) {
	if b == 0 {
		return 0, fmt.Errorf("mod by zero")
	}
	return a % b, nil
}

func AddFloat(a, b float64) float64 { return a + b }
func SubFloat(a, b float64) float64 { return a - b }
func MulFloat(a, b float64) float64 { return a * b }

func DivFloat(a, b float64) (float64, error) {
	if b == 0 {
		return 0, fmt.Errorf("divide by zero")
	}
	return a / b, nil
}

type FilterOp uint8

const (
	FilterEqual FilterOp = iota + 1
	FilterNotEqual
	FilterLess
	FilterLessEqual
	FilterGreater
	FilterGreaterEqual
)

func FilterOrdered[T cmp.Ordered](col []T, valid Validity, lit T, op FilterOp, in SelectionMask, out *SelectionMask) int {
	if len(out.words) < len(in.words) {
		panic("FilterOrdered: out mask too small")
	}
	out.rows = in.rows
	var total int
	rows := in.rows
	for wi, inWord := range in.words {
		if inWord == 0 {
			out.words[wi] = 0
			continue
		}
		base := wi << 6
		end := 64
		if base+end > rows {
			end = rows - base
		}
		var validWord uint64 = ^uint64(0)
		if valid != nil {
			validWord = valid[wi]
		}
		passWord := buildPassWord(col, lit, op, base, end)
		outWord := inWord & validWord & passWord
		out.words[wi] = outWord
		total += bits.OnesCount64(outWord)
	}
	out.allSet = total == rows
	return total
}

func FilterBytes(col *VarBytes, valid Validity, lit []byte, op FilterOp, in SelectionMask, out *SelectionMask) int {
	if len(out.words) < len(in.words) {
		panic("FilterBytes: out mask too small")
	}
	out.rows = in.rows
	var total int
	rows := in.rows
	for wi, inWord := range in.words {
		if inWord == 0 {
			out.words[wi] = 0
			continue
		}
		base := wi << 6
		end := 64
		if base+end > rows {
			end = rows - base
		}
		var validWord uint64 = ^uint64(0)
		if valid != nil {
			validWord = valid[wi]
		}
		var passWord uint64
		for i := range end {
			row := base + i
			if filterBytesRow(col.Bytes(row), lit, op) {
				passWord |= uint64(1) << uint(i)
			}
		}
		outWord := inWord & validWord & passWord
		out.words[wi] = outWord
		total += bits.OnesCount64(outWord)
	}
	out.allSet = total == rows
	return total
}

func filterBytesRow(row, lit []byte, op FilterOp) bool {
	switch op {
	case FilterEqual:
		return bytes.Equal(row, lit)
	case FilterNotEqual:
		return !bytes.Equal(row, lit)
	case FilterLess:
		return bytes.Compare(row, lit) < 0
	case FilterLessEqual:
		return bytes.Compare(row, lit) <= 0
	case FilterGreater:
		return bytes.Compare(row, lit) > 0
	case FilterGreaterEqual:
		return bytes.Compare(row, lit) >= 0
	}
	return false
}

func BetweenOrdered[T cmp.Ordered](col []T, valid Validity, lo, hi T, in SelectionMask, out *SelectionMask) int {
	if len(out.words) < len(in.words) {
		panic("BetweenOrdered: out mask too small")
	}
	out.rows = in.rows
	var total int
	rows := in.rows
	for wi, inWord := range in.words {
		if inWord == 0 {
			out.words[wi] = 0
			continue
		}
		base := wi << 6
		end := 64
		if base+end > rows {
			end = rows - base
		}
		var validWord uint64 = ^uint64(0)
		if valid != nil {
			validWord = valid[wi]
		}
		var passWord uint64
		for i := range end {
			v := col[base+i]
			if v >= lo && v <= hi {
				passWord |= uint64(1) << uint(i)
			}
		}
		outWord := inWord & validWord & passWord
		out.words[wi] = outWord
		total += bits.OnesCount64(outWord)
	}
	out.allSet = total == rows
	return total
}

func buildPassWord[T cmp.Ordered](col []T, lit T, op FilterOp, base, end int) uint64 {
	var passWord uint64
	switch op {
	case FilterEqual:
		for i := range end {
			if col[base+i] == lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterNotEqual:
		for i := range end {
			if col[base+i] != lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterLess:
		for i := range end {
			if col[base+i] < lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterLessEqual:
		for i := range end {
			if col[base+i] <= lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterGreater:
		for i := range end {
			if col[base+i] > lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterGreaterEqual:
		for i := range end {
			if col[base+i] >= lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	}
	return passWord
}
