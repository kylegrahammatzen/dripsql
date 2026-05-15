// Vectorized filter kernels narrow a SelectionMask in place against col op lit.
// Word-stride loop over the input mask, no per-row closure or boxing.
package types

import (
	"bytes"
	"cmp"
	"math/bits"
)

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
	if op != FilterEqual && op != FilterNotEqual {
		panic("FilterBytes: only Equal / NotEqual supported")
	}
	if len(out.words) < len(in.words) {
		panic("FilterBytes: out mask too small")
	}
	out.rows = in.rows
	var total int
	rows := in.rows
	wantEqual := op == FilterEqual
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
		for i := 0; i < end; i++ {
			row := base + i
			match := bytes.Equal(col.Bytes(row), lit)
			if match == wantEqual {
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
		for i := 0; i < end; i++ {
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
		for i := 0; i < end; i++ {
			if col[base+i] == lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterNotEqual:
		for i := 0; i < end; i++ {
			if col[base+i] != lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterLess:
		for i := 0; i < end; i++ {
			if col[base+i] < lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterLessEqual:
		for i := 0; i < end; i++ {
			if col[base+i] <= lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterGreater:
		for i := 0; i < end; i++ {
			if col[base+i] > lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	case FilterGreaterEqual:
		for i := 0; i < end; i++ {
			if col[base+i] >= lit {
				passWord |= uint64(1) << uint(i)
			}
		}
	}
	return passWord
}
