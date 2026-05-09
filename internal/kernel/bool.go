package kernel

import (
	mathbits "math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func IsTrue(bits []uint64, n int, valid vector.Validity, sel vector.Sel, out vector.Sel) vector.Sel {
	if sel == nil {
		if valid == nil {
			return isTrueAll(bits, n, out)
		}
		return isTrueValidAll(bits, n, valid, out)
	}
	if valid == nil {
		return isTrueSelected(bits, sel, out)
	}
	return isTrueSelectedValid(bits, valid, sel, out)
}

func isTrueAll(bits []uint64, n int, out vector.Sel) vector.Sel {
	out = ensureSelLen(out, n)
	rows := 0
	fullWords := n >> 6
	for wordIndex := 0; wordIndex < fullWords; wordIndex++ {
		rows = appendSetBits(out, rows, wordIndex, bits[wordIndex])
	}
	if rem := n & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		rows = appendSetBits(out, rows, fullWords, bits[fullWords]&mask)
	}
	return out[:rows]
}

func isTrueValidAll(bits []uint64, n int, valid vector.Validity, out vector.Sel) vector.Sel {
	out = ensureSelLen(out, n)
	rows := 0
	fullWords := n >> 6
	for wordIndex := 0; wordIndex < fullWords; wordIndex++ {
		rows = appendSetBits(out, rows, wordIndex, bits[wordIndex]&valid[wordIndex])
	}
	if rem := n & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		rows = appendSetBits(out, rows, fullWords, bits[fullWords]&valid[fullWords]&mask)
	}
	return out[:rows]
}

func isTrueSelected(bits []uint64, sel vector.Sel, out vector.Sel) vector.Sel {
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		if bits[i>>6]&(uint64(1)<<uint(i&63)) != 0 {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func isTrueSelectedValid(bits []uint64, valid vector.Validity, sel vector.Sel, out vector.Sel) vector.Sel {
	out = ensureSelLen(out, len(sel))
	n := 0
	for _, row := range sel {
		i := int(row)
		mask := uint64(1) << uint(i&63)
		if bits[i>>6]&mask != 0 && valid[i>>6]&mask != 0 {
			out[n] = row
			n++
		}
	}
	return out[:n]
}

func CountTrue(bits []uint64, n int, valid vector.Validity, sel vector.Sel) int {
	if sel == nil {
		if valid == nil {
			return countTrueAll(bits, n)
		}
		return countTrueValidAll(bits, n, valid)
	}
	return countTrueSelected(bits, valid, sel)
}

func countTrueAll(bits []uint64, n int) int {
	count := 0
	fullWords := n >> 6
	for i := 0; i < fullWords; i++ {
		count += mathbits.OnesCount64(bits[i])
	}
	if rem := n & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		count += mathbits.OnesCount64(bits[fullWords] & mask)
	}
	return count
}

func countTrueValidAll(bits []uint64, n int, valid vector.Validity) int {
	count := 0
	fullWords := n >> 6
	for i := 0; i < fullWords; i++ {
		count += mathbits.OnesCount64(bits[i] & valid[i])
	}
	if rem := n & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		count += mathbits.OnesCount64(bits[fullWords] & valid[fullWords] & mask)
	}
	return count
}

func countTrueSelected(bits []uint64, valid vector.Validity, sel vector.Sel) int {
	count := 0
	if valid == nil {
		for _, row := range sel {
			i := int(row)
			if bits[i>>6]&(uint64(1)<<uint(i&63)) != 0 {
				count++
			}
		}
		return count
	}
	for _, row := range sel {
		i := int(row)
		mask := uint64(1) << uint(i&63)
		if bits[i>>6]&mask != 0 && valid[i>>6]&mask != 0 {
			count++
		}
	}
	return count
}

func appendSetBits(out vector.Sel, rows int, wordIndex int, word uint64) int {
	for word != 0 {
		bit := mathbits.TrailingZeros64(word)
		out[rows] = vector.Row(wordIndex*64 + bit)
		rows++
		word &^= uint64(1) << uint(bit)
	}
	return rows
}

func AndBool(dst []uint64, left []uint64, right []uint64, n int) []uint64 {
	words := vector.ValidityWords(n)
	if cap(dst) < words {
		dst = make([]uint64, words)
	} else {
		dst = dst[:words]
	}
	for i := 0; i < words; i++ {
		dst[i] = left[i] & right[i]
	}
	// Keep the mask defensive for callers that pass unmasked scratch words.
	maskLastWord(dst, n)
	return dst
}

func OrBool(dst []uint64, left []uint64, right []uint64, n int) []uint64 {
	words := vector.ValidityWords(n)
	if cap(dst) < words {
		dst = make([]uint64, words)
	} else {
		dst = dst[:words]
	}
	for i := 0; i < words; i++ {
		dst[i] = left[i] | right[i]
	}
	maskLastWord(dst, n)
	return dst
}

func maskLastWord(bits []uint64, n int) {
	if rem := n & 63; rem != 0 && len(bits) > 0 {
		bits[len(bits)-1] &= (uint64(1) << uint(rem)) - 1
	}
}
