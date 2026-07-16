// SelectionMask is a packed row-selection bitmap.
// Tail-bit invariant: bits beyond rows are always zero, maintained by FillAll/NotCount/Resize/Clear so the other ops can trust it.
package vector

import "math/bits"

type SelectionMask struct {
	words  []uint64
	rows   int
	allSet bool
}

func NewSelectionMask(rows int) SelectionMask {
	return SelectionMask{
		words:  make([]uint64, ValidityWords(rows)),
		rows:   rows,
		allSet: rows == 0,
	}
}

func (s *SelectionMask) Resize(rows int) {
	words := ValidityWords(rows)
	if cap(s.words) < words {
		s.words = make([]uint64, words)
	} else {
		s.words = s.words[:words]
		clear(s.words)
	}
	s.rows = rows
	s.allSet = rows == 0
}

func (s SelectionMask) Rows() int { return s.rows }

// CopyFrom reuses s's word buffer when large enough so recycled masks avoid reallocating.
func (s *SelectionMask) CopyFrom(other *SelectionMask) {
	s.Resize(other.rows)
	copy(s.words, other.words)
	s.allSet = other.allSet
}

func (s SelectionMask) IsAllSet() bool { return s.allSet }

func (s *SelectionMask) FillAll() int {
	if s.rows == 0 {
		s.allSet = true
		return 0
	}
	for i := range s.words {
		s.words[i] = ^uint64(0)
	}
	if rem := s.rows & 63; rem != 0 {
		s.words[len(s.words)-1] = (uint64(1) << uint(rem)) - 1
	}
	s.allSet = true
	return s.rows
}

func (s *SelectionMask) Clear() {
	clear(s.words)
	s.allSet = s.rows == 0
}

func (s *SelectionMask) Set(row int) {
	s.words[row>>6] |= uint64(1) << uint(row&63)
	s.allSet = false
}

func (s *SelectionMask) Unset(row int) {
	s.words[row>>6] &^= uint64(1) << uint(row&63)
	s.allSet = false
}

// SetRange sets bits [start, end). Clamps to [0, rows). Middle words are filled at once
// so contiguous range marking is cheap for large selections.
func (s *SelectionMask) SetRange(start, end int) {
	if start < 0 {
		start = 0
	}
	if end > s.rows {
		end = s.rows
	}
	if start >= end {
		return
	}
	firstWord := start >> 6
	lastWord := (end - 1) >> 6
	firstBit := uint(start & 63)
	lastBit := uint((end - 1) & 63)
	if firstWord == lastWord {
		mask := (^uint64(0) << firstBit) & (^uint64(0) >> (63 - lastBit))
		s.words[firstWord] |= mask
	} else {
		s.words[firstWord] |= ^uint64(0) << firstBit
		for wi := firstWord + 1; wi < lastWord; wi++ {
			s.words[wi] = ^uint64(0)
		}
		s.words[lastWord] |= ^uint64(0) >> (63 - lastBit)
	}
	if !(start == 0 && end == s.rows) {
		s.allSet = false
	}
}

func NewSelectionRange(rows, start, end int) SelectionMask {
	out := NewSelectionMask(rows)
	out.SetRange(start, end)
	return out
}

// Limit walks set rows once, skipping the first `skip` and emitting up to `take` into a
// fresh mask. `take < 0` means unlimited. Returns the new mask plus visible-row counts
// (seen, sent) so callers avoid a second PopCount pass.
func (s SelectionMask) Limit(rows int, skip int64, take int64) (SelectionMask, int64, int64) {
	out := NewSelectionMask(rows)
	var seen, sent int64
	s.IterSet(func(row int) {
		if skip > 0 {
			skip--
			seen++
			return
		}
		if take >= 0 && sent >= take {
			return
		}
		out.Set(row)
		seen++
		sent++
	})
	return out, seen, sent
}

func (s SelectionMask) IsSet(row int) bool {
	return s.words[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func (s SelectionMask) PopCount() int {
	count := 0
	for _, word := range s.words {
		count += bits.OnesCount64(word)
	}
	return count
}

func (s SelectionMask) IterSet(fn func(row int)) {
	for w, word := range s.words {
		base := w << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := base + bit
			if row >= s.rows {
				return
			}
			fn(row)
			word &= word - 1
		}
	}
}

// AndCount requires equal-shape inputs from the caller and does not re-mask the tail since AND cannot introduce 1-bits the inputs lacked.
func (s *SelectionMask) AndCount(other SelectionMask) int {
	count := 0
	for i := range s.words {
		s.words[i] &= other.words[i]
		count += bits.OnesCount64(s.words[i])
	}
	s.allSet = count == s.rows
	return count
}

// AndNotCount clears rows of s that are set in other and returns the remaining count.
func (s *SelectionMask) AndNotCount(other SelectionMask) int {
	count := 0
	for i := range s.words {
		s.words[i] &^= other.words[i]
		count += bits.OnesCount64(s.words[i])
	}
	s.allSet = count == s.rows
	return count
}

// AndNotValidity keeps only rows whose validity bit is zero.
// A nil validity means every row is valid so the mask clears to empty.
func (s *SelectionMask) AndNotValidity(v Validity) {
	if v == nil {
		s.Clear()
		return
	}
	for i := range s.words {
		s.words[i] &^= v[i]
	}
	s.allSet = false
}

// AndValidity masks s by a Validity bitmap using word-level AND.
// A nil validity means all rows are valid and is a no-op.
func (s *SelectionMask) AndValidity(v Validity) {
	if v == nil {
		return
	}
	for i := range s.words {
		s.words[i] &= v[i]
	}
	s.allSet = false
}

// OrCount requires equal-shape inputs from the caller.
func (s *SelectionMask) OrCount(other SelectionMask) int {
	count := 0
	for i := range s.words {
		s.words[i] |= other.words[i]
		count += bits.OnesCount64(s.words[i])
	}
	s.allSet = count == s.rows
	return count
}

func (s *SelectionMask) NotCount() int {
	count := 0
	last := len(s.words) - 1
	for i := range s.words {
		s.words[i] = ^s.words[i]
		if i == last {
			if rem := s.rows & 63; rem != 0 {
				s.words[i] &= (uint64(1) << uint(rem)) - 1
			}
		}
		count += bits.OnesCount64(s.words[i])
	}
	s.allSet = count == s.rows
	return count
}
