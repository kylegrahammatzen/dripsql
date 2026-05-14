package types

import "math/bits"

// SelectionMask is a packed bitmap of selected rows with a cached allSet flag for the common all-rows path.
type SelectionMask struct {
	Words  []uint64
	Rows   int
	allSet bool
}

// NewSelectionMask returns an empty mask sized for rows rows.
func NewSelectionMask(rows int) SelectionMask {
	return SelectionMask{Words: make([]uint64, ValidityWords(rows)), Rows: rows}
}

// Resize prepares the mask for rows rows, reusing the backing slice when capacity allows.
func (s *SelectionMask) Resize(rows int) {
	words := ValidityWords(rows)
	if cap(s.Words) < words {
		s.Words = make([]uint64, words)
	} else {
		s.Words = s.Words[:words]
		for i := range s.Words {
			s.Words[i] = 0
		}
	}
	s.Rows = rows
	s.allSet = false
}

// FillAll marks every row selected and returns the count.
func (s *SelectionMask) FillAll() int {
	if s.Rows == 0 {
		s.allSet = true
		return 0
	}
	for i := range s.Words {
		s.Words[i] = ^uint64(0)
	}
	if rem := s.Rows & 63; rem != 0 {
		s.Words[len(s.Words)-1] = (uint64(1) << uint(rem)) - 1
	}
	s.allSet = true
	return s.Rows
}

// IsAllSet returns the cached all-rows-selected flag.
func (s SelectionMask) IsAllSet() bool { return s.allSet }

// Set marks row as selected and clears the allSet cache.
func (s *SelectionMask) Set(row int) {
	s.Words[row>>6] |= uint64(1) << uint(row&63)
	s.allSet = false
}

// SetUnsafe marks row as selected without touching the allSet cache; callers that maintain the invariant use this in tight loops.
func (s *SelectionMask) SetUnsafe(row int) {
	s.Words[row>>6] |= uint64(1) << uint(row&63)
}

// IsSet reports whether row is selected.
func (s SelectionMask) IsSet(row int) bool {
	return s.Words[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

// PopCount returns the number of selected rows.
func (s SelectionMask) PopCount() int {
	count := 0
	for _, word := range s.Words {
		count += bits.OnesCount64(word)
	}
	return count
}

// IterSet calls fn for each selected row in ascending order.
func (s SelectionMask) IterSet(fn func(row int)) {
	for w, word := range s.Words {
		base := w << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			row := base + bit
			if row >= s.Rows {
				return
			}
			fn(row)
			word &= word - 1
		}
	}
}

// AndCount intersects s with other in place and returns the new popcount.
func (s *SelectionMask) AndCount(other SelectionMask) int {
	count := 0
	for i := range s.Words {
		s.Words[i] &= other.Words[i]
		count += bits.OnesCount64(s.Words[i])
	}
	s.allSet = false
	return count
}

// OrCount unions s with other in place and returns the new popcount.
func (s *SelectionMask) OrCount(other SelectionMask) int {
	count := 0
	for i := range s.Words {
		s.Words[i] |= other.Words[i]
		count += bits.OnesCount64(s.Words[i])
	}
	if rem := s.Rows & 63; rem != 0 {
		mask := (uint64(1) << uint(rem)) - 1
		s.Words[len(s.Words)-1] &= mask
	}
	if count == s.Rows {
		s.allSet = true
	} else {
		s.allSet = false
	}
	return count
}

// NotCount inverts the mask in place and returns the new popcount.
func (s *SelectionMask) NotCount() int {
	count := 0
	last := len(s.Words) - 1
	for i := range s.Words {
		s.Words[i] = ^s.Words[i]
		if i == last {
			if rem := s.Rows & 63; rem != 0 {
				s.Words[i] &= (uint64(1) << uint(rem)) - 1
			}
		}
		count += bits.OnesCount64(s.Words[i])
	}
	if count == s.Rows {
		s.allSet = true
	} else {
		s.allSet = false
	}
	return count
}
