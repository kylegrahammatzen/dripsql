package types

import (
	"fmt"
	"math/bits"
)

type SelectionMask struct {
	Words []uint64
	Rows  int
}

func NewSelectionMask(rows int) SelectionMask {
	if rows < 0 {
		panic(fmt.Sprintf("NewSelectionMask: negative rows %d", rows))
	}
	return SelectionMask{
		Words: make([]uint64, ValidityWords(rows)),
		Rows:  rows,
	}
}

func (m *SelectionMask) Resize(rows int) {
	if rows < 0 {
		panic(fmt.Sprintf("SelectionMask.Resize: negative rows %d", rows))
	}
	wordCount := ValidityWords(rows)
	if cap(m.Words) < wordCount {
		m.Words = make([]uint64, wordCount)
	} else {
		m.Words = m.Words[:wordCount]
		clear(m.Words)
	}
	m.Rows = rows
}

func (m *SelectionMask) Set(row int) {
	if uint(row) >= uint(m.Rows) {
		panic(fmt.Sprintf("SelectionMask.Set: row %d out of range [0,%d)", row, m.Rows))
	}
	m.SetUnsafe(row)
}

func (m *SelectionMask) SetUnsafe(row int) {
	m.Words[row>>6] |= uint64(1) << uint(row&63)
}

func (m *SelectionMask) Unset(row int) {
	if uint(row) >= uint(m.Rows) {
		panic(fmt.Sprintf("SelectionMask.Unset: row %d out of range [0,%d)", row, m.Rows))
	}
	m.Words[row>>6] &^= uint64(1) << uint(row&63)
}

func (m SelectionMask) IsSet(row int) bool {
	if uint(row) >= uint(m.Rows) {
		return false
	}
	return m.Words[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func (m *SelectionMask) Reset() {
	clear(m.Words)
}

func (m *SelectionMask) FillAll() int {
	for i := range m.Words {
		m.Words[i] = ^uint64(0)
	}
	m.maskTail()
	return m.Rows
}

func (m SelectionMask) PopCount() int {
	wordCount := ValidityWords(m.Rows)
	if wordCount == 0 {
		return 0
	}
	if len(m.Words) < wordCount {
		panic(fmt.Sprintf("SelectionMask.PopCount: %d words for %d rows", len(m.Words), m.Rows))
	}
	words := m.Words[:wordCount]
	last := len(words) - 1
	count := 0
	for _, w := range words[:last] {
		count += bits.OnesCount64(w)
	}
	count += bits.OnesCount64(words[last] & tailMask(m.Rows))
	return count
}

func (m *SelectionMask) And(other SelectionMask) {
	_ = m.AndCount(other)
}

func (m *SelectionMask) Or(other SelectionMask) {
	_ = m.OrCount(other)
}

func (m *SelectionMask) AndCount(other SelectionMask) int {
	m.checkCompatible(other)
	if len(m.Words) == 0 {
		return 0
	}
	last := len(m.Words) - 1
	count := 0
	for i := range m.Words {
		w := m.Words[i] & other.Words[i]
		if i == last {
			w &= tailMask(m.Rows)
		}
		m.Words[i] = w
		count += bits.OnesCount64(w)
	}
	return count
}

func (m *SelectionMask) OrCount(other SelectionMask) int {
	m.checkCompatible(other)
	if len(m.Words) == 0 {
		return 0
	}
	last := len(m.Words) - 1
	count := 0
	for i := range m.Words {
		w := m.Words[i] | other.Words[i]
		if i == last {
			w &= tailMask(m.Rows)
		}
		m.Words[i] = w
		count += bits.OnesCount64(w)
	}
	return count
}

func (m *SelectionMask) NotCount() int {
	if len(m.Words) == 0 {
		return 0
	}
	last := len(m.Words) - 1
	count := 0
	for i := range m.Words {
		w := ^m.Words[i]
		if i == last {
			w &= tailMask(m.Rows)
		}
		m.Words[i] = w
		count += bits.OnesCount64(w)
	}
	return count
}

func (m SelectionMask) IterSet(fn func(row int)) {
	wordCount := ValidityWords(m.Rows)
	if wordCount == 0 {
		return
	}
	if len(m.Words) < wordCount {
		panic(fmt.Sprintf("SelectionMask.IterSet: %d words for %d rows", len(m.Words), m.Rows))
	}
	words := m.Words[:wordCount]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= tailMask(m.Rows)
		}
		base := wordIdx << 6
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			fn(base + bit)
			word &= word - 1
		}
	}
}

func (m SelectionMask) AppendToSel(dst Sel) Sel {
	m.IterSet(func(row int) {
		dst = append(dst, Row(row))
	})
	return dst
}

func (m SelectionMask) ToSel() Sel {
	return m.AppendToSel(make(Sel, 0, m.PopCount()))
}

func (m SelectionMask) checkCompatible(other SelectionMask) {
	if m.Rows != other.Rows {
		panic(fmt.Sprintf("SelectionMask: row mismatch %d vs %d", m.Rows, other.Rows))
	}
	if len(m.Words) != len(other.Words) {
		panic(fmt.Sprintf("SelectionMask: word mismatch %d vs %d", len(m.Words), len(other.Words)))
	}

}

func (m *SelectionMask) maskTail() {
	if m.Rows == 0 {
		clear(m.Words)
		return
	}
	if rem := m.Rows & 63; rem != 0 && len(m.Words) != 0 {
		m.Words[len(m.Words)-1] &= tailMask(m.Rows)
	}
}

func tailMask(rows int) uint64 {
	if rem := rows & 63; rem != 0 {
		return (uint64(1) << uint(rem)) - 1
	}
	return ^uint64(0)
}
