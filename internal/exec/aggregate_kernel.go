package exec

import (
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func countValidSelected(valid types.Validity, sel types.SelectionMask) int64 {
	if sel.Rows == 0 {
		return 0
	}
	if valid == nil {
		return int64(sel.PopCount())
	}
	if selectionAll(sel) {
		return int64(types.ValidCount(valid, sel.Rows))
	}
	var count int64
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= aggregateTailMask(sel.Rows)
		}
		base := wordIdx << 6
		for word != 0 {
			row := base + bits.TrailingZeros64(word)
			if types.IsValid(valid, row) {
				count++
			}
			word &= word - 1
		}
	}
	return count
}

func sumInt64Selected(values []int64, valid types.Validity, sel types.SelectionMask, base int64) (int64, int64, bool) {
	sum := base
	var count int64
	if selectionAll(sel) {
		if valid == nil {
			for row := 0; row < sel.Rows; row++ {
				next, ok := AddInt64(sum, values[row])
				if !ok {
					return 0, 0, true
				}
				sum = next
				count++
			}
			return sum, count, false
		}
		for row := 0; row < sel.Rows; row++ {
			if !types.IsValid(valid, row) {
				continue
			}
			next, ok := AddInt64(sum, values[row])
			if !ok {
				return 0, 0, true
			}
			sum = next
			count++
		}
		return sum, count, false
	}
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= aggregateTailMask(sel.Rows)
		}
		baseRow := wordIdx << 6
		for word != 0 {
			row := baseRow + bits.TrailingZeros64(word)
			if types.IsValid(valid, row) {
				next, ok := AddInt64(sum, values[row])
				if !ok {
					return 0, 0, true
				}
				sum = next
				count++
			}
			word &= word - 1
		}
	}
	return sum, count, false
}

func sumInt32Selected(values []int32, valid types.Validity, sel types.SelectionMask) (int64, int64) {
	var sum int64
	var count int64
	if selectionAll(sel) {
		if valid == nil {
			for row := 0; row < sel.Rows; row++ {
				sum += int64(values[row])
				count++
			}
			return sum, count
		}
		for row := 0; row < sel.Rows; row++ {
			if types.IsValid(valid, row) {
				sum += int64(values[row])
				count++
			}
		}
		return sum, count
	}
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= aggregateTailMask(sel.Rows)
		}
		base := wordIdx << 6
		for word != 0 {
			row := base + bits.TrailingZeros64(word)
			if types.IsValid(valid, row) {
				sum += int64(values[row])
				count++
			}
			word &= word - 1
		}
	}
	return sum, count
}

func minMaxInt64Selected(values []int64, valid types.Validity, sel types.SelectionMask, min bool) (int64, bool) {
	var out int64
	set := false
	if selectionAll(sel) {
		for row := 0; row < sel.Rows; row++ {
			if !types.IsValid(valid, row) {
				continue
			}
			value := values[row]
			if !set || (min && value < out) || (!min && value > out) {
				out = value
				set = true
			}
		}
		return out, set
	}
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= aggregateTailMask(sel.Rows)
		}
		base := wordIdx << 6
		for word != 0 {
			row := base + bits.TrailingZeros64(word)
			if types.IsValid(valid, row) {
				value := values[row]
				if !set || (min && value < out) || (!min && value > out) {
					out = value
					set = true
				}
			}
			word &= word - 1
		}
	}
	return out, set
}

func groupStringCountSelected(v types.Vec, sel types.SelectionMask, counts map[string]int64) error {
	switch v.Encoding {
	case types.EncodingFlat:
		groupFlatStringCountSelected(v, sel, counts)
		return nil
	case types.EncodingDictionary:
		return groupDictStringCountSelected(v, sel, counts)
	default:
		return fmt.Errorf("group string unsupported encoding %s", v.Encoding)
	}
}

func groupFlatStringCountSelected(v types.Vec, sel types.SelectionMask, counts map[string]int64) {
	if selectionAll(sel) {
		for row := 0; row < sel.Rows; row++ {
			if types.IsValid(v.Valid, row) {
				addFlatStringCount(counts, v, row, 1)
			}
		}
		return
	}
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	for wordIdx, word := range words {
		if wordIdx == last {
			word &= aggregateTailMask(sel.Rows)
		}
		base := wordIdx << 6
		for word != 0 {
			row := base + bits.TrailingZeros64(word)
			if types.IsValid(v.Valid, row) {
				addFlatStringCount(counts, v, row, 1)
			}
			word &= word - 1
		}
	}
}

func groupDictStringCountSelected(v types.Vec, sel types.SelectionMask, counts map[string]int64) error {
	if v.Encoded == nil {
		return fmt.Errorf("dictionary vector missing encoded state")
	}
	enc := v.Encoded
	if len(enc.DictIDs) < v.Len {
		return fmt.Errorf("dictionary ids length %d is shorter than rows %d", len(enc.DictIDs), v.Len)
	}
	var idCounts [256]int64
	if selectionAll(sel) {
		for row := 0; row < sel.Rows; row++ {
			if types.IsValid(v.Valid, row) {
				idCounts[enc.DictIDs[row]]++
			}
		}
	} else {
		words := sel.Words[:types.ValidityWords(sel.Rows)]
		last := len(words) - 1
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					idCounts[enc.DictIDs[row]]++
				}
				word &= word - 1
			}
		}
	}
	for id, count := range idCounts {
		if count == 0 {
			continue
		}
		if id >= enc.DictValues.Rows() {
			return fmt.Errorf("dictionary id %d exceeds dictionary size %d", id, enc.DictValues.Rows())
		}
		addDictStringCount(counts, enc.DictValues, id, count)
	}
	return nil
}

func groupAnyCountSelected(v types.Vec, sel types.SelectionMask, counts map[GroupKey]int64) error {
	if selectionAll(sel) {
		return groupAnyCountAll(v, sel.Rows, counts)
	}
	words := sel.Words[:types.ValidityWords(sel.Rows)]
	last := len(words) - 1
	switch v.Kind {
	case types.VecBool:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, Bool: v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0}]++
				}
				word &= word - 1
			}
		}
	case types.VecInt16:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, I64: int64(v.I16[row])}]++
				}
				word &= word - 1
			}
		}
	case types.VecInt32, types.VecDate:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, I64: int64(v.I32[row])}]++
				}
				word &= word - 1
			}
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, I64: v.I64[row]}]++
				}
				word &= word - 1
			}
		}
	case types.VecUUID:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, UUID: v.UUID[row]}]++
				}
				word &= word - 1
			}
		}
	case types.VecEnum32:
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, U32: v.U32[row]}]++
				}
				word &= word - 1
			}
		}
	case types.VecBytes:
		if v.Encoding != types.EncodingFlat {
			return fmt.Errorf("group bytes unsupported encoding %s", v.Encoding)
		}
		for wordIdx, word := range words {
			if wordIdx == last {
				word &= aggregateTailMask(sel.Rows)
			}
			base := wordIdx << 6
			for word != 0 {
				row := base + bits.TrailingZeros64(word)
				if types.IsValid(v.Valid, row) {
					counts[GroupKey{Kind: v.Kind, Bytes: v.Var.StringCopy(row)}]++
				}
				word &= word - 1
			}
		}
	default:
		return fmt.Errorf("unsupported group kind %s", v.Kind)
	}
	return nil
}

func addFlatStringCount(counts map[string]int64, v types.Vec, row int, count int64) {
	value := v.Var.String(row)
	if _, ok := counts[value]; ok {
		counts[value] += count
		return
	}
	counts[v.Var.StringCopy(row)] += count
}

func addDictStringCount(counts map[string]int64, values types.VarBytes, row int, count int64) {
	value := values.String(row)
	if _, ok := counts[value]; ok {
		counts[value] += count
		return
	}
	counts[values.StringCopy(row)] += count
}

func groupAnyCountAll(v types.Vec, rows int, counts map[GroupKey]int64) error {
	switch v.Kind {
	case types.VecBool:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, Bool: v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0}]++
			}
		}
	case types.VecInt16:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, I64: int64(v.I16[row])}]++
			}
		}
	case types.VecInt32, types.VecDate:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, I64: int64(v.I32[row])}]++
			}
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, I64: v.I64[row]}]++
			}
		}
	case types.VecUUID:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, UUID: v.UUID[row]}]++
			}
		}
	case types.VecEnum32:
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, U32: v.U32[row]}]++
			}
		}
	case types.VecBytes:
		if v.Encoding != types.EncodingFlat {
			return fmt.Errorf("group bytes unsupported encoding %s", v.Encoding)
		}
		for row := 0; row < rows; row++ {
			if types.IsValid(v.Valid, row) {
				counts[GroupKey{Kind: v.Kind, Bytes: v.Var.StringCopy(row)}]++
			}
		}
	default:
		return fmt.Errorf("unsupported group kind %s", v.Kind)
	}
	return nil
}

func selectionAll(sel types.SelectionMask) bool {
	wordCount := types.ValidityWords(sel.Rows)
	if wordCount == 0 {
		return true
	}
	if len(sel.Words) < wordCount {
		return false
	}
	words := sel.Words[:wordCount]
	last := len(words) - 1
	for i, word := range words {
		want := ^uint64(0)
		if i == last {
			want = aggregateTailMask(sel.Rows)
		}
		if word != want {
			return false
		}
	}
	return true
}

func aggregateTailMask(rows int) uint64 {
	if rem := rows & 63; rem != 0 {
		return (uint64(1) << uint(rem)) - 1
	}
	return ^uint64(0)
}
