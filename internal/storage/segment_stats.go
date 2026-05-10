package storage

import (
	"math"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func applyPageStats(page *PageMeta, v types.Vec) {
	page.AllValid = page.NullCount == 0
	page.AllNull = page.NullCount == page.Rows
	switch v.Kind {
	case types.VecBool:
		page.Bool = boolStats(v.BoolBits, v.Len, v.Valid)
	case types.VecInt16:
		page.Int32 = int16Stats(v.I16[:v.Len], v.Valid)
		page.Int32Values = int16ValueStats(v.I16[:v.Len], v.Valid)
	case types.VecInt32, types.VecDate:
		page.Int32 = int32Stats(v.I32[:v.Len], v.Valid)
		page.Int32Values = int32ValueStats(v.I32[:v.Len], v.Valid)
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		page.Int64 = int64Stats(v.I64[:v.Len], v.Valid)
		page.Int64Values = int64ValueStats(v.I64[:v.Len], v.Valid)
	case types.VecText, types.VecBytes, types.VecJSON:
		page.Text = textStats(v.Var, v.Valid)
	}
}

func boolStats(values []uint64, rows int, valid types.Validity) *BoolStats {
	var out BoolStats
	for row := 0; row < rows; row++ {
		if !types.IsValid(valid, row) {
			continue
		}
		if values[row>>6]&(uint64(1)<<uint(row&63)) != 0 {
			out.HasTrue = true
		} else {
			out.HasFalse = true
		}
		if out.HasTrue && out.HasFalse {
			return &out
		}
	}
	if !out.HasTrue && !out.HasFalse {
		return nil
	}
	return &out
}

func int16Stats(values []int16, valid types.Validity) *Int32Stats {
	out := Int32Stats{SumValid: true}
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		v := int32(value)
		out.Sum += int64(v)
		if !ok {
			out.Min, out.Max, ok = v, v, true
			continue
		}
		if v < out.Min {
			out.Min = v
		}
		if v > out.Max {
			out.Max = v
		}
	}
	if !ok {
		return nil
	}
	return &out
}

func int32Stats(values []int32, valid types.Validity) *Int32Stats {
	out := Int32Stats{SumValid: true}
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		out.Sum += int64(value)
		if !ok {
			out.Min, out.Max, ok = value, value, true
			continue
		}
		if value < out.Min {
			out.Min = value
		}
		if value > out.Max {
			out.Max = value
		}
	}
	if !ok {
		return nil
	}
	return &out
}

func int64Stats(values []int64, valid types.Validity) *Int64Stats {
	out := Int64Stats{SumValid: true}
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		if out.SumValid {
			if sum, ok := addInt64Stat(out.Sum, value); ok {
				out.Sum = sum
			} else {
				out.SumValid = false
			}
		}
		if !ok {
			out.Min, out.Max, ok = value, value, true
			continue
		}
		if value < out.Min {
			out.Min = value
		}
		if value > out.Max {
			out.Max = value
		}
	}
	if !ok {
		return nil
	}
	return &out
}

func int16ValueStats(values []int16, valid types.Validity) *Int32ValueStats {
	out := &Int32ValueStats{Values: make([]int32, 0, min(len(values), ValueStatsMaxValues))}
	seen := make(map[int32]struct{}, min(len(values), ValueStatsMaxValues))
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		ok = true
		addInt32ValueStat(out, seen, int32(value))
	}
	if !ok {
		return nil
	}
	return out
}

func int32ValueStats(values []int32, valid types.Validity) *Int32ValueStats {
	out := &Int32ValueStats{Values: make([]int32, 0, min(len(values), ValueStatsMaxValues))}
	seen := make(map[int32]struct{}, min(len(values), ValueStatsMaxValues))
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		ok = true
		addInt32ValueStat(out, seen, value)
	}
	if !ok {
		return nil
	}
	return out
}

func int64ValueStats(values []int64, valid types.Validity) *Int64ValueStats {
	out := &Int64ValueStats{Values: make([]int64, 0, min(len(values), ValueStatsMaxValues))}
	seen := make(map[int64]struct{}, min(len(values), ValueStatsMaxValues))
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		ok = true
		addInt64ValueStat(out, seen, value)
	}
	if !ok {
		return nil
	}
	return out
}

func addInt32ValueStat(out *Int32ValueStats, seen map[int32]struct{}, value int32) {
	if _, ok := seen[value]; ok {
		return
	}
	if len(out.Values) >= ValueStatsMaxValues {
		out.Truncated = true
		return
	}
	seen[value] = struct{}{}
	out.Values = append(out.Values, value)
}

func addInt64ValueStat(out *Int64ValueStats, seen map[int64]struct{}, value int64) {
	if _, ok := seen[value]; ok {
		return
	}
	if len(out.Values) >= ValueStatsMaxValues {
		out.Truncated = true
		return
	}
	seen[value] = struct{}{}
	out.Values = append(out.Values, value)
}

func addInt64Stat(left int64, right int64) (int64, bool) {
	if (right > 0 && left > math.MaxInt64-right) || (right < 0 && left < math.MinInt64-right) {
		return 0, false
	}
	return left + right, true
}

func textStats(values types.VarBytes, valid types.Validity) *TextStats {
	out := &TextStats{Values: make([]string, 0, min(values.Rows(), TextStatsMaxValues)), Counts: make([]uint32, 0, min(values.Rows(), TextStatsMaxValues))}
	seen := make(map[string]int, min(values.Rows(), TextStatsMaxValues))
	hashes := make([]uint16, 0, values.Rows())
	for row := 0; row < values.Rows(); row++ {
		if !types.IsValid(valid, row) {
			continue
		}
		hashes = append(hashes, textHash16Bytes(values.Bytes(row)))
		value := values.String(row)
		if index, ok := seen[value]; ok {
			out.Counts[index]++
			continue
		}
		if len(out.Values) >= TextStatsMaxValues {
			out.Truncated = true
			continue
		}
		seen[value] = len(out.Values)
		out.Values = append(out.Values, values.StringCopy(row))
		out.Counts = append(out.Counts, 1)
	}
	if len(out.Values) == 0 {
		return nil
	}
	slices.Sort(hashes)
	out.Hashes = compactSortedUint16(hashes)
	return out
}

func compactSortedUint16(values []uint16) []uint16 {
	if len(values) < 2 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func textHash16String(value string) uint16 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= prime64
	}
	return uint16(hash ^ (hash >> 32) ^ (hash >> 16))
}

func textHash16Bytes(value []byte) uint16 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range value {
		hash ^= uint64(b)
		hash *= prime64
	}
	return uint16(hash ^ (hash >> 32) ^ (hash >> 16))
}

func mergeBoolStats(left *BoolStats, right *BoolStats) *BoolStats {
	if right == nil {
		return left
	}
	if left == nil {
		copy := *right
		return &copy
	}
	left.HasTrue = left.HasTrue || right.HasTrue
	left.HasFalse = left.HasFalse || right.HasFalse
	return left
}

func mergeInt32Stats(left *Int32Stats, right *Int32Stats) *Int32Stats {
	if right == nil {
		return left
	}
	if left == nil {
		copy := *right
		return &copy
	}
	if left.SumValid && right.SumValid {
		left.Sum += right.Sum
	} else {
		left.SumValid = false
	}
	if right.Min < left.Min {
		left.Min = right.Min
	}
	if right.Max > left.Max {
		left.Max = right.Max
	}
	return left
}

func mergeInt64Stats(left *Int64Stats, right *Int64Stats) *Int64Stats {
	if right == nil {
		return left
	}
	if left == nil {
		copy := *right
		return &copy
	}
	if left.SumValid && right.SumValid {
		if sum, ok := addInt64Stat(left.Sum, right.Sum); ok {
			left.Sum = sum
		} else {
			left.SumValid = false
		}
	} else {
		left.SumValid = false
	}
	if right.Min < left.Min {
		left.Min = right.Min
	}
	if right.Max > left.Max {
		left.Max = right.Max
	}
	return left
}

func mergeTextStats(left *TextStats, right *TextStats) *TextStats {
	if right == nil {
		return left
	}
	if left == nil {
		return &TextStats{Values: append([]string(nil), right.Values...), Counts: append([]uint32(nil), right.Counts...), Truncated: right.Truncated || len(right.Counts) != len(right.Values)}
	}
	if len(left.Counts) != len(left.Values) || len(right.Counts) != len(right.Values) {
		left.Truncated = true
	}
	seen := make(map[string]int, len(left.Values)+len(right.Values))
	for i, value := range left.Values {
		seen[value] = i
	}
	for i, value := range right.Values {
		if index, ok := seen[value]; ok {
			if i < len(right.Counts) && index < len(left.Counts) {
				left.Counts[index] += right.Counts[i]
			}
			continue
		}
		if len(left.Values) >= TextStatsMaxValues {
			left.Truncated = true
			break
		}
		seen[value] = len(left.Values)
		left.Values = append(left.Values, value)
		if i < len(right.Counts) {
			left.Counts = append(left.Counts, right.Counts[i])
		} else {
			left.Counts = append(left.Counts, 0)
			left.Truncated = true
		}
	}
	left.Truncated = left.Truncated || right.Truncated
	return left
}
