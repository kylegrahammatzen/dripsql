package storage

import (
	"math"
	"strconv"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	DefaultPageRows     = types.StandardBatchRows
	DefaultSegmentRows  = 64 * types.StandardBatchRows
	TextStatsMaxValues  = 64
	ValueStatsMaxValues = 64
)

const (
	textSegmentBloomBitsPerValue = 16
	textPageBloomBitsPerValue    = 16
	textSegmentBloomMinWords     = 1024
	textSegmentBloomMaxWords     = 128 * 1024
	textSegmentBloomProbes       = 3
	textPageBloomProbes          = 8
)

type Int32Stats struct {
	Min      int32
	Max      int32
	Sum      int64
	SumValid bool
}

type Int64Stats struct {
	Min      int64
	Max      int64
	Sum      int64
	SumValid bool
}

type BoolStats struct {
	HasTrue  bool
	HasFalse bool
}

// ValueStats records up to ValueStatsMaxValues distinct numeric values seen in
// a page, used by predicate pruning to skip pages that cannot contain a
// matching value. Truncated signals the writer gave up before reaching the
// cap, in which case the values list is unreliable for membership checks.
type ValueStats[T int32 | int64] struct {
	Values    []T
	Truncated bool
}

type Int32ValueStats = ValueStats[int32]
type Int64ValueStats = ValueStats[int64]

type TextStats struct {
	DataBytes uint64
	Values    []string
	Counts    []uint32
	HashBloom []uint64
	Truncated bool
	hashes    []uint32
}

type UUIDStats struct {
	HashBloom []uint64
	hashes    []uint32
}

type ExecStats struct {
	SegmentsTotal     int64
	SegmentsCandidate int64
	PagesTotal        int64
	PagesCandidate    int64
	RowsTotal         int64
	RowsCandidate     int64
	RowsMatched       int64
	PayloadBytesRead  int64
}

func (s *ExecStats) ObserveSegment(candidate bool) {
	if s == nil {
		return
	}
	s.SegmentsTotal++
	if candidate {
		s.SegmentsCandidate++
	}
}

func (s *ExecStats) ObservePage(rows int, payloadBytes int, matched int, candidate bool) {
	if s == nil {
		return
	}
	s.PagesTotal++
	s.RowsTotal += int64(rows)
	if candidate {
		s.PagesCandidate++
		s.RowsCandidate += int64(rows)
	}
	s.RowsMatched += int64(matched)
	s.PayloadBytesRead += int64(payloadBytes)
}

func applyPageStats(page *PageMeta, v types.Vec) {
	page.AllValid = page.NullCount == 0
	page.AllNull = page.NullCount == page.Rows
	switch v.Kind {
	case types.VecBool:
		page.Bool = boolStats(v.BoolBits, v.Len, v.Valid)
	case types.VecInt16:
		page.Int32 = int32Stats(v.I16[:v.Len], v.Valid)
		page.Int32Values = int32ValueStats(v.I16[:v.Len], v.Valid)
	case types.VecInt32, types.VecDate:
		page.Int32 = int32Stats(v.I32[:v.Len], v.Valid)
		page.Int32Values = int32ValueStats(v.I32[:v.Len], v.Valid)
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		page.Int64 = int64Stats(v.I64[:v.Len], v.Valid)
		page.Int64Values = int64ValueStats(v.I64[:v.Len], v.Valid)
	case types.VecText, types.VecBytes, types.VecJSON:
		page.Text = textStats(v.Var, v.Valid)
	case types.VecUUID:
		page.UUID = uuidStats(v.UUID[:v.Len], v.Valid)
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

func int32Stats[T ~int16 | ~int32](values []T, valid types.Validity) *Int32Stats {
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

func int32ValueStats[T ~int16 | ~int32](values []T, valid types.Validity) *Int32ValueStats {
	out := &Int32ValueStats{Values: make([]int32, 0, min(len(values), ValueStatsMaxValues))}
	seen := make(map[int32]struct{}, min(len(values), ValueStatsMaxValues))
	ok := false
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		ok = true
		addValueStat(out, seen, int32(value))
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
		addValueStat(out, seen, value)
	}
	if !ok {
		return nil
	}
	return out
}

func addValueStat[T int32 | int64](out *ValueStats[T], seen map[T]struct{}, value T) {
	if out.Truncated {
		return
	}
	if _, ok := seen[value]; ok {
		return
	}
	if len(out.Values) >= ValueStatsMaxValues {
		out.Truncated = true
		out.Values = nil
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
	out := &TextStats{DataBytes: uint64(len(values.Data)), Values: make([]string, 0, min(values.Rows(), TextStatsMaxValues)), Counts: make([]uint32, 0, min(values.Rows(), TextStatsMaxValues))}
	seen := make(map[string]int, min(values.Rows(), TextStatsMaxValues))
	var hashes []uint32
	ok := false
	for row := 0; row < values.Rows(); row++ {
		if !types.IsValid(valid, row) {
			continue
		}
		ok = true
		if out.Truncated {
			hashes = append(hashes, textHash32Bytes(values.Bytes(row)))
			continue
		}
		value := values.String(row)
		if index, ok := seen[value]; ok {
			out.Counts[index]++
			continue
		}
		if len(out.Values) >= TextStatsMaxValues {
			out.Truncated = true
			hashes = make([]uint32, 0, values.Rows())
			for _, existing := range out.Values {
				hashes = append(hashes, textHash32String(existing))
			}
			hashes = append(hashes, textHash32Bytes(values.Bytes(row)))
			out.Values = nil
			out.Counts = nil
			seen = nil
			continue
		}
		seen[value] = len(out.Values)
		out.Values = append(out.Values, values.StringCopy(row))
		out.Counts = append(out.Counts, 1)
	}
	if !ok {
		return nil
	}
	if out.Truncated {
		out.hashes = hashes
	}
	return out
}

func compactSortedUint32(values []uint32) []uint32 {
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

func textHash32String(value string) uint32 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= prime64
	}
	return uint32(hash ^ (hash >> 32))
}

func textHash32Bytes(value []byte) uint32 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range value {
		hash ^= uint64(b)
		hash *= prime64
	}
	return uint32(hash ^ (hash >> 32))
}

func uuidStats(values []types.UUID16, valid types.Validity) *UUIDStats {
	hashes := make([]uint32, 0, len(values))
	for row, value := range values {
		if !types.IsValid(valid, row) {
			continue
		}
		hashes = append(hashes, uuidHash32(value))
	}
	if len(hashes) == 0 {
		return nil
	}
	return &UUIDStats{hashes: hashes}
}

func uuidHash32(value types.UUID16) uint32 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range value {
		hash ^= uint64(b)
		hash *= prime64
	}
	return uint32(hash ^ (hash >> 32))
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
		return &TextStats{DataBytes: right.DataBytes, Values: append([]string(nil), right.Values...), Counts: append([]uint32(nil), right.Counts...), Truncated: right.Truncated || len(right.Counts) != len(right.Values)}
	}
	left.DataBytes += right.DataBytes
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
	left.hashes = nil
	return left
}

func mergeUUIDStats(left *UUIDStats, right *UUIDStats) *UUIDStats {
	if right == nil {
		return left
	}
	if left == nil {
		return &UUIDStats{hashes: append([]uint32(nil), right.hashes...)}
	}
	left.hashes = append(left.hashes, right.hashes...)
	return left
}

func finalizeColumnUUIDBlooms(col *ColumnMeta) {
	if col.UUID == nil {
		return
	}
	valueCount := 0
	for _, page := range col.Pages {
		if page.UUID == nil {
			continue
		}
		if len(page.UUID.hashes) == 0 {
			col.UUID.HashBloom = nil
			return
		}
		valueCount += len(page.UUID.hashes)
	}
	if valueCount == 0 {
		return
	}
	bloom := newTextHashBloom(valueCount)
	for i := range col.Pages {
		page := &col.Pages[i]
		if page.UUID == nil {
			continue
		}
		if len(page.UUID.hashes) == 0 {
			col.UUID.HashBloom = nil
			return
		}
		pageBloom := newTextPageHashBloom(len(page.UUID.hashes))
		for _, hash := range page.UUID.hashes {
			hashBloomAdd(bloom, hash, textSegmentBloomProbes)
			hashBloomAdd(pageBloom, hash, textPageBloomProbes)
		}
		page.UUID.hashes = nil
		page.UUID.HashBloom = pageBloom
	}
	col.UUID.hashes = nil
	col.UUID.HashBloom = bloom
}

func finalizeColumnTextBlooms(col *ColumnMeta) {
	if col.Text == nil || !col.Text.Truncated {
		return
	}
	valueCount := 0
	for _, page := range col.Pages {
		if page.Text == nil {
			continue
		}
		if page.Text.Truncated {
			if len(page.Text.hashes) == 0 {
				col.Text.HashBloom = nil
				return
			}
			valueCount += len(page.Text.hashes)
			continue
		}
		valueCount += len(page.Text.Values)
	}
	if valueCount == 0 {
		return
	}
	bloom := newTextHashBloom(valueCount)
	hasValue := false
	for i := range col.Pages {
		page := &col.Pages[i]
		if page.Text == nil {
			continue
		}
		if page.Text.Truncated {
			if len(page.Text.hashes) == 0 {
				col.Text.HashBloom = nil
				return
			}
			pageBloom := newTextPageHashBloom(len(page.Text.hashes))
			for _, hash := range page.Text.hashes {
				hashBloomAdd(bloom, hash, textSegmentBloomProbes)
				hashBloomAdd(pageBloom, hash, textPageBloomProbes)
			}
			page.Text.hashes = nil
			page.Text.HashBloom = pageBloom
			hasValue = true
			continue
		}
		for _, value := range page.Text.Values {
			hashBloomAdd(bloom, textHash32String(value), textSegmentBloomProbes)
		}
		hasValue = hasValue || len(page.Text.Values) != 0
	}
	if !hasValue {
		return
	}
	col.Text.hashes = nil
	col.Text.HashBloom = bloom
}

func newTextPageHashBloom(valueCount int) []uint64 {
	words := textPageBloomWordsFor(valueCount)
	if words == 0 {
		return nil
	}
	return make([]uint64, words)
}

func newTextHashBloom(valueCount int) []uint64 {
	words := textSegmentBloomWordsFor(valueCount)
	if words == 0 {
		return nil
	}
	return make([]uint64, words)
}

func hashBloomAdd(bloom []uint64, hash uint32, probes uint64) {
	if len(bloom) == 0 {
		return
	}
	for probe := uint64(0); probe < probes; probe++ {
		bit := textHashBloomBit(hash, probe, len(bloom))
		bloom[bit>>6] |= uint64(1) << (bit & 63)
	}
}

func textPageHashBloomHas(bloom []uint64, value string) bool {
	if len(bloom) == 0 {
		return true
	}
	hash := textHash32String(value)
	return hashBloomHas(bloom, hash, textPageBloomProbes)
}

func textSegmentHashBloomHas(bloom []uint64, value string) bool {
	if len(bloom) == 0 {
		return true
	}
	hash := textHash32String(value)
	return hashBloomHas(bloom, hash, textSegmentBloomProbes)
}

func uuidPageHashBloomHas(bloom []uint64, value types.UUID16) bool {
	if len(bloom) == 0 {
		return true
	}
	return hashBloomHas(bloom, uuidHash32(value), textPageBloomProbes)
}

func uuidSegmentHashBloomHas(bloom []uint64, value types.UUID16) bool {
	if len(bloom) == 0 {
		return true
	}
	return hashBloomHas(bloom, uuidHash32(value), textSegmentBloomProbes)
}

func hashBloomHas(bloom []uint64, hash uint32, probes uint64) bool {
	for probe := uint64(0); probe < probes; probe++ {
		bit := textHashBloomBit(hash, probe, len(bloom))
		if bloom[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func textPageBloomWordsFor(valueCount int) int {
	if valueCount <= 0 {
		return 0
	}
	bits := valueCount * textPageBloomBitsPerValue
	return nextPowerOfTwo((bits + 63) / 64)
}

func textSegmentBloomWordsFor(valueCount int) int {
	if valueCount <= 0 {
		return 0
	}
	bits := valueCount * textSegmentBloomBitsPerValue
	words := (bits + 63) / 64
	if words < textSegmentBloomMinWords {
		words = textSegmentBloomMinWords
	}
	if words > textSegmentBloomMaxWords {
		words = textSegmentBloomMaxWords
	}
	return nextPowerOfTwo(words)
}

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}
	value--
	for shift := 1; shift < strconv.IntSize; shift <<= 1 {
		value |= value >> shift
	}
	return value + 1
}

func textHashBloomBit(hash uint32, probe uint64, words int) uint64 {
	x := uint64(hash) + 0x9e3779b97f4a7c15*(probe+1)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x & uint64(words*64-1)
}
