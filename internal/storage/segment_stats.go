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
	// Bloom-filter sizing. We size in bits-per-distinct-value; standard bloom
	// math gives FPR ≈ (1 - e^(-k/(m/n)))^k.
	//
	// Segment-level bloom: at 8 bits/value with 3 probes the FPR is ~3%,
	// which is fine for the "min/max already pruned most segments, bloom
	// catches the rest" role. The previous 16 bits/value at 3 probes was
	// ~0.5% FPR but 2x the storage; the high-cardinality int/UUID columns
	// (user_id, event_uuid, etc.) carried ~500 KB blooms per segment as a
	// result. Halving bits-per-value halves the on-disk bloom footprint
	// and cuts ~50% off the per-segment flate-decompress work during cold
	// open.
	//
	// Page-level bloom stays at 16 bits/value with 8 probes (~0.06% FPR).
	// Page blooms are what gate actual page payload reads, so a false
	// positive directly costs a page-payload decode; the page blooms are
	// also already tiny in absolute terms (~4 KB per page) so shrinking
	// them buys little.
	textSegmentBloomBitsPerValue = 8
	textPageBloomBitsPerValue    = 16
	textSegmentBloomMinWords     = 64
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
// cap; HashBloom is the per-page or per-segment bloom built from the row
// values once the column overflows that cap, so equality predicates can still
// prune high-cardinality int columns.
type ValueStats[T int32 | int64] struct {
	Values    []T
	HashBloom []uint64
	Truncated bool
	hashes    []uint32
}

type Int32ValueStats = ValueStats[int32]
type Int64ValueStats = ValueStats[int64]

type TextStats struct {
	DataBytes uint64
	Values    []string
	Counts    []uint32
	HashBloom []uint64
	Truncated bool
	// GroupSums holds rolled-up sums for sibling int columns, keyed by sibling
	// column name. Each entry is parallel to Values: GroupSums[col][i] is the
	// sum of col across rows where this column equals Values[i]. Populated
	// only on segment-level stats and only when !Truncated.
	GroupSums map[string][]int64
	// GroupCounts holds per-(sibling text col, sibling text value) row counts
	// keyed by this column's Values. GroupCounts[col][value][i] is the count
	// of rows where this column equals Values[i] AND col equals value. Powers
	// `GROUP BY this WHERE col = value`-style metadata-only queries.
	GroupCounts map[string]map[string][]int64
	hashes      []uint32
}

// SumByValue returns the rolled-up sum of intCol for the row group where this
// text column equals Values[idx]. ok=false when stats are truncated, missing,
// out of range, or the int column wasn't tracked at write time.
func (s *TextStats) SumByValue(intCol string, idx int) (int64, bool) {
	if s == nil || s.Truncated {
		return 0, false
	}
	sums, ok := s.GroupSums[intCol]
	if !ok || idx < 0 || idx >= len(sums) {
		return 0, false
	}
	return sums[idx], true
}

// CountByValueAndPeer returns the count of rows where this column equals
// Values[idx] AND siblingCol equals siblingValue. ok=false when stats are
// truncated, the cross-counts weren't tracked, or the indices are out of
// range.
func (s *TextStats) CountByValueAndPeer(siblingCol string, siblingValue string, idx int) (int64, bool) {
	if s == nil || s.Truncated {
		return 0, false
	}
	bySibling, ok := s.GroupCounts[siblingCol]
	if !ok {
		return 0, false
	}
	parallel, ok := bySibling[siblingValue]
	if !ok || idx < 0 || idx >= len(parallel) {
		return 0, false
	}
	return parallel[idx], true
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
	for row := range rows {
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
		addValueStat(out, seen, int32(value), int64(value))
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
		addValueStat(out, seen, value, value)
	}
	if !ok {
		return nil
	}
	return out
}

// addValueStat dedupes by typed value. Once the per-page Values cap
// overflows, it discards Values and starts appending hashes (the writer turns
// those into a per-page bloom for sparse-int pruning).
func addValueStat[T int32 | int64](out *ValueStats[T], seen map[T]struct{}, value T, hashValue int64) {
	if out.Truncated {
		out.hashes = append(out.hashes, intHash32(hashValue))
		return
	}
	if _, ok := seen[value]; ok {
		return
	}
	if len(out.Values) >= ValueStatsMaxValues {
		out.Truncated = true
		hashes := make([]uint32, 0, len(out.Values)+1)
		for _, existing := range out.Values {
			hashes = append(hashes, intHash32(int64(existing)))
		}
		hashes = append(hashes, intHash32(hashValue))
		out.hashes = hashes
		out.Values = nil
		return
	}
	seen[value] = struct{}{}
	out.Values = append(out.Values, value)
}

func intHash32(value int64) uint32 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	u := uint64(value)
	for shift := 0; shift < 64; shift += 8 {
		hash ^= (u >> shift) & 0xff
		hash *= prime64
	}
	return uint32(hash ^ (hash >> 32))
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

// finalizeColumnInt64Blooms walks the column's pages and builds per-page and
// per-segment blooms when at least one page truncated. Mirrors
// finalizeColumnTextBlooms; non-truncated pages contribute their distinct
// Values to the segment bloom but skip the per-page bloom (their Values list
// answers Eq exactly).
func finalizeColumnInt64Blooms(col *ColumnMeta) {
	if !anyPageInt64Truncated(col) {
		clearInt64ValueStatHashes(col)
		return
	}
	valueCount, ok := countTruncatedInt64Hashes(col)
	if !ok {
		clearInt64ValueStatHashes(col)
		return
	}
	bloom := newTextHashBloom(valueCount)
	for i := range col.Pages {
		page := &col.Pages[i]
		if page.Int64Values == nil {
			continue
		}
		if page.Int64Values.Truncated {
			pageBloom := newTextPageHashBloom(len(page.Int64Values.hashes))
			for _, hash := range page.Int64Values.hashes {
				hashBloomAdd(bloom, hash, textSegmentBloomProbes)
				hashBloomAdd(pageBloom, hash, textPageBloomProbes)
			}
			page.Int64Values.HashBloom = pageBloom
		} else {
			for _, value := range page.Int64Values.Values {
				hashBloomAdd(bloom, intHash32(value), textSegmentBloomProbes)
			}
		}
		page.Int64Values.hashes = nil
	}
	col.Int64Values = &Int64ValueStats{Truncated: true, HashBloom: bloom}
}

func finalizeColumnInt32Blooms(col *ColumnMeta) {
	if !anyPageInt32Truncated(col) {
		clearInt32ValueStatHashes(col)
		return
	}
	valueCount, ok := countTruncatedInt32Hashes(col)
	if !ok {
		clearInt32ValueStatHashes(col)
		return
	}
	bloom := newTextHashBloom(valueCount)
	for i := range col.Pages {
		page := &col.Pages[i]
		if page.Int32Values == nil {
			continue
		}
		if page.Int32Values.Truncated {
			pageBloom := newTextPageHashBloom(len(page.Int32Values.hashes))
			for _, hash := range page.Int32Values.hashes {
				hashBloomAdd(bloom, hash, textSegmentBloomProbes)
				hashBloomAdd(pageBloom, hash, textPageBloomProbes)
			}
			page.Int32Values.HashBloom = pageBloom
		} else {
			for _, value := range page.Int32Values.Values {
				hashBloomAdd(bloom, intHash32(int64(value)), textSegmentBloomProbes)
			}
		}
		page.Int32Values.hashes = nil
	}
	col.Int32Values = &Int32ValueStats{Truncated: true, HashBloom: bloom}
}

func anyPageInt64Truncated(col *ColumnMeta) bool {
	for _, page := range col.Pages {
		if page.Int64Values != nil && page.Int64Values.Truncated {
			return true
		}
	}
	return false
}

func anyPageInt32Truncated(col *ColumnMeta) bool {
	for _, page := range col.Pages {
		if page.Int32Values != nil && page.Int32Values.Truncated {
			return true
		}
	}
	return false
}

func countTruncatedInt64Hashes(col *ColumnMeta) (int, bool) {
	total := 0
	for _, page := range col.Pages {
		if page.Int64Values == nil {
			continue
		}
		if page.Int64Values.Truncated {
			if len(page.Int64Values.hashes) == 0 {
				return 0, false
			}
			total += len(page.Int64Values.hashes)
			continue
		}
		total += len(page.Int64Values.Values)
	}
	if total == 0 {
		return 0, false
	}
	return total, true
}

func countTruncatedInt32Hashes(col *ColumnMeta) (int, bool) {
	total := 0
	for _, page := range col.Pages {
		if page.Int32Values == nil {
			continue
		}
		if page.Int32Values.Truncated {
			if len(page.Int32Values.hashes) == 0 {
				return 0, false
			}
			total += len(page.Int32Values.hashes)
			continue
		}
		total += len(page.Int32Values.Values)
	}
	if total == 0 {
		return 0, false
	}
	return total, true
}

func clearInt64ValueStatHashes(col *ColumnMeta) {
	if col.Int64Values != nil {
		col.Int64Values.hashes = nil
	}
	for i := range col.Pages {
		if col.Pages[i].Int64Values != nil {
			col.Pages[i].Int64Values.hashes = nil
		}
	}
}

func clearInt32ValueStatHashes(col *ColumnMeta) {
	if col.Int32Values != nil {
		col.Int32Values.hashes = nil
	}
	for i := range col.Pages {
		if col.Pages[i].Int32Values != nil {
			col.Pages[i].Int32Values.hashes = nil
		}
	}
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
	for probe := range probes {
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
	for probe := range probes {
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
	words := max((bits+63)/64, textSegmentBloomMinWords)
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
