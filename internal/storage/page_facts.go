// Single-pass page analyzer that fills codec.PageFacts and column sinks in
// one walk so codecs and sidecars never re-scan.
package storage

import (
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// One per column, allocated for the lifetime of a WriteSegment call.
type colSink struct {
	intDedup     map[uint64]struct{}
	intKeys      []uint64
	intSum       int64
	intOverflow  bool
	intAny       bool
	intMin       int64
	intMax       int64
	intRangeSeen bool
	varHist      DictHistogram
	varHistSkip  bool
	varAny       bool
	varMinLen    uint32
	varMaxLen    uint32
	varTotal     uint64
	varPageKeys  [][]uint64
	// sawMixedPage means at least one page skipped analysis, so any segment wide
	// artifact built from this sink would silently miss that page's live values.
	sawMixedPage bool
}

// rowsHint sizes the int-dedup map and key slice up front so the typical
// id-like column (one new key per row) avoids the O(log n) growth churn.
// Capped at 1024 because monotonically distinct columns can be huge and the
// hint is just an opener; the map still grows as needed beyond it.
func newColSink(kind vector.VecKind, rowsHint int) *colSink {
	s := &colSink{}
	if kindEligibleForIntFilter(kind) {
		hint := rowsHint
		if hint > 1024 {
			hint = 1024
		}
		if hint < 8 {
			hint = 8
		}
		s.intDedup = make(map[uint64]struct{}, hint)
		s.intKeys = make([]uint64, 0, hint)
	}
	if kind.IsVarBytes() {
		s.varHist = make(DictHistogram, 8)
	}
	return s
}

func analyzePage(v vector.Vec, facts *codec.PageFacts, sink *colSink, pageIdx int) {
	facts.Rows = int(v.Len)
	facts.Kind = v.Kind
	facts.Int = nil
	facts.VarBytes = nil
	facts.Nulls = 0
	if v.Valid != nil {
		facts.Nulls = v.Valid.NullCount(facts.Rows)
	}
	if facts.Rows == 0 || facts.Nulls == facts.Rows {
		return
	}
	switch {
	case kindEligibleForIntFilter(v.Kind):
		analyzeIntPage(v, facts, sink)
	case v.Kind.IsVarBytes():
		analyzeVarBytesPage(v, facts, sink, pageIdx)
	}
}

func analyzeIntPage(v vector.Vec, facts *codec.PageFacts, sink *colSink) {
	rows := facts.Rows
	valid := v.Valid

	var min, max int64
	var firstValue int64
	var firstStep int64
	var prev int64
	var stepKnown bool
	var minDelta, maxDelta int64
	sequenceOK := true
	constantOK := true
	first := true
	count := 0

	for r := range rows {
		if valid != nil && !valid.IsValid(r) {
			sequenceOK = false
			continue
		}
		x := readInt64Key(v, r)
		if sink != nil {
			if !sink.intOverflow {
				if addOverflowsInt64(sink.intSum, x) {
					sink.intOverflow = true
				} else {
					sink.intSum += x
					sink.intAny = true
				}
			}
			k := uint64(x)
			if _, dup := sink.intDedup[k]; !dup {
				sink.intDedup[k] = struct{}{}
				sink.intKeys = append(sink.intKeys, k)
			}
		}
		if first {
			min, max, prev = x, x, x
			firstValue = x
			first = false
			count++
			continue
		}
		if x < min {
			min = x
		}
		if x > max {
			max = x
		}
		if x != prev {
			constantOK = false
		}
		step := x - prev
		if sequenceOK {
			if !stepKnown {
				firstStep = step
				stepKnown = true
			} else if step != firstStep {
				sequenceOK = false
			}
		}
		if !stepKnown || count == 1 {
			minDelta, maxDelta = step, step
		} else {
			if step < minDelta {
				minDelta = step
			}
			if step > maxDelta {
				maxDelta = step
			}
		}
		prev = x
		count++
	}

	if count == 0 {
		return
	}

	span := uint64(max) - uint64(min)
	width := 0
	if span > 0 {
		width = 64 - bits.LeadingZeros64(span)
	}
	if count < 2 {
		sequenceOK = false
	}

	deltaWidth := 0
	deltaOK := false
	if count >= 2 {
		deltaSpan := uint64(maxDelta) - uint64(minDelta)
		if deltaSpan > 0 {
			deltaWidth = 64 - bits.LeadingZeros64(deltaSpan)
			deltaOK = true
		}
	}

	var pageSum int64
	pageOverflow := false
	if sink != nil {
		pageSum = sink.intSum
		pageOverflow = sink.intOverflow
		if !sink.intRangeSeen {
			sink.intMin, sink.intMax = min, max
			sink.intRangeSeen = true
		} else {
			if min < sink.intMin {
				sink.intMin = min
			}
			if max > sink.intMax {
				sink.intMax = max
			}
		}
	}

	facts.Int = &codec.IntFacts{
		Min:          min,
		Max:          max,
		Sum:          pageSum,
		SumOverflow:  pageOverflow,
		ConstantOK:   constantOK,
		SequenceOK:   sequenceOK,
		SequenceStep: firstStep,
		ForBase:      min,
		ForWidth:     width,
		First:        firstValue,
		DeltaBase:    minDelta,
		DeltaWidth:   deltaWidth,
		DeltaOK:      deltaOK,
	}
}

// Writes col stats from sink data for kinds the analyzer summarizes.
// Returns false when the sink does not cover the kind; caller falls back to scan.
func marshalColumnStatsFromSink(kind vector.VecKind, sink *colSink, dst []byte) bool {
	if sink.sawMixedPage {
		return false
	}
	switch kind {
	case vector.VecInt16:
		s := NumericStats[int32]{HasNonNull: sink.intRangeSeen}
		if sink.intRangeSeen {
			s.Min = int32(sink.intMin)
			s.Max = int32(sink.intMax)
		}
		s.MarshalWire(dst)
		return true
	case vector.VecInt32, vector.VecDate:
		s := NumericStats[int32]{HasNonNull: sink.intRangeSeen}
		if sink.intRangeSeen {
			s.Min = int32(sink.intMin)
			s.Max = int32(sink.intMax)
		}
		s.MarshalWire(dst)
		return true
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		s := NumericStats[int64]{HasNonNull: sink.intRangeSeen, Min: sink.intMin, Max: sink.intMax}
		s.MarshalWire(dst)
		return true
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		s := VarBytesStats{HasNonNull: sink.varAny, MinLen: sink.varMinLen, MaxLen: sink.varMaxLen, TotalBytes: sink.varTotal}
		s.MarshalWire(dst)
		return true
	}
	return false
}

// Fast path for the all-valid int case. Falls back to a row scan for kinds
// that analyzePage does not summarize.
func marshalPageStatsFromFacts(kind vector.VecKind, facts *codec.PageFacts, v vector.Vec, dst []byte) {
	if facts.Int == nil {
		marshalPageStats(kind, v, dst)
		return
	}
	switch kind {
	case vector.VecInt16, vector.VecInt32, vector.VecDate:
		s := NumericStats[int32]{Min: int32(facts.Int.Min), Max: int32(facts.Int.Max), HasNonNull: true}
		s.MarshalWire(dst)
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		s := NumericStats[int64]{Min: facts.Int.Min, Max: facts.Int.Max, HasNonNull: true}
		s.MarshalWire(dst)
	default:
		marshalPageStats(kind, v, dst)
	}
}

func analyzeVarBytesPage(v vector.Vec, facts *codec.PageFacts, sink *colSink, pageIdx int) {
	rows := facts.Rows
	valid := v.Valid
	vb := v.Var()

	const dictMax = 256
	pageDict := make(map[string]uint8, 8)
	pageIndices := make([]byte, rows)
	pageEntries := make([][]byte, 0, 8)
	pageCounts := make([]uint64, 0, 8)
	pageBytes := 0
	pageFits := true

	if sink != nil {
		for len(sink.varPageKeys) <= pageIdx {
			sink.varPageKeys = append(sink.varPageKeys, nil)
		}
	}

	for r := range rows {
		if valid != nil && !valid.IsValid(r) {
			continue
		}
		b := vb.Bytes(r)
		l := uint32(len(b))
		if sink != nil {
			sink.varPageKeys[pageIdx] = append(sink.varPageKeys[pageIdx], hashBytesFNV(b))
		}
		if sink != nil {
			if !sink.varAny {
				sink.varMinLen, sink.varMaxLen = l, l
				sink.varAny = true
			} else {
				if l < sink.varMinLen {
					sink.varMinLen = l
				}
				if l > sink.varMaxLen {
					sink.varMaxLen = l
				}
			}
			sink.varTotal += uint64(l)
		}
		if pageFits {
			if code, exists := pageDict[string(b)]; exists {
				pageIndices[r] = code
				pageCounts[code]++
				continue
			}
			if len(pageEntries) < dictMax {
				entry := append([]byte(nil), b...)
				code := uint8(len(pageEntries))
				pageDict[string(entry)] = code
				pageEntries = append(pageEntries, entry)
				pageCounts = append(pageCounts, 1)
				pageIndices[r] = code
				pageBytes += 4 + len(entry)
				continue
			}
			pageFits = false
		}
		// pageFits=false: page dict gave up. Still track sink.varHist row by row so the
		// segment-level histogram stays correct (subject to dictHistMaxDistinct).
		if sink != nil && !sink.varHistSkip {
			if _, exists := sink.varHist[string(b)]; exists {
				sink.varHist[string(b)]++
			} else {
				sink.varHist[string(append([]byte(nil), b...))] = 1
				if len(sink.varHist) > dictHistMaxDistinct {
					sink.varHistSkip = true
					sink.varHist = nil
				}
			}
		}
	}

	// Fold per-page dict counts into the segment histogram. Reusing entry bytes as the
	// map key string avoids one alloc per distinct value (vs alloc per row).
	if sink != nil && !sink.varHistSkip && len(pageEntries) > 0 {
		for i, entry := range pageEntries {
			sink.varHist[string(entry)] += pageCounts[i]
			if len(sink.varHist) > dictHistMaxDistinct {
				sink.varHistSkip = true
				sink.varHist = nil
				break
			}
		}
	}

	vf := &codec.VarBytesFacts{
		DistinctCount: len(sink.varHist),
		HistTruncated: sink.varHistSkip,
		DictFits:      pageFits && len(pageEntries) > 0,
	}
	if vf.DictFits {
		vf.DictBytes = pageBytes
		vf.DictEntries = pageEntries
		vf.DictIndices = pageIndices
	}
	facts.VarBytes = vf
}
