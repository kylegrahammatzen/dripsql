package storage

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// smaDisabled lets benchmarks compare with/without SMA without ripping out
// the writer-side machinery. Test-only switch; production always builds SMA.
var smaDisabled = false

// segmentSMAAccumulator tracks per-(text, int) sums across a single segment's
// batches so the engine can answer `GROUP BY text + SUM(int)` from metadata
// alone. Built once per segment, fed each batch via observeBatch, and rolled
// into the segment-level TextStats by finalize.
//
// Hot-path strategy: every per-batch buffer is reused across batches. Per-row
// dispatch goes through one mini-dict probe (linear over <=16 small strings,
// with a one-element MRU cache that catches the common "same-value-as-previous-row"
// case) and one direct array index into a [256]int64.
type segmentSMAAccumulator struct {
	textCols []int
	intCols  []int

	perText       []perTextState
	batchSums     [256]int64   // scratch for one (text, int) pair at a time
	batchCrossCnt [65536]int64 // scratch for one (text, text) pair, indexed id1*256+id2
	segmentSums   [][]map[string]int64
	// segmentCross[ti][tj] = map[crossKey]int64 — counts of (textCol[ti] value,
	// textCol[tj] value) pairs across the segment. Stored both ways at finalize
	// so each column's TextStats.GroupCounts can be queried directly.
	segmentCross [][]map[crossKey]int64
}

type crossKey struct {
	v1 string
	v2 string
}

type perTextState struct {
	rowID    []uint8
	rowValid []bool
	values   []string // per-batch dict, reset each batch
	// skipForSegment: once a text column overflows the per-batch 256-entry
	// dict, every subsequent batch in the same segment also skips it. The
	// segment-level TextStats will be Truncated regardless, so the SMA would
	// be discarded at finalize anyway — bailing early avoids the per-row scan.
	skipForSegment bool
}

func newSegmentSMAAccumulator(cols []types.Column) *segmentSMAAccumulator {
	a := &segmentSMAAccumulator{}
	for i, col := range cols {
		switch col.V.Kind {
		case types.VecText:
			a.textCols = append(a.textCols, i)
		case types.VecInt32, types.VecInt64:
			a.intCols = append(a.intCols, i)
		}
	}
	if len(a.textCols) == 0 {
		return a
	}
	a.perText = make([]perTextState, len(a.textCols))
	if len(a.intCols) > 0 {
		a.segmentSums = make([][]map[string]int64, len(a.textCols))
		for i := range a.segmentSums {
			a.segmentSums[i] = make([]map[string]int64, len(a.intCols))
			for j := range a.segmentSums[i] {
				a.segmentSums[i][j] = make(map[string]int64)
			}
		}
	}
	if len(a.textCols) >= 2 {
		a.segmentCross = make([][]map[crossKey]int64, len(a.textCols))
		for i := range a.segmentCross {
			a.segmentCross[i] = make([]map[crossKey]int64, len(a.textCols))
			for j := range a.segmentCross[i] {
				if i == j {
					continue
				}
				a.segmentCross[i][j] = make(map[crossKey]int64)
			}
		}
	}
	return a
}

func (a *segmentSMAAccumulator) observeBatch(batch types.Batch) {
	if a == nil || len(a.perText) == 0 {
		return
	}

	// Build per-batch mini-dicts for each text column. Linear search beats
	// map lookup when the dict has <= ~16 entries, and the row-to-row MRU cache
	// catches consecutive duplicates (common after dict-encoding rotates a fixed
	// set of values).
	for ti, tcolIdx := range a.textCols {
		state := &a.perText[ti]
		if state.skipForSegment {
			continue
		}
		state.rowID = ensureUint8(state.rowID, batch.Len)
		state.rowValid = ensureBool(state.rowValid, batch.Len)
		state.values = state.values[:0]
		textVec := batch.Columns[tcolIdx].V
		// MRU cache: if row N's value matches row N-1's, skip the dict probe.
		var lastValue string
		var lastID uint8
		var lastOK bool
		overflow := false
		for row := 0; row < batch.Len; row++ {
			value, ok := textViewAt(textVec, row)
			if !ok {
				state.rowValid[row] = false
				lastOK = false
				continue
			}
			var id uint8
			if lastOK && value == lastValue {
				id = lastID
			} else if id, ok = lookupOrInsert(&state.values, value); !ok {
				// Per-batch cardinality blew past 256. Bail on this text column
				// for this segment — its segment-level TextStats will end up
				// Truncated anyway, so finalize will leave GroupSums nil.
				overflow = true
				break
			} else {
				lastValue = value
				lastID = id
				lastOK = true
			}
			state.rowID[row] = id
			state.rowValid[row] = true
		}
		if overflow {
			state.skipForSegment = true
			state.values = state.values[:0]
		}
	}

	// Per (text, int) pair: scan the int column with array-indexed adds.
	for ti := range a.textCols {
		state := &a.perText[ti]
		if state.skipForSegment || len(state.values) == 0 || len(a.segmentSums) == 0 {
			break
		}
		for ji, icolIdx := range a.intCols {
			intVec := batch.Columns[icolIdx].V
			for k := range a.batchSums {
				a.batchSums[k] = 0
			}
			switch intVec.Kind {
			case types.VecInt64:
				src := intVec.I64
				if intVec.Valid == nil {
					for row := 0; row < batch.Len; row++ {
						if !state.rowValid[row] {
							continue
						}
						a.batchSums[state.rowID[row]] += src[row]
					}
				} else {
					for row := 0; row < batch.Len; row++ {
						if !state.rowValid[row] || !types.IsValid(intVec.Valid, row) {
							continue
						}
						a.batchSums[state.rowID[row]] += src[row]
					}
				}
			case types.VecInt32:
				src := intVec.I32
				if intVec.Valid == nil {
					for row := 0; row < batch.Len; row++ {
						if !state.rowValid[row] {
							continue
						}
						a.batchSums[state.rowID[row]] += int64(src[row])
					}
				} else {
					for row := 0; row < batch.Len; row++ {
						if !state.rowValid[row] || !types.IsValid(intVec.Valid, row) {
							continue
						}
						a.batchSums[state.rowID[row]] += int64(src[row])
					}
				}
			}
			segMap := a.segmentSums[ti][ji]
			for id, value := range state.values {
				if a.batchSums[id] == 0 {
					continue
				}
				if _, present := segMap[value]; !present {
					value = cloneString(value)
				}
				segMap[value] += a.batchSums[id]
			}
		}
	}

	// Per (text, text) pair: accumulate cross-counts via id1*256+id2 indexing.
	if len(a.segmentCross) == 0 {
		return
	}
	for ti := range a.textCols {
		state1 := &a.perText[ti]
		if state1.skipForSegment || len(state1.values) == 0 {
			continue
		}
		for tj := range a.textCols {
			if ti == tj {
				continue
			}
			state2 := &a.perText[tj]
			if state2.skipForSegment || len(state2.values) == 0 {
				continue
			}
			n1 := len(state1.values)
			n2 := len(state2.values)
			// Zero only the cells we'll touch (n1*n2 contiguous block in batchCrossCnt).
			for k := 0; k < n1*256; k += 256 {
				for off := range n2 {
					a.batchCrossCnt[k+off] = 0
				}
			}
			for row := 0; row < batch.Len; row++ {
				if !state1.rowValid[row] || !state2.rowValid[row] {
					continue
				}
				a.batchCrossCnt[int(state1.rowID[row])*256+int(state2.rowID[row])]++
			}
			segMap := a.segmentCross[ti][tj]
			for id1, v1 := range state1.values {
				base := id1 * 256
				for id2, v2 := range state2.values {
					count := a.batchCrossCnt[base+id2]
					if count == 0 {
						continue
					}
					key := crossKey{v1: v1, v2: v2}
					if _, present := segMap[key]; !present {
						key = crossKey{v1: cloneString(v1), v2: cloneString(v2)}
					}
					segMap[key] += count
				}
			}
		}
	}
}

func (a *segmentSMAAccumulator) finalize(cols []ColumnMeta) {
	if a == nil || len(a.perText) == 0 {
		return
	}
	for ti, tcolIdx := range a.textCols {
		col := &cols[tcolIdx]
		if col.Text == nil || col.Text.Truncated {
			continue
		}
		if len(a.segmentSums) > 0 {
			col.Text.GroupSums = make(map[string][]int64, len(a.intCols))
			for ji, icolIdx := range a.intCols {
				intColName := cols[icolIdx].Name
				sums := a.segmentSums[ti][ji]
				parallel := make([]int64, len(col.Text.Values))
				for i, value := range col.Text.Values {
					parallel[i] = sums[value]
				}
				col.Text.GroupSums[intColName] = parallel
			}
		}
		if len(a.segmentCross) == 0 {
			continue
		}
		// Cross-counts: this column's GroupCounts[siblingCol][siblingValue]
		// gets a slice parallel to this column's Values.
		col.Text.GroupCounts = make(map[string]map[string][]int64, len(a.textCols)-1)
		for tj, tcolIdx2 := range a.textCols {
			if ti == tj {
				continue
			}
			sibling := &cols[tcolIdx2]
			if sibling.Text == nil || sibling.Text.Truncated {
				continue
			}
			pairs := a.segmentCross[ti][tj]
			if len(pairs) == 0 {
				continue
			}
			bySibling := make(map[string][]int64, len(sibling.Text.Values))
			for _, siblingValue := range sibling.Text.Values {
				parallel := make([]int64, len(col.Text.Values))
				for i, ownValue := range col.Text.Values {
					parallel[i] = pairs[crossKey{v1: ownValue, v2: siblingValue}]
				}
				bySibling[siblingValue] = parallel
			}
			col.Text.GroupCounts[cols[tcolIdx2].Name] = bySibling
		}
	}
}

func textViewAt(v types.Vec, row int) (string, bool) {
	if v.Valid != nil && !types.IsValid(v.Valid, row) {
		return "", false
	}
	switch v.Encoding {
	case types.EncodingDictionary:
		if v.Encoded == nil || row >= len(v.Encoded.DictIDs) {
			return "", false
		}
		id := int(v.Encoded.DictIDs[row])
		if id >= v.Encoded.DictValues.Rows() {
			return "", false
		}
		return v.Encoded.DictValues.String(id), true
	case types.EncodingFlat:
		return v.Var.String(row), true
	}
	return "", false
}

// lookupOrInsert returns the uint8 index of value in dict, appending if
// missing. ok=false signals the dict already holds 256 entries (the [256]int64
// scratch can't index further) so the caller should bail on this column.
// Linear scan beats a Go map for the small dicts (<= ~16 entries) typical of
// low-cardinality text columns.
func lookupOrInsert(dict *[]string, value string) (uint8, bool) {
	for i, v := range *dict {
		if v == value {
			return uint8(i), true
		}
	}
	if len(*dict) >= 256 {
		return 0, false
	}
	id := uint8(len(*dict))
	*dict = append(*dict, value)
	return id, true
}

func ensureUint8(buf []uint8, n int) []uint8 {
	if cap(buf) < n {
		return make([]uint8, n)
	}
	return buf[:n]
}

func ensureBool(buf []bool, n int) []bool {
	if cap(buf) < n {
		return make([]bool, n)
	}
	return buf[:n]
}

func cloneString(s string) string {
	b := make([]byte, len(s))
	copy(b, s)
	return string(b)
}
