// Scan walks segments, prunes via predicate stats, decodes surviving pages, and streams matching batches to a callback.
// Single-threaded. Parallel scanning is a later layer on top of this primitive.
package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type ScanFn func(batch types.Batch, sel *types.SelectionMask) error

type ScanOpts struct {
	Segments  []*Segment
	Columns   []string
	Predicate Predicate
	TopK      *TopKPushdown
	// ReadTs is the MVCC visibility cutoff. Segments with CommitTs > ReadTs are skipped.
	// Zero means "no cutoff" (latest), matching the pre-MVCC behavior. Engine-level
	// snapshots set this to nextCommitTs.Load() at statement start.
	ReadTs uint64
}

// TopKPushdown is storage-owned scan metadata for ORDER BY ... LIMIT/OFFSET over a single
// int-like column. Storage uses per-page min/max stats to skip pages whose values cannot
// reach the top K+Offset rows. SortOp remains the correctness layer; pushdown only
// reduces what storage decodes.
type TopKPushdown struct {
	Column string
	Desc   bool
	K      int64
	Offset int64
}

func Scan(opts ScanOpts, fn ScanFn) error {
	if fn == nil {
		return fmt.Errorf("Scan: fn is nil")
	}
	if len(opts.Segments) == 0 {
		return nil
	}
	projection := opts.Columns
	if len(projection) == 0 {
		first := opts.Segments[0]
		projection = make([]string, len(first.Cols))
		for i, c := range first.Cols {
			projection[i] = c.Name
		}
	}
	predCols := PredicateColumns(opts.Predicate)
	decode := projection
	if len(predCols) != 0 {
		decode = unionNames(projection, predCols)
	}
	var topKPages map[[2]int]bool
	if opts.TopK != nil && opts.Predicate == nil {
		topKPages = selectTopKPages(opts.Segments, opts.TopK)
	}
	var predNeeded []bool
	if len(predCols) != 0 {
		predNeeded = make([]bool, len(decode))
		for i, name := range decode {
			for _, p := range predCols {
				if strings.EqualFold(name, p) {
					predNeeded[i] = true
					break
				}
			}
		}
	}
	for si, seg := range opts.Segments {
		if opts.ReadTs != 0 && seg.CommitTs > opts.ReadTs {
			continue
		}
		decodeIdx, projIdx, err := resolveSegmentColumns(seg, decode, projection)
		if err != nil {
			return fmt.Errorf("Scan: %w", err)
		}
		var bp BoundPredicate
		if opts.Predicate != nil {
			bp, err = BindPredicate(opts.Predicate, SegmentSchema(seg))
			if err != nil {
				return fmt.Errorf("Scan: bind predicate: %w", err)
			}
			seg.LoadPageStats()
			if bp.PruneSegment(seg) {
				continue
			}
		}
		var pageMask []bool
		if topKPages != nil {
			pageCount := len(seg.Cols[decodeIdx[0]].Pages)
			pageMask = make([]bool, pageCount)
			for pi := range pageCount {
				pageMask[pi] = topKPages[[2]int{si, pi}]
			}
		}
		if err := scanSegment(seg, decode, decodeIdx, projIdx, predNeeded, bp, pageMask, fn); err != nil {
			return err
		}
	}
	return nil
}

// selectTopKPages returns pages that cannot be proven irrelevant to the top K+Offset
// rows. Returns nil to signal no pruning (caller should scan everything).
func selectTopKPages(segments []*Segment, tk *TopKPushdown) map[[2]int]bool {
	if tk.K <= 0 || tk.Offset < 0 {
		return nil
	}
	need := tk.K + tk.Offset
	if need <= 0 {
		return nil
	}
	type pageRef struct {
		seg, page int
		rows      uint32
		min, max  int64
	}
	var refs []pageRef
	var total int64
	for si, seg := range segments {
		if seg.DV != nil {
			return nil
		}
		colIdx := -1
		for j := range seg.Cols {
			if strings.EqualFold(seg.Cols[j].Name, tk.Column) {
				colIdx = j
				break
			}
		}
		if colIdx < 0 || !topKKindEligible(seg.Cols[colIdx].Kind) {
			return nil
		}
		seg.LoadPageStats()
		col := &seg.Cols[colIdx]
		if len(col.PageStats) != len(col.Pages) {
			return nil
		}
		for pi, page := range col.Pages {
			if page.NullCount != 0 {
				return nil
			}
			min, max, ok := pageMinMaxInt(col, pi)
			if !ok {
				return nil
			}
			r := pageRef{seg: si, page: pi, rows: page.Rows, min: min, max: max}
			refs = append(refs, r)
			total += int64(page.Rows)
		}
	}
	if total <= need {
		return nil
	}
	// DESC: page r is dominated by pages with min > r.max. Sort by min ascending,
	// binary-search the first index past r.max, read a precomputed suffix sum of rows.
	// ASC mirrors with max ascending + prefix sum for indices with max < r.min.
	sorted := make([]pageRef, len(refs))
	copy(sorted, refs)
	if tk.Desc {
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].min < sorted[j].min })
	} else {
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].max < sorted[j].max })
	}
	cum := make([]int64, len(sorted)+1)
	if tk.Desc {
		for i := len(sorted) - 1; i >= 0; i-- {
			cum[i] = cum[i+1] + int64(sorted[i].rows)
		}
	} else {
		for i := 0; i < len(sorted); i++ {
			cum[i+1] = cum[i] + int64(sorted[i].rows)
		}
	}
	selected := make(map[[2]int]bool, len(refs))
	pruned := false
	for _, r := range refs {
		var betterRows int64
		if tk.Desc {
			idx := sort.Search(len(sorted), func(i int) bool { return sorted[i].min > r.max })
			betterRows = cum[idx]
		} else {
			idx := sort.Search(len(sorted), func(i int) bool { return sorted[i].max >= r.min })
			betterRows = cum[idx]
		}
		if betterRows >= need {
			pruned = true
			continue
		}
		selected[[2]int{r.seg, r.page}] = true
	}
	if !pruned {
		return nil
	}
	return selected
}

func topKKindEligible(k types.VecKind) bool {
	switch k {
	case types.VecInt16, types.VecInt32, types.VecInt64,
		types.VecDate, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		return true
	}
	return false
}

func unionNames(a, b []string) []string {
	out := append([]string(nil), a...)
	seen := make(map[string]struct{}, len(a)+len(b))
	for _, n := range a {
		seen[strings.ToLower(n)] = struct{}{}
	}
	for _, n := range b {
		key := strings.ToLower(n)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, n)
	}
	return out
}

func resolveSegmentColumns(seg *Segment, decode, projection []string) (decodeIdx, projIdx []int, err error) {
	decodeIdx = make([]int, len(decode))
	for i, name := range decode {
		found := -1
		for j := range seg.Cols {
			if strings.EqualFold(seg.Cols[j].Name, name) {
				found = j
				break
			}
		}
		if found < 0 {
			return nil, nil, fmt.Errorf("column %q missing in segment", name)
		}
		decodeIdx[i] = found
	}
	projIdx = make([]int, len(projection))
	for i, name := range projection {
		found := -1
		for j, dname := range decode {
			if strings.EqualFold(dname, name) {
				found = j
				break
			}
		}
		if found < 0 {
			return nil, nil, fmt.Errorf("projection %q not in decode set", name)
		}
		projIdx[i] = found
	}
	return decodeIdx, projIdx, nil
}

func scanSegment(seg *Segment, decode []string, decodeIdx, projIdx []int, predNeeded []bool, bp BoundPredicate, pageMask []bool, fn ScanFn) error {
	if len(decodeIdx) == 0 {
		return nil
	}
	pageCount := len(seg.Cols[decodeIdx[0]].Pages)
	for _, ci := range decodeIdx[1:] {
		if len(seg.Cols[ci].Pages) != pageCount {
			return fmt.Errorf("scan: column %q has %d pages, expected %d", seg.Cols[ci].Name, len(seg.Cols[ci].Pages), pageCount)
		}
	}
	decoded := make([]types.Column, len(decode))
	projected := make([]types.Column, len(projIdx))
	var sel types.SelectionMask
	var scratch []byte
	predOnly := predicateOnlyMask(decodeIdx, projIdx)
	encEval, _ := bp.(EncodedEvaluator)
	for i, ci := range decodeIdx {
		decoded[i].Name = seg.Cols[ci].Name
		decoded[i].EnumLabels = seg.Cols[ci].EnumLabels
		if seg.Cols[ci].Kind != types.VecEnum32 {
			t, err := types.TypeFromVecKind(seg.Cols[ci].Kind, "")
			if err != nil {
				return fmt.Errorf("scan: col %q: %w", seg.Cols[ci].Name, err)
			}
			decoded[i].Type = t
		}
	}
	decodeOne := func(i, ci, pi, pageRows int) error {
		var (
			v   types.Vec
			err error
		)
		v, scratch, err = seg.ReadPageInto(ci, pi, scratch)
		if err != nil {
			return fmt.Errorf("scan: col %q page %d: %w", decode[i], pi, err)
		}
		if int(v.Len) != pageRows {
			return fmt.Errorf("scan: col %q page %d rows %d != %d", decode[i], pi, v.Len, pageRows)
		}
		decoded[i].V = v
		return nil
	}
	for pi := range pageCount {
		if pageMask != nil && !pageMask[pi] {
			continue
		}
		if bp != nil && bp.PrunePage(seg, pi) {
			continue
		}
		ci0 := decodeIdx[0]
		pageRows := int(seg.Cols[ci0].Pages[pi].Rows)
		pageRowStart := seg.Cols[ci0].Pages[pi].RowStart
		encodedDone := false
		if encEval != nil && hasAny(predOnly) {
			handled, newScratch, err := encEval.EvalEncoded(seg, pi, &sel, scratch)
			scratch = newScratch
			if err != nil {
				return fmt.Errorf("scan: EvalEncoded page %d: %w", pi, err)
			}
			if handled {
				encodedDone = true
			}
		}
		for i, ci := range decodeIdx {
			if encodedDone && predOnly[i] {
				continue
			}
			if predNeeded != nil && !predNeeded[i] {
				continue
			}
			if err := decodeOne(i, ci, pi, pageRows); err != nil {
				return err
			}
		}
		if !encodedDone {
			batch := types.Batch{Len: pageRows, Columns: decoded}
			if sel.Rows() != pageRows {
				sel.Resize(pageRows)
			} else {
				sel.Clear()
			}
			if bp == nil {
				sel.FillAll()
			} else {
				bp.Eval(batch, &sel)
			}
		}
		if seg.DV != nil {
			for r := range pageRows {
				if !seg.DV.IsValid(int(pageRowStart) + r) {
					sel.Unset(r)
				}
			}
		}
		if sel.PopCount() == 0 {
			continue
		}
		if predNeeded != nil {
			for i, ci := range decodeIdx {
				if predNeeded[i] {
					continue
				}
				if err := decodeOne(i, ci, pi, pageRows); err != nil {
					return err
				}
			}
		}
		for i, di := range projIdx {
			projected[i] = decoded[di]
		}
		out := types.Batch{Len: pageRows, Columns: projected}
		if err := fn(out, &sel); err != nil {
			return err
		}
	}
	return nil
}

func predicateOnlyMask(decodeIdx, projIdx []int) []bool {
	mask := make([]bool, len(decodeIdx))
	for i := range mask {
		mask[i] = true
	}
	for _, p := range projIdx {
		if p >= 0 && p < len(mask) {
			mask[p] = false
		}
	}
	return mask
}

func hasAny(mask []bool) bool {
	for _, b := range mask {
		if b {
			return true
		}
	}
	return false
}
