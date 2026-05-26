// Scan walks segments, prunes via predicate stats, decodes surviving pages, and streams matching batches to a callback.
// Single-threaded. Parallel scanning is a later layer on top of this primitive.
package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// ScanFn batches are valid only until the callback returns.
type ScanFn func(batch vector.Batch, sel *vector.SelectionMask) error

type ScanOpts struct {
	Segments       []*Segment
	Columns        []string
	ColumnIDs      []uint64
	ColumnKinds    []vector.VecKind
	ColumnDefaults []ScanDefault
	Pred           *Pred
	TopK           *TopKPushdown
	ReadTs         uint64
}

type ScanDefault struct {
	Set   bool
	Null  bool
	I64   int64
	F64   float64
	Bytes []byte
	Bool  bool
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
	var predCols []string
	var compiled *CompiledPred
	if opts.Pred != nil {
		predCols = opts.Pred.Columns()
		c, err := CompilePred(*opts.Pred)
		if err != nil {
			return err
		}
		compiled = &c
	}
	decode := projection
	if len(predCols) != 0 {
		decode = unionNames(projection, predCols)
	}
	var topKPages map[[2]int]bool
	if opts.TopK != nil && opts.Pred == nil {
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
		var predIDs []uint64
		if opts.Pred != nil {
			predIDs = opts.Pred.ColumnIDs()
		}
		synthKinds := decodeSynthKinds(decode, opts.Columns, opts.ColumnKinds)
		synthDefaults := decodeSynthDefaults(decode, opts.Columns, opts.ColumnDefaults)
		decodeIdx, projIdx, err := resolveSegmentColumns(seg, decode, projection, decodeIDs(decode, opts.Columns, opts.ColumnIDs, predCols, predIDs), synthKinds)
		if err != nil {
			return fmt.Errorf("Scan: %w", err)
		}
		if compiled != nil {
			if predTruthOnSyntheticNulls(*opts.Pred, seg, opts.Columns, opts.ColumnIDs, opts.ColumnDefaults) == predAlwaysFalse {
				continue
			}
			seg.LoadPageStats()
			if compiled.Skips(seg) {
				continue
			}
		}
		var pageMask []bool
		if topKPages != nil {
			anchor := -1
			for _, ci := range decodeIdx {
				if ci >= 0 {
					anchor = ci
					break
				}
			}
			if anchor < 0 {
				anchor = 0
			}
			pageCount := len(seg.Cols[anchor].Pages)
			pageMask = make([]bool, pageCount)
			for pi := range pageCount {
				pageMask[pi] = topKPages[[2]int{si, pi}]
			}
		}
		if err := scanSegment(seg, decode, decodeIdx, projIdx, predNeeded, synthKinds, synthDefaults, compiled, pageMask, fn); err != nil {
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
		for i := range len(sorted) {
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

func topKKindEligible(k vector.VecKind) bool {
	switch k {
	case vector.VecInt16, vector.VecInt32, vector.VecInt64,
		vector.VecDate, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
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

// A zero entry in the result means no id known and signals name fallback in resolveSegmentColumns.
func decodeIDs(decode []string, projNames []string, projIDs []uint64, predNames []string, predIDs []uint64) []uint64 {
	if len(projIDs) != len(projNames) {
		projIDs = nil
	}
	if len(predIDs) != len(predNames) {
		predIDs = nil
	}
	if len(projIDs) == 0 && len(predIDs) == 0 {
		return nil
	}
	byName := make(map[string]uint64, len(projNames)+len(predNames))
	for i, n := range projNames {
		if i < len(projIDs) {
			byName[strings.ToLower(n)] = projIDs[i]
		}
	}
	for i, n := range predNames {
		key := strings.ToLower(n)
		if _, ok := byName[key]; ok {
			continue
		}
		if i < len(predIDs) {
			byName[key] = predIDs[i]
		}
	}
	out := make([]uint64, len(decode))
	for i, n := range decode {
		out[i] = byName[strings.ToLower(n)]
	}
	return out
}

type predTruth uint8

const (
	predUnknown predTruth = iota
	predAlwaysFalse
	predAlwaysTrue
)

// Three-valued evaluation of a predicate against the synthetic vectors that absent
// columns would produce in this segment. AND/OR/NOT compose, and any leaf touching a
// column the segment actually has stays Unknown so the normal stats path still runs.
func predTruthOnSyntheticNulls(p Pred, seg *Segment, projNames []string, projIDs []uint64, projDefaults []ScanDefault) predTruth {
	switch p.Op {
	case OpAnd:
		result := predAlwaysTrue
		for _, c := range p.Children {
			switch predTruthOnSyntheticNulls(c, seg, projNames, projIDs, projDefaults) {
			case predAlwaysFalse:
				return predAlwaysFalse
			case predUnknown:
				result = predUnknown
			}
		}
		return result
	case OpOr:
		result := predAlwaysFalse
		for _, c := range p.Children {
			switch predTruthOnSyntheticNulls(c, seg, projNames, projIDs, projDefaults) {
			case predAlwaysTrue:
				return predAlwaysTrue
			case predUnknown:
				result = predUnknown
			}
		}
		return result
	case OpNot:
		if len(p.Children) != 1 {
			return predUnknown
		}
		switch predTruthOnSyntheticNulls(p.Children[0], seg, projNames, projIDs, projDefaults) {
		case predAlwaysFalse:
			return predAlwaysTrue
		case predAlwaysTrue:
			return predAlwaysFalse
		}
		return predUnknown
	}
	if seg.TableID == 0 || p.ColID == 0 {
		return predUnknown
	}
	for _, c := range seg.Cols {
		if c.ColumnID == p.ColID {
			return predUnknown
		}
	}
	nullDefault := true
	if len(projIDs) == len(projNames) && len(projDefaults) == len(projNames) {
		for i, id := range projIDs {
			if id != p.ColID {
				continue
			}
			if projDefaults[i].Set && !projDefaults[i].Null {
				nullDefault = false
			}
			break
		}
	}
	switch p.Op {
	case OpIsNull:
		if nullDefault {
			return predAlwaysTrue
		}
		return predAlwaysFalse
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		if nullDefault {
			return predAlwaysFalse
		}
	}
	return predUnknown
}

func fillSyntheticVec(kind vector.VecKind, rows int, def ScanDefault) vector.Vec {
	v := vector.NewVec(kind, rows)
	v.Valid = vector.NewValidity(rows)
	if !def.Set || def.Null {
		for r := 0; r < rows; r++ {
			v.Valid.SetInvalid(r)
		}
		return v
	}
	switch kind {
	case vector.VecInt16:
		b := v.I16()
		for r := range b {
			b[r] = int16(def.I64)
		}
	case vector.VecInt32:
		b := v.I32()
		for r := range b {
			b[r] = int32(def.I64)
		}
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDate, vector.VecDecimal64:
		b := v.I64()
		for r := range b {
			b[r] = def.I64
		}
	case vector.VecFloat32:
		b := v.F32()
		for r := range b {
			b[r] = float32(def.F64)
		}
	case vector.VecFloat64:
		b := v.F64()
		for r := range b {
			b[r] = def.F64
		}
	case vector.VecBool:
		bits := v.BoolBits()
		if def.Bool {
			for r := 0; r < rows; r++ {
				bits[r/8] |= 1 << (uint(r) % 8)
			}
		}
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		v = vector.NewVarVec(kind, rows, len(def.Bytes)*rows)
		v.Valid = vector.NewValidity(rows)
		vb := v.Var()
		for r := 0; r < rows; r++ {
			vb.AppendBytes(r, def.Bytes)
		}
	}
	return v
}

func widenVec(src vector.Vec, dst vector.VecKind) (vector.Vec, error) {
	if src.Kind == dst {
		return src, nil
	}
	out := vector.NewVec(dst, int(src.Len))
	out.Valid = src.Valid
	switch {
	case src.Kind == vector.VecInt16 && dst == vector.VecInt32:
		sb := src.I16()
		db := out.I32()
		for i := range sb {
			db[i] = int32(sb[i])
		}
	case src.Kind == vector.VecInt16 && dst == vector.VecInt64:
		sb := src.I16()
		db := out.I64()
		for i := range sb {
			db[i] = int64(sb[i])
		}
	case src.Kind == vector.VecInt32 && dst == vector.VecInt64:
		sb := src.I32()
		db := out.I64()
		for i := range sb {
			db[i] = int64(sb[i])
		}
	case src.Kind == vector.VecInt16 && dst == vector.VecFloat32:
		sb := src.I16()
		db := out.F32()
		for i := range sb {
			db[i] = float32(sb[i])
		}
	case src.Kind == vector.VecInt16 && dst == vector.VecFloat64:
		sb := src.I16()
		db := out.F64()
		for i := range sb {
			db[i] = float64(sb[i])
		}
	case src.Kind == vector.VecInt32 && dst == vector.VecFloat64:
		sb := src.I32()
		db := out.F64()
		for i := range sb {
			db[i] = float64(sb[i])
		}
	case src.Kind == vector.VecInt64 && dst == vector.VecFloat64:
		sb := src.I64()
		db := out.F64()
		for i := range sb {
			db[i] = float64(sb[i])
		}
	case src.Kind == vector.VecFloat32 && dst == vector.VecFloat64:
		sb := src.F32()
		db := out.F64()
		for i := range sb {
			db[i] = float64(sb[i])
		}
	default:
		return vector.Vec{}, fmt.Errorf("widenVec: unsupported %v -> %v", src.Kind, dst)
	}
	return out, nil
}

func decodeSynthDefaults(decode []string, projNames []string, projDefaults []ScanDefault) []ScanDefault {
	if len(projDefaults) != len(projNames) || len(projDefaults) == 0 {
		return nil
	}
	byName := make(map[string]ScanDefault, len(projNames))
	for i, n := range projNames {
		byName[strings.ToLower(n)] = projDefaults[i]
	}
	out := make([]ScanDefault, len(decode))
	for i, n := range decode {
		out[i] = byName[strings.ToLower(n)]
	}
	return out
}

func decodeSynthKinds(decode []string, projNames []string, projKinds []vector.VecKind) []vector.VecKind {
	if len(projKinds) != len(projNames) || len(projKinds) == 0 {
		return nil
	}
	byName := make(map[string]vector.VecKind, len(projNames))
	for i, n := range projNames {
		byName[strings.ToLower(n)] = projKinds[i]
	}
	out := make([]vector.VecKind, len(decode))
	for i, n := range decode {
		out[i] = byName[strings.ToLower(n)]
	}
	return out
}

func resolveSegmentColumns(seg *Segment, decode, projection []string, decodeIDs []uint64, synthKinds []vector.VecKind) (decodeIdx, projIdx []int, err error) {
	useID := seg.TableID != 0 && len(decodeIDs) == len(decode)
	canSynth := len(synthKinds) == len(decode)
	decodeIdx = make([]int, len(decode))
	for i, name := range decode {
		found := -1
		if useID && decodeIDs[i] != 0 {
			for j := range seg.Cols {
				if seg.Cols[j].ColumnID == decodeIDs[i] {
					found = j
					break
				}
			}
		} else {
			for j := range seg.Cols {
				if strings.EqualFold(seg.Cols[j].Name, name) {
					found = j
					break
				}
			}
			if found < 0 {
				if canSynth && synthKinds[i] != 0 {
					decodeIdx[i] = -1
					continue
				}
				return nil, nil, fmt.Errorf("column %q missing in segment", name)
			}
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

func scanSegment(seg *Segment, decode []string, decodeIdx, projIdx []int, predNeeded []bool, synthKinds []vector.VecKind, synthDefaults []ScanDefault, pred *CompiledPred, pageMask []bool, fn ScanFn) error {
	if len(decodeIdx) == 0 {
		return nil
	}
	if err := seg.ValidateColumns(); err != nil {
		return err
	}
	anchor := -1
	for _, ci := range decodeIdx {
		if ci >= 0 {
			anchor = ci
			break
		}
	}
	if anchor < 0 {
		if len(seg.Cols) == 0 {
			return nil
		}
		anchor = 0
	}
	pageCount := len(seg.Cols[anchor].Pages)
	for _, ci := range decodeIdx {
		if ci < 0 {
			continue
		}
		if len(seg.Cols[ci].Pages) != pageCount {
			return fmt.Errorf("scan: column %q has %d pages, expected %d", seg.Cols[ci].Name, len(seg.Cols[ci].Pages), pageCount)
		}
	}
	decoded := make([]vector.Column, len(decode))
	projected := make([]vector.Column, len(projIdx))
	var sel vector.SelectionMask
	var scratch []byte
	predOnly := predicateOnlyMask(decodeIdx, projIdx)
	for i, ci := range decodeIdx {
		// Emit the caller's name so a renamed column resolves downstream even though the segment footer still has the old one.
		decoded[i].Name = decode[i]
		if ci < 0 {
			kind := vector.VecInt64
			if i < len(synthKinds) && synthKinds[i] != 0 {
				kind = synthKinds[i]
			}
			t, err := vector.TypeFromVecKind(kind, "")
			if err != nil {
				return fmt.Errorf("scan: synthesised col %q: %w", decode[i], err)
			}
			decoded[i].Type = t
			continue
		}
		decoded[i].EnumLabels = seg.Cols[ci].EnumLabels
		if seg.Cols[ci].Kind != vector.VecEnum32 {
			kind := seg.Cols[ci].Kind
			if i < len(synthKinds) && synthKinds[i] != 0 && synthKinds[i] != kind {
				kind = synthKinds[i]
			}
			t, err := vector.TypeFromVecKind(kind, "")
			if err != nil {
				return fmt.Errorf("scan: col %q: %w", seg.Cols[ci].Name, err)
			}
			decoded[i].Type = t
		}
	}
	decodeOne := func(i, ci, pi, pageRows int) error {
		var (
			v   vector.Vec
			err error
		)
		segKind := seg.Cols[ci].Kind
		targetKind := segKind
		if i < len(synthKinds) && synthKinds[i] != 0 {
			targetKind = synthKinds[i]
		}
		if targetKind != segKind {
			var tmp vector.Vec
			scratch, err = seg.readPageIntoValidated(ci, pi, scratch, &tmp)
			if err != nil {
				return fmt.Errorf("scan: col %q page %d: %w", decode[i], pi, err)
			}
			if int(tmp.Len) != pageRows {
				return fmt.Errorf("scan: col %q page %d rows %d != %d", decode[i], pi, tmp.Len, pageRows)
			}
			cast, err := widenVec(tmp, targetKind)
			if err != nil {
				return fmt.Errorf("scan: col %q page %d: %w", decode[i], pi, err)
			}
			decoded[i].V = cast
			return nil
		}
		if segKind.FixedWidth() > 0 {
			scratch, err = seg.readPageIntoValidated(ci, pi, scratch, &decoded[i].V)
			v = decoded[i].V
		} else {
			scratch, err = seg.readPageIntoValidated(ci, pi, scratch, &v)
		}
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
		if pred != nil && pred.SkipsPage(seg, pi) {
			continue
		}
		pageRows := int(seg.Cols[anchor].Pages[pi].Rows)
		pageRowStart := seg.Cols[anchor].Pages[pi].RowStart
		encodedDone := false
		if pred != nil && pred.MatchesPage(seg, pi) {
			if sel.Rows() != pageRows {
				sel.Resize(pageRows)
			}
			sel.FillAll()
			encodedDone = true
		} else if pred != nil && (hasAny(predOnly) || shouldApplyEncodedBeforeDecode(pred)) {
			handled, newScratch, err := pred.ApplyEncoded(seg, pi, &sel, scratch)
			scratch = newScratch
			if err != nil {
				return fmt.Errorf("scan: ApplyEncoded page %d: %w", pi, err)
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
			if ci < 0 {
				// Column was added after this segment was written so fill from the column's initial default.
				kind := decoded[i].V.Kind
				if kind == 0 {
					if i < len(synthKinds) && synthKinds[i] != 0 {
						kind = synthKinds[i]
					} else {
						kind = vector.VecInt64
					}
				}
				var def ScanDefault
				if i < len(synthDefaults) {
					def = synthDefaults[i]
				}
				decoded[i].V = fillSyntheticVec(kind, pageRows, def)
				continue
			}
			if err := decodeOne(i, ci, pi, pageRows); err != nil {
				return err
			}
		}
		if !encodedDone {
			batch := vector.Batch{Len: pageRows, Columns: decoded}
			if sel.Rows() != pageRows {
				sel.Resize(pageRows)
			} else {
				sel.Clear()
			}
			if pred == nil {
				sel.FillAll()
			} else {
				sel.FillAll()
				pred.Apply(batch, &sel)
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
		out := vector.Batch{Len: pageRows, Columns: projected}
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

func shouldApplyEncodedBeforeDecode(pred *CompiledPred) bool {
	if pred == nil {
		return false
	}
	_, ok := pred.bp.(boundEqBytes)
	return ok
}
