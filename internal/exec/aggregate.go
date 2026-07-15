// AggregateOp drains its source, builds per-group accumulators, and emits chunks of StandardBatchRows.
// HAVING is applied per chunk by narrowing the emitted SelectionMask after each chunk materializes.
package exec

import (
	"context"
	"fmt"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type AggregateOp struct {
	Source     Operator
	GroupBy    []sql.BoundExpr
	Aggregates []sql.AggSpec
	Hidden     []sql.AggSpec
	Having     *sql.BoundExpr

	state    operatorState
	built    bool
	cursor   int
	groups   []aggGroup
	specs    []sql.AggSpec
	specKind []vector.VecKind
}

type aggAccum struct {
	count int64
	sum   int64
	min   int64
	max   int64
	fsum  float64
	fmin  float64
	fmax  float64
	init  bool
}

type aggGroup struct {
	key  any
	aggs []aggAccum
}

func (a *AggregateOp) Open(ctx context.Context) error {
	prev := a.state
	if err := a.state.open(); err != nil {
		return err
	}
	if err := a.Source.Open(ctx); err != nil {
		a.state = prev
		return err
	}
	a.built = false
	a.cursor = 0
	a.groups = nil
	a.specs = nil
	return nil
}

func (a *AggregateOp) Next() (vector.Batch, bool, error) {
	if err := a.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	if !a.built {
		if err := a.build(); err != nil {
			return vector.Batch{}, false, err
		}
		a.built = true
	}
	for a.cursor < len(a.groups) {
		end := min(a.cursor+vector.StandardBatchRows, len(a.groups))
		batch, sel, err := a.materializeChunk(a.cursor, end)
		a.cursor = end
		if err != nil {
			return vector.Batch{}, false, err
		}
		if a.Having != nil {
			filtered, err := a.applyHaving(batch, sel)
			if err != nil {
				return vector.Batch{}, false, err
			}
			sel = filtered
		}
		if sel.PopCount() == 0 {
			continue
		}
		if err := batch.SetSel(sel); err != nil {
			return vector.Batch{}, false, fmt.Errorf("aggregate: %w", err)
		}
		return batch, true, nil
	}
	return vector.Batch{}, false, nil
}

func (a *AggregateOp) build() error {
	for _, g := range a.GroupBy {
		if g.Op != sql.ExprColumn {
			return fmt.Errorf("aggregate: only column-ref GROUP BY is supported")
		}
	}
	a.specs = append(append([]sql.AggSpec{}, a.Aggregates...), a.Hidden...)

	workers := 1
	if cs, ok := a.Source.(concurrentSource); ok && cs.concurrentDrainWorkers() > workers {
		workers = cs.concurrentDrainWorkers()
	}
	states := make([]*aggBuild, workers)
	for i := range states {
		states[i] = newAggBuild(a.GroupBy, a.specs)
	}
	if workers == 1 {
		if err := states[0].drain(a.Source); err != nil {
			return err
		}
	} else {
		var wg sync.WaitGroup
		errCh := make(chan error, workers)
		for _, st := range states {
			wg.Go(func() {
				if err := st.drain(a.Source); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			})
		}
		wg.Wait()
		select {
		case err := <-errCh:
			return err
		default:
		}
	}
	merged := states[0]
	for _, st := range states[1:] {
		merged.merge(st)
	}
	a.groups = merged.groups
	a.specKind = merged.specKind
	return nil
}

// aggBuild is one drain worker's accumulation state so parallel sources can be
// consumed by several goroutines and folded together afterwards.
type aggBuild struct {
	groupBy    []sql.BoundExpr
	specs      []sql.AggSpec
	argSpecs   []int
	argKinds   []vector.VecKind
	specCols   []aggCol
	specKind   []vector.VecKind
	groupCols  []int
	groupKinds []vector.VecKind
	groups     []aggGroup
	intIdx     map[int64]int
	strIdx     map[string]int
	anyIdx     map[any]int
	compIdx    map[string]int
}

func newAggBuild(groupBy []sql.BoundExpr, specs []sql.AggSpec) *aggBuild {
	st := &aggBuild{
		groupBy: groupBy,
		specs:   specs,
		intIdx:  map[int64]int{},
		strIdx:  map[string]int{},
		anyIdx:  map[any]int{},
		compIdx: map[string]int{},
	}
	for i := range specs {
		if specs[i].ArgExpr != nil {
			vk, err := vector.VecKindOf(specs[i].ArgExpr.Type)
			if err != nil {
				vk = vector.VecInvalid
			}
			st.argSpecs = append(st.argSpecs, i)
			st.argKinds = append(st.argKinds, vk)
		}
	}
	if len(groupBy) == 0 {
		st.groups = append(st.groups, aggGroup{aggs: make([]aggAccum, len(specs))})
	}
	return st
}

// extendArgs appends computed argument columns to a copied column set so the rest of
// the build consumes plain named columns and the source batch stays untouched for recycling.
func (st *aggBuild) extendArgs(batch vector.Batch) (vector.Batch, error) {
	if len(st.argSpecs) == 0 {
		return batch, nil
	}
	cols := append(make([]vector.Column, 0, len(batch.Columns)+len(st.argSpecs)), batch.Columns...)
	for k, si := range st.argSpecs {
		spec := &st.specs[si]
		vk := st.argKinds[k]
		if vk == vector.VecInvalid {
			return vector.Batch{}, fmt.Errorf("aggregate: argument %q has no physical kind", spec.ArgName)
		}
		v, valid, ok, err := vecEvalArith(batch, *spec.ArgExpr, batch.Sel, batch.Len, vk)
		if err != nil {
			return vector.Batch{}, err
		}
		if !ok {
			v, valid, err = materializeVec(newEvalCtx(batch), *spec.ArgExpr, batch.Sel, batch.Len, vk)
			if err != nil {
				return vector.Batch{}, err
			}
		}
		v.Valid = valid
		cols = append(cols, vector.Column{Name: spec.ArgName, Type: spec.ArgExpr.Type, V: v})
	}
	return vector.Batch{Len: batch.Len, Columns: cols, Sel: batch.Sel}, nil
}

// Group keys are always copied into the lookup maps, so consumed batches can go
// straight back to the scan's recycle pool.
func (st *aggBuild) drain(source Operator) error {
	rec, _ := source.(batchRecycler)
	for {
		batch, ok, err := source.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := st.consume(batch); err != nil {
			return err
		}
		if rec != nil {
			rec.Recycle(batch)
		}
	}
}

func (st *aggBuild) consume(batch vector.Batch) error {
	batch, err := st.extendArgs(batch)
	if err != nil {
		return err
	}
	if st.specCols == nil {
		st.groupCols = make([]int, len(st.groupBy))
		st.groupKinds = make([]vector.VecKind, len(st.groupBy))
		for i, g := range st.groupBy {
			idx := batchColumnIndex(batch, g.Column)
			if idx == -1 {
				return fmt.Errorf("aggregate: group column %q not in batch", g.Column)
			}
			st.groupCols[i] = idx
			st.groupKinds[i] = batch.Columns[idx].V.Kind
		}
		st.specCols = make([]aggCol, len(st.specs))
		st.specKind = make([]vector.VecKind, len(st.specs))
		for i, spec := range st.specs {
			if spec.Star {
				st.specCols[i] = aggCol{idx: -1}
				continue
			}
			ci := batchColumnIndex(batch, spec.ArgName)
			if ci == -1 {
				return fmt.Errorf("aggregate: column %q not in batch", spec.ArgName)
			}
			st.specCols[i] = aggCol{idx: ci, vk: batch.Columns[ci].V.Kind}
			st.specKind[i] = batch.Columns[ci].V.Kind
		}
	}
	switch len(st.groupBy) {
	case 0:
		return st.aggregateBatchNoGroup(batch)
	case 1:
		col := &batch.Columns[st.groupCols[0]]
		switch st.groupKinds[0] {
		case vector.VecInt16, vector.VecInt32, vector.VecDate, vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			return st.aggregateBatchIntKey(batch, col, st.groupKinds[0])
		case vector.VecText, vector.VecBytes, vector.VecJSON:
			return st.aggregateBatchTextKey(batch, col)
		default:
			return st.aggregateBatchAnyKey(batch, col, st.groupKinds[0])
		}
	default:
		return st.aggregateBatchCompositeKey(batch)
	}
}

// merge folds another worker's partial groups into st. The single-key dispatch
// mirrors consume so keys land in the same lookup map they were built with.
func (st *aggBuild) merge(other *aggBuild) {
	if other.specCols != nil && st.specCols == nil {
		st.specCols = other.specCols
		st.specKind = other.specKind
		st.groupCols = other.groupCols
		st.groupKinds = other.groupKinds
	}
	if len(st.groupBy) == 0 {
		mergeGroupAccums(&st.groups[0], &other.groups[0])
		return
	}
	if len(other.groups) == 0 {
		return
	}
	if len(st.groupBy) > 1 {
		for key, gi := range other.compIdx {
			tgt, ok := st.compIdx[key]
			if !ok {
				tgt = len(st.groups)
				st.compIdx[key] = tgt
				st.groups = append(st.groups, aggGroup{key: other.groups[gi].key, aggs: make([]aggAccum, len(st.specs))})
			}
			mergeGroupAccums(&st.groups[tgt], &other.groups[gi])
		}
		return
	}
	for gi := range other.groups {
		g := &other.groups[gi]
		var tgt int
		var ok bool
		switch st.groupKinds[0] {
		case vector.VecInt16, vector.VecInt32, vector.VecDate, vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			key := g.key.(int64)
			tgt, ok = st.intIdx[key]
			if !ok {
				tgt = len(st.groups)
				st.intIdx[key] = tgt
			}
		case vector.VecText, vector.VecBytes, vector.VecJSON:
			key := g.key.(string)
			tgt, ok = st.strIdx[key]
			if !ok {
				tgt = len(st.groups)
				st.strIdx[key] = tgt
			}
		default:
			tgt, ok = st.anyIdx[g.key]
			if !ok {
				tgt = len(st.groups)
				st.anyIdx[g.key] = tgt
			}
		}
		if !ok {
			st.groups = append(st.groups, aggGroup{key: g.key, aggs: make([]aggAccum, len(st.specs))})
		}
		mergeGroupAccums(&st.groups[tgt], g)
	}
}

func mergeGroupAccums(dst, src *aggGroup) {
	for i := range dst.aggs {
		mergeAccum(&dst.aggs[i], src.aggs[i])
	}
}

func mergeAccum(dst *aggAccum, src aggAccum) {
	dst.count += src.count
	dst.sum += src.sum
	dst.fsum += src.fsum
	if !src.init {
		return
	}
	if !dst.init {
		dst.min, dst.max, dst.fmin, dst.fmax = src.min, src.max, src.fmin, src.fmax
		dst.init = true
		return
	}
	dst.min = min(dst.min, src.min)
	dst.max = max(dst.max, src.max)
	dst.fmin = min(dst.fmin, src.fmin)
	dst.fmax = max(dst.fmax, src.fmax)
}

func (st *aggBuild) aggregateBatchCompositeKey(batch vector.Batch) error {
	if len(st.groupCols) <= 8 {
		allDict := true
		for _, gc := range st.groupCols {
			if batch.Columns[gc].Dict == nil {
				allDict = false
				break
			}
		}
		if allDict {
			return st.aggregateBatchCompositeAllDict(batch)
		}
	}
	var loopErr error
	var keyBuf []byte
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		for _, gc := range st.groupCols {
			if v := batch.Columns[gc].V.Valid; v != nil && !v.IsValid(row) {
				return
			}
		}
		keyBuf = encodeCompositeKey(keyBuf[:0], batch.Columns, st.groupCols, st.groupKinds, row)
		gIdx, ok := st.compIdx[string(keyBuf)]
		if !ok {
			gIdx = len(st.groups)
			keyCopy := make([]byte, len(keyBuf))
			copy(keyCopy, keyBuf)
			st.compIdx[string(keyCopy)] = gIdx
			rowKeys := make([]any, len(st.groupCols))
			for i, gc := range st.groupCols {
				rowKeys[i] = readGroupKeyDict(&batch.Columns[gc], st.groupKinds[i], row)
			}
			st.groups = append(st.groups, aggGroup{key: rowKeys, aggs: make([]aggAccum, len(st.specs))})
		}
		if err := st.updateRow(&st.groups[gIdx], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// aggregateBatchCompositeAllDict packs up to eight per-row dict codes into one
// uint64 combo key resolved through a small linear cache, so no per-row byte key
// is built and no string hashing happens. New groups register under the canonical
// encoded key so merge dedups against groups built by the string path.
func (st *aggBuild) aggregateBatchCompositeAllDict(batch vector.Batch) error {
	var dicts [8]*vector.DictCol
	k := len(st.groupCols)
	for i, gc := range st.groupCols {
		dicts[i] = batch.Columns[gc].Dict
	}
	const comboCap = 32
	var comboKeys [comboCap]uint64
	var comboVals [comboCap]int
	comboN := 0
	var overflow map[uint64]int
	var keyBuf []byte
	resolve := func(ck uint64, row int) int {
		keyBuf = keyBuf[:0]
		for i := range k {
			d := dicts[i]
			e := d.Entries[d.Codes[row]]
			n := uint32(len(e))
			keyBuf = append(keyBuf, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
			keyBuf = append(keyBuf, e...)
			keyBuf = append(keyBuf, 0x00)
		}
		gIdx, ok := st.compIdx[string(keyBuf)]
		if !ok {
			gIdx = len(st.groups)
			st.compIdx[string(keyBuf)] = gIdx
			rowKeys := make([]any, k)
			for i := range k {
				d := dicts[i]
				rowKeys[i] = string(d.Entries[d.Codes[row]])
			}
			st.groups = append(st.groups, aggGroup{key: rowKeys, aggs: make([]aggAccum, len(st.specs))})
		}
		if comboN < comboCap {
			comboKeys[comboN] = ck
			comboVals[comboN] = gIdx
			comboN++
		} else {
			if overflow == nil {
				overflow = make(map[uint64]int)
			}
			overflow[ck] = gIdx
		}
		return gIdx
	}
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		var ck uint64
		for i := range k {
			ck |= uint64(dicts[i].Codes[row]) << (uint(i) * 8)
		}
		gIdx := -1
		for i := range comboN {
			if comboKeys[i] == ck {
				gIdx = comboVals[i]
				break
			}
		}
		if gIdx < 0 && overflow != nil {
			if v, ok := overflow[ck]; ok {
				gIdx = v
			}
		}
		if gIdx < 0 {
			gIdx = resolve(ck, row)
		}
		if err := st.updateRow(&st.groups[gIdx], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

func readGroupKeyDict(col *vector.Column, vk vector.VecKind, row int) any {
	if col.Dict != nil {
		return string(col.Dict.Entries[col.Dict.Codes[row]])
	}
	return readGroupKey(col, vk, row)
}

func encodeCompositeKey(dst []byte, cols []vector.Column, groupCols []int, groupKinds []vector.VecKind, row int) []byte {
	for i, gc := range groupCols {
		col := &cols[gc]
		if col.Dict != nil {
			e := col.Dict.Entries[col.Dict.Codes[row]]
			n := uint32(len(e))
			dst = append(dst, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
			dst = append(dst, e...)
		} else {
			dst = appendKeyBytes(dst, col, groupKinds[i], row)
		}
		dst = append(dst, 0x00)
	}
	return dst
}

func appendKeyBytes(dst []byte, col *vector.Column, vk vector.VecKind, row int) []byte {
	switch vk {
	case vector.VecInt16:
		x := uint16(col.V.I16()[row])
		return append(dst, byte(x), byte(x>>8))
	case vector.VecInt32, vector.VecDate:
		x := uint32(col.V.I32()[row])
		return append(dst, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		x := uint64(col.V.I64()[row])
		return append(dst,
			byte(x), byte(x>>8), byte(x>>16), byte(x>>24),
			byte(x>>32), byte(x>>40), byte(x>>48), byte(x>>56))
	case vector.VecBool:
		if col.V.BoolBits()[row>>3]&(1<<(row&7)) != 0 {
			return append(dst, 1)
		}
		return append(dst, 0)
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		b := col.V.Var().Bytes(row)
		n := uint32(len(b))
		dst = append(dst, byte(n), byte(n>>8), byte(n>>16), byte(n>>24))
		return append(dst, b...)
	case vector.VecEnum32:
		x := col.V.U32()[row]
		return append(dst, byte(x), byte(x>>8), byte(x>>16), byte(x>>24))
	case vector.VecUUID:
		return append(dst, col.V.FixedBytes()[row*16:row*16+16]...)
	}
	return dst
}

func (st *aggBuild) updateRow(g *aggGroup, batch vector.Batch, row int) error {
	for i, spec := range st.specs {
		if err := updateAccumFast(&g.aggs[i], &batch.Columns[max(st.specCols[i].idx, 0)], st.specCols[i], spec, row); err != nil {
			return err
		}
	}
	return nil
}

func (st *aggBuild) aggregateBatchNoGroup(batch vector.Batch) error {
	g := &st.groups[0]
	if st.allCountStar() {
		n := int64(batch.Sel.PopCount())
		for i := range st.specs {
			g.aggs[i].count += n
		}
		return nil
	}
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		if err := st.updateRow(g, batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// allCountStar reports whether every spec is count(*) so the batch loop can sum PopCount directly.
func (st *aggBuild) allCountStar() bool {
	for _, s := range st.specs {
		if s.Func != sql.AggregateCount || !s.Star {
			return false
		}
	}
	return true
}

func (st *aggBuild) aggregateBatchIntKey(batch vector.Batch, col *vector.Column, vk vector.VecKind) error {
	valid := col.V.Valid
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		if valid != nil && !valid.IsValid(row) {
			return
		}
		var key int64
		switch vk {
		case vector.VecInt16:
			key = int64(col.V.I16()[row])
		case vector.VecInt32, vector.VecDate:
			key = int64(col.V.I32()[row])
		default:
			key = col.V.I64()[row]
		}
		gIdx, ok := st.intIdx[key]
		if !ok {
			gIdx = len(st.groups)
			st.intIdx[key] = gIdx
			st.groups = append(st.groups, aggGroup{key: key, aggs: make([]aggAccum, len(st.specs))})
		}
		if err := st.updateRow(&st.groups[gIdx], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// `idx[string(b)]` is the well-known compiler trick that avoids the per-lookup string
// allocation. The miss branch materializes the key only when we record a new group.
func (st *aggBuild) aggregateBatchTextKey(batch vector.Batch, col *vector.Column) error {
	if col.Dict != nil {
		return st.aggregateBatchTextKeyDict(batch, col.Dict)
	}
	if st.canAggregateTextKeyCountSumInt64() {
		st.aggregateBatchTextKeyCountSumInt64(batch, col)
		return nil
	}
	valid := col.V.Valid
	vb := col.V.Var()
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		if valid != nil && !valid.IsValid(row) {
			return
		}
		b := vb.Bytes(row)
		gIdx, ok := st.strIdx[string(b)]
		if !ok {
			key := string(b)
			gIdx = len(st.groups)
			st.strIdx[key] = gIdx
			st.groups = append(st.groups, aggGroup{key: key, aggs: make([]aggAccum, len(st.specs))})
		}
		if err := st.updateRow(&st.groups[gIdx], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// Dictionary pages resolve each distinct entry to a group slot at most once, then
// rows accumulate through a code-indexed table with no per-row hashing. Entries are
// resolved lazily so codes no selected row references never create phantom groups.
func (st *aggBuild) aggregateBatchTextKeyDict(batch vector.Batch, d *vector.DictCol) error {
	var lut [256]int32
	for i := range d.Entries {
		lut[i] = -1
	}
	resolve := func(c byte) int32 {
		key := string(d.Entries[c])
		gIdx, ok := st.strIdx[key]
		if !ok {
			gIdx = len(st.groups)
			st.strIdx[key] = gIdx
			st.groups = append(st.groups, aggGroup{key: key, aggs: make([]aggAccum, len(st.specs))})
		}
		lut[c] = int32(gIdx)
		return int32(gIdx)
	}
	codes := d.Codes
	if st.canAggregateTextKeyCountSumInt64() {
		countStar := st.specCols[0].idx == -1
		var countValid vector.Validity
		if !countStar {
			countValid = batch.Columns[st.specCols[0].idx].V.Valid
		}
		sumCol := &batch.Columns[st.specCols[1].idx]
		sumValid := sumCol.V.Valid
		sumVals := sumCol.V.I64()
		if batch.Sel.IsAllSet() && countValid == nil && sumValid == nil {
			for row, c := range codes {
				gi := lut[c]
				if gi < 0 {
					gi = resolve(c)
				}
				g := &st.groups[gi]
				g.aggs[0].count++
				acc := &g.aggs[1]
				acc.count++
				acc.sum += sumVals[row]
				acc.init = true
			}
			return nil
		}
		batch.Sel.IterSet(func(row int) {
			gi := lut[codes[row]]
			if gi < 0 {
				gi = resolve(codes[row])
			}
			g := &st.groups[gi]
			if countStar || countValid == nil || countValid.IsValid(row) {
				g.aggs[0].count++
			}
			if sumValid == nil || sumValid.IsValid(row) {
				acc := &g.aggs[1]
				acc.count++
				acc.sum += sumVals[row]
				acc.init = true
			}
		})
		return nil
	}
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		gi := lut[codes[row]]
		if gi < 0 {
			gi = resolve(codes[row])
		}
		if err := st.updateRow(&st.groups[gi], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

func (st *aggBuild) canAggregateTextKeyCountSumInt64() bool {
	return len(st.specs) == 2 &&
		len(st.specCols) == 2 &&
		st.specs[0].Func == sql.AggregateCount &&
		st.specs[1].Func == sql.AggregateSum &&
		st.specCols[1].idx >= 0 &&
		st.specCols[1].vk == vector.VecInt64
}

func (st *aggBuild) aggregateBatchTextKeyCountSumInt64(batch vector.Batch, col *vector.Column) {
	valid := col.V.Valid
	vb := col.V.Var()
	countStar := st.specCols[0].idx == -1
	var countValid vector.Validity
	if !countStar {
		countValid = batch.Columns[st.specCols[0].idx].V.Valid
	}
	sumCol := &batch.Columns[st.specCols[1].idx]
	sumValid := sumCol.V.Valid
	sumVals := sumCol.V.I64()
	var shortKeys [16][2]uint64
	var shortVals [16]int
	var shortN int
	batch.Sel.IterSet(func(row int) {
		if valid != nil && !valid.IsValid(row) {
			return
		}
		gIdx := -1
		if key, ok := vb.InlineKey(row); ok {
			for i := range shortN {
				if shortKeys[i] == key {
					gIdx = shortVals[i]
					break
				}
			}
			if gIdx < 0 && shortN < len(shortKeys) {
				b := vb.Bytes(row)
				var ok bool
				gIdx, ok = st.strIdx[string(b)]
				if !ok {
					name := string(b)
					gIdx = len(st.groups)
					st.strIdx[name] = gIdx
					st.groups = append(st.groups, aggGroup{key: name, aggs: make([]aggAccum, len(st.specs))})
				}
				shortKeys[shortN] = key
				shortVals[shortN] = gIdx
				shortN++
			}
		}
		if gIdx < 0 {
			b := vb.Bytes(row)
			var ok bool
			gIdx, ok = st.strIdx[string(b)]
			if !ok {
				key := string(b)
				gIdx = len(st.groups)
				st.strIdx[key] = gIdx
				st.groups = append(st.groups, aggGroup{key: key, aggs: make([]aggAccum, len(st.specs))})
			}
		}
		g := &st.groups[gIdx]
		if countStar || countValid == nil || countValid.IsValid(row) {
			g.aggs[0].count++
		}
		if sumValid == nil || sumValid.IsValid(row) {
			acc := &g.aggs[1]
			acc.count++
			acc.sum += sumVals[row]
			acc.init = true
		}
	})
}

func (st *aggBuild) aggregateBatchAnyKey(batch vector.Batch, col *vector.Column, vk vector.VecKind) error {
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
			return
		}
		key := readGroupKey(col, vk, row)
		if key == nil {
			return
		}
		gIdx, ok := st.anyIdx[key]
		if !ok {
			gIdx = len(st.groups)
			st.anyIdx[key] = gIdx
			st.groups = append(st.groups, aggGroup{key: key, aggs: make([]aggAccum, len(st.specs))})
		}
		if err := st.updateRow(&st.groups[gIdx], batch, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

type aggCol struct {
	idx int
	vk  vector.VecKind
}

func batchColumnIndex(batch vector.Batch, name string) int {
	for i := range batch.Columns {
		if batch.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

func readGroupKey(col *vector.Column, vk vector.VecKind, row int) any {
	switch vk {
	case vector.VecInt16:
		return int64(col.V.I16()[row])
	case vector.VecInt32, vector.VecDate:
		return int64(col.V.I32()[row])
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		return col.V.I64()[row]
	case vector.VecBool:
		return col.V.BoolBits()[row>>3]&(1<<(row&7)) != 0
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		return string(col.V.Var().Bytes(row))
	case vector.VecUUID:
		return vector.FormatUUID(col.V.UUID()[row])
	case vector.VecEnum32:
		code := col.V.U32()[row]
		if code == 0 || int(code-1) >= len(col.EnumLabels) {
			return nil
		}
		return col.EnumLabels[code-1]
	}
	return nil
}

func updateAccumFast(acc *aggAccum, col *vector.Column, rc aggCol, spec sql.AggSpec, row int) error {
	if rc.idx == -1 {
		acc.count++
		return nil
	}
	if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
		return nil
	}
	if spec.Func == sql.AggregateCount {
		acc.count++
		return nil
	}
	if rc.vk == vector.VecFloat32 || rc.vk == vector.VecFloat64 {
		var x float64
		if rc.vk == vector.VecFloat64 {
			x = col.V.F64()[row]
		} else {
			x = float64(col.V.F32()[row])
		}
		acc.count++
		switch spec.Func {
		case sql.AggregateSum, sql.AggregateAvg:
			acc.fsum += x
		case sql.AggregateMin:
			if !acc.init || x < acc.fmin {
				acc.fmin = x
			}
		case sql.AggregateMax:
			if !acc.init || x > acc.fmax {
				acc.fmax = x
			}
		}
		acc.init = true
		return nil
	}
	var x int64
	switch rc.vk {
	case vector.VecInt16:
		x = int64(col.V.I16()[row])
	case vector.VecInt32, vector.VecDate:
		x = int64(col.V.I32()[row])
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		x = col.V.I64()[row]
	default:
		return fmt.Errorf("aggregate: %v on non-numeric column %q (kind %v)", spec.Func, spec.ArgName, rc.vk)
	}
	acc.count++
	switch spec.Func {
	case sql.AggregateSum, sql.AggregateAvg:
		acc.sum += x
	case sql.AggregateMin:
		if !acc.init || x < acc.min {
			acc.min = x
		}
	case sql.AggregateMax:
		if !acc.init || x > acc.max {
			acc.max = x
		}
	}
	acc.init = true
	return nil
}

func (a *AggregateOp) materializeChunk(start, end int) (vector.Batch, *vector.SelectionMask, error) {
	chunk := a.groups[start:end]
	rows := len(chunk)
	var cols []vector.Column

	if len(a.GroupBy) == 1 {
		expr := a.GroupBy[0]
		v, err := buildGroupKeyVec(expr.Type, rows, func(i int) any { return chunk[i].key })
		if err != nil {
			return vector.Batch{}, nil, err
		}
		cols = append(cols, vector.Column{Name: expr.Column, Type: expr.Type, V: v})
	}
	if len(a.GroupBy) > 1 {
		// Composite-key groups store their per-column raw values as []any in group.key.
		// Build one output vector per GROUP BY expression.
		for keyIdx, expr := range a.GroupBy {
			v, err := buildGroupKeyVec(expr.Type, rows, func(i int) any {
				keys, ok := chunk[i].key.([]any)
				if !ok || keyIdx >= len(keys) {
					return nil
				}
				return keys[keyIdx]
			})
			if err != nil {
				return vector.Batch{}, nil, err
			}
			cols = append(cols, vector.Column{Name: expr.Column, Type: expr.Type, V: v})
		}
	}

	for i, spec := range a.specs {
		argKind := vector.VecInvalid
		if i < len(a.specKind) {
			argKind = a.specKind[i]
		}
		v, valid, err := buildAggregateVec(spec, argKind, chunk, i)
		if err != nil {
			return vector.Batch{}, nil, err
		}
		v.Valid = valid
		name := aggregateColumnName(spec)
		t := aggregateOutputType(spec, argKind)
		cols = append(cols, vector.Column{Name: name, Type: t, V: v})
	}

	out, err := vector.NewBatch(cols)
	if err != nil {
		return vector.Batch{}, nil, fmt.Errorf("aggregate: NewBatch: %w", err)
	}
	out.Len = rows
	for i := range out.Columns {
		out.Columns[i].V.Len = int32(rows)
	}

	sel := vector.NewSelectionMask(rows)
	sel.FillAll()
	return out, &sel, nil
}

func (a *AggregateOp) applyHaving(batch vector.Batch, sel *vector.SelectionMask) (*vector.SelectionMask, error) {
	if err := batch.SetSel(sel); err != nil {
		return nil, fmt.Errorf("aggregate having: %w", err)
	}
	res, err := filterPredicate(batch, *sel, *a.Having, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	return &res.sel, nil
}

func aggregateColumnName(spec sql.AggSpec) string {
	if spec.Alias != "" {
		return spec.Alias
	}
	switch spec.Func {
	case sql.AggregateSum:
		return "sum"
	case sql.AggregateMin:
		return "min"
	case sql.AggregateMax:
		return "max"
	case sql.AggregateAvg:
		return "avg"
	default:
		return "count"
	}
}

func buildGroupKeyVec(t schema.Type, rows int, key func(int) any) (vector.Vec, error) {
	vk, err := vector.VecKindOf(t)
	if err != nil {
		return vector.Vec{}, fmt.Errorf("aggregate: group key kind: %w", err)
	}
	switch vk {
	case vector.VecInt16, vector.VecInt32, vector.VecDate, vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		v := vector.NewVec(vk, rows)
		for i := range rows {
			x, ok := asInt64(key(i))
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not int", key(i))
			}
			switch vk {
			case vector.VecInt16:
				v.I16()[i] = int16(x)
			case vector.VecInt32, vector.VecDate:
				v.I32()[i] = int32(x)
			default:
				v.I64()[i] = x
			}
		}
		return v, nil
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		v := vector.NewVarVec(vk, rows, 0)
		for i := range rows {
			s, ok := key(i).(string)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not string", key(i))
			}
			v.Var().AppendString(i, s)
		}
		return v, nil
	case vector.VecBool:
		v := vector.NewVec(vector.VecBool, rows)
		bits := v.BoolBits()
		for i := range rows {
			b, ok := key(i).(bool)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not bool", key(i))
			}
			if b {
				bits[i>>3] |= 1 << (i & 7)
			}
		}
		return v, nil
	}
	return vector.Vec{}, fmt.Errorf("aggregate: group key kind %v not supported", vk)
}

func aggregateOutputType(spec sql.AggSpec, argKind vector.VecKind) schema.Type {
	if spec.Func == sql.AggregateAvg {
		return schema.Float64
	}
	if argKind == vector.VecFloat32 || argKind == vector.VecFloat64 {
		switch spec.Func {
		case sql.AggregateSum, sql.AggregateMin, sql.AggregateMax:
			return schema.Float64
		}
	}
	return schema.Int64
}

// count is always int64 output and never null, everything else outputs one vector
// whose kind follows aggregateOutputType and goes null for groups that saw no values.
func buildAggregateVec(spec sql.AggSpec, argKind vector.VecKind, groups []aggGroup, slot int) (vector.Vec, vector.Validity, error) {
	rows := len(groups)
	isFloat := argKind == vector.VecFloat32 || argKind == vector.VecFloat64
	var i64 []int64
	var f64 []float64
	var v vector.Vec
	if spec.Func == sql.AggregateAvg || (isFloat && spec.Func != sql.AggregateCount) {
		v = vector.NewVec(vector.VecFloat64, rows)
		f64 = v.F64()
	} else {
		v = vector.NewVec(vector.VecInt64, rows)
		i64 = v.I64()
	}
	var valid vector.Validity
	for i, g := range groups {
		acc := g.aggs[slot]
		if spec.Func == sql.AggregateCount {
			i64[i] = acc.count
			continue
		}
		if !acc.init {
			if valid == nil {
				valid = vector.NewAllValid(rows)
			}
			valid.SetInvalid(i)
			continue
		}
		switch spec.Func {
		case sql.AggregateAvg:
			if isFloat {
				f64[i] = acc.fsum / float64(acc.count)
			} else {
				f64[i] = float64(acc.sum) / float64(acc.count)
			}
		case sql.AggregateSum:
			if isFloat {
				f64[i] = acc.fsum
			} else {
				i64[i] = acc.sum
			}
		case sql.AggregateMin:
			if isFloat {
				f64[i] = acc.fmin
			} else {
				i64[i] = acc.min
			}
		case sql.AggregateMax:
			if isFloat {
				f64[i] = acc.fmax
			} else {
				i64[i] = acc.max
			}
		default:
			return vector.Vec{}, nil, fmt.Errorf("aggregate: unsupported func %v", spec.Func)
		}
	}
	return v, valid, nil
}

func (a *AggregateOp) Close() error {
	a.state.close()
	return a.Source.Close()
}
