// AggregateOp drains its source, builds per-group accumulators, and emits chunks of StandardBatchRows.
// HAVING is applied per chunk by narrowing the emitted SelectionMask after each chunk materializes.
package exec

import (
	"context"
	"fmt"

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
		end := a.cursor + vector.StandardBatchRows
		if end > len(a.groups) {
			end = len(a.groups)
		}
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
	nKeys := len(a.GroupBy)
	if nKeys == 0 {
		a.groups = append(a.groups, aggGroup{aggs: make([]aggAccum, len(a.specs))})
	}

	intIdx := map[int64]int{}
	strIdx := map[string]int{}
	anyIdx := map[any]int{}
	compositeIdx := map[string]int{}

	var specCols []aggCol
	groupCols := make([]int, nKeys)
	groupKinds := make([]vector.VecKind, nKeys)
	for {
		batch, ok, err := a.Source.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if specCols == nil {
			for i, g := range a.GroupBy {
				idx := batchColumnIndex(batch, g.Column)
				if idx == -1 {
					return fmt.Errorf("aggregate: group column %q not in batch", g.Column)
				}
				groupCols[i] = idx
				groupKinds[i] = batch.Columns[idx].V.Kind
			}
			specCols = make([]aggCol, len(a.specs))
			a.specKind = make([]vector.VecKind, len(a.specs))
			for i, spec := range a.specs {
				if spec.Star {
					specCols[i] = aggCol{idx: -1}
					continue
				}
				ci := batchColumnIndex(batch, spec.ArgName)
				if ci == -1 {
					return fmt.Errorf("aggregate: column %q not in batch", spec.ArgName)
				}
				specCols[i] = aggCol{idx: ci, vk: batch.Columns[ci].V.Kind}
				a.specKind[i] = batch.Columns[ci].V.Kind
			}
		}
		switch nKeys {
		case 0:
			if err := a.aggregateBatchNoGroup(batch, specCols); err != nil {
				return err
			}
		case 1:
			col := &batch.Columns[groupCols[0]]
			switch groupKinds[0] {
			case vector.VecInt16, vector.VecInt32, vector.VecDate, vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
				if err := a.aggregateBatchIntKey(batch, col, groupKinds[0], specCols, intIdx); err != nil {
					return err
				}
			case vector.VecText, vector.VecBytes, vector.VecJSON:
				if err := a.aggregateBatchTextKey(batch, col, specCols, strIdx); err != nil {
					return err
				}
			default:
				if err := a.aggregateBatchAnyKey(batch, col, groupKinds[0], specCols, anyIdx); err != nil {
					return err
				}
			}
		default:
			if err := a.aggregateBatchCompositeKey(batch, groupCols, groupKinds, specCols, compositeIdx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *AggregateOp) aggregateBatchCompositeKey(batch vector.Batch, groupCols []int, groupKinds []vector.VecKind, specCols []aggCol, idx map[string]int) error {
	var loopErr error
	var keyBuf []byte
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		for _, gc := range groupCols {
			if v := batch.Columns[gc].V.Valid; v != nil && !v.IsValid(row) {
				return
			}
		}
		keyBuf = encodeCompositeKey(keyBuf[:0], batch.Columns, groupCols, groupKinds, row)
		gIdx, ok := idx[string(keyBuf)]
		if !ok {
			gIdx = len(a.groups)
			keyCopy := make([]byte, len(keyBuf))
			copy(keyCopy, keyBuf)
			idx[string(keyCopy)] = gIdx
			rowKeys := make([]any, len(groupCols))
			for i, gc := range groupCols {
				rowKeys[i] = readGroupKey(&batch.Columns[gc], groupKinds[i], row)
			}
			a.groups = append(a.groups, aggGroup{key: rowKeys, aggs: make([]aggAccum, len(a.specs))})
		}
		if err := a.updateRow(&a.groups[gIdx], batch, specCols, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

func encodeCompositeKey(dst []byte, cols []vector.Column, groupCols []int, groupKinds []vector.VecKind, row int) []byte {
	for i, gc := range groupCols {
		col := &cols[gc]
		dst = appendKeyBytes(dst, col, groupKinds[i], row)
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

func (a *AggregateOp) updateRow(g *aggGroup, batch vector.Batch, specCols []aggCol, row int) error {
	for i, spec := range a.specs {
		if err := updateAccumFast(&g.aggs[i], &batch.Columns[max(specCols[i].idx, 0)], specCols[i], spec, row); err != nil {
			return err
		}
	}
	return nil
}

func (a *AggregateOp) aggregateBatchNoGroup(batch vector.Batch, specCols []aggCol) error {
	g := &a.groups[0]
	if a.allCountStar() {
		n := int64(batch.Sel.PopCount())
		for i := range a.specs {
			g.aggs[i].count += n
		}
		return nil
	}
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		if err := a.updateRow(g, batch, specCols, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// allCountStar reports whether every spec is count(*) so the batch loop can sum PopCount directly.
func (a *AggregateOp) allCountStar() bool {
	for _, s := range a.specs {
		if s.Func != sql.AggregateCount || !s.Star {
			return false
		}
	}
	return true
}

func (a *AggregateOp) aggregateBatchIntKey(batch vector.Batch, col *vector.Column, vk vector.VecKind, specCols []aggCol, idx map[int64]int) error {
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
		gIdx, ok := idx[key]
		if !ok {
			gIdx = len(a.groups)
			idx[key] = gIdx
			a.groups = append(a.groups, aggGroup{key: key, aggs: make([]aggAccum, len(a.specs))})
		}
		if err := a.updateRow(&a.groups[gIdx], batch, specCols, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

// `idx[string(b)]` is the well-known compiler trick that avoids the per-lookup string
// allocation. The miss branch materializes the key only when we record a new group.
func (a *AggregateOp) aggregateBatchTextKey(batch vector.Batch, col *vector.Column, specCols []aggCol, idx map[string]int) error {
	if a.canAggregateTextKeyCountSumInt64(specCols) {
		a.aggregateBatchTextKeyCountSumInt64(batch, col, specCols, idx)
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
		gIdx, ok := idx[string(b)]
		if !ok {
			key := string(b)
			gIdx = len(a.groups)
			idx[key] = gIdx
			a.groups = append(a.groups, aggGroup{key: key, aggs: make([]aggAccum, len(a.specs))})
		}
		if err := a.updateRow(&a.groups[gIdx], batch, specCols, row); err != nil {
			loopErr = err
		}
	})
	return loopErr
}

func (a *AggregateOp) canAggregateTextKeyCountSumInt64(specCols []aggCol) bool {
	return len(a.specs) == 2 &&
		len(specCols) == 2 &&
		a.specs[0].Func == sql.AggregateCount &&
		a.specs[1].Func == sql.AggregateSum &&
		specCols[1].idx >= 0 &&
		specCols[1].vk == vector.VecInt64
}

func (a *AggregateOp) aggregateBatchTextKeyCountSumInt64(batch vector.Batch, col *vector.Column, specCols []aggCol, idx map[string]int) {
	valid := col.V.Valid
	vb := col.V.Var()
	countStar := specCols[0].idx == -1
	var countValid vector.Validity
	if !countStar {
		countValid = batch.Columns[specCols[0].idx].V.Valid
	}
	sumCol := &batch.Columns[specCols[1].idx]
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
				gIdx, ok = idx[string(b)]
				if !ok {
					name := string(b)
					gIdx = len(a.groups)
					idx[name] = gIdx
					a.groups = append(a.groups, aggGroup{key: name, aggs: make([]aggAccum, len(a.specs))})
				}
				shortKeys[shortN] = key
				shortVals[shortN] = gIdx
				shortN++
			}
		}
		if gIdx < 0 {
			b := vb.Bytes(row)
			var ok bool
			gIdx, ok = idx[string(b)]
			if !ok {
				key := string(b)
				gIdx = len(a.groups)
				idx[key] = gIdx
				a.groups = append(a.groups, aggGroup{key: key, aggs: make([]aggAccum, len(a.specs))})
			}
		}
		g := &a.groups[gIdx]
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

func (a *AggregateOp) aggregateBatchAnyKey(batch vector.Batch, col *vector.Column, vk vector.VecKind, specCols []aggCol, idx map[any]int) error {
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
		gIdx, ok := idx[key]
		if !ok {
			gIdx = len(a.groups)
			idx[key] = gIdx
			a.groups = append(a.groups, aggGroup{key: key, aggs: make([]aggAccum, len(a.specs))})
		}
		if err := a.updateRow(&a.groups[gIdx], batch, specCols, row); err != nil {
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
		v, err := buildGroupKeyVec(expr.Type, chunk)
		if err != nil {
			return vector.Batch{}, nil, err
		}
		cols = append(cols, vector.Column{Name: expr.Column, Type: expr.Type, V: v})
	}
	if len(a.GroupBy) > 1 {
		// Composite-key groups store their per-column raw values as []any in group.key.
		// Build one output vector per GROUP BY expression.
		for keyIdx, expr := range a.GroupBy {
			v, err := buildCompositeKeyColumnVec(expr.Type, chunk, keyIdx)
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

func buildCompositeKeyColumnVec(t schema.Type, groups []aggGroup, keyIdx int) (vector.Vec, error) {
	pickKey := func(g aggGroup) any {
		keys, ok := g.key.([]any)
		if !ok || keyIdx >= len(keys) {
			return nil
		}
		return keys[keyIdx]
	}
	projected := make([]aggGroup, len(groups))
	for i, g := range groups {
		projected[i] = aggGroup{key: pickKey(g)}
	}
	return buildGroupKeyVec(t, projected)
}

func buildGroupKeyVec(t schema.Type, groups []aggGroup) (vector.Vec, error) {
	rows := len(groups)
	vk, err := vector.VecKindOf(t)
	if err != nil {
		return vector.Vec{}, fmt.Errorf("aggregate: group key kind: %w", err)
	}
	switch vk {
	case vector.VecInt16:
		v := vector.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I16()[i] = int16(x)
		}
		return v, nil
	case vector.VecInt32, vector.VecDate:
		v := vector.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I32()[i] = int32(x)
		}
		return v, nil
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		v := vector.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I64()[i] = x
		}
		return v, nil
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		v := vector.NewVarVec(vk, rows, 0)
		for i, g := range groups {
			s, ok := g.key.(string)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not string", g.key)
			}
			v.Var().AppendString(i, s)
		}
		return v, nil
	case vector.VecBool:
		v := vector.NewVec(vector.VecBool, rows)
		bits := v.BoolBits()
		for i, g := range groups {
			b, ok := g.key.(bool)
			if !ok {
				return vector.Vec{}, fmt.Errorf("aggregate: group key %v not bool", g.key)
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

func buildAggregateVec(spec sql.AggSpec, argKind vector.VecKind, groups []aggGroup, slot int) (vector.Vec, vector.Validity, error) {
	rows := len(groups)
	if spec.Func == sql.AggregateAvg {
		v := vector.NewVec(vector.VecFloat64, rows)
		data := v.F64()
		var valid vector.Validity
		setInvalid := func(i int) {
			if valid == nil {
				valid = vector.NewAllValid(rows)
			}
			valid.SetInvalid(i)
		}
		for i, g := range groups {
			acc := g.aggs[slot]
			if !acc.init || acc.count == 0 {
				setInvalid(i)
				continue
			}
			if argKind == vector.VecFloat32 || argKind == vector.VecFloat64 {
				data[i] = acc.fsum / float64(acc.count)
			} else {
				data[i] = float64(acc.sum) / float64(acc.count)
			}
		}
		return v, valid, nil
	}
	if argKind == vector.VecFloat32 || argKind == vector.VecFloat64 {
		v := vector.NewVec(vector.VecFloat64, rows)
		data := v.F64()
		var valid vector.Validity
		setInvalid := func(i int) {
			if valid == nil {
				valid = vector.NewAllValid(rows)
			}
			valid.SetInvalid(i)
		}
		for i, g := range groups {
			acc := g.aggs[slot]
			switch spec.Func {
			case sql.AggregateSum:
				if !acc.init {
					setInvalid(i)
					continue
				}
				data[i] = acc.fsum
			case sql.AggregateMin:
				if !acc.init {
					setInvalid(i)
					continue
				}
				data[i] = acc.fmin
			case sql.AggregateMax:
				if !acc.init {
					setInvalid(i)
					continue
				}
				data[i] = acc.fmax
			default:
				return vector.Vec{}, nil, fmt.Errorf("aggregate: unsupported func %v on float", spec.Func)
			}
		}
		return v, valid, nil
	}
	v := vector.NewVec(vector.VecInt64, rows)
	data := v.I64()
	var valid vector.Validity
	setInvalid := func(i int) {
		if valid == nil {
			valid = vector.NewAllValid(rows)
		}
		valid.SetInvalid(i)
	}
	for i, g := range groups {
		acc := g.aggs[slot]
		switch spec.Func {
		case sql.AggregateCount:
			data[i] = acc.count
		case sql.AggregateSum:
			if !acc.init {
				setInvalid(i)
				continue
			}
			data[i] = acc.sum
		case sql.AggregateMin:
			if !acc.init {
				setInvalid(i)
				continue
			}
			data[i] = acc.min
		case sql.AggregateMax:
			if !acc.init {
				setInvalid(i)
				continue
			}
			data[i] = acc.max
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
