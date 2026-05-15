// HashJoinOp builds an in-memory hash table from the right side then probes with each left batch.
// Plan config sits on HashJoinOp itself, mutable execution state lives in hashJoinRuntime.
package exec

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type HashJoinOp struct {
	Kind         sql.JoinKind
	Left         Operator
	Right        Operator
	LeftKeys     []sql.BoundExpr
	RightKeys    []sql.BoundExpr
	LeftOutputs  []sql.BoundOutput
	RightOutputs []sql.BoundOutput

	state operatorState
	rt    hashJoinRuntime
}

type hashJoinRuntime struct {
	built       bool
	leftDrained bool

	build hashJoinBuild
	probe hashJoinProbe
	drain hashJoinDrain
	key   keyEncoder
}

type hashJoinBuild struct {
	batches []bufferedBatch
	index   map[uint64][]hashBucket
	matched []types.SelectionMask
}

type hashBucket struct {
	key  []byte
	rows []rightRowRef
}

type hashJoinProbe struct {
	leftSchema []columnTemplate
	batch      types.Batch
	pairs      []joinPair
}

type hashJoinDrain struct {
	batch int
	row   int
}

type keyEncoder struct {
	buf  []byte
	seed maphash.Seed
}

type joinKeyCol struct {
	idx        int
	kind       types.VecKind
	enumLabels []string
}

// resolveJoinKeyCols pre-resolves the column index for each join key. splitJoinEquality
// already requires keys to be column references, so the type switch in encodeRow can
// dispatch on VecKind without going through evalCtx or ColumnByName per row.
func resolveJoinKeyCols(batch types.Batch, keys []sql.BoundExpr) ([]joinKeyCol, error) {
	cols := make([]joinKeyCol, len(keys))
	for i, k := range keys {
		if k.Op != sql.ExprColumn {
			return nil, fmt.Errorf("hashjoin: join key must be a column reference, got op %v", k.Op)
		}
		col, ok := batch.ColumnByName(k.Column)
		if !ok {
			return nil, fmt.Errorf("hashjoin: column %q not in batch", k.Column)
		}
		idx := -1
		for j := range batch.Columns {
			if &batch.Columns[j] == col {
				idx = j
				break
			}
		}
		if idx < 0 {
			for j := range batch.Columns {
				if batch.Columns[j].Name == col.Name {
					idx = j
					break
				}
			}
		}
		cols[i] = joinKeyCol{idx: idx, kind: col.V.Kind, enumLabels: col.EnumLabels}
	}
	return cols, nil
}

type columnTemplate struct {
	Name       string
	Type       types.Type
	EnumLabels []string
	Kind       types.VecKind
}

type joinPair struct {
	leftRow  int
	ref      rightRowRef
	nullFill bool
}

type rightRowRef struct {
	batch int
	row   int
}

func (h *HashJoinOp) Open(ctx context.Context) error {
	prev := h.state
	if err := h.state.open(); err != nil {
		return err
	}
	if err := h.Left.Open(ctx); err != nil {
		h.state = prev
		return err
	}
	if err := h.Right.Open(ctx); err != nil {
		h.Left.Close()
		h.state = prev
		return err
	}
	h.rt.reset()
	return nil
}

func (rt *hashJoinRuntime) reset() {
	rt.built = false
	rt.leftDrained = false

	rt.build.batches = nil
	rt.build.index = nil
	rt.build.matched = nil

	rt.probe.leftSchema = nil
	rt.probe.batch = types.Batch{}
	rt.probe.pairs = nil

	rt.drain.batch = 0
	rt.drain.row = 0

	rt.key.buf = rt.key.buf[:0]
	rt.key.seed = maphash.MakeSeed()
}

func (h *HashJoinOp) Next() (types.Batch, bool, error) {
	if err := h.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	if !h.rt.built {
		if err := h.buildIndex(); err != nil {
			return types.Batch{}, false, err
		}
		h.rt.built = true
	}
	if !h.rt.leftDrained {
		for {
			if len(h.rt.probe.pairs) > 0 {
				return h.flushPending()
			}
			leftBatch, ok, err := h.Left.Next()
			if err != nil {
				return types.Batch{}, false, err
			}
			if !ok {
				h.rt.leftDrained = true
				break
			}
			if h.rt.probe.leftSchema == nil {
				h.rt.probe.leftSchema = templatesFromColumns(leftBatch.Columns)
			}
			h.rt.probe.batch = leftBatch
			if err := h.collectPairs(leftBatch, leftBatch.Sel); err != nil {
				return types.Batch{}, false, err
			}
			if len(h.rt.probe.pairs) == 0 {
				continue
			}
			return h.flushPending()
		}
	}
	if h.Kind != sql.JoinRight && h.Kind != sql.JoinFull {
		return types.Batch{}, false, nil
	}
	return h.emitUnmatchedRight()
}

func (h *HashJoinOp) buildIndex() error {
	build := &h.rt.build
	for {
		batch, ok, err := h.Right.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		build.batches = append(build.batches, bufferedBatch{batch: batch, sel: selectionForBatch(batch)})
	}
	build.index = make(map[uint64][]hashBucket, len(build.batches)*types.StandardBatchRows)
	if h.Kind == sql.JoinRight || h.Kind == sql.JoinFull {
		build.matched = make([]types.SelectionMask, len(build.batches))
		for bi, bb := range build.batches {
			build.matched[bi] = types.NewSelectionMask(bb.batch.Len)
		}
	}
	for bi, bb := range build.batches {
		cols, err := resolveJoinKeyCols(bb.batch, h.RightKeys)
		if err != nil {
			return err
		}
		var loopErr error
		bb.sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			key, sum, ok, err := h.rt.key.encodeRow(bb.batch, cols, row)
			if err != nil {
				loopErr = err
				return
			}
			if !ok {
				return
			}
			build.add(key, sum, rightRowRef{batch: bi, row: row})
		})
		if loopErr != nil {
			return loopErr
		}
	}
	return nil
}

func (b *hashJoinBuild) add(key []byte, sum uint64, ref rightRowRef) {
	buckets := b.index[sum]
	for i := range buckets {
		if bytes.Equal(buckets[i].key, key) {
			buckets[i].rows = append(buckets[i].rows, ref)
			return
		}
	}
	owned := append([]byte(nil), key...)
	b.index[sum] = append(buckets, hashBucket{
		key:  owned,
		rows: []rightRowRef{ref},
	})
}

func (b *hashJoinBuild) lookup(key []byte, sum uint64) []rightRowRef {
	buckets := b.index[sum]
	for i := range buckets {
		if bytes.Equal(buckets[i].key, key) {
			return buckets[i].rows
		}
	}
	return nil
}

func (h *HashJoinOp) collectPairs(leftBatch types.Batch, leftSel *types.SelectionMask) error {
	probe := &h.rt.probe
	build := &h.rt.build
	probe.pairs = probe.pairs[:0]
	cols, err := resolveJoinKeyCols(leftBatch, h.LeftKeys)
	if err != nil {
		return err
	}
	leftOuter := h.Kind == sql.JoinLeft || h.Kind == sql.JoinFull
	var loopErr error
	leftSel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		key, sum, ok, err := h.rt.key.encodeRow(leftBatch, cols, row)
		if err != nil {
			loopErr = err
			return
		}
		if !ok {
			if leftOuter {
				probe.pairs = append(probe.pairs, joinPair{leftRow: row, nullFill: true})
			}
			return
		}
		matches := build.lookup(key, sum)
		if len(matches) == 0 {
			if leftOuter {
				probe.pairs = append(probe.pairs, joinPair{leftRow: row, nullFill: true})
			}
			return
		}
		for _, ref := range matches {
			if build.matched != nil {
				build.matched[ref.batch].Set(ref.row)
			}
			probe.pairs = append(probe.pairs, joinPair{leftRow: row, ref: ref})
		}
	})
	return loopErr
}

func (e *keyEncoder) encodeRow(batch types.Batch, cols []joinKeyCol, row int) ([]byte, uint64, bool, error) {
	e.buf = e.buf[:0]
	for _, kc := range cols {
		col := &batch.Columns[kc.idx]
		if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
			return nil, 0, false, nil
		}
		switch kc.kind {
		case types.VecInt16:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(int64(col.V.I16()[row])))
		case types.VecInt32, types.VecDate:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(int64(col.V.I32()[row])))
		case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(col.V.I64()[row]))
		case types.VecFloat32:
			x := float64(col.V.F32()[row])
			if x == 0 {
				x = 0
			}
			e.buf = append(e.buf, 'f')
			e.buf = binary.BigEndian.AppendUint64(e.buf, math.Float64bits(x))
		case types.VecFloat64:
			x := col.V.F64()[row]
			if x == 0 {
				x = 0
			}
			e.buf = append(e.buf, 'f')
			e.buf = binary.BigEndian.AppendUint64(e.buf, math.Float64bits(x))
		case types.VecBool:
			e.buf = append(e.buf, 'b')
			if col.V.BoolBits()[row>>3]&(1<<(row&7)) != 0 {
				e.buf = append(e.buf, 1)
			} else {
				e.buf = append(e.buf, 0)
			}
		case types.VecText, types.VecBytes, types.VecJSON:
			b := col.V.Var().Bytes(row)
			e.buf = append(e.buf, 's')
			e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(len(b)))
			e.buf = append(e.buf, b...)
		case types.VecUUID:
			s := types.FormatUUID(col.V.UUID()[row])
			e.buf = append(e.buf, 's')
			e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(len(s)))
			e.buf = append(e.buf, s...)
		case types.VecEnum32:
			code := col.V.U32()[row]
			if code == 0 || int(code-1) >= len(kc.enumLabels) {
				return nil, 0, false, fmt.Errorf("hashjoin: enum code %d out of range", code)
			}
			label := kc.enumLabels[code-1]
			e.buf = append(e.buf, 's')
			e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(len(label)))
			e.buf = append(e.buf, label...)
		default:
			return nil, 0, false, fmt.Errorf("hashjoin: unsupported key kind %v", kc.kind)
		}
	}
	return e.buf, maphash.Bytes(e.seed, e.buf), true, nil
}

// appendJoinKey writes a type-tagged deterministic byte form. Tags guard against future
// type drift even though the binder already rejects mismatched key types across sides.
func appendJoinKey(dst []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case int64:
		dst = append(dst, 'i')
		return binary.BigEndian.AppendUint64(dst, uint64(x)), nil
	case float64:
		if x == 0 {
			x = 0
		}
		dst = append(dst, 'f')
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(x)), nil
	case bool:
		if x {
			return append(dst, 'b', 1), nil
		}
		return append(dst, 'b', 0), nil
	case string:
		dst = append(dst, 's')
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(x)))
		return append(dst, x...), nil
	case []byte:
		dst = append(dst, 'x')
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(x)))
		return append(dst, x...), nil
	}
	return nil, fmt.Errorf("hashjoin: unsupported key type %T", v)
}

func (h *HashJoinOp) flushPending() (types.Batch, bool, error) {
	probe := &h.rt.probe
	build := &h.rt.build
	chunk := probe.pairs
	if len(chunk) > types.StandardBatchRows {
		chunk = chunk[:types.StandardBatchRows]
		probe.pairs = probe.pairs[types.StandardBatchRows:]
	} else {
		probe.pairs = nil
	}
	leftBatch := probe.batch
	if len(probe.pairs) == 0 {
		probe.batch = types.Batch{}
	}

	rows := len(chunk)
	totalCols := len(leftBatch.Columns)
	if len(build.batches) > 0 {
		totalCols += len(build.batches[0].batch.Columns)
	}
	cols := make([]types.Column, 0, totalCols)
	for _, c := range leftBatch.Columns {
		v, err := types.NewVecForKind(c.V.Kind, rows)
		if err != nil {
			return types.Batch{}, false, err
		}
		cols = append(cols, types.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v})
	}
	if len(build.batches) > 0 {
		for _, c := range build.batches[0].batch.Columns {
			v, err := types.NewVecForKind(c.V.Kind, rows)
			if err != nil {
				return types.Batch{}, false, err
			}
			cols = append(cols, types.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v})
		}
	}
	leftWidth := len(leftBatch.Columns)
	for dst, p := range chunk {
		for ci, sc := range leftBatch.Columns {
			if err := types.CopyVecRow(sc.V, p.leftRow, &cols[ci].V, dst); err != nil {
				return types.Batch{}, false, err
			}
		}
		if p.nullFill {
			for ci := leftWidth; ci < len(cols); ci++ {
				if cols[ci].V.Valid == nil {
					cols[ci].V.Valid = types.NewAllValid(int(cols[ci].V.Len))
				}
				cols[ci].V.Valid.SetInvalid(dst)
			}
			continue
		}
		rb := build.batches[p.ref.batch].batch
		for ci, sc := range rb.Columns {
			if err := types.CopyVecRow(sc.V, p.ref.row, &cols[leftWidth+ci].V, dst); err != nil {
				return types.Batch{}, false, err
			}
		}
	}
	out, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, false, fmt.Errorf("hashjoin: NewBatch: %w", err)
	}
	sel := types.NewSelectionMask(rows)
	sel.FillAll()
	out.Sel = &sel
	return out, true, nil
}

func (h *HashJoinOp) emitUnmatchedRight() (types.Batch, bool, error) {
	build := &h.rt.build
	if len(build.batches) == 0 {
		return types.Batch{}, false, nil
	}
	leftTpls, err := h.leftSchemaTemplates()
	if err != nil {
		return types.Batch{}, false, err
	}
	var refs []rightRowRef
	for h.rt.drain.batch < len(build.batches) {
		bb := build.batches[h.rt.drain.batch]
		for h.rt.drain.row < bb.batch.Len {
			r := h.rt.drain.row
			h.rt.drain.row++
			if !bb.sel.IsSet(r) {
				continue
			}
			if build.matched[h.rt.drain.batch].IsSet(r) {
				continue
			}
			refs = append(refs, rightRowRef{batch: h.rt.drain.batch, row: r})
			if len(refs) >= types.StandardBatchRows {
				break
			}
		}
		if len(refs) >= types.StandardBatchRows {
			break
		}
		h.rt.drain.batch++
		h.rt.drain.row = 0
	}
	if len(refs) == 0 {
		return types.Batch{}, false, nil
	}

	rows := len(refs)
	leftCols, err := allocColumns(leftTpls, rows)
	if err != nil {
		return types.Batch{}, false, err
	}
	rightCols := make([]types.Column, len(build.batches[0].batch.Columns))
	for i, c := range build.batches[0].batch.Columns {
		v, err := types.NewVecForKind(c.V.Kind, rows)
		if err != nil {
			return types.Batch{}, false, err
		}
		rightCols[i] = types.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
	}
	cols := append(leftCols, rightCols...)
	leftWidth := len(leftCols)
	for dst, r := range refs {
		for ci := 0; ci < leftWidth; ci++ {
			if cols[ci].V.Valid == nil {
				cols[ci].V.Valid = types.NewAllValid(int(cols[ci].V.Len))
			}
			cols[ci].V.Valid.SetInvalid(dst)
		}
		rb := build.batches[r.batch].batch
		for ci, sc := range rb.Columns {
			if err := types.CopyVecRow(sc.V, r.row, &cols[leftWidth+ci].V, dst); err != nil {
				return types.Batch{}, false, err
			}
		}
	}
	out, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, false, fmt.Errorf("hashjoin: NewBatch: %w", err)
	}
	sel := types.NewSelectionMask(rows)
	sel.FillAll()
	out.Sel = &sel
	return out, true, nil
}

func (h *HashJoinOp) leftSchemaTemplates() ([]columnTemplate, error) {
	if h.rt.probe.leftSchema != nil {
		return h.rt.probe.leftSchema, nil
	}
	if len(h.LeftOutputs) == 0 {
		return nil, fmt.Errorf("hashjoin: %v with empty left side requires a left-schema template", h.Kind)
	}
	tpls := make([]columnTemplate, len(h.LeftOutputs))
	for i, o := range h.LeftOutputs {
		vk, err := types.VecKindOf(o.Expr.Type)
		if err != nil {
			return nil, fmt.Errorf("hashjoin: left output %q: %w", o.Expr.Column, err)
		}
		name := o.Alias
		if name == "" {
			name = o.Expr.Column
		}
		tpls[i] = columnTemplate{Name: name, Type: o.Expr.Type, Kind: vk}
	}
	return tpls, nil
}

func templatesFromColumns(cols []types.Column) []columnTemplate {
	out := make([]columnTemplate, len(cols))
	for i, c := range cols {
		out[i] = columnTemplate{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, Kind: c.V.Kind}
	}
	return out
}

func allocColumns(tpls []columnTemplate, rows int) ([]types.Column, error) {
	cols := make([]types.Column, len(tpls))
	for i, t := range tpls {
		v, err := types.NewVecForKind(t.Kind, rows)
		if err != nil {
			return nil, err
		}
		cols[i] = types.Column{Name: t.Name, Type: t.Type, EnumLabels: t.EnumLabels, V: v}
	}
	return cols, nil
}

func (h *HashJoinOp) Close() error {
	h.state.close()
	leftErr := h.Left.Close()
	rightErr := h.Right.Close()
	if leftErr != nil {
		return leftErr
	}
	return rightErr
}
