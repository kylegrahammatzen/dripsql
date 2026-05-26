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

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
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
	key   keyEncoder

	leftKeys  []joinKeyCol
	rightKeys []joinKeyCol
	leftTpls  []columnTemplate
	rightTpls []columnTemplate

	probeBatch vector.Batch
	pending    []joinRow

	drainBatch int
	drainRow   int
}

type hashJoinBuild struct {
	batches []bufferedBatch
	index   map[uint64][]hashBucket
	matched []vector.SelectionMask
}

type hashBucket struct {
	key  []byte
	rows []rightRowRef
}

type keyEncoder struct {
	buf  []byte
	seed maphash.Seed
}

type joinKeyCol struct {
	idx        int
	kind       vector.VecKind
	enumLabels []string
}

type columnTemplate struct {
	Name       string
	Type       schema.Type
	EnumLabels []string
	Kind       vector.VecKind
}

// joinRow flags whether each side contributes real data or a null fill. Probe-matched rows
// set both, left-outer misses set only hasLeft, right-outer drain rows set only hasRight.
type joinRow struct {
	leftRow  int
	right    rightRowRef
	hasLeft  bool
	hasRight bool
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
	h.rt = hashJoinRuntime{key: keyEncoder{seed: maphash.MakeSeed()}}
	return nil
}

func (h *HashJoinOp) Next() (vector.Batch, bool, error) {
	if err := h.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	if !h.rt.built {
		if err := h.buildIndex(); err != nil {
			return vector.Batch{}, false, err
		}
		h.rt.built = true
	}
	if !h.rt.leftDrained {
		for {
			if len(h.rt.pending) > 0 {
				return h.emitChunk()
			}
			leftBatch, ok, err := h.Left.Next()
			if err != nil {
				return vector.Batch{}, false, err
			}
			if !ok {
				h.rt.leftDrained = true
				break
			}
			if err := h.resolveLeft(leftBatch); err != nil {
				return vector.Batch{}, false, err
			}
			h.rt.probeBatch = leftBatch
			if err := h.collectRows(leftBatch); err != nil {
				return vector.Batch{}, false, err
			}
			if len(h.rt.pending) == 0 {
				continue
			}
			return h.emitChunk()
		}
	}
	if h.Kind != sql.JoinRight && h.Kind != sql.JoinFull {
		return vector.Batch{}, false, nil
	}
	return h.drainUnmatchedRight()
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
		if err := h.resolveRight(batch); err != nil {
			return err
		}
		build.batches = append(build.batches, bufferedBatch{batch: batch, sel: selectionForBatch(batch)})
	}
	build.index = make(map[uint64][]hashBucket, len(build.batches)*vector.StandardBatchRows)
	if h.Kind == sql.JoinRight || h.Kind == sql.JoinFull {
		build.matched = make([]vector.SelectionMask, len(build.batches))
		for bi, bb := range build.batches {
			build.matched[bi] = vector.NewSelectionMask(bb.batch.Len)
		}
	}
	for bi, bb := range build.batches {
		var loopErr error
		bb.sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			key, sum, ok, err := h.rt.key.encodeRow(bb.batch, h.rt.rightKeys, row)
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

func (h *HashJoinOp) resolveLeft(batch vector.Batch) error {
	if h.rt.leftKeys != nil {
		return nil
	}
	cols, err := resolveJoinKeyCols(batch, h.LeftKeys)
	if err != nil {
		return err
	}
	h.rt.leftKeys = cols
	h.rt.leftTpls = templatesFromColumns(batch.Columns)
	return nil
}

func (h *HashJoinOp) resolveRight(batch vector.Batch) error {
	if h.rt.rightKeys != nil {
		return nil
	}
	cols, err := resolveJoinKeyCols(batch, h.RightKeys)
	if err != nil {
		return err
	}
	h.rt.rightKeys = cols
	h.rt.rightTpls = templatesFromColumns(batch.Columns)
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
	b.index[sum] = append(buckets, hashBucket{key: owned, rows: []rightRowRef{ref}})
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

func (h *HashJoinOp) collectRows(left vector.Batch) error {
	rt := &h.rt
	build := &rt.build
	rt.pending = rt.pending[:0]
	leftOuter := h.Kind == sql.JoinLeft || h.Kind == sql.JoinFull
	var loopErr error
	left.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		key, sum, ok, err := rt.key.encodeRow(left, rt.leftKeys, row)
		if err != nil {
			loopErr = err
			return
		}
		if !ok {
			if leftOuter {
				rt.pending = append(rt.pending, joinRow{leftRow: row, hasLeft: true})
			}
			return
		}
		matches := build.lookup(key, sum)
		if len(matches) == 0 {
			if leftOuter {
				rt.pending = append(rt.pending, joinRow{leftRow: row, hasLeft: true})
			}
			return
		}
		for _, ref := range matches {
			if build.matched != nil {
				build.matched[ref.batch].Set(ref.row)
			}
			rt.pending = append(rt.pending, joinRow{leftRow: row, right: ref, hasLeft: true, hasRight: true})
		}
	})
	return loopErr
}

// resolveJoinKeyCols pre-resolves the column index for each join key. splitJoinEquality
// already requires keys to be column references, so the type switch in encodeRow can
// dispatch on VecKind without going through evalCtx or ColumnByName per row.
func resolveJoinKeyCols(batch vector.Batch, keys []sql.BoundExpr) ([]joinKeyCol, error) {
	cols := make([]joinKeyCol, len(keys))
	for i, k := range keys {
		if k.Op != sql.ExprColumn {
			return nil, fmt.Errorf("hashjoin: join key must be a column reference, got op %v", k.Op)
		}
		idx := -1
		for j := range batch.Columns {
			if batch.Columns[j].Name == k.Column {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("hashjoin: column %q not in batch", k.Column)
		}
		c := &batch.Columns[idx]
		cols[i] = joinKeyCol{idx: idx, kind: c.V.Kind, enumLabels: c.EnumLabels}
	}
	return cols, nil
}

// encodeRow writes a type-tagged deterministic byte form of the row's join key. Tags
// guard against type drift between sides. -0.0 is canonicalised to +0.0 so float keys
// compare equal across rows that wrote the negative sign bit.
func (e *keyEncoder) encodeRow(batch vector.Batch, cols []joinKeyCol, row int) ([]byte, uint64, bool, error) {
	e.buf = e.buf[:0]
	for _, kc := range cols {
		col := &batch.Columns[kc.idx]
		if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
			return nil, 0, false, nil
		}
		switch kc.kind {
		case vector.VecInt16:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(int64(col.V.I16()[row])))
		case vector.VecInt32, vector.VecDate:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(int64(col.V.I32()[row])))
		case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			e.buf = append(e.buf, 'i')
			e.buf = binary.BigEndian.AppendUint64(e.buf, uint64(col.V.I64()[row]))
		case vector.VecFloat32:
			x := float64(col.V.F32()[row])
			if x == 0 {
				x = 0
			}
			e.buf = append(e.buf, 'f')
			e.buf = binary.BigEndian.AppendUint64(e.buf, math.Float64bits(x))
		case vector.VecFloat64:
			x := col.V.F64()[row]
			if x == 0 {
				x = 0
			}
			e.buf = append(e.buf, 'f')
			e.buf = binary.BigEndian.AppendUint64(e.buf, math.Float64bits(x))
		case vector.VecBool:
			e.buf = append(e.buf, 'b')
			if col.V.BoolBits()[row>>3]&(1<<(row&7)) != 0 {
				e.buf = append(e.buf, 1)
			} else {
				e.buf = append(e.buf, 0)
			}
		case vector.VecText, vector.VecBytes, vector.VecJSON:
			b := col.V.Var().Bytes(row)
			e.buf = append(e.buf, 's')
			e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(len(b)))
			e.buf = append(e.buf, b...)
		case vector.VecUUID:
			s := vector.FormatUUID(col.V.UUID()[row])
			e.buf = append(e.buf, 's')
			e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(len(s)))
			e.buf = append(e.buf, s...)
		case vector.VecEnum32:
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

func (h *HashJoinOp) emitChunk() (vector.Batch, bool, error) {
	chunk := h.rt.pending
	if len(chunk) > vector.StandardBatchRows {
		chunk = chunk[:vector.StandardBatchRows]
		h.rt.pending = h.rt.pending[vector.StandardBatchRows:]
	} else {
		h.rt.pending = nil
	}
	left := h.rt.probeBatch
	if len(h.rt.pending) == 0 {
		h.rt.probeBatch = vector.Batch{}
	}
	return h.emitRows(&left, chunk)
}

func (h *HashJoinOp) drainUnmatchedRight() (vector.Batch, bool, error) {
	build := &h.rt.build
	if len(build.batches) == 0 {
		return vector.Batch{}, false, nil
	}
	rows := h.rt.pending[:0]
	for h.rt.drainBatch < len(build.batches) {
		bb := build.batches[h.rt.drainBatch]
		for h.rt.drainRow < bb.batch.Len {
			r := h.rt.drainRow
			h.rt.drainRow++
			if !bb.sel.IsSet(r) {
				continue
			}
			if build.matched[h.rt.drainBatch].IsSet(r) {
				continue
			}
			rows = append(rows, joinRow{right: rightRowRef{batch: h.rt.drainBatch, row: r}, hasRight: true})
			if len(rows) >= vector.StandardBatchRows {
				break
			}
		}
		if len(rows) >= vector.StandardBatchRows {
			break
		}
		h.rt.drainBatch++
		h.rt.drainRow = 0
	}
	h.rt.pending = rows[:0]
	if len(rows) == 0 {
		return vector.Batch{}, false, nil
	}
	return h.emitRows(nil, rows)
}

func (h *HashJoinOp) emitRows(left *vector.Batch, rows []joinRow) (vector.Batch, bool, error) {
	if len(rows) == 0 {
		return vector.Batch{}, false, nil
	}
	build := &h.rt.build
	n := len(rows)

	leftTpls, err := h.leftTemplates(left)
	if err != nil {
		return vector.Batch{}, false, err
	}
	rightTpls, err := h.rightTemplates()
	if err != nil {
		return vector.Batch{}, false, err
	}
	leftCols, err := allocColumns(leftTpls, n)
	if err != nil {
		return vector.Batch{}, false, err
	}
	rightCols, err := allocColumns(rightTpls, n)
	if err != nil {
		return vector.Batch{}, false, err
	}
	cols := append(leftCols, rightCols...)
	leftWidth := len(leftCols)

	for dst, r := range rows {
		if r.hasLeft && left != nil {
			for ci, sc := range left.Columns {
				if err := vector.CopyVecRow(sc.V, r.leftRow, &cols[ci].V, dst); err != nil {
					return vector.Batch{}, false, err
				}
			}
		} else {
			nullFillRow(cols[:leftWidth], dst)
		}
		if r.hasRight {
			rb := build.batches[r.right.batch].batch
			for ci, sc := range rb.Columns {
				if err := vector.CopyVecRow(sc.V, r.right.row, &cols[leftWidth+ci].V, dst); err != nil {
					return vector.Batch{}, false, err
				}
			}
		} else {
			nullFillRow(cols[leftWidth:], dst)
		}
	}

	out, err := vector.NewBatch(cols)
	if err != nil {
		return vector.Batch{}, false, fmt.Errorf("hashjoin: NewBatch: %w", err)
	}
	sel := vector.NewSelectionMask(n)
	sel.FillAll()
	if err := out.SetSel(&sel); err != nil {
		return vector.Batch{}, false, fmt.Errorf("hashjoin: %w", err)
	}
	return out, true, nil
}

func nullFillRow(cols []vector.Column, row int) {
	for i := range cols {
		if cols[i].V.Valid == nil {
			cols[i].V.Valid = vector.NewAllValid(int(cols[i].V.Len))
		}
		cols[i].V.Valid.SetInvalid(row)
	}
}

func (h *HashJoinOp) leftTemplates(left *vector.Batch) ([]columnTemplate, error) {
	if left != nil && len(left.Columns) > 0 {
		return templatesFromColumns(left.Columns), nil
	}
	if h.rt.leftTpls != nil {
		return h.rt.leftTpls, nil
	}
	return templatesFromOutputs(h.LeftOutputs)
}

func (h *HashJoinOp) rightTemplates() ([]columnTemplate, error) {
	if h.rt.rightTpls != nil {
		return h.rt.rightTpls, nil
	}
	return templatesFromOutputs(h.RightOutputs)
}

func templatesFromColumns(cols []vector.Column) []columnTemplate {
	out := make([]columnTemplate, len(cols))
	for i, c := range cols {
		out[i] = columnTemplate{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, Kind: c.V.Kind}
	}
	return out
}

func templatesFromOutputs(outputs []sql.BoundOutput) ([]columnTemplate, error) {
	if len(outputs) == 0 {
		return nil, fmt.Errorf("hashjoin: no output schema available")
	}
	tpls := make([]columnTemplate, len(outputs))
	for i, o := range outputs {
		kind, err := vector.VecKindOf(o.Expr.Type)
		if err != nil {
			return nil, err
		}
		name := o.Alias
		if name == "" {
			name = o.Expr.Column
		}
		tpls[i] = columnTemplate{Name: name, Type: o.Expr.Type, Kind: kind}
	}
	return tpls, nil
}

func allocColumns(tpls []columnTemplate, rows int) ([]vector.Column, error) {
	cols := make([]vector.Column, len(tpls))
	for i, t := range tpls {
		v, err := vector.NewVecForKind(t.Kind, rows)
		if err != nil {
			return nil, err
		}
		cols[i] = vector.Column{Name: t.Name, Type: t.Type, EnumLabels: t.EnumLabels, V: v}
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
