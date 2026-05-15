// AggregateOp drains its source, builds per-group accumulators, and emits chunks of StandardBatchRows.
// HAVING is applied per chunk by narrowing the emitted SelectionMask after each chunk materializes.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type AggregateOp struct {
	Source     Operator
	GroupBy    []sql.BoundExpr
	Aggregates []sql.AggSpec
	Hidden     []sql.AggSpec
	Having     *sql.BoundExpr

	state  operatorState
	built  bool
	cursor int
	groups []aggGroup
	specs  []sql.AggSpec
}

type aggAccum struct {
	count int64
	sum   int64
	min   int64
	max   int64
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

func (a *AggregateOp) Next() (types.Batch, bool, error) {
	if err := a.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	if !a.built {
		if err := a.build(); err != nil {
			return types.Batch{}, false, err
		}
		a.built = true
	}
	for a.cursor < len(a.groups) {
		end := a.cursor + types.StandardBatchRows
		if end > len(a.groups) {
			end = len(a.groups)
		}
		batch, sel, err := a.materializeChunk(a.cursor, end)
		a.cursor = end
		if err != nil {
			return types.Batch{}, false, err
		}
		if a.Having != nil {
			filtered, err := a.applyHaving(batch, sel)
			if err != nil {
				return types.Batch{}, false, err
			}
			sel = filtered
		}
		if sel.PopCount() == 0 {
			continue
		}
		batch.Sel = sel
		return batch, true, nil
	}
	return types.Batch{}, false, nil
}

func (a *AggregateOp) build() error {
	if len(a.GroupBy) > 1 {
		return fmt.Errorf("aggregate: only one GROUP BY expression is supported")
	}
	if len(a.GroupBy) == 1 && a.GroupBy[0].Op != sql.ExprColumn {
		return fmt.Errorf("aggregate: only column-ref GROUP BY is supported")
	}

	a.specs = append(append([]sql.AggSpec{}, a.Aggregates...), a.Hidden...)
	groupIdx := make(map[any]int)

	getGroup := func(key any) *aggGroup {
		idx, ok := groupIdx[key]
		if !ok {
			idx = len(a.groups)
			groupIdx[key] = idx
			a.groups = append(a.groups, aggGroup{key: key, aggs: make([]aggAccum, len(a.specs))})
		}
		return &a.groups[idx]
	}

	grouped := len(a.GroupBy) == 1
	specCols := make([]aggCol, len(a.specs))
	for {
		batch, ok, err := a.Source.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		groupIdx := -1
		var groupKind types.VecKind
		if grouped {
			groupIdx = batchColumnIndex(batch, a.GroupBy[0].Column)
			if groupIdx == -1 {
				return fmt.Errorf("aggregate: group column %q not in batch", a.GroupBy[0].Column)
			}
			groupKind = batch.Columns[groupIdx].V.Kind
		}
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
		}
		var loopErr error
		batch.Sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			var key any
			if grouped {
				col := &batch.Columns[groupIdx]
				if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
					return
				}
				key = readGroupKey(col, groupKind, row)
				if key == nil {
					return
				}
			}
			g := getGroup(key)
			for i, spec := range a.specs {
				if err := updateAccumFast(&g.aggs[i], &batch.Columns[max(specCols[i].idx, 0)], specCols[i], spec, row); err != nil {
					loopErr = err
					return
				}
			}
		})
		if loopErr != nil {
			return loopErr
		}
	}

	if !grouped && len(a.groups) == 0 {
		a.groups = append(a.groups, aggGroup{key: nil, aggs: make([]aggAccum, len(a.specs))})
	}
	return nil
}

type aggCol struct {
	idx int
	vk  types.VecKind
}

func batchColumnIndex(batch types.Batch, name string) int {
	for i := range batch.Columns {
		if batch.Columns[i].Name == name {
			return i
		}
	}
	return -1
}

func readGroupKey(col *types.Column, vk types.VecKind, row int) any {
	switch vk {
	case types.VecInt16:
		return int64(col.V.I16()[row])
	case types.VecInt32, types.VecDate:
		return int64(col.V.I32()[row])
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		return col.V.I64()[row]
	case types.VecBool:
		return col.V.BoolBits()[row>>3]&(1<<(row&7)) != 0
	case types.VecText, types.VecBytes, types.VecJSON:
		return string(col.V.Var().Bytes(row))
	case types.VecUUID:
		return types.FormatUUID(col.V.UUID()[row])
	case types.VecEnum32:
		code := col.V.U32()[row]
		if code == 0 || int(code-1) >= len(col.EnumLabels) {
			return nil
		}
		return col.EnumLabels[code-1]
	}
	return nil
}

func updateAccumFast(acc *aggAccum, col *types.Column, rc aggCol, spec sql.AggSpec, row int) error {
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
	var x int64
	switch rc.vk {
	case types.VecInt16:
		x = int64(col.V.I16()[row])
	case types.VecInt32, types.VecDate:
		x = int64(col.V.I32()[row])
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		x = col.V.I64()[row]
	default:
		return fmt.Errorf("aggregate: %v on non-integer column %q (kind %v)", spec.Func, spec.ArgName, rc.vk)
	}
	acc.count++
	switch spec.Func {
	case sql.AggregateSum:
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

func (a *AggregateOp) materializeChunk(start, end int) (types.Batch, *types.SelectionMask, error) {
	chunk := a.groups[start:end]
	rows := len(chunk)
	var cols []types.Column

	if len(a.GroupBy) == 1 {
		expr := a.GroupBy[0]
		v, err := buildGroupKeyVec(expr.Type, chunk)
		if err != nil {
			return types.Batch{}, nil, err
		}
		cols = append(cols, types.Column{Name: expr.Column, Type: expr.Type, V: v})
	}

	for i, spec := range a.specs {
		v, valid, err := buildAggregateVec(spec, chunk, i)
		if err != nil {
			return types.Batch{}, nil, err
		}
		v.Valid = valid
		name := aggregateColumnName(spec)
		cols = append(cols, types.Column{Name: name, Type: types.Int64, V: v})
	}

	out, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, nil, fmt.Errorf("aggregate: NewBatch: %w", err)
	}
	out.Len = rows
	for i := range out.Columns {
		out.Columns[i].V.Len = int32(rows)
	}

	sel := types.NewSelectionMask(rows)
	sel.FillAll()
	return out, &sel, nil
}

func (a *AggregateOp) applyHaving(batch types.Batch, sel *types.SelectionMask) (*types.SelectionMask, error) {
	ctx := newEvalCtx(batch)
	filtered := types.NewSelectionMask(batch.Len)
	var loopErr error
	sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		pass, err := ctx.EvalBool(*a.Having, row)
		if err != nil {
			loopErr = err
			return
		}
		if pass {
			filtered.Set(row)
		}
	})
	if loopErr != nil {
		return nil, loopErr
	}
	return &filtered, nil
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
	default:
		return "count"
	}
}

func buildGroupKeyVec(t types.Type, groups []aggGroup) (types.Vec, error) {
	rows := len(groups)
	vk, err := types.VecKindOf(t)
	if err != nil {
		return types.Vec{}, fmt.Errorf("aggregate: group key kind: %w", err)
	}
	switch vk {
	case types.VecInt16:
		v := types.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return types.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I16()[i] = int16(x)
		}
		return v, nil
	case types.VecInt32, types.VecDate:
		v := types.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return types.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I32()[i] = int32(x)
		}
		return v, nil
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		v := types.NewVec(vk, rows)
		for i, g := range groups {
			x, ok := asInt64(g.key)
			if !ok {
				return types.Vec{}, fmt.Errorf("aggregate: group key %v not int", g.key)
			}
			v.I64()[i] = x
		}
		return v, nil
	case types.VecText, types.VecBytes, types.VecJSON:
		v := types.NewVarVec(vk, rows, 0)
		for i, g := range groups {
			s, ok := g.key.(string)
			if !ok {
				return types.Vec{}, fmt.Errorf("aggregate: group key %v not string", g.key)
			}
			v.Var().AppendString(i, s)
		}
		return v, nil
	case types.VecBool:
		v := types.NewVec(types.VecBool, rows)
		bits := v.BoolBits()
		for i, g := range groups {
			b, ok := g.key.(bool)
			if !ok {
				return types.Vec{}, fmt.Errorf("aggregate: group key %v not bool", g.key)
			}
			if b {
				bits[i>>3] |= 1 << (i & 7)
			}
		}
		return v, nil
	}
	return types.Vec{}, fmt.Errorf("aggregate: group key kind %v not supported", vk)
}

func buildAggregateVec(spec sql.AggSpec, groups []aggGroup, slot int) (types.Vec, types.Validity, error) {
	rows := len(groups)
	v := types.NewVec(types.VecInt64, rows)
	data := v.I64()
	var valid types.Validity
	for i, g := range groups {
		acc := g.aggs[slot]
		switch spec.Func {
		case sql.AggregateCount:
			data[i] = acc.count
		case sql.AggregateSum:
			if !acc.init {
				if valid == nil {
					valid = types.NewAllValid(rows)
				}
				valid.SetInvalid(i)
				continue
			}
			data[i] = acc.sum
		case sql.AggregateMin:
			if !acc.init {
				if valid == nil {
					valid = types.NewAllValid(rows)
				}
				valid.SetInvalid(i)
				continue
			}
			data[i] = acc.min
		case sql.AggregateMax:
			if !acc.init {
				if valid == nil {
					valid = types.NewAllValid(rows)
				}
				valid.SetInvalid(i)
				continue
			}
			data[i] = acc.max
		default:
			return types.Vec{}, nil, fmt.Errorf("aggregate: unsupported func %v", spec.Func)
		}
	}
	return v, valid, nil
}


func (a *AggregateOp) Close() error {
	a.state.close()
	return a.Source.Close()
}
