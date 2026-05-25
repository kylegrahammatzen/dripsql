// Window function operator. Buffers the source, sorts by (partition, order),
// computes per-partition ranks or aggregates, scatters back to source order.
package exec

import (
	"context"
	"fmt"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type WindowOp struct {
	Source Operator
	Funcs  []sql.WindowFunc

	state    operatorState
	built    bool
	cursor   int
	rows     []windowRow
	order    []int
	winCols  []windowColumn
	batches  []types.Batch
}

type windowColumn struct {
	i64     []int64
	f64     []float64
	isFloat bool
	valid   []bool
}

type windowRow struct {
	batchIdx int
	rowIdx   int
}

func (w *WindowOp) Open(ctx context.Context) error {
	prev := w.state
	if err := w.state.open(); err != nil {
		return err
	}
	if err := w.Source.Open(ctx); err != nil {
		w.state = prev
		return err
	}
	w.built = false
	w.cursor = 0
	w.rows = nil
	w.order = nil
	w.winCols = nil
	w.batches = nil
	return nil
}

func (w *WindowOp) buildBatches() error {
	for {
		batch, ok, err := w.Source.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		visible := batch.VisibleLen()
		if visible == 0 {
			continue
		}
		bi := len(w.batches)
		w.batches = append(w.batches, batch)
		iter := batch.Sel
		if iter == nil {
			for row := range batch.Len {
				w.rows = append(w.rows, windowRow{batchIdx: bi, rowIdx: row})
			}
			continue
		}
		iter.IterSet(func(row int) {
			w.rows = append(w.rows, windowRow{batchIdx: bi, rowIdx: row})
		})
	}
}

func (w *WindowOp) build() error {
	if err := w.buildBatches(); err != nil {
		return err
	}
	if len(w.rows) == 0 {
		return nil
	}
	w.winCols = make([]windowColumn, len(w.Funcs))
	for fi, wf := range w.Funcs {
		col, err := w.computeWindowColumn(wf)
		if err != nil {
			return err
		}
		w.winCols[fi] = col
	}
	return nil
}

func (w *WindowOp) computeWindowColumn(wf sql.WindowFunc) (windowColumn, error) {
	n := len(w.rows)
	if wf.Func.IsAggregate() {
		out := windowColumn{}
		argFloat := wf.Arg != nil && (wf.Arg.Type.Kind == types.KindFloat32 || wf.Arg.Type.Kind == types.KindFloat64)
		out.isFloat = argFloat || wf.Func == sql.WindowAvg
		if out.isFloat {
			out.f64 = make([]float64, n)
		} else {
			out.i64 = make([]int64, n)
		}
		out.valid = make([]bool, n)
		if err := w.computeAggregate(wf, &out); err != nil {
			return windowColumn{}, err
		}
		return out, nil
	}
	out := windowColumn{i64: make([]int64, n), valid: nil}
	if err := w.computeRankLike(wf, out.i64); err != nil {
		return windowColumn{}, err
	}
	return out, nil
}

func (w *WindowOp) computeRankLike(wf sql.WindowFunc, col []int64) error {
	n := len(w.rows)
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	cmp, err := w.makeWindowCmp(wf)
	if err != nil {
		return err
	}
	slices.SortStableFunc(perm, cmp.byPartitionThenOrder)
	switch wf.Func {
	case sql.WindowRowNumber:
		var rank int64
		for i, src := range perm {
			if i > 0 && cmp.partitionChanged(perm[i-1], src) {
				rank = 0
			}
			rank++
			col[src] = rank
		}
	case sql.WindowRank:
		var rank, peerCount int64
		for i, src := range perm {
			if i == 0 || cmp.partitionChanged(perm[i-1], src) {
				rank = 1
				peerCount = 1
			} else if cmp.orderChanged(perm[i-1], src) {
				rank += peerCount
				peerCount = 1
			} else {
				peerCount++
			}
			col[src] = rank
		}
	case sql.WindowDenseRank:
		var rank int64
		for i, src := range perm {
			if i == 0 || cmp.partitionChanged(perm[i-1], src) {
				rank = 1
			} else if cmp.orderChanged(perm[i-1], src) {
				rank++
			}
			col[src] = rank
		}
	default:
		return fmt.Errorf("window: unsupported func %v", wf.Func)
	}
	return nil
}

func (w *WindowOp) computeAggregate(wf sql.WindowFunc, out *windowColumn) error {
	n := len(w.rows)
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	cmp, err := w.makeWindowCmp(wf)
	if err != nil {
		return err
	}
	slices.SortStableFunc(perm, cmp.byPartitionThenOrder)
	if wf.Frame != nil {
		if wf.Frame.IsRange {
			return w.fillRangeAggregate(wf, perm, cmp, out)
		}
		return w.fillFramedAggregate(wf, perm, cmp, out)
	}
	if len(wf.OrderBy) == 0 {
		return w.fillPartitionAggregate(wf, perm, cmp, out)
	}
	return w.fillRunningAggregate(wf, perm, cmp, out)
}

func (w *WindowOp) fillFramedAggregate(wf sql.WindowFunc, perm []int, cmp windowCmp, out *windowColumn) error {
	partStart := 0
	for i := 0; i <= len(perm); i++ {
		isBoundary := i == len(perm) || (i > partStart && cmp.partitionChanged(perm[i-1], perm[i]))
		if !isBoundary {
			continue
		}
		partition := perm[partStart:i]
		for pi, src := range partition {
			lo, hi := frameRange(wf.Frame, pi, len(partition))
			var acc windowAcc
			acc.init(wf, out.isFloat)
			for k := lo; k <= hi; k++ {
				v, err := w.argValue(wf, partition[k])
				if err != nil {
					return err
				}
				acc.update(wf, v)
			}
			val, valid := acc.finalize(wf)
			if out.isFloat {
				out.f64[src] = val.f
			} else {
				out.i64[src] = val.i
			}
			out.valid[src] = valid
		}
		partStart = i
	}
	return nil
}

func (w *WindowOp) fillRangeAggregate(wf sql.WindowFunc, perm []int, cmp windowCmp, out *windowColumn) error {
	if len(wf.OrderBy) != 1 {
		return fmt.Errorf("window: RANGE frames require exactly one ORDER BY")
	}
	orderKey := wf.OrderBy[0]
	desc := orderKey.Desc
	partStart := 0
	for i := 0; i <= len(perm); i++ {
		isBoundary := i == len(perm) || (i > partStart && cmp.partitionChanged(perm[i-1], perm[i]))
		if !isBoundary {
			continue
		}
		partition := perm[partStart:i]
		values := make([]float64, len(partition))
		for j, src := range partition {
			v, err := w.evalOrderValue(orderKey.Expr, src)
			if err != nil {
				return err
			}
			values[j] = v
		}
		for pi, src := range partition {
			lo, hi := rangeFrameBounds(wf.Frame, values, pi, desc)
			var acc windowAcc
			acc.init(wf, out.isFloat)
			for k := lo; k <= hi; k++ {
				v, err := w.argValue(wf, partition[k])
				if err != nil {
					return err
				}
				acc.update(wf, v)
			}
			val, valid := acc.finalize(wf)
			if out.isFloat {
				out.f64[src] = val.f
			} else {
				out.i64[src] = val.i
			}
			out.valid[src] = valid
		}
		partStart = i
	}
	return nil
}

func (w *WindowOp) evalOrderValue(expr sql.BoundExpr, row int) (float64, error) {
	wr := w.rows[row]
	ctx := newEvalCtx(w.batches[wr.batchIdx])
	v, err := ctx.eval(expr, wr.rowIdx)
	if err != nil {
		return 0, err
	}
	switch x := v.(type) {
	case int64:
		return float64(x), nil
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case nil:
		return 0, fmt.Errorf("window: NULL order key not supported with RANGE frame")
	}
	return 0, fmt.Errorf("window: RANGE frame order key kind %T not supported", v)
}

func rangeFrameBounds(f *sql.WindowFrameBounds, values []float64, i int, desc bool) (int, int) {
	pivot := values[i]
	lo := 0
	hi := len(values) - 1
	if !desc {
		if f.StartUnbounded {
			lo = 0
		} else if f.StartCurrent {
			lo = i
			for lo > 0 && values[lo-1] == pivot {
				lo--
			}
		} else {
			thresh := pivot - float64(f.StartPreceding)
			for lo < len(values) && values[lo] < thresh {
				lo++
			}
		}
		if f.EndUnbounded {
			hi = len(values) - 1
		} else if f.EndCurrent {
			hi = i
			for hi+1 < len(values) && values[hi+1] == pivot {
				hi++
			}
		} else {
			thresh := pivot + float64(f.EndFollowing)
			for hi >= 0 && values[hi] > thresh {
				hi--
			}
		}
	} else {
		if f.StartUnbounded {
			lo = 0
		} else if f.StartCurrent {
			lo = i
			for lo > 0 && values[lo-1] == pivot {
				lo--
			}
		} else {
			thresh := pivot + float64(f.StartPreceding)
			for lo < len(values) && values[lo] > thresh {
				lo++
			}
		}
		if f.EndUnbounded {
			hi = len(values) - 1
		} else if f.EndCurrent {
			hi = i
			for hi+1 < len(values) && values[hi+1] == pivot {
				hi++
			}
		} else {
			thresh := pivot - float64(f.EndFollowing)
			for hi >= 0 && values[hi] < thresh {
				hi--
			}
		}
	}
	if lo < 0 {
		lo = 0
	}
	if hi >= len(values) {
		hi = len(values) - 1
	}
	return lo, hi
}

func frameRange(f *sql.WindowFrameBounds, i, n int) (int, int) {
	lo := 0
	hi := n - 1
	switch {
	case f.StartUnbounded:
		lo = 0
	case f.StartCurrent:
		lo = i
	default:
		lo = i - int(f.StartPreceding)
	}
	switch {
	case f.EndUnbounded:
		hi = n - 1
	case f.EndCurrent:
		hi = i
	default:
		hi = i + int(f.EndFollowing)
	}
	if lo < 0 {
		lo = 0
	}
	if hi >= n {
		hi = n - 1
	}
	return lo, hi
}

func (w *WindowOp) fillPartitionAggregate(wf sql.WindowFunc, perm []int, cmp windowCmp, out *windowColumn) error {
	start := 0
	for i := 0; i <= len(perm); i++ {
		if i < len(perm) && i > 0 && cmp.partitionChanged(perm[i-1], perm[i]) {
			if err := w.scatterAggregate(wf, perm[start:i], out); err != nil {
				return err
			}
			start = i
		}
		if i == len(perm) {
			if err := w.scatterAggregate(wf, perm[start:i], out); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *WindowOp) scatterAggregate(wf sql.WindowFunc, rows []int, out *windowColumn) error {
	var acc windowAcc
	acc.init(wf, out.isFloat)
	for _, src := range rows {
		v, err := w.argValue(wf, src)
		if err != nil {
			return err
		}
		acc.update(wf, v)
	}
	val, valid := acc.finalize(wf)
	for _, src := range rows {
		if out.isFloat {
			out.f64[src] = val.f
		} else {
			out.i64[src] = val.i
		}
		out.valid[src] = valid
	}
	return nil
}

func (w *WindowOp) fillRunningAggregate(wf sql.WindowFunc, perm []int, cmp windowCmp, out *windowColumn) error {
	var acc windowAcc
	acc.init(wf, out.isFloat)
	for i, src := range perm {
		if i > 0 && cmp.partitionChanged(perm[i-1], src) {
			acc.init(wf, out.isFloat)
		}
		v, err := w.argValue(wf, src)
		if err != nil {
			return err
		}
		acc.update(wf, v)
		val, valid := acc.finalize(wf)
		if out.isFloat {
			out.f64[src] = val.f
		} else {
			out.i64[src] = val.i
		}
		out.valid[src] = valid
	}
	return nil
}

func (w *WindowOp) argValue(wf sql.WindowFunc, row int) (any, error) {
	if wf.Arg == nil {
		return nil, nil
	}
	wr := w.rows[row]
	ctx := newEvalCtx(w.batches[wr.batchIdx])
	return ctx.eval(*wf.Arg, wr.rowIdx)
}

type windowAcc struct {
	count int64
	iSum  int64
	fSum  float64
	iMin  int64
	iMax  int64
	fMin  float64
	fMax  float64
	any   bool
}

func (a *windowAcc) init(wf sql.WindowFunc, isFloat bool) {
	*a = windowAcc{}
}

func (a *windowAcc) update(wf sql.WindowFunc, v any) {
	if v == nil {
		if wf.Func == sql.WindowCount && wf.Arg == nil {
			a.count++
			a.any = true
		}
		return
	}
	a.count++
	a.any = true
	switch x := v.(type) {
	case int64:
		a.iSum += x
		if !a.hasFloatExtremes() {
			if a.count == 1 || x < a.iMin {
				a.iMin = x
			}
			if a.count == 1 || x > a.iMax {
				a.iMax = x
			}
		}
	case float64:
		a.fSum += x
		if a.count == 1 || x < a.fMin {
			a.fMin = x
		}
		if a.count == 1 || x > a.fMax {
			a.fMax = x
		}
	case float32:
		f := float64(x)
		a.fSum += f
		if a.count == 1 || f < a.fMin {
			a.fMin = f
		}
		if a.count == 1 || f > a.fMax {
			a.fMax = f
		}
	}
}

func (a *windowAcc) hasFloatExtremes() bool {
	return a.fSum != 0 || a.fMin != 0 || a.fMax != 0
}

type accVal struct {
	i int64
	f float64
}

func (a *windowAcc) finalize(wf sql.WindowFunc) (accVal, bool) {
	switch wf.Func {
	case sql.WindowCount:
		return accVal{i: a.count}, true
	case sql.WindowSum:
		if !a.any {
			return accVal{}, false
		}
		return accVal{i: a.iSum, f: a.fSum}, true
	case sql.WindowMin:
		if a.count == 0 {
			return accVal{}, false
		}
		return accVal{i: a.iMin, f: a.fMin}, true
	case sql.WindowMax:
		if a.count == 0 {
			return accVal{}, false
		}
		return accVal{i: a.iMax, f: a.fMax}, true
	case sql.WindowAvg:
		if a.count == 0 {
			return accVal{}, false
		}
		if a.fSum != 0 {
			return accVal{f: a.fSum / float64(a.count)}, true
		}
		return accVal{f: float64(a.iSum) / float64(a.count)}, true
	}
	return accVal{}, false
}

type windowCmp struct {
	op        *WindowOp
	partition []sql.BoundExpr
	order     []sql.SortKey
}

func (w *WindowOp) makeWindowCmp(wf sql.WindowFunc) (windowCmp, error) {
	return windowCmp{
		op:        w,
		partition: wf.Partition,
		order:     wf.OrderBy,
	}, nil
}

func (c windowCmp) value(row int, expr sql.BoundExpr) (any, error) {
	wr := c.op.rows[row]
	ctx := newEvalCtx(c.op.batches[wr.batchIdx])
	return ctx.eval(expr, wr.rowIdx)
}

func (c windowCmp) byPartitionThenOrder(a, b int) int {
	if r, ok := c.compareExprs(a, b, c.partition, false); ok && r != 0 {
		return r
	}
	for _, k := range c.order {
		r, ok := c.compareExprs(a, b, []sql.BoundExpr{k.Expr}, k.Desc)
		if !ok {
			continue
		}
		if r != 0 {
			return r
		}
	}
	return 0
}

func (c windowCmp) compareExprs(a, b int, exprs []sql.BoundExpr, desc bool) (int, bool) {
	for _, e := range exprs {
		av, aerr := c.value(a, e)
		bv, berr := c.value(b, e)
		if aerr != nil || berr != nil {
			return 0, false
		}
		r := compareAnyValues(av, bv)
		if desc {
			r = -r
		}
		if r != 0 {
			return r, true
		}
	}
	return 0, true
}

func (c windowCmp) partitionChanged(prev, cur int) bool {
	if len(c.partition) == 0 {
		return false
	}
	r, _ := c.compareExprs(prev, cur, c.partition, false)
	return r != 0
}

func (c windowCmp) orderChanged(prev, cur int) bool {
	if len(c.order) == 0 {
		return false
	}
	for _, k := range c.order {
		r, _ := c.compareExprs(prev, cur, []sql.BoundExpr{k.Expr}, false)
		if r != 0 {
			return true
		}
	}
	return false
}

func compareAnyValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			switch {
			case av < bv:
				return -1
			case av > bv:
				return 1
			}
			return 0
		}
	case bool:
		if bv, ok := b.(bool); ok {
			switch {
			case !av && bv:
				return -1
			case av && !bv:
				return 1
			}
			return 0
		}
	}
	return 0
}

func (w *WindowOp) Next() (types.Batch, bool, error) {
	if err := w.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	if !w.built {
		if err := w.build(); err != nil {
			return types.Batch{}, false, err
		}
		w.built = true
	}
	if w.cursor >= len(w.batches) {
		return types.Batch{}, false, nil
	}
	src := w.batches[w.cursor]
	w.cursor++
	cols := make([]types.Column, 0, len(src.Columns)+len(w.Funcs))
	cols = append(cols, src.Columns...)
	bi := w.cursor - 1
	for fi, wf := range w.Funcs {
		col := w.winCols[fi]
		if col.isFloat {
			v := types.NewVec(types.VecFloat64, src.Len)
			dst := v.F64()
			var valid types.Validity
			for ri, wr := range w.rows {
				if wr.batchIdx != bi {
					continue
				}
				if col.valid != nil && !col.valid[ri] {
					if valid == nil {
						valid = types.NewAllValid(src.Len)
					}
					valid.SetInvalid(wr.rowIdx)
					continue
				}
				dst[wr.rowIdx] = col.f64[ri]
			}
			v.Valid = valid
			cols = append(cols, types.Column{Name: wf.Alias, Type: types.Float64, V: v})
			continue
		}
		v := types.NewVec(types.VecInt64, src.Len)
		dst := v.I64()
		var valid types.Validity
		for ri, wr := range w.rows {
			if wr.batchIdx != bi {
				continue
			}
			if col.valid != nil && !col.valid[ri] {
				if valid == nil {
					valid = types.NewAllValid(src.Len)
				}
				valid.SetInvalid(wr.rowIdx)
				continue
			}
			dst[wr.rowIdx] = col.i64[ri]
		}
		v.Valid = valid
		cols = append(cols, types.Column{Name: wf.Alias, Type: types.Int64, V: v})
	}
	out := types.Batch{Len: src.Len, Columns: cols, Sel: src.Sel}
	return out, true, nil
}

func (w *WindowOp) Close() error {
	w.state.close()
	w.batches = nil
	w.rows = nil
	w.winCols = nil
	return w.Source.Close()
}

