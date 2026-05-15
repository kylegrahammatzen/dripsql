// FilterOp narrows a child's selection mask. One recursive entry point handles AND/OR/NOT
// and vectorizable leaves; anything else falls back to row-by-row. EvalPredicate is the DML entry.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type FilterOp struct {
	Source    Operator
	Predicate sql.BoundExpr

	state operatorState
}

type filterResult struct {
	sel   types.SelectionMask
	count int
}

func (f *FilterOp) Open(ctx context.Context) error {
	prev := f.state
	if err := f.state.open(); err != nil {
		return err
	}
	if err := f.Source.Open(ctx); err != nil {
		f.state = prev
		return err
	}
	return nil
}

func (f *FilterOp) Next() (types.Batch, bool, error) {
	if err := f.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	for {
		batch, ok, err := f.Source.Next()
		if err != nil || !ok {
			return batch, ok, err
		}
		in := selectionForBatch(batch)
		res, err := filterPredicate(batch, in, f.Predicate, nil)
		if err != nil {
			return types.Batch{}, false, err
		}
		if res.count == 0 {
			continue
		}
		batch.Sel = &res.sel
		return batch, true, nil
	}
}

// filterPredicate is the single recursive entry point. AND narrows the right child with the
// left's output mask (instead of the original input) so a selective left predicate skips
// rows on the right; OR keeps both children on the same input and unions the results.
// A non-nil scratch mask is reused in place by leaves so a chain of ANDs needs only one alloc.
func filterPredicate(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (filterResult, error) {
	switch pred.Op {
	case sql.ExprAnd:
		return filterAnd(batch, sel, pred, scratch)
	case sql.ExprOr:
		return filterOr(batch, sel, pred, scratch)
	case sql.ExprNot:
		return filterNotPred(batch, sel, pred, scratch)
	}
	out, count, ok, err := tryFilterLeaf(batch, sel, pred, scratch)
	if err != nil {
		return filterResult{}, err
	}
	if ok {
		return filterResult{sel: out, count: count}, nil
	}
	out, count, err = filterRowByRow(batch, sel, pred, scratch)
	if err != nil {
		return filterResult{}, err
	}
	return filterResult{sel: out, count: count}, nil
}

func filterAnd(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (filterResult, error) {
	if len(pred.Args) != 2 {
		return filterResult{}, fmt.Errorf("filter: AND expects 2 args, got %d", len(pred.Args))
	}
	left, err := filterPredicate(batch, sel, pred.Args[0], scratch)
	if err != nil {
		return filterResult{}, err
	}
	if left.count == 0 {
		return left, nil
	}
	return filterPredicate(batch, left.sel, pred.Args[1], &left.sel)
}

func filterOr(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (filterResult, error) {
	if len(pred.Args) != 2 {
		return filterResult{}, fmt.Errorf("filter: OR expects 2 args, got %d", len(pred.Args))
	}
	left, err := filterPredicate(batch, sel, pred.Args[0], scratch)
	if err != nil {
		return filterResult{}, err
	}
	right, err := filterPredicate(batch, sel, pred.Args[1], nil)
	if err != nil {
		return filterResult{}, err
	}
	count := left.sel.OrCount(right.sel)
	return filterResult{sel: left.sel, count: count}, nil
}

func filterNotPred(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (filterResult, error) {
	if len(pred.Args) != 1 {
		return filterResult{}, fmt.Errorf("filter: NOT expects 1 arg, got %d", len(pred.Args))
	}
	child, err := filterPredicate(batch, sel, pred.Args[0], scratch)
	if err != nil {
		return filterResult{}, err
	}
	child.sel.NotCount()
	count := child.sel.AndCount(sel)
	return filterResult{sel: child.sel, count: count}, nil
}

func tryFilterLeaf(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (types.SelectionMask, int, bool, error) {
	switch pred.Op {
	case sql.ExprEqual, sql.ExprNotEqual, sql.ExprLess, sql.ExprLessEqual, sql.ExprGreater, sql.ExprGreaterEqual:
		return filterColOpLit(batch, sel, pred, scratch)
	case sql.ExprBetween:
		return filterBetween(batch, sel, pred, scratch)
	}
	return types.SelectionMask{}, 0, false, nil
}

// ensureOutMask returns scratch if it already holds rows bits, otherwise a fresh mask.
// Reusing the caller's mask is what makes a chain of ANDs single-alloc.
func ensureOutMask(scratch *types.SelectionMask, rows int) types.SelectionMask {
	if scratch != nil && scratch.Rows() == rows {
		return *scratch
	}
	return types.NewSelectionMask(rows)
}

func filterRowByRow(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (types.SelectionMask, int, error) {
	ctx := newEvalCtx(batch)
	out := ensureOutMask(scratch, batch.Len)
	out.Clear()
	var count int
	var evalErr error
	sel.IterSet(func(row int) {
		if evalErr != nil {
			return
		}
		truth, ferr := ctx.EvalBool(pred, row)
		if ferr != nil {
			evalErr = ferr
			return
		}
		if truth {
			out.Set(row)
			count++
		}
	})
	return out, count, evalErr
}

func filterColOpLit(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (types.SelectionMask, int, bool, error) {
	if len(pred.Args) != 2 {
		return types.SelectionMask{}, 0, false, nil
	}
	kop, ok := mapCompareOp(pred.Op)
	if !ok {
		return types.SelectionMask{}, 0, false, nil
	}
	colExpr, litExpr, swapped := orientColLit(pred.Args[0], pred.Args[1])
	if colExpr.Op != sql.ExprColumn || litExpr.Op != sql.ExprLiteral {
		return types.SelectionMask{}, 0, false, nil
	}
	if swapped {
		kop = swapCompareOp(kop)
	}
	col, ok := batch.ColumnByName(colExpr.Column)
	if !ok || litExpr.Literal == nil {
		return types.SelectionMask{}, 0, false, nil
	}
	out := ensureOutMask(scratch, batch.Len)
	switch col.V.Kind {
	case types.VecInt16:
		lit, ok := litExpr.Literal.(int64)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterOrdered(col.V.I16(), col.V.Valid, int16(lit), kop, sel, &out)
		return out, n, true, nil
	case types.VecInt32, types.VecDate:
		lit, ok := litExpr.Literal.(int64)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterOrdered(col.V.I32(), col.V.Valid, int32(lit), kop, sel, &out)
		return out, n, true, nil
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		lit, ok := litExpr.Literal.(int64)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterOrdered(col.V.I64(), col.V.Valid, lit, kop, sel, &out)
		return out, n, true, nil
	case types.VecFloat32:
		lit, ok := asFloat64(litExpr.Literal)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterOrdered(col.V.F32(), col.V.Valid, float32(lit), kop, sel, &out)
		return out, n, true, nil
	case types.VecFloat64:
		lit, ok := asFloat64(litExpr.Literal)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterOrdered(col.V.F64(), col.V.Valid, lit, kop, sel, &out)
		return out, n, true, nil
	case types.VecText, types.VecBytes, types.VecJSON:
		if kop != types.FilterEqual && kop != types.FilterNotEqual {
			return types.SelectionMask{}, 0, false, nil
		}
		lit, ok := litExpr.Literal.(string)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.FilterBytes(col.V.Var(), col.V.Valid, []byte(lit), kop, sel, &out)
		return out, n, true, nil
	}
	return types.SelectionMask{}, 0, false, nil
}

func filterBetween(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (types.SelectionMask, int, bool, error) {
	if len(pred.Args) != 3 {
		return types.SelectionMask{}, 0, false, nil
	}
	target := pred.Args[0]
	if target.Op != sql.ExprColumn {
		return types.SelectionMask{}, 0, false, nil
	}
	lowExpr, highExpr := pred.Args[1], pred.Args[2]
	if lowExpr.Op != sql.ExprLiteral || highExpr.Op != sql.ExprLiteral {
		return types.SelectionMask{}, 0, false, nil
	}
	col, ok := batch.ColumnByName(target.Column)
	if !ok {
		return types.SelectionMask{}, 0, false, nil
	}
	out := ensureOutMask(scratch, batch.Len)
	switch col.V.Kind {
	case types.VecInt16:
		lo, lok := lowExpr.Literal.(int64)
		hi, hok := highExpr.Literal.(int64)
		if !lok || !hok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.BetweenOrdered(col.V.I16(), col.V.Valid, int16(lo), int16(hi), sel, &out)
		return out, n, true, nil
	case types.VecInt32, types.VecDate:
		lo, lok := lowExpr.Literal.(int64)
		hi, hok := highExpr.Literal.(int64)
		if !lok || !hok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.BetweenOrdered(col.V.I32(), col.V.Valid, int32(lo), int32(hi), sel, &out)
		return out, n, true, nil
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		lo, lok := lowExpr.Literal.(int64)
		hi, hok := highExpr.Literal.(int64)
		if !lok || !hok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.BetweenOrdered(col.V.I64(), col.V.Valid, lo, hi, sel, &out)
		return out, n, true, nil
	case types.VecFloat32:
		lo, lok := asFloat64(lowExpr.Literal)
		hi, hok := asFloat64(highExpr.Literal)
		if !lok || !hok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.BetweenOrdered(col.V.F32(), col.V.Valid, float32(lo), float32(hi), sel, &out)
		return out, n, true, nil
	case types.VecFloat64:
		lo, lok := asFloat64(lowExpr.Literal)
		hi, hok := asFloat64(highExpr.Literal)
		if !lok || !hok {
			return types.SelectionMask{}, 0, false, nil
		}
		n := types.BetweenOrdered(col.V.F64(), col.V.Valid, lo, hi, sel, &out)
		return out, n, true, nil
	}
	return types.SelectionMask{}, 0, false, nil
}

func orientColLit(left, right sql.BoundExpr) (col sql.BoundExpr, lit sql.BoundExpr, swapped bool) {
	if left.Op == sql.ExprColumn && right.Op == sql.ExprLiteral {
		return left, right, false
	}
	if right.Op == sql.ExprColumn && left.Op == sql.ExprLiteral {
		return right, left, true
	}
	return left, right, false
}

func mapCompareOp(op sql.ExprOp) (types.FilterOp, bool) {
	switch op {
	case sql.ExprEqual:
		return types.FilterEqual, true
	case sql.ExprNotEqual:
		return types.FilterNotEqual, true
	case sql.ExprLess:
		return types.FilterLess, true
	case sql.ExprLessEqual:
		return types.FilterLessEqual, true
	case sql.ExprGreater:
		return types.FilterGreater, true
	case sql.ExprGreaterEqual:
		return types.FilterGreaterEqual, true
	}
	return 0, false
}

func swapCompareOp(op types.FilterOp) types.FilterOp {
	switch op {
	case types.FilterLess:
		return types.FilterGreater
	case types.FilterLessEqual:
		return types.FilterGreaterEqual
	case types.FilterGreater:
		return types.FilterLess
	case types.FilterGreaterEqual:
		return types.FilterLessEqual
	}
	return op
}

func (f *FilterOp) Close() error {
	f.state.close()
	return f.Source.Close()
}

// EvalPredicate routes DELETE/UPDATE through the same vectorized path. Honors batch.Sel so
// already-deleted rows are excluded from evaluation.
func EvalPredicate(batch types.Batch, where sql.BoundExpr) (types.SelectionMask, error) {
	if err := checkExecExpr(where); err != nil {
		return types.SelectionMask{}, err
	}
	in := selectionForBatch(batch)
	res, err := filterPredicate(batch, in, where, nil)
	if err != nil {
		return types.SelectionMask{}, err
	}
	return res.sel, nil
}
