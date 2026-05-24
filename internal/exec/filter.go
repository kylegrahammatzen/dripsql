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

	outer    *correlatedOuter
	subBuild func(*sql.Plan) (Operator, error)

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
		res, err := filterPredicate(batch, in, f.Predicate, nil, f.outer, f.subBuild)
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
func filterPredicate(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
	switch pred.Op {
	case sql.ExprAnd:
		return filterAnd(batch, sel, pred, scratch, outer, subBuild)
	case sql.ExprOr:
		return filterOr(batch, sel, pred, scratch, outer, subBuild)
	case sql.ExprNot:
		return filterNotPred(batch, sel, pred, scratch, outer, subBuild)
	}
	out, count, ok, err := tryFilterLeaf(batch, sel, pred, scratch)
	if err != nil {
		return filterResult{}, err
	}
	if ok {
		return filterResult{sel: out, count: count}, nil
	}
	out, count, err = filterRowByRow(batch, sel, pred, scratch, outer, subBuild)
	if err != nil {
		return filterResult{}, err
	}
	return filterResult{sel: out, count: count}, nil
}

func filterAnd(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
	if len(pred.Args) != 2 {
		return filterResult{}, fmt.Errorf("filter: AND expects 2 args, got %d", len(pred.Args))
	}
	left, err := filterPredicate(batch, sel, pred.Args[0], scratch, outer, subBuild)
	if err != nil {
		return filterResult{}, err
	}
	if left.count == 0 {
		return left, nil
	}
	return filterPredicate(batch, left.sel, pred.Args[1], &left.sel, outer, subBuild)
}

func filterOr(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
	if len(pred.Args) != 2 {
		return filterResult{}, fmt.Errorf("filter: OR expects 2 args, got %d", len(pred.Args))
	}
	left, err := filterPredicate(batch, sel, pred.Args[0], scratch, outer, subBuild)
	if err != nil {
		return filterResult{}, err
	}
	right, err := filterPredicate(batch, sel, pred.Args[1], nil, outer, subBuild)
	if err != nil {
		return filterResult{}, err
	}
	count := left.sel.OrCount(right.sel)
	return filterResult{sel: left.sel, count: count}, nil
}

func filterNotPred(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
	if len(pred.Args) != 1 {
		return filterResult{}, fmt.Errorf("filter: NOT expects 1 arg, got %d", len(pred.Args))
	}
	child, err := filterPredicate(batch, sel, pred.Args[0], scratch, outer, subBuild)
	if err != nil {
		return filterResult{}, err
	}
	child.sel.NotCount()
	count := child.sel.AndCount(sel)
	return filterResult{sel: child.sel, count: count}, nil
}

// filterLeaf is a normalized col-op-lit / col BETWEEN lo AND hi shape produced from a
// BoundExpr predicate. tryFilterLeaf dispatches on the column kind once and routes both
// the comparison and BETWEEN forms through the same per-kind branch.
type filterLeaf struct {
	col     sql.BoundExpr
	lo, hi  any
	op      types.FilterOp
	between bool
}

func makeFilterLeaf(pred sql.BoundExpr) (filterLeaf, bool) {
	if pred.Op == sql.ExprBetween {
		if len(pred.Args) != 3 || pred.Args[0].Op != sql.ExprColumn || pred.Args[1].Op != sql.ExprLiteral || pred.Args[2].Op != sql.ExprLiteral {
			return filterLeaf{}, false
		}
		return filterLeaf{col: pred.Args[0], lo: pred.Args[1].Literal, hi: pred.Args[2].Literal, between: true}, true
	}
	op, ok := mapCompareOp(pred.Op)
	if !ok || len(pred.Args) != 2 {
		return filterLeaf{}, false
	}
	left, right := pred.Args[0], pred.Args[1]
	if left.Op == sql.ExprColumn && right.Op == sql.ExprLiteral {
		return filterLeaf{col: left, lo: right.Literal, op: op}, true
	}
	if right.Op == sql.ExprColumn && left.Op == sql.ExprLiteral {
		return filterLeaf{col: right, lo: left.Literal, op: swapCompareOp(op)}, true
	}
	return filterLeaf{}, false
}

func tryFilterLeaf(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask) (types.SelectionMask, int, bool, error) {
	leaf, ok := makeFilterLeaf(pred)
	if !ok || leaf.lo == nil {
		return types.SelectionMask{}, 0, false, nil
	}
	col, ok := batch.ColumnByName(leaf.col.Column)
	if !ok {
		return types.SelectionMask{}, 0, false, nil
	}
	out := ensureOutMask(scratch, batch.Len)
	switch col.V.Kind {
	case types.VecInt16:
		lo, lok := leaf.lo.(int64)
		if !lok {
			return types.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := leaf.hi.(int64)
			if !hok {
				return types.SelectionMask{}, 0, false, nil
			}
			return out, types.BetweenOrdered(col.V.I16(), col.V.Valid, int16(lo), int16(hi), sel, &out), true, nil
		}
		return out, types.FilterOrdered(col.V.I16(), col.V.Valid, int16(lo), leaf.op, sel, &out), true, nil
	case types.VecInt32, types.VecDate:
		lo, lok := leaf.lo.(int64)
		if !lok {
			return types.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := leaf.hi.(int64)
			if !hok {
				return types.SelectionMask{}, 0, false, nil
			}
			return out, types.BetweenOrdered(col.V.I32(), col.V.Valid, int32(lo), int32(hi), sel, &out), true, nil
		}
		return out, types.FilterOrdered(col.V.I32(), col.V.Valid, int32(lo), leaf.op, sel, &out), true, nil
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		lo, lok := leaf.lo.(int64)
		if !lok {
			return types.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := leaf.hi.(int64)
			if !hok {
				return types.SelectionMask{}, 0, false, nil
			}
			return out, types.BetweenOrdered(col.V.I64(), col.V.Valid, lo, hi, sel, &out), true, nil
		}
		return out, types.FilterOrdered(col.V.I64(), col.V.Valid, lo, leaf.op, sel, &out), true, nil
	case types.VecFloat32:
		lo, lok := asFloat64(leaf.lo)
		if !lok {
			return types.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := asFloat64(leaf.hi)
			if !hok {
				return types.SelectionMask{}, 0, false, nil
			}
			return out, types.BetweenOrdered(col.V.F32(), col.V.Valid, float32(lo), float32(hi), sel, &out), true, nil
		}
		return out, types.FilterOrdered(col.V.F32(), col.V.Valid, float32(lo), leaf.op, sel, &out), true, nil
	case types.VecFloat64:
		lo, lok := asFloat64(leaf.lo)
		if !lok {
			return types.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := asFloat64(leaf.hi)
			if !hok {
				return types.SelectionMask{}, 0, false, nil
			}
			return out, types.BetweenOrdered(col.V.F64(), col.V.Valid, lo, hi, sel, &out), true, nil
		}
		return out, types.FilterOrdered(col.V.F64(), col.V.Valid, lo, leaf.op, sel, &out), true, nil
	case types.VecText, types.VecBytes, types.VecJSON:
		if leaf.between || (leaf.op != types.FilterEqual && leaf.op != types.FilterNotEqual) {
			return types.SelectionMask{}, 0, false, nil
		}
		lit, ok := leaf.lo.(string)
		if !ok {
			return types.SelectionMask{}, 0, false, nil
		}
		return out, types.FilterBytes(col.V.Var(), col.V.Valid, []byte(lit), leaf.op, sel, &out), true, nil
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

func filterRowByRow(batch types.Batch, sel types.SelectionMask, pred sql.BoundExpr, scratch *types.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (types.SelectionMask, int, error) {
	ctx := newEvalCtxWith(batch, outer, subBuild)
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
	res, err := filterPredicate(batch, in, where, nil, nil, nil)
	if err != nil {
		return types.SelectionMask{}, err
	}
	return res.sel, nil
}
