// FilterOp narrows a child's selection mask. One recursive entry point handles AND/OR/NOT
// and vectorizable leaves; anything else falls back to row-by-row. EvalPredicate is the DML entry.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type FilterOp struct {
	Source    Operator
	Predicate sql.BoundExpr

	outer    *correlatedOuter
	subBuild func(*sql.Plan) (Operator, error)

	state operatorState
}

type filterResult struct {
	sel   vector.SelectionMask
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

func (f *FilterOp) Next() (vector.Batch, bool, error) {
	if err := f.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	for {
		batch, ok, err := f.Source.Next()
		if err != nil || !ok {
			return batch, ok, err
		}
		in := selectionForBatch(batch)
		res, err := filterPredicate(batch, in, f.Predicate, nil, f.outer, f.subBuild)
		if err != nil {
			return vector.Batch{}, false, err
		}
		if res.count == 0 {
			continue
		}
		if err := batch.SetSel(&res.sel); err != nil {
			return vector.Batch{}, false, fmt.Errorf("filter: %w", err)
		}
		return batch, true, nil
	}
}

// filterPredicate is the single recursive entry point. AND narrows the right child with the
// left's output mask (instead of the original input) so a selective left predicate skips
// rows on the right; OR keeps both children on the same input and unions the results.
// A non-nil scratch mask is reused in place by leaves so a chain of ANDs needs only one alloc.
func filterPredicate(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
	switch pred.Op {
	case sql.ExprAnd:
		return filterAnd(batch, sel, pred, scratch, outer, subBuild)
	case sql.ExprOr:
		return filterOr(batch, sel, pred, scratch, outer, subBuild)
	case sql.ExprNot:
		return filterNotPred(batch, sel, pred, scratch, outer, subBuild)
	}
	out, count, ok, err := applyLeaf(batch, sel, pred, scratch)
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

func filterAnd(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
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

func filterOr(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
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

func filterNotPred(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterResult, error) {
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

// Normalized col-op-lit and col-BETWEEN-lo-AND-hi shape so applyLeaf dispatches on column kind once.
type filterLeaf struct {
	col     sql.BoundExpr
	lo, hi  any
	op      vector.FilterOp
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

func applyLeaf(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask) (vector.SelectionMask, int, bool, error) {
	leaf, ok := makeFilterLeaf(pred)
	if !ok || leaf.lo == nil {
		return vector.SelectionMask{}, 0, false, nil
	}
	col, ok := batch.ColumnByName(leaf.col.Column)
	if !ok {
		return vector.SelectionMask{}, 0, false, nil
	}
	out := ensureOutMask(scratch, batch.Len)
	switch col.V.Kind {
	case vector.VecInt16:
		lo, lok := leaf.lo.(int64)
		if !lok {
			return vector.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := leaf.hi.(int64)
			if !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			return out, vector.BetweenOrdered(col.V.I16(), col.V.Valid, int16(lo), int16(hi), sel, &out), true, nil
		}
		return out, vector.FilterOrdered(col.V.I16(), col.V.Valid, int16(lo), leaf.op, sel, &out), true, nil
	case vector.VecInt32, vector.VecDate:
		lo, lok := leaf.lo.(int64)
		if !lok {
			return vector.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := leaf.hi.(int64)
			if !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			return out, vector.BetweenOrdered(col.V.I32(), col.V.Valid, int32(lo), int32(hi), sel, &out), true, nil
		}
		return out, vector.FilterOrdered(col.V.I32(), col.V.Valid, int32(lo), leaf.op, sel, &out), true, nil
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		lo, lok := asFloat64(leaf.lo)
		if !lok {
			return vector.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := asFloat64(leaf.hi)
			if !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			// Compare int64 column with float64 literal by promoting to float64
			var f64s []float64
			for _, v := range col.V.I64() {
				f64s = append(f64s, float64(v))
			}
			return out, vector.BetweenOrdered(f64s, col.V.Valid, lo, hi, sel, &out), true, nil
		}
		var f64s []float64
		for _, v := range col.V.I64() {
			f64s = append(f64s, float64(v))
		}
		return out, vector.FilterOrdered(f64s, col.V.Valid, lo, leaf.op, sel, &out), true, nil
	case vector.VecFloat32:
		lo, lok := asFloat64(leaf.lo)
		if !lok {
			return vector.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := asFloat64(leaf.hi)
			if !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			return out, vector.BetweenOrdered(col.V.F32(), col.V.Valid, float32(lo), float32(hi), sel, &out), true, nil
		}
		return out, vector.FilterOrdered(col.V.F32(), col.V.Valid, float32(lo), leaf.op, sel, &out), true, nil
	case vector.VecFloat64:
		lo, lok := asFloat64(leaf.lo)
		if !lok {
			return vector.SelectionMask{}, 0, false, nil
		}
		if leaf.between {
			hi, hok := asFloat64(leaf.hi)
			if !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			return out, vector.BetweenOrdered(col.V.F64(), col.V.Valid, lo, hi, sel, &out), true, nil
		}
		return out, vector.FilterOrdered(col.V.F64(), col.V.Valid, lo, leaf.op, sel, &out), true, nil
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		if leaf.between {
			lo, lok := leaf.lo.(string)
			hi, hok := leaf.hi.(string)
			if !lok || !hok {
				return vector.SelectionMask{}, 0, false, nil
			}
			ge := vector.FilterBytes(col.V.Var(), col.V.Valid, []byte(lo), vector.FilterGreaterEqual, sel, &out)
			if ge == 0 {
				return out, 0, true, nil
			}
			return out, vector.FilterBytes(col.V.Var(), col.V.Valid, []byte(hi), vector.FilterLessEqual, out, &out), true, nil
		}
		lit, ok := leaf.lo.(string)
		if !ok {
			return vector.SelectionMask{}, 0, false, nil
		}
		return out, vector.FilterBytes(col.V.Var(), col.V.Valid, []byte(lit), leaf.op, sel, &out), true, nil
	}
	return vector.SelectionMask{}, 0, false, nil
}

// ensureOutMask returns scratch if it already holds rows bits, otherwise a fresh mask.
// Reusing the caller's mask is what makes a chain of ANDs single-alloc.
func ensureOutMask(scratch *vector.SelectionMask, rows int) vector.SelectionMask {
	if scratch != nil && scratch.Rows() == rows {
		return *scratch
	}
	return vector.NewSelectionMask(rows)
}

func filterRowByRow(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (vector.SelectionMask, int, error) {
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

func mapCompareOp(op sql.ExprOp) (vector.FilterOp, bool) {
	switch op {
	case sql.ExprEqual:
		return vector.FilterEqual, true
	case sql.ExprNotEqual:
		return vector.FilterNotEqual, true
	case sql.ExprLess:
		return vector.FilterLess, true
	case sql.ExprLessEqual:
		return vector.FilterLessEqual, true
	case sql.ExprGreater:
		return vector.FilterGreater, true
	case sql.ExprGreaterEqual:
		return vector.FilterGreaterEqual, true
	}
	return 0, false
}

func swapCompareOp(op vector.FilterOp) vector.FilterOp {
	switch op {
	case vector.FilterLess:
		return vector.FilterGreater
	case vector.FilterLessEqual:
		return vector.FilterGreaterEqual
	case vector.FilterGreater:
		return vector.FilterLess
	case vector.FilterGreaterEqual:
		return vector.FilterLessEqual
	}
	return op
}

func (f *FilterOp) Close() error {
	f.state.close()
	return f.Source.Close()
}

// EvalPredicate routes DELETE/UPDATE through the same vectorized path. Honors batch.Sel so
// already-deleted rows are excluded from evaluation.
func EvalPredicate(batch vector.Batch, where sql.BoundExpr) (vector.SelectionMask, error) {
	if err := checkExecExpr(where); err != nil {
		return vector.SelectionMask{}, err
	}
	in := selectionForBatch(batch)
	res, err := filterPredicate(batch, in, where, nil, nil, nil)
	if err != nil {
		return vector.SelectionMask{}, err
	}
	return res.sel, nil
}
