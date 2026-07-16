// FilterOp narrows a child's selection mask through one recursive entry point for AND/OR/NOT and vectorizable leaves, with row-by-row fallback.
// EvalPredicate is the DML entry.
package exec

import (
	"context"
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type FilterOp struct {
	Source    Operator
	Predicate sql.BoundExpr

	outer    *correlatedOuter
	subBuild func(*sql.Plan) (Operator, error)

	threeValued bool
	state       operatorState
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
	f.threeValued = exprContainsNot(f.Predicate)
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
		var res filterResult
		if f.threeValued {
			tp, terr := filterThreeValued(batch, in, f.Predicate, f.outer, f.subBuild)
			res, err = filterResult{sel: tp.t, count: tp.count}, terr
		} else {
			res, err = filterPredicate(batch, in, f.Predicate, nil, f.outer, f.subBuild)
		}
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

// filterPredicate recursively narrows AND's right child with the left's output mask so a selective left skips rows on the right, unions OR children evaluated over the same input, and reuses a non-nil scratch mask in leaves so a chain of ANDs needs only one alloc.
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

// exprContainsNot reports whether the tree has an ExprNot node so the filter switches to the mask-pair path.
func exprContainsNot(e sql.BoundExpr) bool {
	found := false
	sql.WalkExpr(e, func(n sql.BoundExpr) bool {
		found = found || n.Op == sql.ExprNot
		return !found
	})
	return found
}

// filterTP carries t for definitely true rows and p for possibly true rows with t always a subset of p.
type filterTP struct {
	t      vector.SelectionMask
	p      vector.SelectionMask
	count  int
	pcount int
}

// filterThreeValued mirrors filterPredicate under SQL three-valued logic, flipping NOT between the pair as sel minus the opposite child mask so the root answer t excludes unknown rows.
func filterThreeValued(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterTP, error) {
	switch pred.Op {
	case sql.ExprAnd:
		if len(pred.Args) != 2 {
			return filterTP{}, fmt.Errorf("filter: AND expects 2 args, got %d", len(pred.Args))
		}
		left, err := filterThreeValued(batch, sel, pred.Args[0], outer, subBuild)
		if err != nil || left.pcount == 0 {
			return left, err
		}
		right, err := filterThreeValued(batch, left.p, pred.Args[1], outer, subBuild)
		if err != nil {
			return filterTP{}, err
		}
		// Narrowing the right child by left.p makes right.p the conjunction already so only t needs the AND.
		count := left.t.AndCount(right.t)
		return filterTP{t: left.t, p: right.p, count: count, pcount: right.pcount}, nil
	case sql.ExprOr:
		if len(pred.Args) != 2 {
			return filterTP{}, fmt.Errorf("filter: OR expects 2 args, got %d", len(pred.Args))
		}
		left, err := filterThreeValued(batch, sel, pred.Args[0], outer, subBuild)
		if err != nil {
			return filterTP{}, err
		}
		right, err := filterThreeValued(batch, sel, pred.Args[1], outer, subBuild)
		if err != nil {
			return filterTP{}, err
		}
		count := left.t.OrCount(right.t)
		pcount := left.p.OrCount(right.p)
		return filterTP{t: left.t, p: left.p, count: count, pcount: pcount}, nil
	case sql.ExprNot:
		if len(pred.Args) != 1 {
			return filterTP{}, fmt.Errorf("filter: NOT expects 1 arg, got %d", len(pred.Args))
		}
		child, err := filterThreeValued(batch, sel, pred.Args[0], outer, subBuild)
		if err != nil {
			return filterTP{}, err
		}
		var out filterTP
		out.t.CopyFrom(&sel)
		out.count = out.t.AndNotCount(child.p)
		out.p.CopyFrom(&sel)
		out.pcount = out.p.AndNotCount(child.t)
		return out, nil
	}
	return filterLeafThreeValued(batch, sel, pred, outer, subBuild)
}

// filterLeafThreeValued evaluates one leaf into the mask pair, where a vectorized comparison is unknown exactly on selected rows whose referenced column is NULL since the literal side never is.
func filterLeafThreeValued(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (filterTP, error) {
	t, count, ok, err := applyLeaf(batch, sel, pred, nil)
	if err != nil {
		return filterTP{}, err
	}
	if ok {
		p := vector.NewSelectionMask(batch.Len)
		p.CopyFrom(&t)
		pcount := count
		// IS NULL and IS NOT NULL are definite on every row so P stays equal to T.
		if pred.Op != sql.ExprIsNull && pred.Op != sql.ExprIsNotNull {
			var nulls vector.SelectionMask
			sql.WalkExpr(pred, func(e sql.BoundExpr) bool {
				if e.Op != sql.ExprColumn || e.Outer {
					return true
				}
				col, found := batch.ColumnByName(e.Column)
				if !found || col.V.Valid == nil {
					return true
				}
				nulls.CopyFrom(&sel)
				nulls.AndNotValidity(col.V.Valid)
				pcount = p.OrCount(nulls)
				return true
			})
		}
		return filterTP{t: t, p: p, count: count, pcount: pcount}, nil
	}
	ctx := newEvalCtxWith(batch, outer, subBuild)
	out := filterTP{t: vector.NewSelectionMask(batch.Len), p: vector.NewSelectionMask(batch.Len)}
	var evalErr error
	sel.IterSet(func(row int) {
		if evalErr != nil {
			return
		}
		truth, unknown, ferr := ctx.EvalTruth(pred, row)
		if ferr != nil {
			evalErr = ferr
			return
		}
		if truth {
			out.t.Set(row)
			out.p.Set(row)
			out.count++
			out.pcount++
		} else if unknown {
			out.p.Set(row)
			out.pcount++
		}
	})
	return out, evalErr
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
		switch op {
		case vector.FilterLess:
			op = vector.FilterGreater
		case vector.FilterLessEqual:
			op = vector.FilterGreaterEqual
		case vector.FilterGreater:
			op = vector.FilterLess
		case vector.FilterGreaterEqual:
			op = vector.FilterLessEqual
		}
		return filterLeaf{col: right, lo: left.Literal, op: op}, true
	}
	return filterLeaf{}, false
}

type intBoundVerdict int

const (
	boundExact intBoundVerdict = iota
	boundAll
	boundNone
	boundUnsupported
)

// intCompareBound converts a float literal comparison on an int64 column into an exact integer threshold so the typed kernel runs without promoting the column, with 2^63 marking the int64 range edge.
func intCompareBound(f float64, op vector.FilterOp) (int64, vector.FilterOp, intBoundVerdict) {
	const edge = 9223372036854775808.0
	if math.IsNaN(f) {
		return 0, op, boundNone
	}
	switch op {
	case vector.FilterEqual:
		if f != math.Trunc(f) || f < -edge || f >= edge {
			return 0, op, boundNone
		}
		return int64(f), op, boundExact
	case vector.FilterNotEqual:
		if f != math.Trunc(f) || f < -edge || f >= edge {
			return 0, op, boundAll
		}
		return int64(f), op, boundExact
	case vector.FilterLess:
		c := math.Ceil(f)
		if c >= edge {
			return 0, op, boundAll
		}
		if c < -edge {
			return 0, op, boundNone
		}
		return int64(c), vector.FilterLess, boundExact
	case vector.FilterLessEqual:
		fl := math.Floor(f)
		if fl >= edge {
			return 0, op, boundAll
		}
		if fl < -edge {
			return 0, op, boundNone
		}
		return int64(fl), vector.FilterLessEqual, boundExact
	case vector.FilterGreater:
		fl := math.Floor(f)
		if fl >= edge {
			return 0, op, boundNone
		}
		if fl < -edge {
			return 0, op, boundAll
		}
		return int64(fl), vector.FilterGreater, boundExact
	case vector.FilterGreaterEqual:
		c := math.Ceil(f)
		if c >= edge {
			return 0, op, boundNone
		}
		if c < -edge {
			return 0, op, boundAll
		}
		return int64(c), vector.FilterGreaterEqual, boundExact
	}
	return 0, op, boundUnsupported
}

// filterNarrowInt runs one narrow int leaf with kind range clamping shared between int16 and int32.
func filterNarrowInt[T int16 | int32](vals []T, valid vector.Validity, leaf filterLeaf, kmin, kmax int64, sel vector.SelectionMask, out *vector.SelectionMask) (int, bool) {
	lo, lok := leaf.lo.(int64)
	if !lok {
		return 0, false
	}
	if leaf.between {
		hi, hok := leaf.hi.(int64)
		if !hok {
			return 0, false
		}
		if lo > kmax || hi < kmin || lo > hi {
			out.Clear()
			return 0, true
		}
		lo, hi = max(lo, kmin), min(hi, kmax)
		return vector.BetweenOrdered(vals, valid, T(lo), T(hi), sel, out), true
	}
	switch clampVerdict(lo, kmin, kmax, leaf.op) {
	case boundNone:
		out.Clear()
		return 0, true
	case boundAll:
		out.CopyFrom(&sel)
		out.AndValidity(valid)
		return out.PopCount(), true
	}
	return vector.FilterOrdered(vals, valid, T(lo), leaf.op, sel, out), true
}

// clampVerdict decides how a literal outside a narrow int kind's range resolves for the given op.
func clampVerdict(v, lo, hi int64, op vector.FilterOp) intBoundVerdict {
	if v >= lo && v <= hi {
		return boundExact
	}
	high := v > hi
	switch op {
	case vector.FilterEqual:
		return boundNone
	case vector.FilterNotEqual:
		return boundAll
	case vector.FilterLess, vector.FilterLessEqual:
		if high {
			return boundAll
		}
		return boundNone
	case vector.FilterGreater, vector.FilterGreaterEqual:
		if high {
			return boundNone
		}
		return boundAll
	}
	return boundUnsupported
}

// intBetweenBound resolves one BETWEEN endpoint to an inclusive int64 bound, clamping an always-true side to the range edge.
func intBetweenBound(v any, op vector.FilterOp) (int64, intBoundVerdict) {
	if i, ok := v.(int64); ok {
		return i, boundExact
	}
	f, ok := asFloat64(v)
	if !ok {
		return 0, boundUnsupported
	}
	t, _, verdict := intCompareBound(f, op)
	if verdict == boundAll {
		if op == vector.FilterGreaterEqual {
			return math.MinInt64, boundExact
		}
		return math.MaxInt64, boundExact
	}
	return t, verdict
}

func applyLeaf(batch vector.Batch, sel vector.SelectionMask, pred sql.BoundExpr, scratch *vector.SelectionMask) (vector.SelectionMask, int, bool, error) {
	if pred.Op == sql.ExprIsNull || pred.Op == sql.ExprIsNotNull {
		if len(pred.Args) != 1 || pred.Args[0].Op != sql.ExprColumn || pred.Args[0].Outer {
			return vector.SelectionMask{}, 0, false, nil
		}
		col, ok := batch.ColumnByName(pred.Args[0].Column)
		if !ok {
			return vector.SelectionMask{}, 0, false, nil
		}
		out := ensureOutMask(scratch, batch.Len)
		out.CopyFrom(&sel)
		// A nil validity means no nulls so AndNotValidity clears and AndValidity keeps sel.
		if pred.Op == sql.ExprIsNull {
			out.AndNotValidity(col.V.Valid)
		} else {
			out.AndValidity(col.V.Valid)
		}
		return out, out.PopCount(), true, nil
	}
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
		count, handled := filterNarrowInt(col.V.I16(), col.V.Valid, leaf, math.MinInt16, math.MaxInt16, sel, &out)
		if !handled {
			return vector.SelectionMask{}, 0, false, nil
		}
		return out, count, true, nil
	case vector.VecInt32, vector.VecDate:
		count, handled := filterNarrowInt(col.V.I32(), col.V.Valid, leaf, math.MinInt32, math.MaxInt32, sel, &out)
		if !handled {
			return vector.SelectionMask{}, 0, false, nil
		}
		return out, count, true, nil
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		if leaf.between {
			lo, lok := intBetweenBound(leaf.lo, vector.FilterGreaterEqual)
			hi, hok := intBetweenBound(leaf.hi, vector.FilterLessEqual)
			if lok == boundUnsupported || hok == boundUnsupported {
				return vector.SelectionMask{}, 0, false, nil
			}
			if lok == boundNone || hok == boundNone || lo > hi {
				out.Clear()
				return out, 0, true, nil
			}
			return out, vector.BetweenOrdered(col.V.I64(), col.V.Valid, lo, hi, sel, &out), true, nil
		}
		if lo, ok := leaf.lo.(int64); ok {
			return out, vector.FilterOrdered(col.V.I64(), col.V.Valid, lo, leaf.op, sel, &out), true, nil
		}
		f, fok := asFloat64(leaf.lo)
		if !fok {
			return vector.SelectionMask{}, 0, false, nil
		}
		t, op, verdict := intCompareBound(f, leaf.op)
		switch verdict {
		case boundNone:
			out.Clear()
			return out, 0, true, nil
		case boundAll:
			out.CopyFrom(&sel)
			out.AndValidity(col.V.Valid)
			return out, out.PopCount(), true, nil
		}
		return out, vector.FilterOrdered(col.V.I64(), col.V.Valid, t, op, sel, &out), true, nil
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

// ensureOutMask returns scratch when it already holds rows bits and a fresh mask otherwise, the reuse being what makes a chain of ANDs single-alloc.
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

func (f *FilterOp) Close() error {
	f.state.close()
	return f.Source.Close()
}

// EvalPredicate routes DELETE/UPDATE through the same vectorized path, honoring batch.Sel so already-deleted rows are excluded.
func EvalPredicate(batch vector.Batch, where sql.BoundExpr) (vector.SelectionMask, error) {
	in := selectionForBatch(batch)
	if exprContainsNot(where) {
		res, err := filterThreeValued(batch, in, where, nil, nil)
		if err != nil {
			return vector.SelectionMask{}, err
		}
		return res.t, nil
	}
	res, err := filterPredicate(batch, in, where, nil, nil, nil)
	if err != nil {
		return vector.SelectionMask{}, err
	}
	return res.sel, nil
}
