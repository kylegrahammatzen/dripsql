// Lowers a sql.BoundExpr WHERE into a storage.Pred so the scan layer skips and applies before decode.
// Returns ok=false on any unsupported node so the caller keeps the decoded FilterOp path as a fallback.
package exec

import (
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func loweredComparison(expr sql.BoundExpr) (storage.Pred, bool) {
	switch expr.Op {
	case sql.ExprAnd, sql.ExprOr:
		if len(expr.Args) != 2 {
			return storage.Pred{}, false
		}
		l, ok := loweredComparison(expr.Args[0])
		if !ok {
			return storage.Pred{}, false
		}
		r, ok := loweredComparison(expr.Args[1])
		if !ok {
			return storage.Pred{}, false
		}
		op := storage.OpAnd
		if expr.Op == sql.ExprOr {
			op = storage.OpOr
		}
		return storage.Pred{Op: op, Children: []storage.Pred{l, r}}, true

	case sql.ExprBetween:
		if len(expr.Args) != 3 {
			return storage.Pred{}, false
		}
		ge, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprGreaterEqual, Args: []sql.BoundExpr{expr.Args[0], expr.Args[1]}})
		if !ok {
			return storage.Pred{}, false
		}
		le, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprLessEqual, Args: []sql.BoundExpr{expr.Args[0], expr.Args[2]}})
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpAnd, Children: []storage.Pred{ge, le}}, true

	case sql.ExprIn:
		if len(expr.Args) < 2 || expr.Args[0].Op != sql.ExprColumn {
			return storage.Pred{}, false
		}
		children := make([]storage.Pred, 0, len(expr.Args)-1)
		for _, arg := range expr.Args[1:] {
			p, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprEqual, Args: []sql.BoundExpr{expr.Args[0], arg}})
			if !ok {
				return storage.Pred{}, false
			}
			children = append(children, p)
		}
		p := children[0]
		if len(children) > 1 {
			p = storage.Pred{Op: storage.OpOr, Children: children}
		}
		if expr.Not {
			p = storage.Pred{Op: storage.OpNot, Children: []storage.Pred{p}}
		}
		return p, true

	case sql.ExprNotEqual:
		if len(expr.Args) != 2 {
			return storage.Pred{}, false
		}
		eq, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprEqual, Args: expr.Args})
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{eq}}, true

	case sql.ExprNot:
		if len(expr.Args) != 1 {
			return storage.Pred{}, false
		}
		child, ok := loweredComparison(expr.Args[0])
		if !ok || predContainsAnd(child) {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{child}}, true
	}

	if len(expr.Args) != 2 {
		return storage.Pred{}, false
	}
	col, lit := expr.Args[0], expr.Args[1]
	op := expr.Op
	if col.Op == sql.ExprLiteral && lit.Op == sql.ExprColumn {
		col, lit = lit, col
		switch op {
		case sql.ExprLess:
			op = sql.ExprGreater
		case sql.ExprGreater:
			op = sql.ExprLess
		case sql.ExprLessEqual:
			op = sql.ExprGreaterEqual
		case sql.ExprGreaterEqual:
			op = sql.ExprLessEqual
		}
	}
	if col.Op != sql.ExprColumn || lit.Op != sql.ExprLiteral || lit.Literal == nil {
		return storage.Pred{}, false
	}

	colKind := vector.VecInvalid
	if col.Type.Valid() {
		colKind, _ = vector.VecKindOf(col.Type)
	}
	if colKind == vector.VecInvalid {
		switch lit.Literal.(type) {
		case int64:
			colKind = vector.VecInt64
		case float64:
			colKind = vector.VecFloat64
		case string:
			colKind = vector.VecText
		}
	}

	base := storage.Pred{Col: col.Column, ColID: uint64(col.ColumnID), Kind: colKind}
	// Only kinds with storage evaluators lower. float32 stays on the decoded filter path.
	switch colKind {
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		v, ok := lit.Literal.(int64)
		if !ok {
			return storage.Pred{}, false
		}
		// LE and GE rewrite to strict ops so encoded FOR range evaluation applies. Extremes cannot shift.
		switch op {
		case sql.ExprLessEqual:
			if v == math.MaxInt64 {
				return storage.Pred{}, false
			}
			v, op = v+1, sql.ExprLess
		case sql.ExprGreaterEqual:
			if v == math.MinInt64 {
				return storage.Pred{}, false
			}
			v, op = v-1, sql.ExprGreater
		}
		base.I64 = v
	case vector.VecInt16, vector.VecInt32, vector.VecDate:
		v, ok := lit.Literal.(int64)
		if !ok {
			return storage.Pred{}, false
		}
		// Out of range literals stay on the decoded path where clampVerdict resolves them.
		lo, hi := int64(math.MinInt32), int64(math.MaxInt32)
		if colKind == vector.VecInt16 {
			lo, hi = math.MinInt16, math.MaxInt16
		}
		if v < lo || v > hi {
			return storage.Pred{}, false
		}
		base.I64 = v
	case vector.VecFloat64:
		switch x := lit.Literal.(type) {
		case float64:
			base.F64 = x
		case int64:
			base.F64 = float64(x)
		default:
			return storage.Pred{}, false
		}
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		s, ok := lit.Literal.(string)
		if !ok {
			return storage.Pred{}, false
		}
		base.Bytes = []byte(s)
	default:
		return storage.Pred{}, false
	}

	switch op {
	case sql.ExprEqual:
		base.Op = storage.OpEq
	case sql.ExprLess:
		base.Op = storage.OpLt
	case sql.ExprLessEqual:
		base.Op = storage.OpLe
	case sql.ExprGreater:
		base.Op = storage.OpGt
	case sql.ExprGreaterEqual:
		base.Op = storage.OpGe
	default:
		return storage.Pred{}, false
	}
	return base, true
}

// predContainsAnd guards NOT lowering because boundNot's validity re-mask is only sound over leaves and OR trees.
func predContainsAnd(p storage.Pred) bool {
	if p.Op == storage.OpAnd {
		return true
	}
	for _, c := range p.Children {
		if predContainsAnd(c) {
			return true
		}
	}
	return false
}

// splitWhere separates a WHERE tree into a pushable storage predicate and a residual expression where either may be nil.
func splitWhere(expr sql.BoundExpr) (*storage.Pred, *sql.BoundExpr) {
	var pushable []storage.Pred
	var residual []sql.BoundExpr
	var walk func(sql.BoundExpr)
	walk = func(e sql.BoundExpr) {
		if e.Op == sql.ExprAnd && len(e.Args) == 2 {
			walk(e.Args[0])
			walk(e.Args[1])
			return
		}
		if p, ok := loweredComparison(e); ok {
			pushable = append(pushable, p)
			return
		}
		residual = append(residual, e)
	}
	walk(expr)

	var push *storage.Pred
	switch len(pushable) {
	case 0:
	case 1:
		push = &pushable[0]
	default:
		push = &storage.Pred{Op: storage.OpAnd, Children: pushable}
	}
	var res *sql.BoundExpr
	switch len(residual) {
	case 0:
	case 1:
		res = &residual[0]
	default:
		r := residual[0]
		for i := 1; i < len(residual); i++ {
			r = sql.BoundExpr{Op: sql.ExprAnd, Type: schema.Bool, Args: []sql.BoundExpr{r, residual[i]}}
		}
		res = &r
	}
	return push, res
}
