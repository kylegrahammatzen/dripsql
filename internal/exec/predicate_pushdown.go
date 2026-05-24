// Predicate pushdown lowers a sql.BoundExpr WHERE into a storage.Predicate so the scan
// layer evaluates the filter before materializing downstream columns. Returns ok=false on
// any unsupported node so the caller can keep the decoded FilterOp path as a fallback.
package exec

import (
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func loweredPredicate(expr sql.BoundExpr) (storage.Predicate, bool) {
	switch expr.Op {
	case sql.ExprAnd:
		if len(expr.Args) != 2 {
			return nil, false
		}
		l, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return nil, false
		}
		r, ok := loweredPredicate(expr.Args[1])
		if !ok {
			return nil, false
		}
		return storage.And{Children: []storage.Predicate{l, r}}, true
	case sql.ExprOr:
		if len(expr.Args) != 2 {
			return nil, false
		}
		l, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return nil, false
		}
		r, ok := loweredPredicate(expr.Args[1])
		if !ok {
			return nil, false
		}
		return storage.Or{Children: []storage.Predicate{l, r}}, true
	case sql.ExprNot:
		if len(expr.Args) != 1 {
			return nil, false
		}
		c, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return nil, false
		}
		return storage.Not{Child: c}, true
	case sql.ExprEqual, sql.ExprLess, sql.ExprGreater,
		sql.ExprLessEqual, sql.ExprGreaterEqual, sql.ExprNotEqual:
		return loweredComparison(expr)
	case sql.ExprBetween:
		return loweredBetween(expr)
	case sql.ExprIn:
		return loweredIn(expr)
	}
	return nil, false
}

// loweredBetween rewrites `target BETWEEN low AND high` as And{>=low, <=high}, then
// defers to the existing comparison lowering so overflow guards apply at the boundary.
func loweredBetween(expr sql.BoundExpr) (storage.Predicate, bool) {
	if len(expr.Args) != 3 {
		return nil, false
	}
	col, low, high := expr.Args[0], expr.Args[1], expr.Args[2]
	if col.Op != sql.ExprColumn {
		return nil, false
	}
	ge, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprGreaterEqual, Args: []sql.BoundExpr{col, low}})
	if !ok {
		return nil, false
	}
	le, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprLessEqual, Args: []sql.BoundExpr{col, high}})
	if !ok {
		return nil, false
	}
	return storage.And{Children: []storage.Predicate{ge, le}}, true
}

// loweredIn lowers `target IN (v1, v2, ...)` to Or{Eq, Eq, ...}. Empty list is
// the constant FALSE which we can't express today, so bail. NOT IN wraps the
// whole disjunction in Not, which composes with the existing prune logic.
func loweredIn(expr sql.BoundExpr) (storage.Predicate, bool) {
	if len(expr.Args) < 2 {
		return nil, false
	}
	col := expr.Args[0]
	if col.Op != sql.ExprColumn {
		return nil, false
	}
	preds := make([]storage.Predicate, 0, len(expr.Args)-1)
	for _, arg := range expr.Args[1:] {
		if arg.Op != sql.ExprLiteral || arg.Literal == nil {
			return nil, false
		}
		switch v := arg.Literal.(type) {
		case int64:
			preds = append(preds, storage.EqInt64{Column: col.Column, Value: v})
		case string:
			preds = append(preds, storage.EqBytes{Column: col.Column, Value: []byte(v)})
		default:
			return nil, false
		}
	}
	var p storage.Predicate
	if len(preds) == 1 {
		p = preds[0]
	} else {
		p = storage.Or{Children: preds}
	}
	if expr.Not {
		p = storage.Not{Child: p}
	}
	return p, true
}


func loweredComparison(expr sql.BoundExpr) (storage.Predicate, bool) {
	if len(expr.Args) != 2 {
		return nil, false
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
		return nil, false
	}
	switch v := lit.Literal.(type) {
	case int64:
		switch op {
		case sql.ExprEqual:
			return storage.EqInt64{Column: col.Column, Value: v}, true
		case sql.ExprNotEqual:
			return storage.Not{Child: storage.EqInt64{Column: col.Column, Value: v}}, true
		case sql.ExprLess:
			return storage.LtInt64{Column: col.Column, Value: v}, true
		case sql.ExprGreater:
			return storage.GtInt64{Column: col.Column, Value: v}, true
		case sql.ExprLessEqual:
			// x <= v  ==>  x < v+1, guard the int64 overflow at MaxInt64.
			if v == int64(^uint64(0)>>1) {
				return nil, false
			}
			return storage.LtInt64{Column: col.Column, Value: v + 1}, true
		case sql.ExprGreaterEqual:
			// x >= v  ==>  x > v-1, guard the int64 overflow at MinInt64.
			if v == -int64(^uint64(0)>>1) - 1 {
				return nil, false
			}
			return storage.GtInt64{Column: col.Column, Value: v - 1}, true
		}
	case string:
		if op == sql.ExprEqual {
			return storage.EqBytes{Column: col.Column, Value: []byte(v)}, true
		}
	}
	return nil, false
}
