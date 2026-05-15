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
	case sql.ExprEqual, sql.ExprLess, sql.ExprGreater:
		return loweredComparison(expr)
	}
	return nil, false
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
		case sql.ExprLess:
			return storage.LtInt64{Column: col.Column, Value: v}, true
		case sql.ExprGreater:
			return storage.GtInt64{Column: col.Column, Value: v}, true
		}
	case string:
		if op == sql.ExprEqual {
			return storage.EqBytes{Column: col.Column, Value: []byte(v)}, true
		}
	}
	return nil, false
}
