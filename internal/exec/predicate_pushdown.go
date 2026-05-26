// Lowers a sql.BoundExpr WHERE into a storage.Pred so the scan layer skips and applies before decode.
// Returns ok=false on any unsupported node so the caller keeps the decoded FilterOp path as a fallback.
package exec

import (
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

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
		if p, ok := loweredPredicate(e); ok {
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
		p := pushable[0]
		push = &p
	default:
		p := storage.Pred{Op: storage.OpAnd, Children: pushable}
		push = &p
	}
	var res *sql.BoundExpr
	switch len(residual) {
	case 0:
	case 1:
		r := residual[0]
		res = &r
	default:
		r := residual[0]
		for i := 1; i < len(residual); i++ {
			r = sql.BoundExpr{Op: sql.ExprAnd, Type: r.Type, Args: []sql.BoundExpr{r, residual[i]}}
		}
		res = &r
	}
	return push, res
}

func loweredPredicate(expr sql.BoundExpr) (storage.Pred, bool) {
	switch expr.Op {
	case sql.ExprAnd:
		if len(expr.Args) != 2 {
			return storage.Pred{}, false
		}
		l, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return storage.Pred{}, false
		}
		r, ok := loweredPredicate(expr.Args[1])
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpAnd, Children: []storage.Pred{l, r}}, true
	case sql.ExprOr:
		if len(expr.Args) != 2 {
			return storage.Pred{}, false
		}
		l, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return storage.Pred{}, false
		}
		r, ok := loweredPredicate(expr.Args[1])
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpOr, Children: []storage.Pred{l, r}}, true
	case sql.ExprNot:
		if len(expr.Args) != 1 {
			return storage.Pred{}, false
		}
		c, ok := loweredPredicate(expr.Args[0])
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{c}}, true
	case sql.ExprEqual, sql.ExprLess, sql.ExprGreater,
		sql.ExprLessEqual, sql.ExprGreaterEqual, sql.ExprNotEqual:
		return loweredComparison(expr)
	case sql.ExprBetween:
		return loweredBetween(expr)
	case sql.ExprIn:
		return loweredIn(expr)
	}
	return storage.Pred{}, false
}

func loweredBetween(expr sql.BoundExpr) (storage.Pred, bool) {
	if len(expr.Args) != 3 {
		return storage.Pred{}, false
	}
	col, low, high := expr.Args[0], expr.Args[1], expr.Args[2]
	if col.Op != sql.ExprColumn {
		return storage.Pred{}, false
	}
	ge, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprGreaterEqual, Args: []sql.BoundExpr{col, low}})
	if !ok {
		return storage.Pred{}, false
	}
	le, ok := loweredComparison(sql.BoundExpr{Op: sql.ExprLessEqual, Args: []sql.BoundExpr{col, high}})
	if !ok {
		return storage.Pred{}, false
	}
	return storage.Pred{Op: storage.OpAnd, Children: []storage.Pred{ge, le}}, true
}

func loweredIn(expr sql.BoundExpr) (storage.Pred, bool) {
	if len(expr.Args) < 2 {
		return storage.Pred{}, false
	}
	col := expr.Args[0]
	if col.Op != sql.ExprColumn {
		return storage.Pred{}, false
	}
	preds := make([]storage.Pred, 0, len(expr.Args)-1)
	for _, arg := range expr.Args[1:] {
		if arg.Op != sql.ExprLiteral || arg.Literal == nil {
			return storage.Pred{}, false
		}
		switch v := arg.Literal.(type) {
		case int64:
			preds = append(preds, storage.Pred{Op: storage.OpEq, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v})
		case string:
			preds = append(preds, storage.Pred{Op: storage.OpEq, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)})
		default:
			return storage.Pred{}, false
		}
	}
	var p storage.Pred
	if len(preds) == 1 {
		p = preds[0]
	} else {
		p = storage.Pred{Op: storage.OpOr, Children: preds}
	}
	if expr.Not {
		p = storage.Pred{Op: storage.OpNot, Children: []storage.Pred{p}}
	}
	return p, true
}

func loweredComparison(expr sql.BoundExpr) (storage.Pred, bool) {
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
	switch v := lit.Literal.(type) {
	case int64:
		switch op {
		case sql.ExprEqual:
			return storage.Pred{Op: storage.OpEq, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v}, true
		case sql.ExprNotEqual:
			eq := storage.Pred{Op: storage.OpEq, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v}
			return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{eq}}, true
		case sql.ExprLess:
			return storage.Pred{Op: storage.OpLt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v}, true
		case sql.ExprGreater:
			return storage.Pred{Op: storage.OpGt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v}, true
		case sql.ExprLessEqual:
			if v == int64(^uint64(0)>>1) {
				return storage.Pred{}, false
			}
			return storage.Pred{Op: storage.OpLt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v + 1}, true
		case sql.ExprGreaterEqual:
			if v == -int64(^uint64(0)>>1)-1 {
				return storage.Pred{}, false
			}
			return storage.Pred{Op: storage.OpGt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecInt64, I64: v - 1}, true
		}
	case string:
		switch op {
		case sql.ExprEqual:
			return storage.Pred{Op: storage.OpEq, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)}, true
		case sql.ExprLess:
			return storage.Pred{Op: storage.OpLt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)}, true
		case sql.ExprLessEqual:
			return storage.Pred{Op: storage.OpLe, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)}, true
		case sql.ExprGreater:
			return storage.Pred{Op: storage.OpGt, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)}, true
		case sql.ExprGreaterEqual:
			return storage.Pred{Op: storage.OpGe, Col: col.Column, ColID: uint64(col.ColumnID), Kind: vector.VecText, Bytes: []byte(v)}, true
		}
	}
	return storage.Pred{}, false
}
