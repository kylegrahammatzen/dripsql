// Lowered predicate: lowers a sql.BoundExpr WHERE into a storage.Pred so the scan layer skips and applies before decode.
// Returns ok=false on any unsupported node so the caller keeps the decoded FilterOp path as a fallback.
package exec

import (
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// loweredComparison lowers a single comparison expression to a storage.Pred.
// Returns ok=false on any unsupported node so the caller keeps the decoded FilterOp path as a fallback.
func loweredComparison(expr sql.BoundExpr) (storage.Pred, bool) {
	// Handle special cases first
	switch expr.Op {
	case sql.ExprBetween:
		if len(expr.Args) != 3 {
			return storage.Pred{}, false
		}
		col := expr.Args[0]
		if col.Op != sql.ExprColumn {
			return storage.Pred{}, false
		}
		lo := expr.Args[1]
		hi := expr.Args[2]
		if lo.Op != sql.ExprLiteral || hi.Op != sql.ExprLiteral {
			return storage.Pred{}, false
		}
		p1, ok1 := loweredComparison(sql.BoundExpr{
			Op:   sql.ExprGreaterEqual,
			Args: []sql.BoundExpr{expr.Args[0], expr.Args[1]},
		})
		p2, ok2 := loweredComparison(sql.BoundExpr{
			Op:   sql.ExprLessEqual,
			Args: []sql.BoundExpr{expr.Args[0], expr.Args[2]},
		})
		if !ok1 || !ok2 {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpAnd, Children: []storage.Pred{p1, p2}}, true

	case sql.ExprIn:
		if len(expr.Args) < 2 {
			return storage.Pred{}, false
		}
		if expr.Args[0].Op != sql.ExprColumn {
			return storage.Pred{}, false
		}
		var children []storage.Pred
		for i := 1; i < len(expr.Args); i++ {
			if expr.Args[i].Op != sql.ExprLiteral {
				return storage.Pred{}, false
			}
			p, ok := loweredComparison(sql.BoundExpr{
				Op:   sql.ExprEqual,
				Args: []sql.BoundExpr{expr.Args[0], expr.Args[i]},
			})
			if !ok {
				return storage.Pred{}, false
			}
			children = append(children, p)
		}
		if len(children) == 1 {
			if expr.Not {
				return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{children[0]}}, true
			}
			return children[0], true
		}
		or := storage.Pred{Op: storage.OpOr, Children: children}
		if expr.Not {
			return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{or}}, true
		}
		return or, true

	case sql.ExprNotEqual:
		if len(expr.Args) != 2 {
			return storage.Pred{}, false
		}
		col, lit := expr.Args[0], expr.Args[1]
		if col.Op != sql.ExprColumn || lit.Op != sql.ExprLiteral {
			return storage.Pred{}, false
		}
		// != is rewritten as NOT (=)
		p, ok := loweredComparison(sql.BoundExpr{
			Op:   sql.ExprEqual,
			Args: []sql.BoundExpr{col, lit},
		})
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{p}}, true

	case sql.ExprNot:
		if len(expr.Args) != 1 {
			return storage.Pred{}, false
		}
		child, ok := loweredComparison(expr.Args[0])
		if !ok {
			return storage.Pred{}, false
		}
		return storage.Pred{Op: storage.OpNot, Children: []storage.Pred{child}}, true
	}

	// Binary comparisons (len == 2)
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

	// Determine column type from column's Type field, or infer from literal
	colKind := vector.VecInvalid
	if col.Type.Valid() {
		colKind, _ = vector.VecKindOf(col.Type)
	}
	if colKind == vector.VecInvalid {
		// Fall back to literal's type
		switch lit.Literal.(type) {
		case int64:
			colKind = vector.VecInt64
		case float64:
			colKind = vector.VecFloat64
		case string:
			colKind = vector.VecText
		default:
			return storage.Pred{}, false
		}
	}

	// Check for overflow before adjusting values for LessEqual/GreaterEqual
	isIntKind := colKind == vector.VecInt16 || colKind == vector.VecInt32 || colKind == vector.VecDate ||
		colKind == vector.VecInt64 || colKind == vector.VecTimestamp || colKind == vector.VecTime || colKind == vector.VecDecimal64
	if isIntKind {
		v, _ := asInt64(lit.Literal)
		if op == sql.ExprLessEqual && v == int64(^uint64(0)>>1) {
			return storage.Pred{}, false
		}
		if op == sql.ExprGreaterEqual && v == -int64(^uint64(0)>>1)-1 {
			return storage.Pred{}, false
		}
	}

	// Convert literal value to int64 based on column kind
	v, ok := convertLiteral(lit.Literal, colKind)
	if !ok {
		return storage.Pred{}, false
	}

	// Determine if integer kind
	isIntKind = colKind == vector.VecInt16 || colKind == vector.VecInt32 || colKind == vector.VecDate ||
		colKind == vector.VecInt64 || colKind == vector.VecTimestamp || colKind == vector.VecTime || colKind == vector.VecDecimal64

	// For integer types, adjust value and operator for LessEqual/GreaterEqual (use post-reversal op)
	if isIntKind {
		switch op {
		case sql.ExprLessEqual:
			v += 1
			op = sql.ExprLess
		case sql.ExprGreaterEqual:
			v -= 1
			op = sql.ExprGreater
		}
	}

	predOp := opToStorage(op)
	if predOp == storage.OpInvalid {
		return storage.Pred{}, false
	}

	base := storage.Pred{Op: predOp, Col: col.Column, ColID: uint64(col.ColumnID), Kind: colKind}
	switch colKind {
	case vector.VecInt16:
		base.I64 = int64(int16(v))
	case vector.VecInt32, vector.VecDate:
		base.I64 = int64(int32(v))
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		base.I64 = v
	case vector.VecFloat32:
		base.F64 = float64(math.Float32frombits(uint32(v)))
	case vector.VecFloat64:
		base.F64 = math.Float64frombits(uint64(v))
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		s, ok := lit.Literal.(string)
		if !ok {
			return storage.Pred{}, false
		}
		base.Bytes = []byte(s)
	default:
		return storage.Pred{}, false
	}
	return base, true
}

func opToStorage(op sql.ExprOp) storage.PredOp {
	switch op {
	case sql.ExprEqual:
		return storage.OpEq
	case sql.ExprNotEqual:
		return storage.OpNe
	case sql.ExprLess:
		return storage.OpLt
	case sql.ExprLessEqual:
		return storage.OpLe
	case sql.ExprGreater:
		return storage.OpGt
	case sql.ExprGreaterEqual:
		return storage.OpGe
	case sql.ExprNot:
		return storage.OpNot
	case sql.ExprAnd:
		return storage.OpAnd
	case sql.ExprOr:
		return storage.OpOr
	case sql.ExprIn:
		return storage.OpIn
	}
	return storage.OpInvalid
}

func convertLiteral(v any, kind vector.VecKind) (int64, bool) {
	switch x := v.(type) {
	case int64:
		switch kind {
		case vector.VecInt16:
			return int64(int16(x)), true
		case vector.VecInt32, vector.VecDate:
			return int64(int32(x)), true
		case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			return x, true
		}
	case float64:
		switch kind {
	case vector.VecFloat32:
		return int64(math.Float32bits(float32(x))), true
		case vector.VecFloat64:
			return int64(math.Float64bits(x)), true
		}
	case string:
		if kind == vector.VecText || kind == vector.VecBytes || kind == vector.VecJSON {
			return 0, true // value stored in Bytes field
		}
	}
	return 0, false
}

// loweredPredicate lowers a whole WHERE expression into pushable and residual parts.
// Returns (pushable predicate, residual expression). Either can be nil.
func loweredPredicate(expr sql.BoundExpr) (*storage.Pred, *sql.BoundExpr) {
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
			r = sql.BoundExpr{Op: sql.ExprAnd, Type: schema.Bool, Args: []sql.BoundExpr{r, residual[i]}}
		}
		res = &r
	}
	return push, res
}

// splitWhere is the engine entry point: splits a WHERE expression into pushable storage predicate and residual.
func splitWhere(expr sql.BoundExpr) (*storage.Pred, *sql.BoundExpr) {
	return loweredPredicate(expr)
}