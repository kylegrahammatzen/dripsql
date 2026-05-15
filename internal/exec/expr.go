// Row-by-row BoundExpr evaluator. Dispatches by ExprOp; child operands live in Args.
// Returns a Go any per row; the caller knows the expected type.
package exec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type evalCtx struct{ batch types.Batch }

func newEvalCtx(batch types.Batch) *evalCtx { return &evalCtx{batch: batch} }

func (c *evalCtx) EvalBool(expr sql.BoundExpr, row int) (truth bool, err error) {
	v, err := c.eval(expr, row)
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	return ok && b, nil
}

func (c *evalCtx) eval(expr sql.BoundExpr, row int) (any, error) {
	switch expr.Op {
	case sql.ExprLiteral:
		return expr.Literal, nil
	case sql.ExprColumn:
		return c.colValue(expr.Column, row)
	case sql.ExprNot, sql.ExprLower, sql.ExprUpper, sql.ExprLength:
		return c.evalUnary(expr, row)
	case sql.ExprBetween:
		return c.evalBetween(expr, row)
	case sql.ExprIn:
		return c.evalIn(expr, row)
	case sql.ExprCoalesce:
		return c.evalCoalesce(expr, row)
	case sql.ExprSubstring:
		return c.evalSubstring(expr, row)
	case sql.ExprJSONGet, sql.ExprJSONGetText:
		return c.evalJSONPath(expr, row)
	}
	return c.evalBinary(expr, row)
}

func (c *evalCtx) colValue(name string, row int) (any, error) {
	col, ok := c.batch.ColumnByName(name)
	if !ok {
		return nil, fmt.Errorf("eval: column %q not in batch", name)
	}
	return col.ValueAt(row)
}

func (c *evalCtx) evalUnary(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 1 {
		return nil, fmt.Errorf("eval: unary missing operand")
	}
	v, err := c.eval(expr.Args[0], row)
	if err != nil {
		return nil, err
	}
	switch expr.Op {
	case sql.ExprNot:
		if v == nil {
			return nil, nil
		}
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("eval: NOT on non-bool")
		}
		return !b, nil
	case sql.ExprLower:
		if v == nil {
			return nil, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("eval: LOWER on non-text %T", v)
		}
		return strings.ToLower(s), nil
	case sql.ExprUpper:
		if v == nil {
			return nil, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("eval: UPPER on non-text %T", v)
		}
		return strings.ToUpper(s), nil
	case sql.ExprLength:
		if v == nil {
			return nil, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("eval: LENGTH on non-text %T", v)
		}
		return int64(len(s)), nil
	}
	return nil, fmt.Errorf("eval: unsupported unary op %v", expr.Op)
}

func (c *evalCtx) evalBinary(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 2 {
		return nil, fmt.Errorf("eval: binary op %v expects 2 args", expr.Op)
	}
	left, err := c.eval(expr.Args[0], row)
	if err != nil {
		return nil, err
	}
	right, err := c.eval(expr.Args[1], row)
	if err != nil {
		return nil, err
	}
	switch expr.Op {
	case sql.ExprAnd:
		return logicalAnd(left, right), nil
	case sql.ExprOr:
		return logicalOr(left, right), nil
	case sql.ExprEqual, sql.ExprNotEqual, sql.ExprLess, sql.ExprLessEqual, sql.ExprGreater, sql.ExprGreaterEqual:
		return compare(expr.Op, left, right)
	case sql.ExprAdd, sql.ExprSubtract, sql.ExprMultiply, sql.ExprDivide, sql.ExprModulo, sql.ExprIntDivide:
		return arithmetic(expr.Op, left, right)
	case sql.ExprConcat:
		return concat(left, right)
	}
	return nil, fmt.Errorf("eval: unsupported binary op %v", expr.Op)
}

func (c *evalCtx) evalCoalesce(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 2 {
		return nil, fmt.Errorf("eval: coalesce expects 2 args")
	}
	left, err := c.eval(expr.Args[0], row)
	if err != nil {
		return nil, err
	}
	if left != nil {
		return left, nil
	}
	return c.eval(expr.Args[1], row)
}

func (c *evalCtx) evalSubstring(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) < 2 {
		return nil, fmt.Errorf("eval: substring missing arguments")
	}
	text, err := c.eval(expr.Args[0], row)
	if err != nil || text == nil {
		return nil, err
	}
	s, ok := text.(string)
	if !ok {
		return nil, fmt.Errorf("eval: substring on non-text %T", text)
	}
	startRaw, err := c.eval(expr.Args[1], row)
	if err != nil || startRaw == nil {
		return nil, err
	}
	start, ok := asInt64(startRaw)
	if !ok {
		return nil, fmt.Errorf("eval: substring start not integer")
	}
	// SQL substring is 1-based with a clamp to [0, len].
	begin := int(start) - 1
	if begin < 0 {
		begin = 0
	}
	if begin > len(s) {
		begin = len(s)
	}
	end := len(s)
	if len(expr.Args) >= 3 {
		lengthRaw, err := c.eval(expr.Args[2], row)
		if err != nil || lengthRaw == nil {
			return nil, err
		}
		length, ok := asInt64(lengthRaw)
		if !ok {
			return nil, fmt.Errorf("eval: substring length not integer")
		}
		if length < 0 {
			return nil, fmt.Errorf("eval: substring length is negative")
		}
		end = begin + int(length)
		if end > len(s) {
			end = len(s)
		}
	}
	return s[begin:end], nil
}

func (c *evalCtx) evalJSONPath(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 2 {
		return nil, fmt.Errorf("eval: JSON path expects 2 args")
	}
	leftVal, err := c.eval(expr.Args[0], row)
	if err != nil || leftVal == nil {
		return nil, err
	}
	rightVal, err := c.eval(expr.Args[1], row)
	if err != nil || rightVal == nil {
		return nil, err
	}
	js, ok := leftVal.(string)
	if !ok {
		return nil, fmt.Errorf("eval: JSON path on non-text %T", leftVal)
	}
	var parsed any
	if err := json.Unmarshal([]byte(js), &parsed); err != nil {
		return nil, nil
	}
	got, ok := jsonStep(parsed, rightVal)
	if !ok {
		return nil, nil
	}
	if expr.Op == sql.ExprJSONGetText {
		switch t := got.(type) {
		case string:
			return t, nil
		case nil:
			return nil, nil
		default:
			out, err := json.Marshal(got)
			if err != nil {
				return nil, err
			}
			return string(out), nil
		}
	}
	out, err := json.Marshal(got)
	if err != nil {
		return nil, err
	}
	return string(out), nil
}

func jsonStep(value any, key any) (any, bool) {
	switch v := value.(type) {
	case map[string]any:
		k, ok := key.(string)
		if !ok {
			return nil, false
		}
		got, ok := v[k]
		return got, ok
	case []any:
		idx, ok := asInt64(key)
		if !ok {
			return nil, false
		}
		i := int(idx)
		if i < 0 || i >= len(v) {
			return nil, false
		}
		return v[i], true
	}
	return nil, false
}

func (c *evalCtx) evalBetween(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 3 {
		return nil, fmt.Errorf("eval: BETWEEN expects 3 args (target, low, high)")
	}
	target, err := c.eval(expr.Args[0], row)
	if err != nil || target == nil {
		return nil, err
	}
	low, err := c.eval(expr.Args[1], row)
	if err != nil {
		return nil, err
	}
	high, err := c.eval(expr.Args[2], row)
	if err != nil {
		return nil, err
	}
	geLow, err := compare(sql.ExprGreaterEqual, target, low)
	if err != nil {
		return nil, err
	}
	leHigh, err := compare(sql.ExprLessEqual, target, high)
	if err != nil {
		return nil, err
	}
	if geLow == nil || leHigh == nil {
		return nil, nil
	}
	return geLow.(bool) && leHigh.(bool), nil
}

func (c *evalCtx) evalIn(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) < 1 {
		return nil, fmt.Errorf("eval: IN missing target")
	}
	target, err := c.eval(expr.Args[0], row)
	if err != nil || target == nil {
		return nil, err
	}
	for _, candidate := range expr.Args[1:] {
		v, err := c.eval(candidate, row)
		if err != nil {
			return nil, err
		}
		eq, err := compare(sql.ExprEqual, target, v)
		if err != nil {
			return nil, err
		}
		if b, ok := eq.(bool); ok && b {
			return !expr.Not, nil
		}
	}
	return expr.Not, nil
}

func logicalAnd(left, right any) any {
	lb, lok := left.(bool)
	rb, rok := right.(bool)
	if lok && !lb {
		return false
	}
	if rok && !rb {
		return false
	}
	if lok && rok {
		return true
	}
	return nil
}

func logicalOr(left, right any) any {
	lb, lok := left.(bool)
	rb, rok := right.(bool)
	if lok && lb {
		return true
	}
	if rok && rb {
		return true
	}
	if lok && rok {
		return false
	}
	return nil
}

func compare(op sql.ExprOp, left, right any) (any, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	cmp, err := orderingCompare(left, right)
	if err != nil {
		return nil, err
	}
	switch op {
	case sql.ExprEqual:
		return cmp == 0, nil
	case sql.ExprNotEqual:
		return cmp != 0, nil
	case sql.ExprLess:
		return cmp < 0, nil
	case sql.ExprLessEqual:
		return cmp <= 0, nil
	case sql.ExprGreater:
		return cmp > 0, nil
	case sql.ExprGreaterEqual:
		return cmp >= 0, nil
	}
	return nil, fmt.Errorf("compare: unsupported op %v", op)
}

func orderingCompare(left, right any) (int, error) {
	if li, lok := asInt64(left); lok {
		if ri, rok := asInt64(right); rok {
			return types.CmpOrdered(li, ri), nil
		}
	}
	if lf, lok := asFloat64(left); lok {
		if rf, rok := asFloat64(right); rok {
			return types.CmpOrdered(lf, rf), nil
		}
	}
	if ls, lok := left.(string); lok {
		if rs, rok := right.(string); rok {
			return types.CmpOrdered(ls, rs), nil
		}
	}
	if lb, lok := left.(bool); lok {
		if rb, rok := right.(bool); rok {
			return types.CmpBool(lb, rb), nil
		}
	}
	if lbs, lok := left.([]byte); lok {
		if rbs, rok := right.([]byte); rok {
			return types.CmpBytes(lbs, rbs), nil
		}
	}
	return 0, fmt.Errorf("compare: incompatible operand types %T vs %T", left, right)
}

func arithmetic(op sql.ExprOp, left, right any) (any, error) {
	if left == nil || right == nil {
		return nil, nil
	}
	if li, lok := asInt64(left); lok {
		if ri, rok := asInt64(right); rok {
			return intArith(op, li, ri)
		}
	}
	lf, lok := asFloat64(left)
	rf, rok := asFloat64(right)
	if !lok || !rok {
		return nil, fmt.Errorf("arithmetic: non-numeric operands %T %T", left, right)
	}
	return floatArith(op, lf, rf)
}

func intArith(op sql.ExprOp, l, r int64) (any, error) {
	switch op {
	case sql.ExprAdd:
		return types.AddInt(l, r), nil
	case sql.ExprSubtract:
		return types.SubInt(l, r), nil
	case sql.ExprMultiply:
		return types.MulInt(l, r), nil
	case sql.ExprDivide, sql.ExprIntDivide:
		return types.DivInt(l, r)
	case sql.ExprModulo:
		return types.ModInt(l, r)
	}
	return nil, fmt.Errorf("arithmetic: unsupported int op %v", op)
}

func floatArith(op sql.ExprOp, l, r float64) (any, error) {
	switch op {
	case sql.ExprAdd:
		return types.AddFloat(l, r), nil
	case sql.ExprSubtract:
		return types.SubFloat(l, r), nil
	case sql.ExprMultiply:
		return types.MulFloat(l, r), nil
	case sql.ExprDivide:
		return types.DivFloat(l, r)
	}
	return nil, fmt.Errorf("arithmetic: unsupported float op %v", op)
}

func concat(left, right any) (any, error) {
	ls, lok := left.(string)
	rs, rok := right.(string)
	if !lok || !rok {
		return nil, fmt.Errorf("concat: non-text operands")
	}
	return ls + rs, nil
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int16:
		return float64(x), true
	}
	return 0, false
}
