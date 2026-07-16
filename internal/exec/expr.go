// Row-by-row BoundExpr evaluator that dispatches by ExprOp with child operands in Args.
// Each row yields a Go any whose expected type the caller already knows.
package exec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type evalCtx struct {
	batch    vector.Batch
	outer    *correlatedOuter
	subBuild func(plan *sql.Plan) (Operator, error)
}

func newEvalCtx(batch vector.Batch) *evalCtx { return &evalCtx{batch: batch} }

func newEvalCtxWith(batch vector.Batch, outer *correlatedOuter, subBuild func(plan *sql.Plan) (Operator, error)) *evalCtx {
	return &evalCtx{batch: batch, outer: outer, subBuild: subBuild}
}

// EvalTruth evaluates a predicate to SQL three-valued truth where unknown reports a NULL result.
func (c *evalCtx) EvalTruth(expr sql.BoundExpr, row int) (truth bool, unknown bool, err error) {
	v, err := c.eval(expr, row)
	if err != nil {
		return false, false, err
	}
	if v == nil {
		return false, true, nil
	}
	b, ok := v.(bool)
	return ok && b, false, nil
}

func (c *evalCtx) EvalBool(expr sql.BoundExpr, row int) (truth bool, err error) {
	truth, _, err = c.EvalTruth(expr, row)
	return truth, err
}

func (c *evalCtx) eval(expr sql.BoundExpr, row int) (any, error) {
	switch expr.Op {
	case sql.ExprLiteral:
		return expr.Literal, nil
	case sql.ExprColumn:
		if expr.Outer {
			if v, ok := c.outer.get(expr.Column); ok {
				return v, nil
			}
			return nil, fmt.Errorf("eval: outer column %q not bound", expr.Column)
		}
		return c.colValue(expr.Column, row)
	case sql.ExprSubquery:
		return c.evalCorrelatedScalar(expr, row)
	case sql.ExprInSubquery:
		return c.evalCorrelatedIn(expr, row)
	case sql.ExprExists:
		return c.evalCorrelatedExists(expr, row)
	case sql.ExprNot, sql.ExprLower, sql.ExprUpper, sql.ExprLength, sql.ExprAbs:
		return c.evalUnary(expr, row)
	case sql.ExprIsNull, sql.ExprIsNotNull:
		if len(expr.Args) != 1 {
			return nil, fmt.Errorf("eval: IS NULL expects 1 arg")
		}
		v, err := c.eval(expr.Args[0], row)
		if err != nil {
			return nil, err
		}
		if expr.Op == sql.ExprIsNull {
			return v == nil, nil
		}
		return v != nil, nil
	case sql.ExprNullIf:
		return c.evalNullIf(expr, row)
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
	case sql.ExprCase:
		return c.evalCase(expr, row)
	}
	return c.evalBinary(expr, row)
}

func (c *evalCtx) evalCase(expr sql.BoundExpr, row int) (any, error) {
	args := expr.Args
	if len(args) < 1 || len(args)%2 != 1 {
		return nil, fmt.Errorf("eval: CASE arg shape invalid (got %d args)", len(args))
	}
	for i := 0; i+1 < len(args); i += 2 {
		whenVal, err := c.eval(args[i], row)
		if err != nil {
			return nil, err
		}
		b, ok := whenVal.(bool)
		if !ok || !b {
			continue
		}
		return c.eval(args[i+1], row)
	}
	return c.eval(args[len(args)-1], row)
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
	case sql.ExprAbs:
		if v == nil {
			return nil, nil
		}
		switch x := v.(type) {
		case int64:
			if x < 0 {
				return -x, nil
			}
			return x, nil
		case float64:
			if x < 0 {
				return -x, nil
			}
			return x, nil
		default:
			return nil, fmt.Errorf("eval: ABS on non-numeric %T", v)
		}
	}
	return nil, fmt.Errorf("eval: unsupported unary op %v", expr.Op)
}

func (c *evalCtx) evalNullIf(expr sql.BoundExpr, row int) (any, error) {
	if len(expr.Args) != 2 {
		return nil, fmt.Errorf("eval: nullif expects 2 args")
	}
	left, err := c.eval(expr.Args[0], row)
	if err != nil {
		return nil, err
	}
	if left == nil {
		return nil, nil
	}
	right, err := c.eval(expr.Args[1], row)
	if err != nil {
		return nil, err
	}
	if right == nil {
		return left, nil
	}
	eq, err := compare(sql.ExprEqual, left, right)
	if err != nil {
		return nil, err
	}
	if b, ok := eq.(bool); ok && b {
		return nil, nil
	}
	return left, nil
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
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if (lok && !lb) || (rok && !rb) {
			return false, nil
		}
		if lok && rok {
			return true, nil
		}
		return nil, nil
	case sql.ExprOr:
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if (lok && lb) || (rok && rb) {
			return true, nil
		}
		if lok && rok {
			return false, nil
		}
		return nil, nil
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
	begin := min(max(int(start)-1, 0), len(s))
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
		end = min(begin+int(length), len(s))
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
	sawNull := false
	for _, candidate := range expr.Args[1:] {
		v, err := c.eval(candidate, row)
		if err != nil {
			return nil, err
		}
		if v == nil {
			sawNull = true
			continue
		}
		eq, err := compare(sql.ExprEqual, target, v)
		if err != nil {
			return nil, err
		}
		if b, ok := eq.(bool); ok && b {
			return !expr.Not, nil
		}
	}
	// A miss against a list containing NULL is unknown so NOT IN cannot claim the row.
	if sawNull {
		return nil, nil
	}
	return expr.Not, nil
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
			return vector.CmpOrdered(li, ri), nil
		}
		if rf, rok := asFloat64(right); rok {
			return vector.CmpOrdered(float64(li), rf), nil
		}
	}
	if lf, lok := asFloat64(left); lok {
		if rf, rok := asFloat64(right); rok {
			return vector.CmpOrdered(lf, rf), nil
		}
	}
	if ls, lok := left.(string); lok {
		if rs, rok := right.(string); rok {
			return vector.CmpOrdered(ls, rs), nil
		}
	}
	if lb, lok := left.(bool); lok {
		if rb, rok := right.(bool); rok {
			return vector.CmpBool(lb, rb), nil
		}
	}
	if lbs, lok := left.([]byte); lok {
		if rbs, rok := right.([]byte); rok {
			return vector.CmpBytes(lbs, rbs), nil
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
			switch op {
			case sql.ExprAdd:
				return vector.AddInt(li, ri), nil
			case sql.ExprSubtract:
				return vector.SubInt(li, ri), nil
			case sql.ExprMultiply:
				return vector.MulInt(li, ri), nil
			case sql.ExprDivide, sql.ExprIntDivide:
				return vector.DivInt(li, ri)
			case sql.ExprModulo:
				return vector.ModInt(li, ri)
			}
			return nil, fmt.Errorf("arithmetic: unsupported int op %v", op)
		}
	}
	lf, lok := asFloat64(left)
	rf, rok := asFloat64(right)
	if !lok || !rok {
		return nil, fmt.Errorf("arithmetic: non-numeric operands %T %T", left, right)
	}
	switch op {
	case sql.ExprAdd:
		return vector.AddFloat(lf, rf), nil
	case sql.ExprSubtract:
		return vector.SubFloat(lf, rf), nil
	case sql.ExprMultiply:
		return vector.MulFloat(lf, rf), nil
	case sql.ExprDivide:
		return vector.DivFloat(lf, rf)
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
