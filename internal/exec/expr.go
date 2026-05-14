package exec

import (
	"fmt"
	"math"
	"strings"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func projectBoundOutputs(batch types.Batch, sel types.SelectionMask, outputs []v3sql.BoundOutput) (types.Batch, error) {
	cols := make([]types.Column, len(outputs))
	for i, output := range outputs {
		name, err := projectOutputName(output)
		if err != nil {
			return types.Batch{}, err
		}
		if output.Expr.Kind == v3sql.BoundExprColumn {
			col, ok := columnByName(batch, output.Expr.Column)
			if !ok {
				return types.Batch{}, fmt.Errorf("missing projected column %q", output.Expr.Column)
			}
			col.Name = name
			cols[i] = col
			continue
		}
		col, err := evalProjectColumn(batch, sel, name, output.Expr)
		if err != nil {
			return types.Batch{}, err
		}
		cols[i] = col
	}
	return types.NewBatch(cols)
}

func projectOutputName(output v3sql.BoundOutput) (string, error) {
	if output.Alias != "" {
		return output.Alias, nil
	}
	if output.Expr.Kind == v3sql.BoundExprColumn && output.Expr.Column != "" {
		return output.Expr.Column, nil
	}
	return "", fmt.Errorf("project expression requires an alias")
}

func evalProjectColumn(batch types.Batch, sel types.SelectionMask, name string, expr v3sql.BoundExpr) (types.Column, error) {
	typ := expr.Type
	if typ.Kind == types.KindInvalid {
		// SQL NULL is untyped in the binder; store it as an all-null text vector.
		typ = types.Text
	}
	kind, err := types.VecKindOf(typ)
	if err != nil {
		return types.Column{}, err
	}
	if col, ok, err := evalProjectColumnFast(batch, sel, name, typ, kind, expr); ok || err != nil {
		return col, err
	}
	valid := make(types.Validity, types.ValidityWords(batch.Len))
	v := types.Vec{Kind: kind, Encoding: types.EncodingFlat, Len: batch.Len, Valid: valid}
	switch kind {
	case types.VecBool:
		v.BoolBits = make([]uint64, types.ValidityWords(batch.Len))
		if err := fillProjectBool(batch, sel, expr, valid, v.BoolBits); err != nil {
			return types.Column{}, err
		}
	case types.VecInt16:
		v.I16 = make([]int16, batch.Len)
		if err := fillProjectInt16(batch, sel, expr, valid, v.I16); err != nil {
			return types.Column{}, err
		}
	case types.VecInt32, types.VecDate:
		v.I32 = make([]int32, batch.Len)
		if err := fillProjectInt32(batch, sel, expr, valid, v.I32); err != nil {
			return types.Column{}, err
		}
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		v.I64 = make([]int64, batch.Len)
		if err := fillProjectInt64(batch, sel, expr, valid, v.I64); err != nil {
			return types.Column{}, err
		}
	case types.VecFloat32:
		v.F32 = make([]float32, batch.Len)
		if err := fillProjectFloat32(batch, sel, expr, valid, v.F32); err != nil {
			return types.Column{}, err
		}
	case types.VecFloat64:
		v.F64 = make([]float64, batch.Len)
		if err := fillProjectFloat64(batch, sel, expr, valid, v.F64); err != nil {
			return types.Column{}, err
		}
	case types.VecText, types.VecBytes, types.VecJSON:
		varText, err := fillProjectVarBytes(batch, sel, expr, valid)
		if err != nil {
			return types.Column{}, err
		}
		v.Var = varText
	case types.VecUUID:
		v.UUID = make([]types.UUID16, batch.Len)
		if err := fillProjectUUID(batch, sel, expr, valid, v.UUID); err != nil {
			return types.Column{}, err
		}
	case types.VecEnum32:
		v.U32 = make([]uint32, batch.Len)
		if err := fillProjectU32(batch, sel, expr, valid, v.U32); err != nil {
			return types.Column{}, err
		}
	default:
		return types.Column{}, fmt.Errorf("project unsupported vector kind %s", kind)
	}
	return types.Column{Name: name, Type: typ, V: v}, nil
}

func evalProjectColumnFast(batch types.Batch, sel types.SelectionMask, name string, typ types.Type, kind types.VecKind, expr v3sql.BoundExpr) (types.Column, bool, error) {
	if kind != types.VecInt64 {
		return types.Column{}, false, nil
	}
	plan, ok := int64BinaryProjectPlan(expr)
	if !ok {
		return types.Column{}, false, nil
	}
	col, ok := columnByName(batch, plan.column)
	if !ok {
		return types.Column{}, true, fmt.Errorf("missing projected column %q", plan.column)
	}
	values, ok := int64ProjectValues(col.V)
	if !ok {
		return types.Column{}, false, nil
	}
	valid := make(types.Validity, types.ValidityWords(batch.Len))
	out := make([]int64, batch.Len)
	var projectErr error
	sel.IterSet(func(row int) {
		if projectErr != nil || !types.IsValid(col.V.Valid, row) {
			return
		}
		left := values[row]
		right := plan.literal
		if plan.literalLeft {
			left, right = plan.literal, values[row]
		}
		value, err := evalInt64ProjectBinary(left, plan.op, right)
		if err != nil {
			projectErr = err
			return
		}
		out[row] = value
		types.SetValid(valid, row)
	})
	if projectErr != nil {
		return types.Column{}, true, projectErr
	}
	return types.Column{Name: name, Type: typ, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: batch.Len, Valid: valid, I64: out}}, true, nil
}

type int64ProjectPlan struct {
	column      string
	literal     int64
	op          v3sql.BoundOp
	literalLeft bool
}

func int64BinaryProjectPlan(expr v3sql.BoundExpr) (int64ProjectPlan, bool) {
	if expr.Kind != v3sql.BoundExprBinary || !isInt64ProjectOp(expr.Op) || expr.Left == nil || expr.Right == nil {
		return int64ProjectPlan{}, false
	}
	if expr.Left.Kind == v3sql.BoundExprColumn && expr.Right.Kind == v3sql.BoundExprLiteral {
		literal, ok := projectIntValue(expr.Right.Literal)
		return int64ProjectPlan{column: expr.Left.Column, literal: literal, op: expr.Op}, ok
	}
	if expr.Left.Kind == v3sql.BoundExprLiteral && expr.Right.Kind == v3sql.BoundExprColumn {
		literal, ok := projectIntValue(expr.Left.Literal)
		return int64ProjectPlan{column: expr.Right.Column, literal: literal, op: expr.Op, literalLeft: true}, ok
	}
	return int64ProjectPlan{}, false
}

func isInt64ProjectOp(op v3sql.BoundOp) bool {
	switch op {
	case v3sql.BoundOpAdd, v3sql.BoundOpSubtract, v3sql.BoundOpMultiply, v3sql.BoundOpDivide, v3sql.BoundOpModulo, v3sql.BoundOpIntDivide:
		return true
	default:
		return false
	}
}

func int64ProjectValues(v types.Vec) ([]int64, bool) {
	if v.Encoding != types.EncodingFlat {
		return nil, false
	}
	switch v.Kind {
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		return v.I64, true
	default:
		return nil, false
	}
}

func evalInt64ProjectBinary(left int64, op v3sql.BoundOp, right int64) (int64, error) {
	switch op {
	case v3sql.BoundOpAdd:
		return left + right, nil
	case v3sql.BoundOpSubtract:
		return left - right, nil
	case v3sql.BoundOpMultiply:
		return left * right, nil
	case v3sql.BoundOpDivide, v3sql.BoundOpIntDivide:
		if right == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return left / right, nil
	case v3sql.BoundOpModulo:
		if right == 0 {
			return 0, fmt.Errorf("modulo by zero")
		}
		return left % right, nil
	default:
		return 0, fmt.Errorf("project unsupported arithmetic operator %d", op)
	}
}

func fillProjectBool(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []uint64) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		boolValue, ok := value.(bool)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want bool", exprName(expr), value)
			return
		}
		if boolValue {
			out[row>>6] |= uint64(1) << uint(row&63)
		}
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectInt16(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []int16) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		intValue, ok := projectIntValue(value)
		if !ok || intValue < math.MinInt16 || intValue > math.MaxInt16 {
			pushErr = fmt.Errorf("project expression %q produced %T, want int16", exprName(expr), value)
			return
		}
		out[row] = int16(intValue)
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectInt32(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []int32) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		intValue, ok := projectIntValue(value)
		if !ok || intValue < math.MinInt32 || intValue > math.MaxInt32 {
			pushErr = fmt.Errorf("project expression %q produced %T, want int32", exprName(expr), value)
			return
		}
		out[row] = int32(intValue)
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectInt64(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []int64) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		intValue, ok := projectIntValue(value)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want int64", exprName(expr), value)
			return
		}
		out[row] = intValue
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectFloat32(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []float32) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		floatValue, ok := projectNumericValue(value)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want float32", exprName(expr), value)
			return
		}
		out[row] = float32(floatValue)
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectFloat64(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []float64) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		floatValue, ok := projectNumericValue(value)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want float64", exprName(expr), value)
			return
		}
		out[row] = floatValue
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectVarBytes(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity) (types.VarBytes, error) {
	varText := types.NewVarBytes(batch.Len, 0)
	for row := 0; row < batch.Len; row++ {
		if !sel.IsSet(row) {
			varText.AppendString(row, "")
			continue
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			return types.VarBytes{}, err
		}
		if !ok {
			varText.AppendString(row, "")
			continue
		}
		text, ok := value.(string)
		if !ok {
			return types.VarBytes{}, fmt.Errorf("project expression %q produced %T, want text", exprName(expr), value)
		}
		varText.AppendString(row, text)
		types.SetValid(valid, row)
	}
	return varText, nil
}

func fillProjectUUID(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []types.UUID16) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		uuid, ok := value.(types.UUID16)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want uuid", exprName(expr), value)
			return
		}
		out[row] = uuid
		types.SetValid(valid, row)
	})
	return pushErr
}

func fillProjectU32(batch types.Batch, sel types.SelectionMask, expr v3sql.BoundExpr, valid types.Validity, out []uint32) error {
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		value, ok, err := evalProjectValue(batch, row, expr)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		u32, ok := value.(uint32)
		if !ok {
			pushErr = fmt.Errorf("project expression %q produced %T, want enum", exprName(expr), value)
			return
		}
		out[row] = u32
		types.SetValid(valid, row)
	})
	return pushErr
}

func evalProjectValue(batch types.Batch, row int, expr v3sql.BoundExpr) (any, bool, error) {
	switch expr.Kind {
	case v3sql.BoundExprColumn:
		col, ok := columnByName(batch, expr.Column)
		if !ok {
			return nil, false, fmt.Errorf("missing expression column %q", expr.Column)
		}
		return projectColumnValue(col.V, row)
	case v3sql.BoundExprLiteral:
		if expr.Literal == nil {
			return nil, false, nil
		}
		return expr.Literal, true, nil
	case v3sql.BoundExprBinary:
		return evalProjectBinary(batch, row, expr)
	case v3sql.BoundExprUnary:
		return evalProjectUnary(batch, row, expr)
	default:
		return nil, false, fmt.Errorf("project unsupported expression kind %d", expr.Kind)
	}
}

func projectColumnValue(v types.Vec, row int) (any, bool, error) {
	if !types.IsValid(v.Valid, row) {
		return nil, false, nil
	}
	switch v.Kind {
	case types.VecBool:
		return v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0, true, nil
	case types.VecInt16:
		return v.I16[row], true, nil
	case types.VecInt32, types.VecDate:
		return v.I32[row], true, nil
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		return v.I64[row], true, nil
	case types.VecFloat32:
		return v.F32[row], true, nil
	case types.VecFloat64:
		return v.F64[row], true, nil
	case types.VecText, types.VecBytes, types.VecJSON:
		value, ok := v.TextCopy(row)
		if !ok {
			return nil, false, fmt.Errorf("project unsupported text encoding %s", v.Encoding)
		}
		return value, true, nil
	case types.VecUUID:
		return v.UUID[row], true, nil
	case types.VecEnum32:
		return v.U32[row], true, nil
	default:
		return nil, false, fmt.Errorf("project unsupported column kind %s", v.Kind)
	}
}

func evalProjectBinary(batch types.Batch, row int, expr v3sql.BoundExpr) (any, bool, error) {
	if expr.Left == nil || expr.Right == nil {
		return nil, false, nil
	}
	left, leftOK, err := evalProjectValue(batch, row, *expr.Left)
	if err != nil {
		return nil, false, err
	}
	right, rightOK, err := evalProjectValue(batch, row, *expr.Right)
	if err != nil {
		return nil, false, err
	}
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	switch expr.Op {
	case v3sql.BoundOpAdd, v3sql.BoundOpSubtract, v3sql.BoundOpMultiply, v3sql.BoundOpDivide, v3sql.BoundOpModulo, v3sql.BoundOpIntDivide:
		return computeProjectArithmetic(left, expr.Op, right)
	case v3sql.BoundOpConcat:
		leftText, leftOK := left.(string)
		rightText, rightOK := right.(string)
		if !leftOK || !rightOK {
			return nil, false, nil
		}
		return leftText + rightText, true, nil
	case v3sql.BoundOpJSONGet, v3sql.BoundOpJSONGetText:
		return evalProjectJSONPath(left, expr.Op, right)
	default:
		return nil, false, fmt.Errorf("project unsupported binary expression operator %d", expr.Op)
	}
}

func evalProjectUnary(batch types.Batch, row int, expr v3sql.BoundExpr) (any, bool, error) {
	if expr.Left == nil {
		return nil, false, nil
	}
	value, ok, err := evalProjectValue(batch, row, *expr.Left)
	if err != nil || !ok {
		return nil, false, err
	}
	switch expr.Op {
	case v3sql.BoundOpLower:
		text, ok := value.(string)
		if !ok {
			return nil, false, nil
		}
		return strings.ToLower(text), true, nil
	case v3sql.BoundOpUpper:
		text, ok := value.(string)
		if !ok {
			return nil, false, nil
		}
		return strings.ToUpper(text), true, nil
	case v3sql.BoundOpLength:
		text, ok := value.(string)
		if !ok {
			return nil, false, nil
		}
		return int64(len(text)), true, nil
	default:
		return nil, false, fmt.Errorf("project unsupported unary expression operator %d", expr.Op)
	}
}

func computeProjectArithmetic(left any, op v3sql.BoundOp, right any) (any, bool, error) {
	if _, ok := projectFloatValue(left); ok {
		return computeProjectFloatArithmetic(left, op, right)
	}
	if _, ok := projectFloatValue(right); ok {
		return computeProjectFloatArithmetic(left, op, right)
	}
	leftInt, ok := projectIntValue(left)
	if !ok {
		return nil, false, nil
	}
	rightInt, ok := projectIntValue(right)
	if !ok {
		return nil, false, nil
	}
	switch op {
	case v3sql.BoundOpAdd:
		return leftInt + rightInt, true, nil
	case v3sql.BoundOpSubtract:
		return leftInt - rightInt, true, nil
	case v3sql.BoundOpMultiply:
		return leftInt * rightInt, true, nil
	case v3sql.BoundOpDivide, v3sql.BoundOpIntDivide:
		if rightInt == 0 {
			return nil, false, fmt.Errorf("division by zero")
		}
		return leftInt / rightInt, true, nil
	case v3sql.BoundOpModulo:
		if rightInt == 0 {
			return nil, false, fmt.Errorf("modulo by zero")
		}
		return leftInt % rightInt, true, nil
	default:
		return nil, false, fmt.Errorf("project unsupported arithmetic operator %d", op)
	}
}

func computeProjectFloatArithmetic(left any, op v3sql.BoundOp, right any) (any, bool, error) {
	leftFloat, leftOK := projectNumericValue(left)
	rightFloat, rightOK := projectNumericValue(right)
	if !leftOK || !rightOK {
		return nil, false, nil
	}
	switch op {
	case v3sql.BoundOpAdd:
		return leftFloat + rightFloat, true, nil
	case v3sql.BoundOpSubtract:
		return leftFloat - rightFloat, true, nil
	case v3sql.BoundOpMultiply:
		return leftFloat * rightFloat, true, nil
	case v3sql.BoundOpDivide:
		if rightFloat == 0 {
			return nil, false, fmt.Errorf("division by zero")
		}
		return leftFloat / rightFloat, true, nil
	case v3sql.BoundOpModulo, v3sql.BoundOpIntDivide:
		return nil, false, fmt.Errorf("MOD and DIV require integer operands")
	default:
		return nil, false, fmt.Errorf("project unsupported arithmetic operator %d", op)
	}
}

func projectIntValue(value any) (int64, bool) {
	switch value := value.(type) {
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	default:
		return 0, false
	}
}

func projectFloatValue(value any) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func projectNumericValue(value any) (float64, bool) {
	if value, ok := projectFloatValue(value); ok {
		return value, true
	}
	if value, ok := projectIntValue(value); ok {
		return float64(value), true
	}
	return 0, false
}

func exprName(expr v3sql.BoundExpr) string {
	if expr.Column != "" {
		return expr.Column
	}
	return expr.Type.String()
}
