package exec

import (
	"fmt"
	"strings"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func evalFilterExprSelected(batch types.Batch, input types.SelectionMask, expr v3sql.BoundExpr, out *types.SelectionMask) (int, error) {
	out.Resize(batch.Len)
	matched := 0
	var evalErr error
	input.IterSet(func(row int) {
		if evalErr != nil {
			return
		}
		ok, err := evalFilterBool(batch, row, expr)
		if err != nil {
			evalErr = err
			return
		}
		if ok {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched, evalErr
}

func evalFilterBool(batch types.Batch, row int, expr v3sql.BoundExpr) (bool, error) {
	switch expr.Kind {
	case v3sql.BoundExprBinary:
		if expr.Left == nil || expr.Right == nil {
			return false, nil
		}
		switch expr.Op {
		case v3sql.BoundOpAnd:
			left, err := evalFilterBool(batch, row, *expr.Left)
			if err != nil || !left {
				return false, err
			}
			return evalFilterBool(batch, row, *expr.Right)
		case v3sql.BoundOpOr:
			left, err := evalFilterBool(batch, row, *expr.Left)
			if err != nil || left {
				return left, err
			}
			return evalFilterBool(batch, row, *expr.Right)
		case v3sql.BoundOpEqual, v3sql.BoundOpNotEqual, v3sql.BoundOpLess, v3sql.BoundOpLessEqual, v3sql.BoundOpGreater, v3sql.BoundOpGreaterEqual:
			return evalFilterCompare(batch, row, expr)
		default:
			value, ok, err := evalProjectValue(batch, row, expr)
			if err != nil || !ok {
				return false, err
			}
			boolValue, ok := value.(bool)
			if !ok {
				return false, fmt.Errorf("filter expression produced %T, want bool", value)
			}
			return boolValue, nil
		}
	case v3sql.BoundExprUnary:
		if expr.Op == v3sql.BoundOpNot && expr.Left != nil {
			value, err := evalFilterBool(batch, row, *expr.Left)
			if err != nil {
				return false, err
			}
			return !value, nil
		}
	case v3sql.BoundExprBetween:
		return evalFilterBetween(batch, row, expr)
	case v3sql.BoundExprIn:
		return evalFilterIn(batch, row, expr)
	}
	value, ok, err := evalProjectValue(batch, row, expr)
	if err != nil || !ok {
		return false, err
	}
	boolValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("filter expression produced %T, want bool", value)
	}
	return boolValue, nil
}

func evalFilterValue(batch types.Batch, row int, expr v3sql.BoundExpr) (any, bool, error) {
	if expr.Type.Kind == types.KindBool {
		switch expr.Kind {
		case v3sql.BoundExprBinary, v3sql.BoundExprUnary, v3sql.BoundExprBetween, v3sql.BoundExprIn:
			value, err := evalFilterBool(batch, row, expr)
			return value, true, err
		}
	}
	return evalProjectValue(batch, row, expr)
}

func evalFilterCompare(batch types.Batch, row int, expr v3sql.BoundExpr) (bool, error) {
	left, leftOK, err := evalFilterValue(batch, row, *expr.Left)
	if err != nil || !leftOK {
		return false, err
	}
	right, rightOK, err := evalFilterValue(batch, row, *expr.Right)
	if err != nil || !rightOK {
		return false, err
	}
	cmp, ok := compareFilterValues(left, right)
	if !ok {
		return false, fmt.Errorf("filter comparison operands have incompatible values %T and %T", left, right)
	}
	switch expr.Op {
	case v3sql.BoundOpEqual:
		return cmp == 0, nil
	case v3sql.BoundOpNotEqual:
		return cmp != 0, nil
	case v3sql.BoundOpLess:
		return cmp < 0, nil
	case v3sql.BoundOpLessEqual:
		return cmp <= 0, nil
	case v3sql.BoundOpGreater:
		return cmp > 0, nil
	case v3sql.BoundOpGreaterEqual:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("filter unsupported comparison op %d", expr.Op)
	}
}

func evalFilterBetween(batch types.Batch, row int, expr v3sql.BoundExpr) (bool, error) {
	if expr.Left == nil || len(expr.Args) != 2 {
		return false, nil
	}
	value, valueOK, err := evalFilterValue(batch, row, *expr.Left)
	if err != nil || !valueOK {
		return false, err
	}
	low, lowOK, err := evalFilterValue(batch, row, expr.Args[0])
	if err != nil || !lowOK {
		return false, err
	}
	high, highOK, err := evalFilterValue(batch, row, expr.Args[1])
	if err != nil || !highOK {
		return false, err
	}
	loCmp, ok := compareFilterValues(value, low)
	if !ok {
		return false, fmt.Errorf("filter BETWEEN lower bound has incompatible value %T", low)
	}
	hiCmp, ok := compareFilterValues(value, high)
	if !ok {
		return false, fmt.Errorf("filter BETWEEN upper bound has incompatible value %T", high)
	}
	return loCmp >= 0 && hiCmp <= 0, nil
}

func evalFilterIn(batch types.Batch, row int, expr v3sql.BoundExpr) (bool, error) {
	if expr.Left == nil {
		return false, nil
	}
	value, valueOK, err := evalFilterValue(batch, row, *expr.Left)
	if err != nil || !valueOK {
		return false, err
	}
	matched := false
	for _, arg := range expr.Args {
		candidate, candidateOK, err := evalFilterValue(batch, row, arg)
		if err != nil {
			return false, err
		}
		if !candidateOK {
			continue
		}
		cmp, ok := compareFilterValues(value, candidate)
		if ok && cmp == 0 {
			matched = true
			break
		}
	}
	if expr.Not {
		return !matched, nil
	}
	return matched, nil
}

func compareFilterValues(left any, right any) (int, bool) {
	if leftNum, ok := projectNumericValue(left); ok {
		rightNum, ok := projectNumericValue(right)
		if !ok {
			return 0, false
		}
		return compareOrdered(leftNum, rightNum), true
	}
	switch leftValue := left.(type) {
	case bool:
		rightValue, ok := right.(bool)
		if !ok {
			return 0, false
		}
		if leftValue == rightValue {
			return 0, true
		}
		if !leftValue {
			return -1, true
		}
		return 1, true
	case string:
		rightValue, ok := right.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(leftValue, rightValue), true
	case uint32:
		rightValue, ok := right.(uint32)
		if !ok {
			return 0, false
		}
		return compareOrdered(leftValue, rightValue), true
	case types.UUID16:
		rightValue, ok := right.(types.UUID16)
		if !ok {
			return 0, false
		}
		if leftValue == rightValue {
			return 0, true
		}
		return strings.Compare(string(leftValue[:]), string(rightValue[:])), true
	default:
		return 0, false
	}
}
