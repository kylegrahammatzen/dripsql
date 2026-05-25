// Scalar expression binding: AST Expr -> typed BoundExpr against a column index.
// WHERE / HAVING rule sets live here so that the SELECT binder is a thin orchestrator.
package sql

import (
	"fmt"
	"math"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func BindExpr(columns []BoundColumnDef, expr Expr) (BoundExpr, error) {
	return bindExpr(buildColumnIndex(columns), expr)
}

func bindExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch e := expr.(type) {
	case *ColumnRef:
		name := e.Name
		if e.Qualifier != "" {
			name = e.Qualifier + "." + e.Name
		}
		col, ok := findColumn(columns, name)
		if !ok {
			if activeBindPlanner != nil {
				if outer, ok2 := activeBindPlanner.lookupOuter(name); ok2 {
					return BoundExpr{Op: ExprColumn, Type: outer.Type, Column: outer.Name, ColumnID: outer.ID, Outer: true}, nil
				}
			}
			return BoundExpr{}, fmt.Errorf("missing column %q", name)
		}
		return BoundExpr{Op: ExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}, nil
	case *Literal:
		return literalExpr(e.Value), nil
	case *FuncCall:
		if _, ok := aggregateFuncByName(e.Name); ok {
			return BoundExpr{}, fmt.Errorf("aggregate %s is only allowed in SELECT or HAVING", e.Name)
		}
		return bindScalarCall(columns, e)
	case *BinaryExpr:
		return bindBinaryExpr(columns, e)
	case *BetweenExpr:
		target, err := bindExpr(columns, e.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		low, err := bindExpr(columns, e.Low)
		if err != nil {
			return BoundExpr{}, err
		}
		high, err := bindExpr(columns, e.High)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprBetween, Type: types.Bool, Args: []BoundExpr{target, low, high}}, nil
	case *InExpr:
		target, err := bindExpr(columns, e.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		if len(e.Values) == 1 {
			if sub, ok := e.Values[0].(*SubqueryExpr); ok {
				return bindInSubqueryExpr(columns, target, sub, e.Not)
			}
		}
		args := make([]BoundExpr, 0, len(e.Values)+1)
		args = append(args, target)
		for _, v := range e.Values {
			bv, err := bindExpr(columns, v)
			if err != nil {
				return BoundExpr{}, err
			}
			args = append(args, bv)
		}
		return BoundExpr{Op: ExprIn, Type: types.Bool, Args: args, Not: e.Not}, nil
	case *AndExpr:
		left, right, err := bindBinary(columns, e.Left, e.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprAnd, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *OrExpr:
		left, right, err := bindBinary(columns, e.Left, e.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprOr, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *NotExpr:
		child, err := bindExpr(columns, e.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprNot, Type: types.Bool, Args: []BoundExpr{child}}, nil
	case *CaseExpr:
		return bindCaseExpr(columns, e)
	case *SubqueryExpr:
		return bindSubqueryExpr(columns, e)
	case *ExistsExpr:
		return bindExistsExpr(columns, e)
	default:
		return BoundExpr{}, fmt.Errorf("unsupported expression %T", expr)
	}
}

// Set by Planner.planSelect for the duration of one statement. Subquery
// binding consults this since bindExpr otherwise has no planner reference.
var activeBindPlanner *Planner

func bindInSubqueryExpr(columns map[string]BoundColumnDef, target BoundExpr, e *SubqueryExpr, not bool) (BoundExpr, error) {
	if e.Query == nil {
		return BoundExpr{}, fmt.Errorf("IN subquery: nil inner SELECT")
	}
	if activeBindPlanner == nil {
		return BoundExpr{}, fmt.Errorf("IN subquery: no planner context")
	}
	activeBindPlanner.pushOuter(columns)
	sub, err := activeBindPlanner.planSelect(e.Query)
	refs := activeBindPlanner.popOuter()
	if err != nil {
		return BoundExpr{}, fmt.Errorf("IN subquery: %w", err)
	}
	if sub.Kind != PlanQuery || sub.Rel == nil {
		return BoundExpr{}, fmt.Errorf("IN subquery: inner did not yield a query plan")
	}
	if len(sub.Rel.Outputs) != 1 {
		return BoundExpr{}, fmt.Errorf("IN subquery: inner must return exactly one column, got %d", len(sub.Rel.Outputs))
	}
	sub.OuterRefs = refs
	return BoundExpr{Op: ExprInSubquery, Type: types.Bool, Args: []BoundExpr{target}, SubPlan: sub, Not: not}, nil
}

func bindExistsExpr(columns map[string]BoundColumnDef, e *ExistsExpr) (BoundExpr, error) {
	if e.Query == nil {
		return BoundExpr{}, fmt.Errorf("EXISTS: nil inner SELECT")
	}
	if activeBindPlanner == nil {
		return BoundExpr{}, fmt.Errorf("EXISTS: no planner context")
	}
	activeBindPlanner.pushOuter(columns)
	sub, err := activeBindPlanner.planSelect(e.Query)
	refs := activeBindPlanner.popOuter()
	if err != nil {
		return BoundExpr{}, fmt.Errorf("EXISTS: %w", err)
	}
	if sub.Kind != PlanQuery || sub.Rel == nil {
		return BoundExpr{}, fmt.Errorf("EXISTS: inner did not yield a query plan")
	}
	sub.OuterRefs = refs
	return BoundExpr{Op: ExprExists, Type: types.Bool, SubPlan: sub, Not: e.Not}, nil
}

func bindSubqueryExpr(columns map[string]BoundColumnDef, e *SubqueryExpr) (BoundExpr, error) {
	if e.Query == nil {
		return BoundExpr{}, fmt.Errorf("subquery: nil inner SELECT")
	}
	if activeBindPlanner == nil {
		return BoundExpr{}, fmt.Errorf("subquery: no planner context")
	}
	activeBindPlanner.pushOuter(columns)
	sub, err := activeBindPlanner.planSelect(e.Query)
	refs := activeBindPlanner.popOuter()
	if err != nil {
		return BoundExpr{}, fmt.Errorf("subquery: %w", err)
	}
	if sub.Kind != PlanQuery || sub.Rel == nil {
		return BoundExpr{}, fmt.Errorf("subquery: inner did not yield a query plan")
	}
	if len(sub.Rel.Outputs) != 1 {
		return BoundExpr{}, fmt.Errorf("subquery: scalar subquery must return exactly one column, got %d", len(sub.Rel.Outputs))
	}
	out := sub.Rel.Outputs[0]
	sub.OuterRefs = refs
	return BoundExpr{Op: ExprSubquery, Type: out.Expr.Type, SubPlan: sub}, nil
}

func bindCaseExpr(columns map[string]BoundColumnDef, e *CaseExpr) (BoundExpr, error) {
	if len(e.When) == 0 {
		return BoundExpr{}, fmt.Errorf("CASE requires at least one WHEN clause")
	}
	args := make([]BoundExpr, 0, 2*len(e.When)+1)
	var resultType types.Type
	for i, wc := range e.When {
		whenBound, err := bindExpr(columns, wc.When)
		if err != nil {
			return BoundExpr{}, fmt.Errorf("CASE WHEN %d: %w", i, err)
		}
		if whenBound.Type.Kind != types.KindBool {
			return BoundExpr{}, fmt.Errorf("CASE WHEN %d: predicate must be boolean, got %s", i, whenBound.Type)
		}
		thenBound, err := bindExpr(columns, wc.Then)
		if err != nil {
			return BoundExpr{}, fmt.Errorf("CASE THEN %d: %w", i, err)
		}
		if i == 0 {
			resultType = thenBound.Type
		} else if thenBound.Type.Kind != resultType.Kind {
			return BoundExpr{}, fmt.Errorf("CASE THEN %d type %s does not match earlier THEN type %s", i, thenBound.Type, resultType)
		}
		args = append(args, whenBound, thenBound)
	}
	if e.Else != nil {
		elseBound, err := bindExpr(columns, e.Else)
		if err != nil {
			return BoundExpr{}, fmt.Errorf("CASE ELSE: %w", err)
		}
		if elseBound.Type.Kind != resultType.Kind {
			return BoundExpr{}, fmt.Errorf("CASE ELSE type %s does not match THEN type %s", elseBound.Type, resultType)
		}
		args = append(args, elseBound)
	} else {
		args = append(args, BoundExpr{Op: ExprLiteral, Type: resultType, Literal: nil})
	}
	return BoundExpr{Op: ExprCase, Type: resultType, Args: args}, nil
}

func bindBinaryExpr(columns map[string]BoundColumnDef, e *BinaryExpr) (BoundExpr, error) {
	left, right, err := bindBinary(columns, e.Left, e.Right)
	if err != nil {
		return BoundExpr{}, err
	}
	if e.Op == BinaryJSONGet || e.Op == BinaryJSONGetText {
		return bindJSONPathExpr(e.Op, left, right)
	}
	if isArithmeticOp(e.Op) {
		op, _ := arithmeticOp(e.Op)
		typ, err := arithmeticResultType(op, left, right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: op, Type: typ, Args: []BoundExpr{left, right}}, nil
	}
	if e.Op == BinaryConcat {
		if left.Type.Kind != types.KindText {
			return BoundExpr{}, fmt.Errorf("text expressions require a text left")
		}
		if right.Type.Kind != types.KindText {
			return BoundExpr{}, fmt.Errorf("text expressions require a text right")
		}
		return BoundExpr{Op: ExprConcat, Type: types.Text, Args: []BoundExpr{left, right}}, nil
	}
	op, err := bindBinaryOp(e.Op)
	if err != nil {
		return BoundExpr{}, err
	}
	if err := validateWhereComparison(left, op, right); err != nil {
		return BoundExpr{}, err
	}
	return BoundExpr{Op: op, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
}

func bindJSONPathExpr(op BinaryOp, left, right BoundExpr) (BoundExpr, error) {
	if left.Type.Kind != types.KindJSON && left.Type.Kind != types.KindText {
		return BoundExpr{}, fmt.Errorf("JSON path operator requires a JSON or text left operand")
	}
	if right.Type.Kind != types.KindText && !isIntegerExpr(right) {
		return BoundExpr{}, fmt.Errorf("JSON path operator requires a text key or integer index")
	}
	out := types.JSON
	bound := ExprJSONGet
	if op == BinaryJSONGetText {
		out = types.Text
		bound = ExprJSONGetText
	}
	return BoundExpr{Op: bound, Type: out, Args: []BoundExpr{left, right}}, nil
}

func bindBinary(columns map[string]BoundColumnDef, l, r Expr) (BoundExpr, BoundExpr, error) {
	left, err := bindExpr(columns, l)
	if err != nil {
		return BoundExpr{}, BoundExpr{}, err
	}
	right, err := bindExpr(columns, r)
	if err != nil {
		return BoundExpr{}, BoundExpr{}, err
	}
	return left, right, nil
}

func bindScalarCall(columns map[string]BoundColumnDef, call *FuncCall) (BoundExpr, error) {
	name := types.NormalizeName(call.Name)
	switch name {
	case "lower", "upper":
		if len(call.Args) != 1 {
			return BoundExpr{}, fmt.Errorf("%s() requires exactly one argument", name)
		}
		arg, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		op := ExprLower
		if name == "upper" {
			op = ExprUpper
		}
		return BoundExpr{Op: op, Type: types.Text, Args: []BoundExpr{arg}}, nil
	case "substring", "substr":
		if len(call.Args) < 2 || len(call.Args) > 3 {
			return BoundExpr{}, fmt.Errorf("substring() requires 2 or 3 arguments")
		}
		text, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		start, err := bindExpr(columns, call.Args[1])
		if err != nil {
			return BoundExpr{}, err
		}
		if !isIntegerExpr(start) {
			return BoundExpr{}, fmt.Errorf("substring() start must be an integer")
		}
		args := []BoundExpr{text, start}
		if len(call.Args) == 3 {
			length, err := bindExpr(columns, call.Args[2])
			if err != nil {
				return BoundExpr{}, err
			}
			if !isIntegerExpr(length) {
				return BoundExpr{}, fmt.Errorf("substring() length must be an integer")
			}
			args = append(args, length)
		}
		return BoundExpr{Op: ExprSubstring, Type: types.Text, Args: args}, nil
	case "length":
		if len(call.Args) != 1 {
			return BoundExpr{}, fmt.Errorf("length() requires exactly one argument")
		}
		arg, err := bindExpr(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		switch arg.Type.Kind {
		case types.KindText, types.KindBytes, types.KindJSON:
		default:
			return BoundExpr{}, fmt.Errorf("length() argument must be text, bytes, or json")
		}
		return BoundExpr{Op: ExprLength, Type: types.Int64, Args: []BoundExpr{arg}}, nil
	case "coalesce":
		if len(call.Args) == 0 {
			return BoundExpr{}, fmt.Errorf("coalesce() requires at least one argument")
		}
		args := make([]BoundExpr, 0, len(call.Args))
		var commonType types.Type
		for _, raw := range call.Args {
			bound, err := bindExpr(columns, raw)
			if err != nil {
				return BoundExpr{}, err
			}
			if bound.Type.Kind != types.KindInvalid {
				if commonType.Kind == types.KindInvalid {
					commonType = bound.Type
				} else if bound.Type.Kind != commonType.Kind || (commonType.Kind == types.KindNamed && bound.Type.Name != commonType.Name) {
					return BoundExpr{}, fmt.Errorf("coalesce() arguments must share a common type")
				}
			}
			args = append(args, bound)
		}
		if commonType.Kind == types.KindInvalid {
			return BoundExpr{}, fmt.Errorf("coalesce() requires at least one typed argument")
		}
		out := args[0]
		out.Type = commonType
		for i := 1; i < len(args); i++ {
			right := args[i]
			right.Type = commonType
			out = BoundExpr{Op: ExprCoalesce, Type: commonType, Args: []BoundExpr{out, right}}
		}
		return out, nil
	case "concat":
		if len(call.Args) == 0 {
			return BoundExpr{}, fmt.Errorf("concat() requires at least one argument")
		}
		out, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		for i := 1; i < len(call.Args); i++ {
			right, err := bindTextArg(columns, call.Args[i])
			if err != nil {
				return BoundExpr{}, err
			}
			out = BoundExpr{Op: ExprConcat, Type: types.Text, Args: []BoundExpr{out, right}}
		}
		return out, nil
	case "abs":
		if len(call.Args) != 1 {
			return BoundExpr{}, fmt.Errorf("abs() requires exactly one argument")
		}
		arg, err := bindExpr(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		switch arg.Type.Kind {
		case types.KindInt16, types.KindInt32, types.KindInt64, types.KindFloat32, types.KindFloat64:
		default:
			return BoundExpr{}, fmt.Errorf("abs() argument must be numeric")
		}
		return BoundExpr{Op: ExprAbs, Type: arg.Type, Args: []BoundExpr{arg}}, nil
	case "nullif":
		if len(call.Args) != 2 {
			return BoundExpr{}, fmt.Errorf("nullif() requires exactly two arguments")
		}
		left, err := bindExpr(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindExpr(columns, call.Args[1])
		if err != nil {
			return BoundExpr{}, err
		}
		if left.Type.Kind != right.Type.Kind {
			return BoundExpr{}, fmt.Errorf("nullif() arguments must share a type")
		}
		return BoundExpr{Op: ExprNullIf, Type: left.Type, Args: []BoundExpr{left, right}}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported scalar function %q", name)
	}
}

func bindTextArg(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	bound, err := bindExpr(columns, expr)
	if err != nil {
		return BoundExpr{}, err
	}
	if bound.Type.Kind != types.KindText {
		return BoundExpr{}, fmt.Errorf("text expressions require a text argument")
	}
	return bound, nil
}

func bindBinaryOp(op BinaryOp) (ExprOp, error) {
	switch op {
	case BinaryEqual:
		return ExprEqual, nil
	case BinaryNotEqual:
		return ExprNotEqual, nil
	case BinaryLess:
		return ExprLess, nil
	case BinaryLessEqual:
		return ExprLessEqual, nil
	case BinaryGreater:
		return ExprGreater, nil
	case BinaryGreaterEqual:
		return ExprGreaterEqual, nil
	default:
		return ExprInvalid, fmt.Errorf("unsupported WHERE operator")
	}
}

func isArithmeticOp(op BinaryOp) bool {
	_, ok := arithmeticOp(op)
	return ok
}

func arithmeticOp(op BinaryOp) (ExprOp, bool) {
	switch op {
	case BinaryAdd:
		return ExprAdd, true
	case BinarySubtract:
		return ExprSubtract, true
	case BinaryMultiply:
		return ExprMultiply, true
	case BinaryDivide:
		return ExprDivide, true
	case BinaryModulo:
		return ExprModulo, true
	case BinaryIntDivide:
		return ExprIntDivide, true
	default:
		return ExprInvalid, false
	}
}

func literalExpr(value Value) BoundExpr {
	expr := BoundExpr{Op: ExprLiteral, Literal: literalValue(value)}
	switch value.Kind {
	case ValueBool:
		expr.Type = types.Bool
	case ValueInt:
		expr.Type = types.Int64
	case ValueFloat:
		expr.Type = types.Float64
	case ValueString:
		expr.Type = types.Text
	}
	return expr
}

func literalValue(value Value) any {
	switch value.Kind {
	case ValueBool:
		return value.Bool
	case ValueInt:
		return value.Int
	case ValueFloat:
		return value.Float
	case ValueString:
		return value.String
	case ValueNull:
		return nil
	default:
		return nil
	}
}

func arithmeticResultType(op ExprOp, left BoundExpr, right BoundExpr) (types.Type, error) {
	if op == ExprModulo || op == ExprIntDivide {
		if !isIntegerExpr(left) || !isIntegerExpr(right) {
			return types.Type{}, fmt.Errorf("MOD and DIV require integer operands")
		}
		return types.Int64, nil
	}
	if !isNumericExpr(left) || !isNumericExpr(right) {
		return types.Type{}, fmt.Errorf("arithmetic expressions require numeric operands")
	}
	if classOf(left)&cFloat != 0 || classOf(right)&cFloat != 0 {
		return types.Float64, nil
	}
	return types.Int64, nil
}

type cmpRules struct {
	label               string
	intRangeCheck       func(col, lit BoundExpr) error
	allowFloat          bool
	textOrderingAllowed bool
	uuidExtraCheck      func(BoundExpr) error
	mismatchError       func(col, lit BoundExpr) error
}

var whereCmpRules = cmpRules{
	label:               "WHERE",
	intRangeCheck:       checkInt32LiteralRange,
	allowFloat:          true,
	textOrderingAllowed: true,
	mismatchError:       columnLiteralMismatchError,
}

var havingCmpRules = cmpRules{
	label:               "HAVING",
	intRangeCheck:       noopIntRangeCheck,
	textOrderingAllowed: true,
	uuidExtraCheck:      validateHavingUUIDBound,
	mismatchError:       havingMismatchError,
}

func noopIntRangeCheck(BoundExpr, BoundExpr) error { return nil }

func validateWhereComparison(left BoundExpr, op ExprOp, right BoundExpr) error {
	return validateComparison(left, op, right, whereCmpRules)
}

func validateComparison(left BoundExpr, op ExprOp, right BoundExpr, r cmpRules) error {
	if isIntegerExpr(left) && isIntegerExpr(right) {
		if err := r.intRangeCheck(left, right); err != nil {
			return err
		}
		return r.intRangeCheck(right, left)
	}
	if r.allowFloat && isNumericExpr(left) && isNumericExpr(right) {
		return nil
	}
	if left.Type.Kind == types.KindText && right.Type.Kind == types.KindText {
		if !r.textOrderingAllowed && isOrderingFilter(op) {
			return fmt.Errorf("%s text comparisons only support = and !=", r.label)
		}
		return nil
	}
	if left.Type.Kind == types.KindBool && right.Type.Kind == types.KindBool {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s bool comparisons only support = and !=", r.label)
		}
		return nil
	}
	if areXComparable(types.KindDate, left, right) || areXComparable(types.KindTimestamp, left, right) {
		return nil
	}
	if areXComparable(types.KindUUID, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s uuid comparisons only support = and !=", r.label)
		}
		if r.uuidExtraCheck != nil {
			if err := r.uuidExtraCheck(left); err != nil {
				return err
			}
			if err := r.uuidExtraCheck(right); err != nil {
				return err
			}
		}
		return nil
	}
	if areXComparable(types.KindBytes, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s bytes comparisons only support = and !=", r.label)
		}
		return nil
	}
	if areXComparable(types.KindNamed, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s enum comparisons only support = and !=", r.label)
		}
		return nil
	}
	if err := r.mismatchError(left, right); err != nil {
		return err
	}
	if err := r.mismatchError(right, left); err != nil {
		return err
	}
	return fmt.Errorf("%s comparison operands have incompatible types", r.label)
}

func columnLiteralMismatchError(col BoundExpr, lit BoundExpr) error {
	if col.Op != ExprColumn || lit.Op != ExprLiteral {
		return nil
	}
	return fmt.Errorf("WHERE column %q expects %s literal", col.Column, literalKindName(col.Type.Kind))
}

func havingMismatchError(col BoundExpr, lit BoundExpr) error {
	if col.Op != ExprColumn || lit.Op != ExprLiteral {
		return nil
	}
	switch col.Type.Kind {
	case types.KindBool:
		return fmt.Errorf("HAVING column %q expects bool literal", col.Column)
	case types.KindInt16, types.KindInt32, types.KindInt64:
		return fmt.Errorf("HAVING column %q expects int literal", col.Column)
	case types.KindFloat32, types.KindFloat64:
		return fmt.Errorf("HAVING column %q expects numeric literal", col.Column)
	case types.KindTimestamp:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid timestamp literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects timestamp string literal", col.Column)
	case types.KindDate:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid date literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects date string literal", col.Column)
	case types.KindUUID:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid uuid literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects uuid string literal", col.Column)
	default:
		return fmt.Errorf("HAVING column %q expects string literal", col.Column)
	}
}

func literalKindName(kind types.Kind) string {
	switch kind {
	case types.KindBool:
		return "bool"
	case types.KindInt16, types.KindInt32, types.KindInt64:
		return "int64"
	case types.KindFloat32, types.KindFloat64:
		return "numeric"
	case types.KindText, types.KindBytes:
		return "string"
	case types.KindDate:
		return "date string"
	case types.KindTimestamp:
		return "timestamp string"
	case types.KindUUID:
		return "uuid string"
	case types.KindNamed:
		return "enum string"
	default:
		return "compatible"
	}
}

func checkInt32LiteralRange(col BoundExpr, lit BoundExpr) error {
	if col.Op != ExprColumn || lit.Op != ExprLiteral {
		return nil
	}
	if col.Type.Kind != types.KindInt16 && col.Type.Kind != types.KindInt32 {
		return nil
	}
	value, ok := lit.Literal.(int64)
	if !ok || (value >= math.MinInt32 && value <= math.MaxInt32) {
		return nil
	}
	return fmt.Errorf("WHERE column %q int32 literal out of range", col.Column)
}

type betweenRules struct {
	label      string
	allowFloat bool
	allowText  bool
	mismatch   string
}

var (
	whereBetweenRules  = betweenRules{label: "WHERE", allowFloat: true, mismatch: "WHERE BETWEEN is only supported for numeric and temporal expressions"}
	havingBetweenRules = betweenRules{label: "HAVING", allowText: true, mismatch: "HAVING BETWEEN operands have incompatible types"}
)

func validateBetween(target, low, high BoundExpr, r betweenRules) error {
	if isIntegerExpr(target) && isIntegerExpr(low) && isIntegerExpr(high) {
		return nil
	}
	if r.allowFloat && isNumericExpr(target) && isNumericExpr(low) && isNumericExpr(high) {
		return nil
	}
	if r.allowText && target.Type.Kind == types.KindText && low.Type.Kind == types.KindText && high.Type.Kind == types.KindText {
		return nil
	}
	if target.Type.Kind == types.KindDate && textBound(types.KindDate, low) && textBound(types.KindDate, high) {
		return nil
	}
	if target.Type.Kind == types.KindTimestamp && textBound(types.KindTimestamp, low) && textBound(types.KindTimestamp, high) {
		return nil
	}
	return fmt.Errorf("%s", r.mismatch)
}

func validateWhereInValue(target BoundExpr, value BoundExpr) error {
	if isIntegerExpr(target) && isIntegerExpr(value) {
		return nil
	}
	if isNumericExpr(target) && isNumericExpr(value) {
		return nil
	}
	if target.Type.Kind == types.KindText && value.Type.Kind == types.KindText {
		return nil
	}
	if target.Type.Kind == types.KindBool && value.Type.Kind == types.KindBool {
		return nil
	}
	if target.Type.Kind == types.KindDate && textBound(types.KindDate, value) {
		return nil
	}
	if target.Type.Kind == types.KindTimestamp && textBound(types.KindTimestamp, value) {
		return nil
	}
	if target.Type.Kind == types.KindUUID && textBound(types.KindUUID, value) {
		return nil
	}
	if target.Type.Kind == types.KindBytes && textBound(types.KindBytes, value) {
		return nil
	}
	if target.Type.Kind == types.KindNamed && textBound(types.KindNamed, value) {
		return nil
	}
	return fmt.Errorf("WHERE IN operands have incompatible types")
}

func normalizeBound(target BoundExpr, bound *BoundExpr) {
	if bound.Op != ExprLiteral || bound.Type.Kind != types.KindText {
		return
	}
	value, ok := bound.Literal.(string)
	if !ok {
		return
	}
	switch target.Type.Kind {
	case types.KindTimestamp:
		if v, ok := normalizeTimestampString(value); ok {
			bound.Literal = v
		}
	case types.KindUUID:
		if v, ok := normalizeUUIDString(value); ok {
			bound.Literal = v
		}
	}
}

func normalizeComparison(left, right *BoundExpr) {
	normalizeBound(*left, right)
	normalizeBound(*right, left)
}

func normalizeTimestampString(s string) (string, bool) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", false
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z"), true
}

func normalizeUUIDString(s string) (string, bool) {
	u, err := types.ParseUUID(s)
	if err != nil {
		return "", false
	}
	return types.FormatUUID(u), true
}

func validateHavingUUIDBound(expr BoundExpr) error {
	if expr.Type.Kind == types.KindUUID || expr.Op != ExprLiteral || expr.Type.Kind != types.KindText {
		return nil
	}
	value, ok := expr.Literal.(string)
	if !ok {
		return nil
	}
	if _, err := types.ParseUUID(value); err != nil {
		return fmt.Errorf("HAVING invalid uuid literal %q", value)
	}
	return nil
}

type tclass uint16

const (
	cInt tclass = 1 << iota
	cFloat
	cText
	cBool
	cDate
	cTimestamp
	cUUID
	cBytes
	cNamed
)

const cNumeric = cInt | cFloat

func classOf(e BoundExpr) tclass {
	switch e.Type.Kind {
	case types.KindInt16, types.KindInt32, types.KindInt64:
		return cInt
	case types.KindFloat32, types.KindFloat64:
		return cFloat
	case types.KindText:
		return cText
	case types.KindBool:
		return cBool
	case types.KindDate:
		return cDate
	case types.KindTimestamp:
		return cTimestamp
	case types.KindUUID:
		return cUUID
	case types.KindBytes:
		return cBytes
	case types.KindNamed:
		return cNamed
	default:
		return 0
	}
}

func isIntegerExpr(e BoundExpr) bool { return classOf(e)&cInt != 0 }
func isNumericExpr(e BoundExpr) bool { return classOf(e)&cNumeric != 0 }

// textBound reports whether expr is comparable with a target-kind expression.
// Non-literal exprs must match the target kind exactly. Text literals accept the target
// only if their value parses (date, timestamp) or the kind is bytes/uuid/named (string-shape).
func textBound(target types.Kind, expr BoundExpr) bool {
	if expr.Type.Kind == target {
		return true
	}
	if expr.Op != ExprLiteral || expr.Type.Kind != types.KindText {
		return false
	}
	value, ok := expr.Literal.(string)
	if !ok {
		return false
	}
	switch target {
	case types.KindDate:
		_, err := time.Parse("2006-01-02", value)
		return err == nil
	case types.KindTimestamp:
		_, err := time.Parse(time.RFC3339Nano, value)
		return err == nil
	case types.KindUUID:
		_, err := types.ParseUUID(value)
		return err == nil
	case types.KindBytes, types.KindNamed:
		return true
	}
	return false
}

func areXComparable(target types.Kind, left, right BoundExpr) bool {
	if left.Type.Kind == target {
		if target == types.KindNamed && right.Type.Kind == types.KindNamed && left.Type.Name != right.Type.Name {
			return false
		}
		return textBound(target, right)
	}
	if right.Type.Kind == target {
		if target == types.KindNamed && left.Type.Kind == types.KindNamed && left.Type.Name != right.Type.Name {
			return false
		}
		return textBound(target, left)
	}
	return false
}

func isOrderingFilter(op ExprOp) bool {
	switch op {
	case ExprLess, ExprLessEqual, ExprGreater, ExprGreaterEqual:
		return true
	default:
		return false
	}
}

func buildColumnIndex(columns []BoundColumnDef) map[string]BoundColumnDef {
	index := make(map[string]BoundColumnDef, len(columns))
	for _, col := range columns {
		index[types.NormalizeName(col.Name)] = col
	}
	return index
}

// buildColumnIndexQualified is buildColumnIndex plus `<qualifier>.<col>` aliases
// for each given qualifier (table name and/or alias). Lets single-table scopes
// accept the standard `tbl.col` and `alias.col` syntax that joined scopes already
// require. Bare names still resolve.
func buildColumnIndexQualified(columns []BoundColumnDef, qualifiers ...string) map[string]BoundColumnDef {
	index := buildColumnIndex(columns)
	for _, q := range qualifiers {
		if q == "" {
			continue
		}
		prefix := types.NormalizeName(q) + "."
		for _, col := range columns {
			index[prefix+types.NormalizeName(col.Name)] = col
		}
	}
	return index
}

// joinedSource pairs an alias (or the table's own name when no alias was given) with its bound table.
type joinedSource struct {
	Alias string
	Def   BoundTableDef
}

// buildJoinedColumnIndex maps `alias.col` to its BoundColumnDef. Each column's Name is rewritten
// to its qualified form so the joined batch and the binder agree. Unqualified base names are added
// when unique; lookups for an ambiguous bare name return ok=false to force qualification.
func buildJoinedColumnIndex(sources []joinedSource) map[string]BoundColumnDef {
	index := make(map[string]BoundColumnDef)
	seenBare := make(map[string]int)
	for _, s := range sources {
		alias := types.NormalizeName(s.Alias)
		for _, col := range s.Def.Columns {
			qualified := alias + "." + types.NormalizeName(col.Name)
			rebound := col
			rebound.Name = qualified
			index[qualified] = rebound
			seenBare[types.NormalizeName(col.Name)]++
		}
	}
	for _, s := range sources {
		alias := types.NormalizeName(s.Alias)
		for _, col := range s.Def.Columns {
			bare := types.NormalizeName(col.Name)
			if seenBare[bare] != 1 {
				continue
			}
			index[bare] = index[alias+"."+bare]
		}
	}
	return index
}

func findColumn(columns map[string]BoundColumnDef, name string) (BoundColumnDef, bool) {
	col, ok := columns[types.NormalizeName(name)]
	return col, ok
}

func aggregateFuncByName(name string) (AggregateFunc, bool) {
	switch types.NormalizeName(name) {
	case "count":
		return AggregateCount, true
	case "sum":
		return AggregateSum, true
	case "min":
		return AggregateMin, true
	case "max":
		return AggregateMax, true
	case "avg":
		return AggregateAvg, true
	default:
		return AggregateInvalid, false
	}
}
