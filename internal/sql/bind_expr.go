package sql

import (
	"fmt"
	"math"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// BindExpr lowers an AST expression into a bound scalar expression. Aggregate
// calls and StarRef are handled by SELECT binding, not by scalar binding.
func BindExpr(columns []BoundColumnDef, expr Expr) (BoundExpr, error) {
	return bindExpr(buildColumnIndex(columns), expr)
}

func bindExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch e := expr.(type) {
	case *ColumnRef:
		col, ok := findColumn(columns, e.Name)
		if !ok {
			return BoundExpr{}, fmt.Errorf("missing column %q", e.Name)
		}
		return BoundExpr{Kind: BoundExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}, nil
	case *Literal:
		return literalExpr(e.Value), nil
	case *FuncCall:
		if isAggregateName(e.Name) {
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
		return BoundExpr{Kind: BoundExprBetween, Type: types.Bool, Left: &target, Args: []BoundExpr{low, high}}, nil
	case *InExpr:
		target, err := bindExpr(columns, e.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		values := make([]BoundExpr, 0, len(e.Values))
		for _, v := range e.Values {
			bv, err := bindExpr(columns, v)
			if err != nil {
				return BoundExpr{}, err
			}
			values = append(values, bv)
		}
		return BoundExpr{Kind: BoundExprIn, Type: types.Bool, Left: &target, Args: values, Not: e.Not}, nil
	case *AndExpr:
		return bindBoolBinary(columns, e.Left, e.Right, BoundOpAnd)
	case *OrExpr:
		return bindBoolBinary(columns, e.Left, e.Right, BoundOpOr)
	case *NotExpr:
		child, err := bindExpr(columns, e.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprUnary, Type: types.Bool, Op: BoundOpNot, Left: &child}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported expression %T", expr)
	}
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
		return BoundExpr{Kind: BoundExprBinary, Type: typ, Op: op, Left: &left, Right: &right}, nil
	}
	if e.Op == BinaryConcat {
		if left.Type.Kind != types.KindText {
			return BoundExpr{}, fmt.Errorf("text expressions require a text left")
		}
		if right.Type.Kind != types.KindText {
			return BoundExpr{}, fmt.Errorf("text expressions require a text right")
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Text, Op: BoundOpConcat, Left: &left, Right: &right}, nil
	}
	op, err := bindBinaryOp(e.Op)
	if err != nil {
		return BoundExpr{}, err
	}
	return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: filterOpToExprOp(op), Left: &left, Right: &right}, nil
}

func bindJSONPathExpr(op BinaryOp, left, right BoundExpr) (BoundExpr, error) {
	if left.Type.Kind != types.KindJSON && left.Type.Kind != types.KindText {
		return BoundExpr{}, fmt.Errorf("JSON path operator requires a JSON or text left operand")
	}
	if right.Type.Kind != types.KindText && !isIntegerExpr(right) {
		return BoundExpr{}, fmt.Errorf("JSON path operator requires a text key or integer index")
	}
	out := types.JSON
	bound := BoundOpJSONGet
	if op == BinaryJSONGetText {
		out = types.Text
		bound = BoundOpJSONGetText
	}
	return BoundExpr{Kind: BoundExprBinary, Type: out, Op: bound, Left: &left, Right: &right}, nil
}

func bindBoolBinary(columns map[string]BoundColumnDef, l, r Expr, op BoundOp) (BoundExpr, error) {
	left, right, err := bindBinary(columns, l, r)
	if err != nil {
		return BoundExpr{}, err
	}
	return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: op, Left: &left, Right: &right}, nil
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
	name := normalizeName(call.Name)
	switch name {
	case "lower", "upper":
		if len(call.Args) != 1 {
			return BoundExpr{}, fmt.Errorf("%s() requires exactly one argument", name)
		}
		arg, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return BoundExpr{}, err
		}
		op := BoundOpLower
		if name == "upper" {
			op = BoundOpUpper
		}
		return BoundExpr{Kind: BoundExprUnary, Type: types.Text, Op: op, Left: &arg}, nil
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
		return BoundExpr{Kind: BoundExprUnary, Type: types.Int64, Op: BoundOpLength, Left: &arg}, nil
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
			left := out
			out = BoundExpr{Kind: BoundExprBinary, Type: types.Text, Op: BoundOpConcat, Left: &left, Right: &right}
		}
		return out, nil
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

func bindBinaryOp(op BinaryOp) (FilterOp, error) {
	switch op {
	case BinaryEqual:
		return FilterEqual, nil
	case BinaryNotEqual:
		return FilterNotEqual, nil
	case BinaryLess:
		return FilterLess, nil
	case BinaryLessEqual:
		return FilterLessEqual, nil
	case BinaryGreater:
		return FilterGreater, nil
	case BinaryGreaterEqual:
		return FilterGreaterEqual, nil
	default:
		return 0, fmt.Errorf("unsupported WHERE operator")
	}
}

func isArithmeticOp(op BinaryOp) bool {
	_, ok := arithmeticOp(op)
	return ok
}

func arithmeticOp(op BinaryOp) (BoundOp, bool) {
	switch op {
	case BinaryAdd:
		return BoundOpAdd, true
	case BinarySubtract:
		return BoundOpSubtract, true
	case BinaryMultiply:
		return BoundOpMultiply, true
	case BinaryDivide:
		return BoundOpDivide, true
	case BinaryModulo:
		return BoundOpModulo, true
	case BinaryIntDivide:
		return BoundOpIntDivide, true
	default:
		return BoundOpInvalid, false
	}
}

func literalExpr(value Value) BoundExpr {
	expr := BoundExpr{Kind: BoundExprLiteral, Literal: literalValue(value)}
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

func arithmeticResultType(op BoundOp, left BoundExpr, right BoundExpr) (types.Type, error) {
	if op == BoundOpModulo || op == BoundOpIntDivide {
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
	label:         "WHERE",
	intRangeCheck: checkInt32LiteralRange,
	allowFloat:    true,
	mismatchError: columnLiteralMismatchError,
}

var havingCmpRules = cmpRules{
	label:               "HAVING",
	intRangeCheck:       checkCountLiteralRange,
	textOrderingAllowed: true,
	uuidExtraCheck:      validateHavingUUIDBound,
	mismatchError:       havingMismatchError,
}

func validateWhereComparison(left BoundExpr, op FilterOp, right BoundExpr) error {
	return validateComparison(left, op, right, whereCmpRules)
}

func validateComparison(left BoundExpr, op FilterOp, right BoundExpr, r cmpRules) error {
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
	if col.Kind != BoundExprColumn || lit.Kind != BoundExprLiteral {
		return nil
	}
	return fmt.Errorf("WHERE column %q expects %s literal", col.Column, literalKindName(col.Type.Kind))
}

func havingMismatchError(col BoundExpr, lit BoundExpr) error {
	if col.Kind != BoundExprColumn || lit.Kind != BoundExprLiteral {
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
	if col.Kind != BoundExprColumn || lit.Kind != BoundExprLiteral {
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

func checkCountLiteralRange(col BoundExpr, lit BoundExpr) error {
	if col.Kind != BoundExprColumn || lit.Kind != BoundExprLiteral {
		return nil
	}
	if normalizeName(col.Column) != "count" {
		return nil
	}
	if value, ok := lit.Literal.(int64); ok && value < 0 {
		return fmt.Errorf("HAVING count literal must be non-negative")
	}
	return nil
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

type literalNormalizer struct {
	targetKind types.Kind
	normalize  func(string) (string, bool)
}

var literalNormalizers = []literalNormalizer{
	{types.KindTimestamp, normalizeTimestampString},
	{types.KindUUID, normalizeUUIDString},
}

func normalizeBound(target BoundExpr, bound *BoundExpr) {
	if bound.Kind != BoundExprLiteral || bound.Type.Kind != types.KindText {
		return
	}
	value, ok := bound.Literal.(string)
	if !ok {
		return
	}
	for _, n := range literalNormalizers {
		if target.Type.Kind != n.targetKind {
			continue
		}
		if v, ok := n.normalize(value); ok {
			bound.Literal = v
		}
		return
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
	if expr.Type.Kind == types.KindUUID || expr.Kind != BoundExprLiteral || expr.Type.Kind != types.KindText {
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

func textBound(target types.Kind, expr BoundExpr) bool {
	if expr.Type.Kind == target {
		return true
	}
	if expr.Kind != BoundExprLiteral || expr.Type.Kind != types.KindText {
		return target == types.KindUUID || target == types.KindBytes || target == types.KindNamed
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
	case types.KindUUID, types.KindBytes, types.KindNamed:
		return true
	}
	return false
}

func areXComparable(target types.Kind, left, right BoundExpr) bool {
	if left.Type.Kind == target {
		return textBound(target, right)
	}
	if right.Type.Kind == target {
		return textBound(target, left)
	}
	return false
}

func filterOpToExprOp(op FilterOp) BoundOp {
	switch op {
	case FilterEqual:
		return BoundOpEqual
	case FilterNotEqual:
		return BoundOpNotEqual
	case FilterLess:
		return BoundOpLess
	case FilterLessEqual:
		return BoundOpLessEqual
	case FilterGreater:
		return BoundOpGreater
	case FilterGreaterEqual:
		return BoundOpGreaterEqual
	default:
		return BoundOpInvalid
	}
}

func isOrderingFilter(op FilterOp) bool {
	switch op {
	case FilterLess, FilterLessEqual, FilterGreater, FilterGreaterEqual:
		return true
	default:
		return false
	}
}

func buildColumnIndex(columns []BoundColumnDef) map[string]BoundColumnDef {
	index := make(map[string]BoundColumnDef, len(columns))
	for _, col := range columns {
		index[normalizeName(col.Name)] = col
	}
	return index
}

func findColumn(columns map[string]BoundColumnDef, name string) (BoundColumnDef, bool) {
	col, ok := columns[normalizeName(name)]
	return col, ok
}

func aggregateFuncByName(name string) (AggregateFunc, bool) {
	switch normalizeName(name) {
	case "count":
		return AggregateCount, true
	case "sum":
		return AggregateSum, true
	case "min":
		return AggregateMin, true
	case "max":
		return AggregateMax, true
	default:
		return AggregateInvalid, false
	}
}
