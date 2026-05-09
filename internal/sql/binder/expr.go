package binder

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

// bindExpr lowers an AST expression into a logical.Expr. Aggregate calls and
// StarRef are not supported here — callers handle those at the AST level.
func bindExpr(columns map[string]catalog.ColumnDef, expr ast.Expr) (logical.Expr, error) {
	switch e := expr.(type) {
	case *ast.ColumnRef:
		col, ok := findColumn(columns, e.Name)
		if !ok {
			return logical.Expr{}, fmt.Errorf("missing column %q", e.Name)
		}
		return logical.Expr{Kind: logical.ExprColumn, Type: col.Type, Column: col.Name}, nil
	case *ast.Literal:
		return literalExpr(e.Value), nil
	case *ast.FuncCall:
		if isAggregateName(e.Name) {
			return logical.Expr{}, fmt.Errorf("aggregate %s is only allowed in SELECT or HAVING", e.Name)
		}
		return bindScalarCall(columns, e)
	case *ast.BinaryExpr:
		return bindBinaryExpr(columns, e)
	case *ast.BetweenExpr:
		target, err := bindExpr(columns, e.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		low, err := bindExpr(columns, e.Low)
		if err != nil {
			return logical.Expr{}, err
		}
		high, err := bindExpr(columns, e.High)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprBetween, Type: sqltype.Bool, Left: &target, Args: []logical.Expr{low, high}}, nil
	case *ast.InExpr:
		target, err := bindExpr(columns, e.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		values := make([]logical.Expr, 0, len(e.Values))
		for _, v := range e.Values {
			bv, err := bindExpr(columns, v)
			if err != nil {
				return logical.Expr{}, err
			}
			values = append(values, bv)
		}
		return logical.Expr{Kind: logical.ExprIn, Type: sqltype.Bool, Left: &target, Args: values, Not: e.Not}, nil
	case *ast.AndExpr:
		return bindBoolBinary(columns, e.Left, e.Right, logical.OpAnd)
	case *ast.OrExpr:
		return bindBoolBinary(columns, e.Left, e.Right, logical.OpOr)
	case *ast.NotExpr:
		child, err := bindExpr(columns, e.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprUnary, Type: sqltype.Bool, Op: logical.OpNot, Left: &child}, nil
	default:
		return logical.Expr{}, fmt.Errorf("unsupported expression %T", expr)
	}
}

func bindBinaryExpr(columns map[string]catalog.ColumnDef, e *ast.BinaryExpr) (logical.Expr, error) {
	left, right, err := bindBinary(columns, e.Left, e.Right)
	if err != nil {
		return logical.Expr{}, err
	}
	if isArithmeticOp(e.Op) {
		op := arithOps[e.Op]
		typ, err := arithmeticResultType(op, left, right)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprBinary, Type: typ, Op: op, Left: &left, Right: &right}, nil
	}
	if e.Op == ast.BinaryConcat {
		if left.Type.Kind != sqltype.KindText {
			return logical.Expr{}, fmt.Errorf("text expressions require a text left")
		}
		if right.Type.Kind != sqltype.KindText {
			return logical.Expr{}, fmt.Errorf("text expressions require a text right")
		}
		return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Text, Op: logical.OpConcat, Left: &left, Right: &right}, nil
	}
	op, err := bindBinaryOp(e.Op)
	if err != nil {
		return logical.Expr{}, err
	}
	return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: filterOpToExprOp(op), Left: &left, Right: &right}, nil
}

func bindBoolBinary(columns map[string]catalog.ColumnDef, l, r ast.Expr, op logical.Op) (logical.Expr, error) {
	left, right, err := bindBinary(columns, l, r)
	if err != nil {
		return logical.Expr{}, err
	}
	return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: op, Left: &left, Right: &right}, nil
}

func bindBinary(columns map[string]catalog.ColumnDef, l, r ast.Expr) (logical.Expr, logical.Expr, error) {
	left, err := bindExpr(columns, l)
	if err != nil {
		return logical.Expr{}, logical.Expr{}, err
	}
	right, err := bindExpr(columns, r)
	if err != nil {
		return logical.Expr{}, logical.Expr{}, err
	}
	return left, right, nil
}

func bindScalarCall(columns map[string]catalog.ColumnDef, call *ast.FuncCall) (logical.Expr, error) {
	name := normalizeName(call.Name)
	switch name {
	case "lower", "upper":
		if len(call.Args) != 1 {
			return logical.Expr{}, fmt.Errorf("%s() requires exactly one argument", name)
		}
		arg, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return logical.Expr{}, err
		}
		op := logical.OpLower
		if name == "upper" {
			op = logical.OpUpper
		}
		return logical.Expr{Kind: logical.ExprUnary, Type: sqltype.Text, Op: op, Left: &arg}, nil
	case "concat":
		if len(call.Args) == 0 {
			return logical.Expr{}, fmt.Errorf("concat() requires at least one argument")
		}
		out, err := bindTextArg(columns, call.Args[0])
		if err != nil {
			return logical.Expr{}, err
		}
		for i := 1; i < len(call.Args); i++ {
			right, err := bindTextArg(columns, call.Args[i])
			if err != nil {
				return logical.Expr{}, err
			}
			left := out
			out = logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Text, Op: logical.OpConcat, Left: &left, Right: &right}
		}
		return out, nil
	default:
		return logical.Expr{}, fmt.Errorf("unsupported scalar function %q", name)
	}
}

func bindTextArg(columns map[string]catalog.ColumnDef, expr ast.Expr) (logical.Expr, error) {
	bound, err := bindExpr(columns, expr)
	if err != nil {
		return logical.Expr{}, err
	}
	if bound.Type.Kind != sqltype.KindText {
		return logical.Expr{}, fmt.Errorf("text expressions require a text argument")
	}
	return bound, nil
}

var binaryFilterOps = map[ast.BinaryOp]logical.FilterOp{
	ast.BinaryEqual:        logical.FilterEqual,
	ast.BinaryNotEqual:     logical.FilterNotEqual,
	ast.BinaryLess:         logical.FilterLess,
	ast.BinaryLessEqual:    logical.FilterLessEqual,
	ast.BinaryGreater:      logical.FilterGreater,
	ast.BinaryGreaterEqual: logical.FilterGreaterEqual,
}

func bindBinaryOp(op ast.BinaryOp) (logical.FilterOp, error) {
	if out, ok := binaryFilterOps[op]; ok {
		return out, nil
	}
	return 0, fmt.Errorf("unsupported WHERE operator")
}

func isArithmeticOp(op ast.BinaryOp) bool {
	_, ok := arithOps[op]
	return ok
}

func isAggregateName(name string) bool {
	_, ok := nameToAgg[normalizeName(name)]
	return ok
}
