package explain

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

// PlanNode is one node in the plan tree that the renderer walks. The shape is
// flat on purpose: we have no joins and only one source per query today.
type PlanNode struct {
	Op       string      `json:"op"`
	Table    string      `json:"table,omitempty"`
	Children []*PlanNode `json:"children,omitempty"`
}

// PlanFromLogical walks the bound logical query and produces a plan tree
// suitable for the EXPLAIN renderer. The current shape has a root operator
// (Count/Sum/Min/Max/Group/Project) over a single ReadSegments source.
func PlanFromLogical(plan logical.Query) *PlanNode {
	source := &PlanNode{Op: "ReadSegments", Table: plan.Table.Name}
	root := &PlanNode{Children: []*PlanNode{source}}

	switch plan.Kind {
	case logical.QueryAggregate:
		root.Op = aggregateOp(plan)
	case logical.QueryScan:
		root.Op = projectOp(plan)
	default:
		root.Op = "Unknown"
	}
	return root
}

func aggregateOp(plan logical.Query) string {
	body := aggregateBody(plan)
	if plan.GroupColumn != "" {
		return fmt.Sprintf("Group(%s), %s", plan.GroupColumn, body)
	}
	return body
}

func aggregateBody(plan logical.Query) string {
	switch plan.Aggregate {
	case logical.AggregateCount:
		if plan.AggregateColumn == "" {
			return "Count"
		}
		return fmt.Sprintf("Count(%s)", plan.AggregateColumn)
	case logical.AggregateSum:
		return fmt.Sprintf("Sum(%s)", plan.AggregateColumn)
	case logical.AggregateMin:
		return fmt.Sprintf("Min(%s)", plan.AggregateColumn)
	case logical.AggregateMax:
		return fmt.Sprintf("Max(%s)", plan.AggregateColumn)
	default:
		return "Aggregate"
	}
}

func projectOp(plan logical.Query) string {
	if len(plan.SelectOutputs) == 0 {
		return "Project"
	}
	parts := make([]string, 0, len(plan.SelectOutputs))
	for _, out := range plan.SelectOutputs {
		name := out.Alias
		if name == "" {
			name = formatExpr(&out.Expr)
		}
		parts = append(parts, name)
	}
	return fmt.Sprintf("Project(%s)", strings.Join(parts, ", "))
}

// PredicatesFromExpr flattens a top-level AND chain into one string per
// conjunct so the report can list each line under "Predicate:". OR and NOT
// stay inline.
func PredicatesFromExpr(expr *logical.Expr) []string {
	if expr == nil {
		return nil
	}
	conjuncts := flattenAnd(expr, nil)
	out := make([]string, 0, len(conjuncts))
	for _, c := range conjuncts {
		s := formatExpr(c)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func flattenAnd(expr *logical.Expr, dst []*logical.Expr) []*logical.Expr {
	if expr == nil {
		return dst
	}
	if expr.Kind == logical.ExprBinary && expr.Op == logical.OpAnd {
		dst = flattenAnd(expr.Left, dst)
		dst = flattenAnd(expr.Right, dst)
		return dst
	}
	return append(dst, expr)
}

func formatExpr(e *logical.Expr) string {
	if e == nil {
		return ""
	}
	switch e.Kind {
	case logical.ExprColumn:
		return e.Column
	case logical.ExprLiteral:
		return formatLiteral(e.Literal, e.Type)
	case logical.ExprUnary:
		return formatUnary(e)
	case logical.ExprBinary:
		return formatBinary(e)
	case logical.ExprBetween:
		return formatBetween(e)
	case logical.ExprIn:
		return formatIn(e)
	}
	return ""
}

func formatUnary(e *logical.Expr) string {
	inner := formatExpr(e.Left)
	switch e.Op {
	case logical.OpNot:
		return "NOT " + parenIfBoolean(e.Left, inner)
	case logical.OpLower:
		return "lower(" + inner + ")"
	case logical.OpUpper:
		return "upper(" + inner + ")"
	}
	return inner
}

func formatBinary(e *logical.Expr) string {
	left := formatExpr(e.Left)
	right := formatExpr(e.Right)
	switch e.Op {
	case logical.OpAnd:
		return left + " AND " + right
	case logical.OpOr:
		return "(" + left + " OR " + right + ")"
	case logical.OpConcat:
		return left + " || " + right
	}
	if sym := comparisonSymbol(e.Op); sym != "" {
		return left + " " + sym + " " + right
	}
	if sym := arithmeticSymbol(e.Op); sym != "" {
		return left + " " + sym + " " + right
	}
	return left + " ? " + right
}

func formatBetween(e *logical.Expr) string {
	target := formatExpr(e.Left)
	if len(e.Args) != 2 {
		return target + " BETWEEN ?"
	}
	low := formatExpr(&e.Args[0])
	high := formatExpr(&e.Args[1])
	return target + " BETWEEN " + low + " AND " + high
}

func formatIn(e *logical.Expr) string {
	target := formatExpr(e.Left)
	parts := make([]string, 0, len(e.Args))
	for i := range e.Args {
		parts = append(parts, formatExpr(&e.Args[i]))
	}
	op := "IN"
	if e.Not {
		op = "NOT IN"
	}
	return target + " " + op + " (" + strings.Join(parts, ", ") + ")"
}

func parenIfBoolean(e *logical.Expr, rendered string) string {
	if e == nil {
		return rendered
	}
	if e.Kind == logical.ExprBinary && (e.Op == logical.OpAnd || e.Op == logical.OpOr) {
		return "(" + rendered + ")"
	}
	return rendered
}

func comparisonSymbol(op logical.Op) string {
	switch op {
	case logical.OpEqual:
		return "="
	case logical.OpNotEqual:
		return "!="
	case logical.OpLess:
		return "<"
	case logical.OpLessEqual:
		return "<="
	case logical.OpGreater:
		return ">"
	case logical.OpGreaterEqual:
		return ">="
	}
	return ""
}

func arithmeticSymbol(op logical.Op) string {
	switch op {
	case logical.OpAdd:
		return "+"
	case logical.OpSubtract:
		return "-"
	case logical.OpMultiply:
		return "*"
	case logical.OpDivide:
		return "/"
	case logical.OpModulo:
		return "%"
	case logical.OpIntDivide:
		return "DIV"
	}
	return ""
}

func formatLiteral(v any, typ sqltype.Type) string {
	if v == nil {
		return "NULL"
	}
	switch x := v.(type) {
	case string:
		if typ.Kind == sqltype.KindText || typ.Kind == sqltype.KindBytes ||
			typ.Kind == sqltype.KindUUID || typ.Kind == sqltype.KindDate ||
			typ.Kind == sqltype.KindTimestamp || typ.Kind == sqltype.KindNamed {
			return "'" + x + "'"
		}
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	}
	return fmt.Sprintf("%v", v)
}
