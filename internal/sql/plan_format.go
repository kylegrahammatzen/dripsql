// Textual operator tree for EXPLAIN and debugging. Two-space indent per depth.
package sql

import (
	"fmt"
	"strings"
)

func (r *Rel) String() string {
	if r == nil {
		return "<nil>"
	}
	var b strings.Builder
	r.format(&b, 0)
	return strings.TrimRight(b.String(), "\n")
}

func (r *Rel) format(b *strings.Builder, depth int) {
	indent := strings.Repeat("  ", depth)
	switch r.Op {
	case RelScan:
		fmt.Fprintf(b, "%sScan %s", indent, r.Table.Name)
		if r.Alias != "" {
			fmt.Fprintf(b, " AS %s", r.Alias)
		}
		if len(r.Columns) > 0 {
			fmt.Fprintf(b, " cols=%s", formatColIDs(r, r.Columns))
		}
		if len(r.PredicateOnly) > 0 {
			fmt.Fprintf(b, " predOnly=%s", formatColIDs(r, r.PredicateOnly))
		}
		if r.Where != nil {
			fmt.Fprintf(b, " where=%s", r.Where.String())
		}
		b.WriteString("\n")
	case RelFilter:
		fmt.Fprintf(b, "%sFilter %s\n", indent, r.Predicate.String())
	case RelProject:
		parts := make([]string, len(r.Projection))
		for i, o := range r.Projection {
			s := o.Expr.String()
			if o.Alias != "" {
				s = s + " AS " + o.Alias
			}
			parts[i] = s
		}
		fmt.Fprintf(b, "%sProject [%s]\n", indent, strings.Join(parts, ", "))
	case RelAggregate:
		fmt.Fprintf(b, "%sAggregate", indent)
		if len(r.GroupBy) > 0 {
			parts := make([]string, len(r.GroupBy))
			for i, e := range r.GroupBy {
				parts[i] = e.String()
			}
			fmt.Fprintf(b, " group=[%s]", strings.Join(parts, ", "))
		}
		if len(r.Aggregates) > 0 {
			aggs := make([]string, len(r.Aggregates))
			for i, a := range r.Aggregates {
				aggs[i] = a.String()
			}
			fmt.Fprintf(b, " aggs=[%s]", strings.Join(aggs, ", "))
		}
		if r.Having != nil {
			fmt.Fprintf(b, " having=%s", r.Having.String())
		}
		b.WriteString("\n")
	case RelJoin:
		fmt.Fprintf(b, "%s%sJoin", indent, r.JoinKind.String())
		keys := make([]string, len(r.JoinKeys))
		for i, k := range r.JoinKeys {
			keys[i] = k.Left.String() + " = " + k.Right.String()
		}
		fmt.Fprintf(b, " on=[%s]\n", strings.Join(keys, ", "))
	case RelSort:
		parts := make([]string, len(r.SortKeys))
		for i, k := range r.SortKeys {
			dir := "asc"
			if k.Desc {
				dir = "desc"
			}
			parts[i] = k.Expr.String() + " " + dir
		}
		fmt.Fprintf(b, "%sSort keys=[%s]", indent, strings.Join(parts, ", "))
		if r.K > 0 {
			fmt.Fprintf(b, " limit=%d", r.K)
		}
		if r.Offset > 0 {
			fmt.Fprintf(b, " offset=%d", r.Offset)
		}
		b.WriteString("\n")
	case RelLimit:
		fmt.Fprintf(b, "%sLimit", indent)
		if r.Limit >= 0 {
			fmt.Fprintf(b, " n=%d", r.Limit)
		}
		if r.Offset > 0 {
			fmt.Fprintf(b, " offset=%d", r.Offset)
		}
		b.WriteString("\n")
	case RelUnion:
		fmt.Fprintf(b, "%sUnion\n", indent)
	default:
		fmt.Fprintf(b, "%sRel(%v)\n", indent, r.Op)
	}
	for _, in := range r.Inputs {
		in.format(b, depth+1)
	}
}

func formatColIDs(r *Rel, ids []ColumnID) string {
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = "?"
		for _, c := range r.Table.Columns {
			if c.ID == id {
				names[i] = c.Name
				break
			}
		}
	}
	return "[" + strings.Join(names, ", ") + "]"
}

func (a AggSpec) String() string {
	name := a.Func.String()
	var arg string
	switch {
	case a.Star:
		arg = "*"
	case a.ArgName != "":
		arg = a.ArgName
	}
	out := name + "(" + arg + ")"
	if a.Alias != "" {
		out = out + " AS " + a.Alias
	}
	return out
}

func (f AggregateFunc) String() string {
	switch f {
	case AggregateCount:
		return "count"
	case AggregateSum:
		return "sum"
	case AggregateMin:
		return "min"
	case AggregateMax:
		return "max"
	case AggregateAvg:
		return "avg"
	}
	return "?"
}

func (k JoinKind) String() string {
	switch k {
	case JoinInner:
		return "Inner"
	case JoinLeft:
		return "Left"
	case JoinRight:
		return "Right"
	case JoinFull:
		return "Full"
	}
	return "Unknown"
}

func (e BoundExpr) String() string {
	switch e.Op {
	case ExprColumn:
		return e.Column
	case ExprLiteral:
		return fmt.Sprintf("%v", e.Literal)
	case ExprNot:
		return "NOT " + e.Args[0].String()
	case ExprIsNull:
		return e.Args[0].String() + " IS NULL"
	case ExprIsNotNull:
		return e.Args[0].String() + " IS NOT NULL"
	case ExprAnd:
		return "(" + e.Args[0].String() + " AND " + e.Args[1].String() + ")"
	case ExprOr:
		return "(" + e.Args[0].String() + " OR " + e.Args[1].String() + ")"
	case ExprBetween:
		s := e.Args[0].String() + " BETWEEN " + e.Args[1].String() + " AND " + e.Args[2].String()
		if e.Not {
			return "NOT (" + s + ")"
		}
		return s
	case ExprIn:
		vals := make([]string, len(e.Args)-1)
		for i, a := range e.Args[1:] {
			vals[i] = a.String()
		}
		op := " IN ("
		if e.Not {
			op = " NOT IN ("
		}
		return e.Args[0].String() + op + strings.Join(vals, ", ") + ")"
	}
	if op := binaryExprSymbol(e.Op); op != "" {
		return "(" + e.Args[0].String() + " " + op + " " + e.Args[1].String() + ")"
	}
	if name := funcExprName(e.Op); name != "" {
		args := make([]string, len(e.Args))
		for i, a := range e.Args {
			args[i] = a.String()
		}
		return name + "(" + strings.Join(args, ", ") + ")"
	}
	return fmt.Sprintf("Expr(%v)", e.Op)
}

func binaryExprSymbol(op ExprOp) string {
	switch op {
	case ExprEqual:
		return "="
	case ExprNotEqual:
		return "<>"
	case ExprLess:
		return "<"
	case ExprLessEqual:
		return "<="
	case ExprGreater:
		return ">"
	case ExprGreaterEqual:
		return ">="
	case ExprAdd:
		return "+"
	case ExprSubtract:
		return "-"
	case ExprMultiply:
		return "*"
	case ExprDivide:
		return "/"
	case ExprModulo:
		return "%"
	case ExprIntDivide:
		return "//"
	case ExprConcat:
		return "||"
	case ExprJSONGet:
		return "->"
	case ExprJSONGetText:
		return "->>"
	}
	return ""
}

func funcExprName(op ExprOp) string {
	switch op {
	case ExprLower:
		return "lower"
	case ExprUpper:
		return "upper"
	case ExprLength:
		return "length"
	case ExprCoalesce:
		return "coalesce"
	case ExprSubstring:
		return "substring"
	case ExprAbs:
		return "abs"
	case ExprNullIf:
		return "nullif"
	case ExprCase:
		return "case"
	}
	return ""
}
