// Substitutes positional parameters into a parsed plan before execution.
package sql

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

// BindParameters returns a fresh plan with every ExprParameter replaced by a typed literal carrying args[index-1].
func BindParameters(plan *Plan, args []any) (*Plan, error) {
	if plan == nil {
		return nil, nil
	}
	if len(args) == 0 {
		found := false
		var visit func(BoundExpr) bool
		visit = func(e BoundExpr) bool {
			if e.Op == ExprParameter {
				found = true
			}
			if !found && e.SubPlan != nil {
				walkPlanExprs(e.SubPlan, visit)
			}
			return !found
		}
		walkPlanExprs(plan, visit)
		if !found {
			return plan, nil
		}
	}
	if err := validateParamIndexes(plan, len(args)); err != nil {
		return nil, err
	}
	return clonePlan(plan, args)
}

func validateParamIndexes(plan *Plan, argCount int) error {
	maxIdx := 0
	var visit func(BoundExpr) bool
	visit = func(e BoundExpr) bool {
		if e.Op == ExprParameter && e.Parameter > maxIdx {
			maxIdx = e.Parameter
		}
		if e.SubPlan != nil {
			walkPlanExprs(e.SubPlan, visit)
		}
		return true
	}
	walkPlanExprs(plan, visit)
	if maxIdx > argCount {
		return fmt.Errorf("sql: query references parameter %d but only %d args supplied", maxIdx, argCount)
	}
	return nil
}

// walkPlanExprs runs WalkExpr over every expression slot in the plan tree, leaving
// SubPlan descent to the visitor.
func walkPlanExprs(plan *Plan, visit func(BoundExpr) bool) {
	if plan == nil {
		return
	}
	if plan.Rel != nil {
		walkRelExprs(plan.Rel, visit)
	}
	if plan.Where != nil {
		WalkExpr(*plan.Where, visit)
	}
	if plan.Inner != nil {
		walkPlanExprs(plan.Inner, visit)
	}
}

func walkRelExprs(rel *Rel, visit func(BoundExpr) bool) {
	if rel == nil {
		return
	}
	if rel.Where != nil {
		WalkExpr(*rel.Where, visit)
	}
	WalkExpr(rel.Predicate, visit)
	if rel.Having != nil {
		WalkExpr(*rel.Having, visit)
	}
	for _, g := range rel.GroupBy {
		WalkExpr(g, visit)
	}
	for _, o := range rel.Projection {
		WalkExpr(o.Expr, visit)
	}
	for _, o := range rel.Outputs {
		WalkExpr(o.Expr, visit)
	}
	for _, k := range rel.SortKeys {
		WalkExpr(k.Expr, visit)
	}
	for _, jk := range rel.JoinKeys {
		WalkExpr(jk.Left, visit)
		WalkExpr(jk.Right, visit)
	}
	for _, in := range rel.Inputs {
		walkRelExprs(in, visit)
	}
}

func clonePlan(plan *Plan, args []any) (*Plan, error) {
	if plan == nil {
		return nil, nil
	}
	out := *plan
	if plan.Rel != nil {
		rel, err := cloneRel(plan.Rel, args)
		if err != nil {
			return nil, err
		}
		out.Rel = rel
	}
	if plan.Where != nil {
		w, err := cloneExpr(*plan.Where, args)
		if err != nil {
			return nil, err
		}
		out.Where = &w
	}
	if plan.Inner != nil {
		inner, err := clonePlan(plan.Inner, args)
		if err != nil {
			return nil, err
		}
		out.Inner = inner
	}
	return &out, nil
}

func cloneRel(rel *Rel, args []any) (*Rel, error) {
	if rel == nil {
		return nil, nil
	}
	out := *rel
	if rel.Where != nil {
		w, err := cloneExpr(*rel.Where, args)
		if err != nil {
			return nil, err
		}
		out.Where = &w
	}
	if rel.Predicate.Op != ExprInvalid {
		p, err := cloneExpr(rel.Predicate, args)
		if err != nil {
			return nil, err
		}
		out.Predicate = p
	}
	if rel.Having != nil {
		h, err := cloneExpr(*rel.Having, args)
		if err != nil {
			return nil, err
		}
		out.Having = &h
	}
	if len(rel.GroupBy) > 0 {
		out.GroupBy = make([]BoundExpr, len(rel.GroupBy))
		for i, g := range rel.GroupBy {
			c, err := cloneExpr(g, args)
			if err != nil {
				return nil, err
			}
			out.GroupBy[i] = c
		}
	}
	if len(rel.Projection) > 0 {
		out.Projection = make([]BoundOutput, len(rel.Projection))
		for i, o := range rel.Projection {
			c, err := cloneExpr(o.Expr, args)
			if err != nil {
				return nil, err
			}
			out.Projection[i] = BoundOutput{Alias: o.Alias, Expr: c}
		}
	}
	if len(rel.Outputs) > 0 {
		out.Outputs = make([]BoundOutput, len(rel.Outputs))
		for i, o := range rel.Outputs {
			c, err := cloneExpr(o.Expr, args)
			if err != nil {
				return nil, err
			}
			out.Outputs[i] = BoundOutput{Alias: o.Alias, Expr: c}
		}
	}
	if len(rel.SortKeys) > 0 {
		out.SortKeys = make([]SortKey, len(rel.SortKeys))
		for i, k := range rel.SortKeys {
			c, err := cloneExpr(k.Expr, args)
			if err != nil {
				return nil, err
			}
			out.SortKeys[i] = SortKey{Expr: c, Desc: k.Desc}
		}
	}
	if len(rel.JoinKeys) > 0 {
		out.JoinKeys = make([]JoinKey, len(rel.JoinKeys))
		for i, jk := range rel.JoinKeys {
			l, err := cloneExpr(jk.Left, args)
			if err != nil {
				return nil, err
			}
			r, err := cloneExpr(jk.Right, args)
			if err != nil {
				return nil, err
			}
			out.JoinKeys[i] = JoinKey{Left: l, Right: r}
		}
	}
	if len(rel.Inputs) > 0 {
		out.Inputs = make([]*Rel, len(rel.Inputs))
		for i, in := range rel.Inputs {
			c, err := cloneRel(in, args)
			if err != nil {
				return nil, err
			}
			out.Inputs[i] = c
		}
	}
	return &out, nil
}

func cloneExpr(expr BoundExpr, args []any) (BoundExpr, error) {
	if expr.Op == ExprParameter {
		if expr.Parameter < 1 || expr.Parameter > len(args) {
			return BoundExpr{}, fmt.Errorf("sql: parameter %d out of range for %d args", expr.Parameter, len(args))
		}
		return paramAsLiteral(args[expr.Parameter-1])
	}
	out := expr
	if len(expr.Args) > 0 {
		out.Args = make([]BoundExpr, len(expr.Args))
		for i, a := range expr.Args {
			c, err := cloneExpr(a, args)
			if err != nil {
				return BoundExpr{}, err
			}
			out.Args[i] = c
		}
	}
	if expr.SubPlan != nil {
		sub, err := clonePlan(expr.SubPlan, args)
		if err != nil {
			return BoundExpr{}, err
		}
		out.SubPlan = sub
	}
	return out, nil
}

func paramAsLiteral(v any) (BoundExpr, error) {
	switch x := v.(type) {
	case nil:
		return BoundExpr{Op: ExprLiteral, Literal: nil}, nil
	case bool:
		return BoundExpr{Op: ExprLiteral, Type: schema.Bool, Literal: x}, nil
	case int:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case int8:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case int16:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case int32:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case int64:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: x}, nil
	case uint:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case uint8:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case uint16:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case uint32:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case uint64:
		return BoundExpr{Op: ExprLiteral, Type: schema.Int64, Literal: int64(x)}, nil
	case float32:
		return BoundExpr{Op: ExprLiteral, Type: schema.Float64, Literal: float64(x)}, nil
	case float64:
		return BoundExpr{Op: ExprLiteral, Type: schema.Float64, Literal: x}, nil
	case string:
		return BoundExpr{Op: ExprLiteral, Type: schema.Text, Literal: x}, nil
	case []byte:
		return BoundExpr{Op: ExprLiteral, Type: schema.Bytes, Literal: x}, nil
	}
	return BoundExpr{}, fmt.Errorf("sql: unsupported parameter type %T", v)
}
