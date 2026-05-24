// Correlated-subquery runtime state. The outer scope's current row binds outer
// column references inside an inner plan; every per-row re-execution rebuilds
// the inner operator tree against the same shared values pointer.
package exec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
)

type correlatedOuter struct {
	values map[string]any
}

func newCorrelatedOuter() *correlatedOuter { return &correlatedOuter{values: map[string]any{}} }

// makeSubBuilder returns a closure used by row-by-row evaluators to build the
// inner operator tree for a correlated subquery. The same outer pointer is
// threaded so nested ExprColumn{Outer} reads see the binding set on this row.
func makeSubBuilder(segments SegmentsFn, outer *correlatedOuter) func(*sql.Plan) (Operator, error) {
	return func(plan *sql.Plan) (Operator, error) {
		return buildPlan(plan, segments, outer)
	}
}

func (c *correlatedOuter) set(name string, v any) { c.values[name] = v }

func (c *correlatedOuter) get(name string) (any, bool) {
	if c == nil {
		return nil, false
	}
	v, ok := c.values[name]
	return v, ok
}

// bindOuterRow snapshots the outer batch row into c.outer for the given names.
// Falls back to nil when the column is missing (treated as SQL NULL by the inner plan).
func (c *evalCtx) bindOuterRow(names []string, row int) error {
	if c.outer == nil {
		c.outer = newCorrelatedOuter()
	}
	for _, name := range names {
		col, ok := c.batch.ColumnByName(name)
		if !ok {
			c.outer.set(name, nil)
			continue
		}
		v, err := col.ValueAt(row)
		if err != nil {
			return err
		}
		c.outer.set(name, v)
	}
	return nil
}

func (c *evalCtx) evalCorrelatedScalar(expr sql.BoundExpr, row int) (any, error) {
	if expr.SubPlan == nil {
		return nil, fmt.Errorf("eval: subquery missing plan")
	}
	if c.subBuild == nil {
		return nil, fmt.Errorf("eval: correlated subquery not wired (no builder)")
	}
	if err := c.bindOuterRow(expr.SubPlan.OuterRefs, row); err != nil {
		return nil, err
	}
	op, err := c.subBuild(expr.SubPlan)
	if err != nil {
		return nil, fmt.Errorf("subquery build: %w", err)
	}
	v, err := drainScalar(op)
	if err != nil {
		return nil, fmt.Errorf("subquery drain: %w", err)
	}
	return v, nil
}

func (c *evalCtx) evalCorrelatedIn(expr sql.BoundExpr, row int) (any, error) {
	if expr.SubPlan == nil {
		return nil, fmt.Errorf("eval: IN subquery missing plan")
	}
	if c.subBuild == nil {
		return nil, fmt.Errorf("eval: correlated IN not wired (no builder)")
	}
	target, err := c.eval(expr.Args[0], row)
	if err != nil {
		return nil, err
	}
	if err := c.bindOuterRow(expr.SubPlan.OuterRefs, row); err != nil {
		return nil, err
	}
	op, err := c.subBuild(expr.SubPlan)
	if err != nil {
		return nil, fmt.Errorf("IN subquery build: %w", err)
	}
	vals, err := drainValues(op)
	if err != nil {
		return nil, fmt.Errorf("IN subquery drain: %w", err)
	}
	match := false
	for _, v := range vals {
		eq, err := compare(sql.ExprEqual, target, v)
		if err != nil {
			return nil, err
		}
		if b, ok := eq.(bool); ok && b {
			match = true
			break
		}
	}
	if expr.Not {
		match = !match
	}
	return match, nil
}

func (c *evalCtx) evalCorrelatedExists(expr sql.BoundExpr, row int) (any, error) {
	if expr.SubPlan == nil {
		return nil, fmt.Errorf("eval: EXISTS subquery missing plan")
	}
	if c.subBuild == nil {
		return nil, fmt.Errorf("eval: correlated EXISTS not wired (no builder)")
	}
	if err := c.bindOuterRow(expr.SubPlan.OuterRefs, row); err != nil {
		return nil, err
	}
	op, err := c.subBuild(expr.SubPlan)
	if err != nil {
		return nil, fmt.Errorf("EXISTS subquery build: %w", err)
	}
	any, err := drainAny(op)
	if err != nil {
		return nil, fmt.Errorf("EXISTS subquery drain: %w", err)
	}
	result := any
	if expr.Not {
		result = !any
	}
	return result, nil
}
