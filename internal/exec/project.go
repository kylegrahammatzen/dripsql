// ProjectOp builds an output batch whose columns are bound expressions.
// Column-ref outputs alias the input Vec; computed outputs materialize via row-by-row eval.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type ProjectOp struct {
	Source  Operator
	Outputs []sql.BoundOutput

	outer    *correlatedOuter
	subBuild func(*sql.Plan) (Operator, error)

	state   operatorState
	plan    []projectStep
	cols    []vector.Column
	planSrc int
}

// srcIdx >= 0 aliases child column at that index; -1 marks a computed expr.
type projectStep struct {
	srcIdx int
	name   string
	out    *sql.BoundOutput
}

func (p *ProjectOp) Open(ctx context.Context) error {
	prev := p.state
	if err := p.state.open(); err != nil {
		return err
	}
	if err := p.Source.Open(ctx); err != nil {
		p.state = prev
		return err
	}
	return nil
}

func (p *ProjectOp) Next() (vector.Batch, bool, error) {
	if err := p.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	batch, ok, err := p.Source.Next()
	if err != nil || !ok {
		return batch, ok, err
	}
	if p.plan == nil || len(batch.Columns) != p.planSrc || !p.planValid(batch) {
		if err := p.bindPlan(batch); err != nil {
			return vector.Batch{}, false, err
		}
	}
	if cap(p.cols) < len(p.plan) {
		p.cols = make([]vector.Column, len(p.plan))
	} else {
		p.cols = p.cols[:len(p.plan)]
	}
	for i, step := range p.plan {
		if step.srcIdx >= 0 {
			src := &batch.Columns[step.srcIdx]
			p.cols[i] = vector.Column{Name: step.name, Type: src.Type, EnumLabels: src.EnumLabels, V: src.V}
			continue
		}
		col, err := projectColumn(batch, batch.Sel, *step.out, p.outer, p.subBuild)
		if err != nil {
			return vector.Batch{}, false, err
		}
		p.cols[i] = col
	}
	return vector.Batch{Len: batch.Len, Columns: p.cols, Sel: batch.Sel}, true, nil
}

func (p *ProjectOp) bindPlan(batch vector.Batch) error {
	p.plan = make([]projectStep, len(p.Outputs))
	p.planSrc = len(batch.Columns)
	for i := range p.Outputs {
		o := &p.Outputs[i]
		step := projectStep{srcIdx: -1, out: o}
		if o.Expr.Op == sql.ExprColumn {
			idx := findColumnIndex(batch, o.Expr.Column)
			if idx < 0 {
				return fmt.Errorf("project: column %q not in input batch", o.Expr.Column)
			}
			step.srcIdx = idx
			step.name = o.Alias
			if step.name == "" {
				step.name = batch.Columns[idx].Name
			}
		}
		p.plan[i] = step
	}
	return nil
}

func (p *ProjectOp) planValid(batch vector.Batch) bool {
	for _, step := range p.plan {
		if step.srcIdx < 0 {
			continue
		}
		if step.srcIdx >= len(batch.Columns) {
			return false
		}
	}
	return true
}

func findColumnIndex(batch vector.Batch, name string) int {
	want := schema.NormalizeName(name)
	for i := range batch.Columns {
		if schema.NormalizeName(batch.Columns[i].Name) == want {
			return i
		}
	}
	return -1
}

func projectColumn(batch vector.Batch, sel *vector.SelectionMask, output sql.BoundOutput, outer *correlatedOuter, subBuild func(*sql.Plan) (Operator, error)) (vector.Column, error) {
	if output.Expr.Op == sql.ExprColumn {
		src, ok := batch.ColumnByName(output.Expr.Column)
		if !ok {
			return vector.Column{}, fmt.Errorf("project: column %q not in input batch", output.Expr.Column)
		}
		name := output.Alias
		if name == "" {
			name = src.Name
		}
		return vector.Column{Name: name, Type: src.Type, EnumLabels: src.EnumLabels, V: src.V}, nil
	}
	vk, err := vector.VecKindOf(output.Expr.Type)
	if err != nil {
		return vector.Column{}, fmt.Errorf("project: column %q has no physical kind: %w", output.Alias, err)
	}
	ctx := newEvalCtxWith(batch, outer, subBuild)
	rows := batch.Len
	v, valid, err := materializeVec(ctx, output.Expr, sel, rows, vk)
	if err != nil {
		return vector.Column{}, err
	}
	v.Valid = valid
	return vector.Column{Name: output.Alias, Type: output.Expr.Type, V: v}, nil
}

// materializeVec evaluates expr row-by-row and writes into a fresh Vec. Validity is allocated
// lazily on the first null and starts AllValid for rows already selected, so a nullless
// computed projection skips the validity slice entirely.
func materializeVec(ctx *evalCtx, expr sql.BoundExpr, sel *vector.SelectionMask, rows int, vk vector.VecKind) (vector.Vec, vector.Validity, error) {
	if v, valid, ok, err := vecEvalArith(ctx.batch, expr, sel, rows, vk); err != nil {
		return vector.Vec{}, nil, err
	} else if ok {
		return v, valid, nil
	}
	var valid vector.Validity
	var loopErr error
	v := newComputedVec(vk, rows)
	sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		raw, err := ctx.eval(expr, row)
		if err != nil {
			loopErr = err
			return
		}
		if raw == nil {
			if valid == nil {
				valid = vector.NewAllValid(rows)
			}
			valid.SetInvalid(row)
			return
		}
		if err := writeComputedRow(&v, vk, row, raw); err != nil {
			loopErr = err
		}
	})
	if loopErr != nil {
		return vector.Vec{}, nil, loopErr
	}
	return v, valid, nil
}

func newComputedVec(vk vector.VecKind, rows int) vector.Vec {
	switch vk {
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		return vector.NewVarVec(vk, rows, 0)
	}
	return vector.NewVec(vk, rows)
}

func writeComputedRow(v *vector.Vec, vk vector.VecKind, row int, raw any) error {
	switch vk {
	case vector.VecInt64:
		x, ok := asInt64(raw)
		if !ok {
			return fmt.Errorf("project: int column got %T", raw)
		}
		v.I64()[row] = x
	case vector.VecFloat64:
		x, ok := asFloat64(raw)
		if !ok {
			return fmt.Errorf("project: float column got %T", raw)
		}
		v.F64()[row] = x
	case vector.VecBool:
		b, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("project: bool column got %T", raw)
		}
		if b {
			bits := v.BoolBits()
			bits[row>>3] |= 1 << (row & 7)
		}
	case vector.VecText, vector.VecJSON:
		s, ok := raw.(string)
		if !ok {
			return fmt.Errorf("project: %v column got %T", vk, raw)
		}
		v.Var().AppendString(row, s)
	case vector.VecBytes:
		switch b := raw.(type) {
		case []byte:
			v.Var().AppendBytes(row, b)
		case string:
			v.Var().AppendString(row, b)
		default:
			return fmt.Errorf("project: bytes column got %T", raw)
		}
	default:
		return fmt.Errorf("project: computed kind %v not supported yet", vk)
	}
	return nil
}

func (p *ProjectOp) Close() error {
	p.state.close()
	return p.Source.Close()
}
