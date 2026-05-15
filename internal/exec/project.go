// ProjectOp builds an output batch whose columns are bound expressions.
// Column-ref outputs alias the input Vec; computed outputs materialize via row-by-row eval.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type ProjectOp struct {
	Source  Operator
	Outputs []sql.BoundOutput

	state operatorState
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

func (p *ProjectOp) Next() (types.Batch, bool, error) {
	if err := p.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	batch, ok, err := p.Source.Next()
	if err != nil || !ok {
		return batch, ok, err
	}
	cols := make([]types.Column, len(p.Outputs))
	for i, o := range p.Outputs {
		col, err := projectColumn(batch, batch.Sel, o)
		if err != nil {
			return types.Batch{}, false, err
		}
		cols[i] = col
	}
	return types.Batch{Len: batch.Len, Columns: cols, Sel: batch.Sel}, true, nil
}

func projectColumn(batch types.Batch, sel *types.SelectionMask, output sql.BoundOutput) (types.Column, error) {
	if output.Expr.Op == sql.ExprColumn {
		src, ok := batch.ColumnByName(output.Expr.Column)
		if !ok {
			return types.Column{}, fmt.Errorf("project: column %q not in input batch", output.Expr.Column)
		}
		name := output.Alias
		if name == "" {
			name = src.Name
		}
		return types.Column{Name: name, Type: src.Type, EnumLabels: src.EnumLabels, V: src.V}, nil
	}
	vk, err := types.VecKindOf(output.Expr.Type)
	if err != nil {
		return types.Column{}, fmt.Errorf("project: column %q has no physical kind: %w", output.Alias, err)
	}
	ctx := newEvalCtx(batch)
	rows := batch.Len
	v, nulls, err := materializeVec(ctx, output.Expr, sel, rows, vk)
	if err != nil {
		return types.Column{}, err
	}
	if len(nulls) > 0 {
		valid := make(types.Validity, types.ValidityWords(rows))
		sel.IterSet(func(row int) { valid.SetValid(row) })
		for _, row := range nulls {
			valid.SetInvalid(row)
		}
		v.Valid = valid
	}
	return types.Column{Name: output.Alias, Type: output.Expr.Type, V: v}, nil
}

func materializeVec(ctx *evalCtx, expr sql.BoundExpr, sel *types.SelectionMask, rows int, vk types.VecKind) (types.Vec, []int, error) {
	var nulls []int
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
			nulls = append(nulls, row)
			return
		}
		if err := writeComputedRow(&v, vk, row, raw); err != nil {
			loopErr = err
		}
	})
	if loopErr != nil {
		return types.Vec{}, nil, loopErr
	}
	return v, nulls, nil
}

func newComputedVec(vk types.VecKind, rows int) types.Vec {
	if vk == types.VecText {
		return types.NewVarVec(vk, rows, 0)
	}
	return types.NewVec(vk, rows)
}

func writeComputedRow(v *types.Vec, vk types.VecKind, row int, raw any) error {
	switch vk {
	case types.VecInt64:
		x, ok := asInt64(raw)
		if !ok {
			return fmt.Errorf("project: int column got %T", raw)
		}
		v.I64()[row] = x
	case types.VecFloat64:
		x, ok := asFloat64(raw)
		if !ok {
			return fmt.Errorf("project: float column got %T", raw)
		}
		v.F64()[row] = x
	case types.VecBool:
		b, ok := raw.(bool)
		if !ok {
			return fmt.Errorf("project: bool column got %T", raw)
		}
		if b {
			bits := v.BoolBits()
			bits[row>>3] |= 1 << (row & 7)
		}
	case types.VecText:
		s, ok := raw.(string)
		if !ok {
			return fmt.Errorf("project: text column got %T", raw)
		}
		v.Var().AppendString(row, s)
	default:
		return fmt.Errorf("project: computed kind %v not supported yet", vk)
	}
	return nil
}

func (p *ProjectOp) Close() error {
	p.state.close()
	return p.Source.Close()
}
