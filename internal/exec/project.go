package exec

import (
	"context"
	"fmt"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Project struct {
	Columns    []string
	Exprs      []v3sql.BoundOutput
	Downstream Consumer

	state operatorState
}

func (p *Project) Open(ctx context.Context) error {
	if p.Downstream == nil {
		return fmt.Errorf("project downstream is nil")
	}
	if err := p.state.open(); err != nil {
		return err
	}
	return p.Downstream.Open(ctx)
}

func (p *Project) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := p.state.requireOpen(); err != nil {
		return err
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	if p.Exprs != nil {
		projected, err := projectBoundOutputs(batch, sel, p.Exprs)
		if err != nil {
			return err
		}
		return p.Downstream.Push(projected, sel)
	}
	if p.Columns == nil {
		return p.Downstream.Push(batch, sel)
	}
	cols := make([]types.Column, len(p.Columns))
	for i, name := range p.Columns {
		col, ok := columnByName(batch, name)
		if !ok {
			return fmt.Errorf("missing projected column %q", name)
		}
		cols[i] = col
	}
	return p.Downstream.Push(types.Batch{Columns: cols, Len: batch.Len}, sel)
}

func (p *Project) Close() error {
	if p.Downstream != nil {
		if err := p.Downstream.Close(); err != nil {
			_ = p.state.close()
			return err
		}
	}
	return p.state.close()
}
