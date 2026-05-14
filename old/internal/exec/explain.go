package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Explain struct {
	Downstream Consumer
	Batches    int64
	Rows       int64
	Selected   int64

	state operatorState
}

func (e *Explain) Open(ctx context.Context) error {
	if e.Downstream == nil {
		return fmt.Errorf("explain downstream is nil")
	}
	if err := e.state.open(); err != nil {
		return err
	}
	return e.Downstream.Open(ctx)
}

func (e *Explain) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := e.state.requireOpen(); err != nil {
		return err
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	e.Batches++
	e.Rows += int64(batch.Len)
	e.Selected += int64(sel.PopCount())
	return e.Downstream.Push(batch, sel)
}

func (e *Explain) Close() error {
	if e.Downstream != nil {
		if err := e.Downstream.Close(); err != nil {
			_ = e.state.close()
			return err
		}
	}
	return e.state.close()
}
