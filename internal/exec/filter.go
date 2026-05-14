package exec

import (
	"context"
	"fmt"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// Predicator is the consumer-side contract Filter requires. It lets tests
// stub the predicate without depending on the concrete storage type. In
// production *storage.BoundPredicate is the sole implementation.
type Predicator interface {
	RequiredColumns() []string
	Eval(batch types.Batch, sel *types.SelectionMask) (int, error)
	EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error)
}

type Filter struct {
	Predicate  Predicator
	Expr       *v3sql.BoundExpr
	Downstream Consumer

	scratch types.SelectionMask
	state   operatorState
}

func (f *Filter) Open(ctx context.Context) error {
	if f.Downstream == nil {
		return fmt.Errorf("filter downstream is nil")
	}
	if err := f.state.open(); err != nil {
		return err
	}
	return f.Downstream.Open(ctx)
}

func (f *Filter) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := f.state.requireOpen(); err != nil {
		return err
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	if f.Predicate == nil {
		if f.Expr != nil {
			matched, err := evalFilterExprSelected(batch, sel, *f.Expr, &f.scratch)
			if err != nil {
				return err
			}
			if matched == 0 {
				return nil
			}
			return f.Downstream.Push(batch, f.scratch)
		}
		if sel.PopCount() == 0 {
			return nil
		}
		return f.Downstream.Push(batch, sel)
	}
	matched, err := f.Predicate.EvalSelected(batch, sel, &f.scratch)
	if err != nil {
		return err
	}
	if matched == 0 {
		return nil
	}
	return f.Downstream.Push(batch, f.scratch)
}

func (f *Filter) Close() error {
	if f.Downstream != nil {
		if err := f.Downstream.Close(); err != nil {
			_ = f.state.close()
			return err
		}
	}
	return f.state.close()
}
