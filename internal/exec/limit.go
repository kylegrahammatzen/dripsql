package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Limit struct {
	// Limit < 0 means unlimited. Limit == 0 emits no rows.
	Limit      int
	Offset     int
	Downstream Consumer

	seen    int
	emitted int
	scratch types.SelectionMask
	state   operatorState
}

func (l *Limit) Open(ctx context.Context) error {
	if l.Downstream == nil {
		return fmt.Errorf("limit downstream is nil")
	}
	if l.Offset < 0 {
		return fmt.Errorf("limit offset must be non-negative")
	}
	if l.Limit < -1 {
		return fmt.Errorf("limit must be non-negative or -1")
	}
	if err := l.state.open(); err != nil {
		return err
	}
	return l.Downstream.Open(ctx)
}

func (l *Limit) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := l.state.requireOpen(); err != nil {
		return err
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	if l.Limit == 0 || (l.Limit > 0 && l.emitted >= l.Limit) {
		return nil
	}
	l.scratch.Resize(batch.Len)
	selected := 0
	sel.IterSet(func(row int) {
		if l.Limit > 0 && l.emitted >= l.Limit {
			return
		}
		if l.seen < l.Offset {
			l.seen++
			return
		}
		l.scratch.SetUnsafe(row)
		l.seen++
		l.emitted++
		selected++
	})
	if selected == 0 {
		return nil
	}
	return l.Downstream.Push(batch, l.scratch)
}

func (l *Limit) Close() error {
	if l.Downstream != nil {
		if err := l.Downstream.Close(); err != nil {
			_ = l.state.close()
			return err
		}
	}
	return l.state.close()
}
