// Operator is the pull-based interface every exec node implements.
// Next returns ok=false when exhausted. Row visibility lives in batch.Sel; nil Sel means
// all batch.Len rows are visible.
package exec

import (
	"context"
	"errors"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var (
	ErrOperatorAlreadyOpen = errors.New("operator already open")
	ErrOperatorNotOpen     = errors.New("operator not open")
	ErrOperatorClosed      = errors.New("operator closed")
)

type Operator interface {
	Open(ctx context.Context) error
	Next() (batch types.Batch, ok bool, err error)
	Close() error
}

type operatorState uint8

const (
	operatorNew operatorState = iota
	operatorOpen
	operatorClosed
)

func (s *operatorState) open() error {
	switch *s {
	case operatorOpen:
		return ErrOperatorAlreadyOpen
	case operatorClosed:
		return ErrOperatorClosed
	default:
		*s = operatorOpen
		return nil
	}
}

func (s operatorState) requireOpen() error {
	switch s {
	case operatorOpen:
		return nil
	case operatorClosed:
		return ErrOperatorClosed
	default:
		return ErrOperatorNotOpen
	}
}

func (s *operatorState) close() {
	*s = operatorClosed
}
