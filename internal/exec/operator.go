package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var (
	ErrOperatorAlreadyOpen = errors.New("operator already open")
	ErrOperatorNotOpen     = errors.New("operator not open")
	ErrOperatorClosed      = errors.New("operator closed")
)

type Consumer interface {
	Open(context.Context) error
	Push(types.Batch, types.SelectionMask) error
	Close() error
}

type Source interface {
	Open(context.Context) error
	Run(Consumer) error
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

func (s *operatorState) close() error {
	if *s == operatorClosed {
		return nil
	}
	*s = operatorClosed
	return nil
}

func validateBatchSelection(batch types.Batch, sel types.SelectionMask) error {
	if sel.Rows != batch.Len {
		return fmt.Errorf("selection rows %d do not match batch length %d", sel.Rows, batch.Len)
	}
	if len(sel.Words) < types.ValidityWords(sel.Rows) {
		return fmt.Errorf("selection has %d words for %d rows", len(sel.Words), sel.Rows)
	}
	return nil
}
