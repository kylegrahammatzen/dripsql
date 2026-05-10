package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Scan struct {
	Iterator storage.SegmentScanIterator

	ctx   context.Context
	state operatorState
}

func (s *Scan) Open(ctx context.Context) error {
	if err := s.state.open(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx = ctx
	if s.Iterator.Context == nil {
		s.Iterator.Context = ctx
	}
	return nil
}

func (s *Scan) Run(consumer Consumer) error {
	if err := s.state.requireOpen(); err != nil {
		return err
	}
	if consumer == nil {
		return fmt.Errorf("scan consumer is nil")
	}
	if err := consumer.Open(s.ctx); err != nil {
		return err
	}
	runErr := s.Iterator.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		return consumer.Push(batch, sel)
	})
	closeErr := consumer.Close()
	if runErr != nil {
		return runErr
	}
	return closeErr
}

func (s *Scan) Close() error {
	return s.state.close()
}
