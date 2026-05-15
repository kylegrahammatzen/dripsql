// ScanOp adapts the push-based storage.Scan to a pull-based Operator via a goroutine + channel.
// The producer goroutine pushes batches into a 1-buffered chan; Next reads them.
package exec

import (
	"context"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type ScanOp struct {
	Opts        storage.ScanOpts
	ColumnAlias string // when set, output columns are renamed to "alias.col"

	ctx    context.Context
	cancel context.CancelFunc
	out    chan types.Batch
	err    chan error
	state  operatorState
}

func (s *ScanOp) Open(ctx context.Context) error {
	if err := s.state.open(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.out = make(chan types.Batch, 1)
	s.err = make(chan error, 1)
	go s.run()
	return nil
}

func (s *ScanOp) run() {
	defer close(s.out)
	err := storage.Scan(s.Opts, func(batch types.Batch, sel *types.SelectionMask) error {
		// storage.Scan reuses decoded columns and the mask across pages.
		// Deep-copy here so each emitted batch survives the next page decode.
		cloned := cloneBatch(batch, s.ColumnAlias)
		clonedSel := sel.Clone()
		cloned.Sel = &clonedSel
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case s.out <- cloned:
			return nil
		}
	})
	s.err <- err
}

func cloneBatch(b types.Batch, alias string) types.Batch {
	cols := make([]types.Column, len(b.Columns))
	for i, c := range b.Columns {
		name := c.Name
		if alias != "" {
			name = alias + "." + c.Name
		}
		cols[i] = types.Column{
			Name:       name,
			Type:       c.Type,
			EnumLabels: c.EnumLabels,
			V:          c.V.Clone(),
		}
	}
	return types.Batch{Len: b.Len, Columns: cols}
}

func (s *ScanOp) Next() (types.Batch, bool, error) {
	if err := s.state.requireOpen(); err != nil {
		return types.Batch{}, false, err
	}
	select {
	case <-s.ctx.Done():
		return types.Batch{}, false, s.ctx.Err()
	case batch, ok := <-s.out:
		if !ok {
			err := <-s.err
			return types.Batch{}, false, err
		}
		return batch, true, nil
	}
}

func (s *ScanOp) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	if s.out != nil {
		for range s.out {
		}
	}
	if s.err != nil {
		select {
		case <-s.err:
		default:
		}
	}
	s.state.close()
	return nil
}

