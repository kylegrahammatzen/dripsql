// ScanOp adapts push-based storage.Scan to pull-based Operator. Parallelism > 1
// partitions segments across workers sharing one output channel; output order is
// unordered. TopK pushdown requires a single pass across all segments to stay sound,
// so callers must leave Parallelism at 1 when Opts.TopK is set.
package exec

import (
	"context"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type ScanOp struct {
	Opts        storage.ScanOpts
	ColumnAlias string
	Parallelism int

	ctx    context.Context
	cancel context.CancelFunc
	out    chan vector.Batch
	err    chan error
	wg     sync.WaitGroup
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

	workers := s.Parallelism
	if workers < 1 || len(s.Opts.Segments) <= 1 || s.Opts.TopK != nil {
		workers = 1
	}
	if workers > len(s.Opts.Segments) {
		workers = len(s.Opts.Segments)
	}

	s.out = make(chan vector.Batch, workers)
	s.err = make(chan error, workers)

	if workers == 1 {
		go s.runShardClose(s.Opts.Segments)
		return nil
	}
	shards := partitionSegments(s.Opts.Segments, workers)
	for _, shard := range shards {
		if len(shard) == 0 {
			continue
		}
		s.wg.Add(1)
		go s.runShard(shard)
	}
	go func() {
		s.wg.Wait()
		close(s.out)
	}()
	return nil
}

func (s *ScanOp) runShardClose(segs []*storage.Segment) {
	defer close(s.out)
	s.runScan(segs)
}

func (s *ScanOp) runShard(segs []*storage.Segment) {
	defer s.wg.Done()
	s.runScan(segs)
}

func (s *ScanOp) runScan(segs []*storage.Segment) {
	opts := s.Opts
	opts.Segments = segs
	err := storage.Scan(opts, func(batch vector.Batch, sel *vector.SelectionMask) error {
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
	if err != nil {
		select {
		case s.err <- err:
		default:
		}
	}
}

func partitionSegments(segs []*storage.Segment, workers int) [][]*storage.Segment {
	out := make([][]*storage.Segment, workers)
	for i, seg := range segs {
		bucket := i % workers
		out[bucket] = append(out[bucket], seg)
	}
	return out
}

func cloneBatch(b vector.Batch, alias string) vector.Batch {
	cols := make([]vector.Column, len(b.Columns))
	for i, c := range b.Columns {
		name := c.Name
		if alias != "" {
			name = alias + "." + c.Name
		}
		cols[i] = vector.Column{
			Name:       name,
			Type:       c.Type,
			EnumLabels: c.EnumLabels,
			V:          c.V.Clone(),
		}
	}
	return vector.Batch{Len: b.Len, Columns: cols}
}

func (s *ScanOp) Next() (vector.Batch, bool, error) {
	if err := s.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	select {
	case <-s.ctx.Done():
		return vector.Batch{}, false, s.ctx.Err()
	case batch, ok := <-s.out:
		if !ok {
			select {
			case err := <-s.err:
				return vector.Batch{}, false, err
			default:
				return vector.Batch{}, false, nil
			}
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
	s.state.close()
	return nil
}
