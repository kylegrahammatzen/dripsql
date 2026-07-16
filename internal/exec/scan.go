// ScanOp adapts push-based storage.Scan to pull-based Operator, sharding segments across workers with unordered output.
// TopK pushdown needs a single pass across all segments, so callers must leave Parallelism at 1 when Opts.TopK is set.
package exec

import (
	"context"
	"fmt"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type ScanOp struct {
	Opts        storage.ScanOpts
	ColumnAlias string
	Parallelism int

	ctx     context.Context
	cancel  context.CancelFunc
	out     chan vector.Batch
	err     chan error
	wg      sync.WaitGroup
	state   operatorState
	workers int
	pool    sync.Pool
}

// concurrentSource marks operators whose Next is safe to call from multiple
// goroutines, reporting how many consumers the source can keep busy.
type concurrentSource interface {
	concurrentDrainWorkers() int
}

func (s *ScanOp) concurrentDrainWorkers() int { return s.workers }

// batchRecycler lets a consumer that fully copies what it needs out of each batch
// hand the buffers back, so the scan's clone step stops allocating per batch.
type batchRecycler interface {
	Recycle(vector.Batch)
}

func (s *ScanOp) Recycle(b vector.Batch) {
	if len(b.Columns) == 0 {
		return
	}
	s.pool.Put(&b)
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
	s.workers = workers

	s.out = make(chan vector.Batch, workers*4)
	s.err = make(chan error, workers)

	if workers == 1 {
		go s.runShardClose(s.Opts.Segments)
		return nil
	}
	shards := make([][]*storage.Segment, workers)
	for i, seg := range s.Opts.Segments {
		shards[i%workers] = append(shards[i%workers], seg)
	}
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
		cloned, mask := s.cloneBatch(batch, s.ColumnAlias)
		mask.CopyFrom(sel)
		if err := cloned.SetSel(mask); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
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

// cloneBatch copies the callback-scoped storage batch into buffers the consumer may
// hold, drawing from the recycle pool so steady-state scans stop allocating per batch.
func (s *ScanOp) cloneBatch(b vector.Batch, alias string) (vector.Batch, *vector.SelectionMask) {
	out := vector.Batch{}
	if p, _ := s.pool.Get().(*vector.Batch); p != nil && len(p.Columns) == len(b.Columns) {
		out = *p
	}
	if out.Columns == nil {
		out.Columns = make([]vector.Column, len(b.Columns))
	}
	out.Len = b.Len
	for i := range b.Columns {
		c := &b.Columns[i]
		name := c.Name
		if alias != "" {
			name = alias + "." + c.Name
		}
		dst := &out.Columns[i]
		dst.Name = name
		dst.Type = c.Type
		dst.EnumLabels = c.EnumLabels
		if c.Dict != nil {
			if dst.Dict == nil {
				dst.Dict = &vector.DictCol{}
			}
			dst.Dict.CopyFrom(c.Dict)
			dst.V = c.V
		} else {
			dst.Dict = nil
			c.V.CloneInto(&dst.V)
		}
	}
	mask := out.Sel
	if mask == nil {
		m := vector.NewSelectionMask(out.Len)
		mask = &m
	}
	out.Sel = nil
	return out, mask
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
