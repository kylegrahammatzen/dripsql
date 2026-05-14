package storage

import (
	"context"
	"sync"
)

// RunParallel runs fn across workers, each over a contiguous slice of
// base.Segments. The function receives a per-worker copy of the iterator
// with Segments, Context, and Stats already adjusted; callers may further
// mutate fields like Predicate inside fn (e.g. to install a fresh evaluator
// per worker — *BoundPredicate carries per-page scratch state and must
// not be shared across goroutines).
//
// Returns the per-worker stats and the first error. The first error cancels
// the shared context so the remaining workers stop quickly. If workers <= 0
// or there are no segments the call returns immediately.
func (base SegmentScanIterator) RunParallel(
	ctx context.Context,
	workers int,
	fn func(workerIdx int, it SegmentScanIterator) error,
) ([]QueryStats, error) {
	if workers <= 0 || len(base.Segments) == 0 {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stats := make([]QueryStats, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		start := w * len(base.Segments) / workers
		end := (w + 1) * len(base.Segments) / workers
		if start == end {
			continue
		}
		it := base
		it.Context = ctx
		it.Segments = base.Segments[start:end]
		it.Stats = &stats[w]
		wg.Add(1)
		go func(w int, it SegmentScanIterator) {
			defer wg.Done()
			if err := fn(w, it); err != nil {
				cancel()
				errCh <- err
			}
		}(w, it)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}
