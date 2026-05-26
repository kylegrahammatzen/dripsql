// LimitOp skips Offset visible rows, then emits N more. N < 0 means unlimited, N == 0 emits nothing.
// All-rows fast path uses SetRange to avoid IterSet on unfiltered batches.
package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type LimitOp struct {
	Source Operator
	N      int64
	Offset int64

	seen  int64
	sent  int64
	state operatorState
}

func (l *LimitOp) Open(ctx context.Context) error {
	prev := l.state
	if err := l.state.open(); err != nil {
		return err
	}
	if err := l.Source.Open(ctx); err != nil {
		l.state = prev
		return err
	}
	l.seen = 0
	l.sent = 0
	return nil
}

func (l *LimitOp) Next() (vector.Batch, bool, error) {
	if err := l.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	if l.done() {
		return vector.Batch{}, false, nil
	}
	for {
		batch, ok, err := l.Source.Next()
		if err != nil || !ok {
			return batch, ok, err
		}
		if batch.Len == 0 {
			continue
		}
		if batch.Sel == nil || batch.Sel.IsAllSet() {
			out, ok := l.limitAllRows(batch)
			if ok {
				return out, true, nil
			}
			if l.done() {
				return vector.Batch{}, false, nil
			}
			continue
		}
		out, seen, sent := l.limitSelectedRows(batch)
		l.seen += seen
		l.sent += sent
		if sent == 0 {
			if l.done() {
				return vector.Batch{}, false, nil
			}
			continue
		}
		if err := batch.SetSel(&out); err != nil {
			return vector.Batch{}, false, fmt.Errorf("limit: %w", err)
		}
		return batch, true, nil
	}
}

func (l *LimitOp) limitAllRows(batch vector.Batch) (vector.Batch, bool) {
	rows := int64(batch.Len)
	start := max(int64(0), l.Offset-l.seen)
	if start >= rows {
		l.seen += rows
		return vector.Batch{}, false
	}
	take := rows - start
	if l.N > 0 {
		take = min(take, l.N-l.sent)
	}
	if take <= 0 {
		l.seen += rows
		return vector.Batch{}, false
	}
	end := start + take
	out := vector.NewSelectionRange(batch.Len, int(start), int(end))
	if err := batch.SetSel(&out); err != nil {
		l.seen += end
		l.sent += take
		return vector.Batch{}, false
	}
	l.seen += end
	l.sent += take
	return batch, true
}

func (l *LimitOp) limitSelectedRows(batch vector.Batch) (vector.SelectionMask, int64, int64) {
	skip := max(int64(0), l.Offset-l.seen)
	take := int64(-1)
	if l.N > 0 {
		take = l.N - l.sent
	}
	return batch.Sel.Limit(batch.Len, skip, take)
}

func (l *LimitOp) done() bool {
	return l.N == 0 || (l.N > 0 && l.sent >= l.N)
}

func (l *LimitOp) Close() error {
	l.state.close()
	return l.Source.Close()
}
