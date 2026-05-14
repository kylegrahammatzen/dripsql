package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestScanRunsIteratorIntoConsumer(t *testing.T) {
	batch := execInt64Batch(t, []int64{1, 2, 3}, nil)
	scan := &Scan{Iterator: storage.SegmentScanIterator{Segments: []storage.ScanSegment{{Pages: []storage.ScanPage{{Batch: batch}}}}}}
	sink := &CountSink{}
	agg := &Aggregate{Sinks: []AggregateSink{sink}}
	if err := scan.Run(agg); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Run before Open err = %v, want ErrOperatorNotOpen", err)
	}
	if err := scan.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := scan.Run(agg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 3 {
		t.Fatalf("Result = %v, %v; want 3, nil", got, err)
	}
	if err := scan.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestScanRespectsCancellation(t *testing.T) {
	batch := execInt64Batch(t, []int64{1}, nil)
	scan := &Scan{Iterator: storage.SegmentScanIterator{Segments: []storage.ScanSegment{{Pages: []storage.ScanPage{{Batch: batch}}}}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scan.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := scan.Run(&Aggregate{Sinks: []AggregateSink{&CountSink{}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
}

func TestScanWithPredicate(t *testing.T) {
	batch := execInt64Batch(t, []int64{42, 7, 42}, nil)
	pred := storage.Predicate{Column: "amount", Op: storage.PredicateOpEq, PredicateValue: storage.PredicateValue{Int64: 42}}
	scan := &Scan{Iterator: storage.SegmentScanIterator{
		Segments:  []storage.ScanSegment{{Pages: []storage.ScanPage{{Batch: batch}}}},
		Predicate: storage.NewPredicateEvaluator(pred),
	}}
	sink := &CountSink{}
	agg := &Aggregate{Sinks: []AggregateSink{sink}}
	if err := scan.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := scan.Run(agg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 2 {
		t.Fatalf("Result = %v, %v; want 2, nil", got, err)
	}
}

type failingConsumer struct {
	err error
}

func (c failingConsumer) Open(context.Context) error { return nil }

func (c failingConsumer) Push(types.Batch, types.SelectionMask) error { return c.err }

func (c failingConsumer) Close() error { return nil }

func TestScanPropagatesConsumerPushError(t *testing.T) {
	wantErr := errors.New("push failed")
	batch := execInt64Batch(t, []int64{1}, nil)
	scan := &Scan{Iterator: storage.SegmentScanIterator{Segments: []storage.ScanSegment{{Pages: []storage.ScanPage{{Batch: batch}}}}}}
	if err := scan.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := scan.Run(failingConsumer{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("Run err = %v, want %v", err, wantErr)
	}
}
