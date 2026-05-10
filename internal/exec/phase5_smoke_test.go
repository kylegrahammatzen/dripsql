package exec

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestPhase5BenchQueryShapes(t *testing.T) {
	ctx := context.Background()
	batches := phase5BenchBatches(t)

	t.Run("event checkout for tenant", func(t *testing.T) {
		got, explain := phase5Count(t, ctx, batches, storage.Predicate{Op: storage.PredicateAnd, Children: []storage.Predicate{
			{Column: "tenant_id", Op: storage.PredicateOpEq, Int64: 42},
			{Column: "event_type", Op: storage.PredicateOpEq, Text: "checkout"},
		}})
		phase5AssertScalar(t, got, int64(2))
		if explain.Selected != got {
			t.Fatalf("explain selected = %d, want %d", explain.Selected, got)
		}
	})

	t.Run("absent tenant", func(t *testing.T) {
		got, explain := phase5Count(t, ctx, batches, storage.Predicate{Column: "tenant_id", Op: storage.PredicateOpEq, Int64: 999999})
		phase5AssertScalar(t, got, int64(0))
		if explain.Selected != 0 {
			t.Fatalf("explain selected = %d, want 0", explain.Selected)
		}
	})

	t.Run("checkout amount for tenant", func(t *testing.T) {
		got, explain := phase5Sum(t, ctx, batches, storage.Predicate{Op: storage.PredicateAnd, Children: []storage.Predicate{
			{Column: "tenant_id", Op: storage.PredicateOpEq, Int64: 42},
			{Column: "event_type", Op: storage.PredicateOpEq, Text: "checkout"},
		}})
		phase5AssertScalar(t, got.Sum, int64(17))
		if explain.Selected != got.Count {
			t.Fatalf("explain selected = %d, want %d", explain.Selected, got.Count)
		}
	})

	t.Run("checkout counts by country", func(t *testing.T) {
		got, explain := phase5GroupCountry(t, ctx, batches, storage.Predicate{Column: "event_type", Op: storage.PredicateOpEq, Text: "checkout"})
		phase5AssertGroupCounts(t, got, map[string]int64{"CA": 1, "US": 2})
		if explain.Selected != 3 {
			t.Fatalf("explain selected = %d, want 3", explain.Selected)
		}
	})
}

func phase5BenchBatches(t *testing.T) []types.Batch {
	t.Helper()
	return []types.Batch{
		phase5BenchBatch(t, []int64{42, 42, 42}, []string{"checkout", "checkout", "login"}, []int64{10, 7, 20}, []string{"US", "CA", "US"}),
		phase5BenchBatch(t, []int64{7, 999}, []string{"checkout", "signup"}, []int64{5, 100}, []string{"US", "MX"}),
	}
}

func phase5BenchBatch(t *testing.T, tenants []int64, events []string, amounts []int64, countries []string) types.Batch {
	t.Helper()
	eventText := types.NewVarBytes(len(events), 0)
	for row, value := range events {
		eventText.AppendString(row, value)
	}
	countryText := types.NewVarBytes(len(countries), 0)
	for row, value := range countries {
		countryText.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(tenants), I64: append([]int64(nil), tenants...)}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(events), Var: eventText}},
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amounts), I64: append([]int64(nil), amounts...)}},
		{Name: "country", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(countries), Var: countryText}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func phase5Count(t *testing.T, ctx context.Context, batches []types.Batch, pred storage.Predicate) (int64, *Explain) {
	t.Helper()
	sink := &CountSink{}
	explain := &Explain{Downstream: &Aggregate{Sinks: []AggregateSink{sink}}}
	phase5RunScan(t, ctx, batches, pred, explain)
	got, err := sink.Result()
	if err != nil {
		t.Fatalf("count Result: %v", err)
	}
	return got.(int64), explain
}

func phase5Sum(t *testing.T, ctx context.Context, batches []types.Batch, pred storage.Predicate) (SumInt64Result, *Explain) {
	t.Helper()
	sink := &SumInt64Sink{Column: "amount"}
	explain := &Explain{Downstream: &Aggregate{Sinks: []AggregateSink{sink}}}
	phase5RunScan(t, ctx, batches, pred, explain)
	got, err := sink.Result()
	if err != nil {
		t.Fatalf("sum Result: %v", err)
	}
	return got.(SumInt64Result), explain
}

func phase5GroupCountry(t *testing.T, ctx context.Context, batches []types.Batch, pred storage.Predicate) (map[string]int64, *Explain) {
	t.Helper()
	sink := &GroupStringCountSink{Column: "country"}
	explain := &Explain{Downstream: &Aggregate{Sinks: []AggregateSink{sink}}}
	phase5RunScan(t, ctx, batches, pred, explain)
	got, err := sink.Result()
	if err != nil {
		t.Fatalf("group Result: %v", err)
	}
	return got.(map[string]int64), explain
}

func phase5RunScan(t *testing.T, ctx context.Context, batches []types.Batch, pred storage.Predicate, consumer Consumer) {
	t.Helper()
	pages := make([]storage.ScanPage, len(batches))
	for i, batch := range batches {
		pages[i] = storage.ScanPage{Batch: batch}
	}
	scan := &Scan{Iterator: storage.SegmentScanIterator{
		Segments:  []storage.ScanSegment{{Pages: pages}},
		Predicate: storage.NewPredicateEvaluator(pred),
	}}
	if err := scan.Open(ctx); err != nil {
		t.Fatalf("scan Open: %v", err)
	}
	if err := scan.Run(consumer); err != nil {
		t.Fatalf("scan Run: %v", err)
	}
	if err := scan.Close(); err != nil {
		t.Fatalf("scan Close: %v", err)
	}
}

func phase5AssertScalar(t *testing.T, got any, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("result = %#v, want %#v", got, want)
	}
}

func phase5AssertGroupCounts(t *testing.T, got map[string]int64, want map[string]int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
	for key, count := range got {
		if want[key] != count {
			t.Fatalf("counts = %#v, want %#v", got, want)
		}
	}
}
