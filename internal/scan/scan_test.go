package scan

import (
	"bytes"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestCountInt64EqualSegment(t *testing.T) {
	data := mustSegment(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel", "signup"})},
	)

	count, err := CountInt64EqualSegment(data, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
}

func TestCountInt64EqualSegmentSkip(t *testing.T) {
	data := mustSegment(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{10, 20, 30})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel"})},
	)

	count, err := CountInt64EqualSegment(data, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
}

func TestCountInt64EqualSegmentRejectsMissingColumn(t *testing.T) {
	data := mustSegment(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
	)

	_, err := CountInt64EqualSegment(data, "missing", 7)
	if err == nil {
		t.Fatal("expected missing column error")
	}
}

func TestCountInt64EqualSegmentRejectsWrongType(t *testing.T) {
	data := mustSegment(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
	)

	_, err := CountInt64EqualSegment(data, "event_type", 7)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
}

func BenchmarkCountInt64EqualSegmentSkip(b *testing.B) {
	data := benchmarkSegment(b, 100_000, 0, 1_000_000)

	b.ReportAllocs()
	for b.Loop() {
		count, err := CountInt64EqualSegment(data, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 0 {
			b.Fatalf("count = %d, want 0", count)
		}
	}
}

func BenchmarkCountInt64EqualSegmentMatch(b *testing.B) {
	data := benchmarkSegment(b, 100_000, 2, 100)

	b.ReportAllocs()
	for b.Loop() {
		count, err := CountInt64EqualSegment(data, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 50_000 {
			b.Fatalf("count = %d, want 50000", count)
		}
	}
}

func mustSegment(t testing.TB, columns ...vector.Column) []byte {
	t.Helper()

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if _, err := storage.WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func benchmarkSegment(b *testing.B, size int, matchEvery int, base int64) []byte {
	b.Helper()

	values := make([]int64, size)
	for row := range values {
		if matchEvery > 0 && row%matchEvery == 0 {
			values[row] = 7
		} else {
			values[row] = base + int64(row)
		}
	}

	labels := make([]string, size)
	for row := range labels {
		labels[row] = "event"
	}

	return mustSegment(b,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)},
		vector.Column{Name: "event_type", Vector: vector.FromString(labels)},
	)
}
