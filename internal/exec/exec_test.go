package exec

import (
	"bytes"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestCount(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
	)

	if got := Count(batch); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}

	batch.Sel = []uint32{0, 2}
	if got := Count(batch); got != 2 {
		t.Fatalf("selected count = %d, want 2", got)
	}
}

func TestFilterInt64Equal(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel", "signup"})},
	)

	got, err := FilterInt64Equal(batch, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{0, 2}
	if !slices.Equal(got.Sel, want) {
		t.Fatalf("selection = %v, want %v", got.Sel, want)
	}
	if Count(got) != 2 {
		t.Fatalf("count = %d, want 2", Count(got))
	}
}

func TestFilterInt64EqualIntoReusesScratch(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
	scratch := make([]uint32, 0, 8)

	got, returned, err := FilterInt64EqualInto(batch, "tenant_id", 7, scratch)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{0, 2}
	if !slices.Equal(got.Sel, want) {
		t.Fatalf("selection = %v, want %v", got.Sel, want)
	}
	if !slices.Equal(returned, want) {
		t.Fatalf("returned scratch = %v, want %v", returned, want)
	}
	if cap(returned) != cap(scratch) {
		t.Fatalf("scratch cap = %d, want %d", cap(returned), cap(scratch))
	}
}

func TestFilterInt64EqualAtInto(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel", "signup"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
	scratch := make([]uint32, 0, batch.Count)

	got, returned, err := FilterInt64EqualAtInto(batch, 1, 7, scratch)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{0, 2}
	if !slices.Equal(got.Sel, want) {
		t.Fatalf("selection = %v, want %v", got.Sel, want)
	}
	if !slices.Equal(returned, want) {
		t.Fatalf("returned scratch = %v, want %v", returned, want)
	}
}

func TestFilterInt64EqualChainsSelection(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
	batch.Sel = []uint32{1, 2, 3}

	got, err := FilterInt64Equal(batch, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}

	want := []uint32{2}
	if !slices.Equal(got.Sel, want) {
		t.Fatalf("selection = %v, want %v", got.Sel, want)
	}
}

func TestFilterInt64EqualAfterSegmentRead(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel", "signup"})},
	)

	var buf bytes.Buffer
	if _, err := storage.WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}

	readBatch, _, err := storage.ReadSegment(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	filtered, err := FilterInt64Equal(readBatch, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if got := Count(filtered); got != 2 {
		t.Fatalf("count after storage scan = %d, want 2", got)
	}
}

func TestFilterInt64EqualRejectsWrongType(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
	)

	_, err := FilterInt64Equal(batch, "event_type", 7)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
}

func TestFilterInt64EqualRejectsOutOfRangeSelection(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
	)
	batch.Sel = []uint32{2}

	_, err := FilterInt64Equal(batch, "tenant_id", 7)
	if err == nil {
		t.Fatal("expected out of range error")
	}
}

func TestFilterInt64EqualAtIntoRejectsBadIndex(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
	)

	_, _, err := FilterInt64EqualAtInto(batch, 1, 7, nil)
	if err == nil {
		t.Fatal("expected bad index error")
	}
}

func BenchmarkFilterInt64EqualSparse(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 10_000)

	b.ReportAllocs()
	for b.Loop() {
		got, err := FilterInt64Equal(batch, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 10 {
			b.Fatalf("count = %d, want 10", Count(got))
		}
	}
}

func BenchmarkFilterInt64EqualIntoSparse(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 10_000)
	scratch := make([]uint32, 0, batch.Count)

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualInto(batch, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 10 {
			b.Fatalf("count = %d, want 10", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualAtIntoSparse(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 10_000)
	scratch := make([]uint32, 0, batch.Count)

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualAtInto(batch, 0, 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 10 {
			b.Fatalf("count = %d, want 10", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualDense(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 2)

	b.ReportAllocs()
	for b.Loop() {
		got, err := FilterInt64Equal(batch, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 50_000 {
			b.Fatalf("count = %d, want 50000", Count(got))
		}
	}
}

func BenchmarkFilterInt64EqualIntoDense(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 2)
	scratch := make([]uint32, 0, batch.Count)

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualInto(batch, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 50_000 {
			b.Fatalf("count = %d, want 50000", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualAtIntoDense(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 2)
	scratch := make([]uint32, 0, batch.Count)

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualAtInto(batch, 0, 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 50_000 {
			b.Fatalf("count = %d, want 50000", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualWithSelection(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 100)
	batch.Sel = make([]uint32, 0, 10_000)
	for row := 0; row < batch.Count; row += 10 {
		batch.Sel = append(batch.Sel, uint32(row))
	}

	b.ReportAllocs()
	for b.Loop() {
		got, err := FilterInt64Equal(batch, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 1_000 {
			b.Fatalf("count = %d, want 1000", Count(got))
		}
	}
}

func BenchmarkFilterInt64EqualIntoWithSelection(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 100)
	batch.Sel = make([]uint32, 0, 10_000)
	for row := 0; row < batch.Count; row += 10 {
		batch.Sel = append(batch.Sel, uint32(row))
	}
	scratch := make([]uint32, 0, len(batch.Sel))

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualInto(batch, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 1_000 {
			b.Fatalf("count = %d, want 1000", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualAtIntoWithSelection(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 100)
	batch.Sel = make([]uint32, 0, 10_000)
	for row := 0; row < batch.Count; row += 10 {
		batch.Sel = append(batch.Sel, uint32(row))
	}
	scratch := make([]uint32, 0, len(batch.Sel))

	b.ReportAllocs()
	for b.Loop() {
		got, returned, err := FilterInt64EqualAtInto(batch, 0, 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 1_000 {
			b.Fatalf("count = %d, want 1000", Count(got))
		}
		scratch = returned
	}
}

func BenchmarkFilterInt64EqualChainsSelection(b *testing.B) {
	batch := benchmarkBatch(b, 100_000, 100)
	selected, err := FilterInt64Equal(batch, "tenant_id", 7)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		got, err := FilterInt64Equal(selected, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if Count(got) != 1_000 {
			b.Fatalf("count = %d, want 1000", Count(got))
		}
	}
}

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func benchmarkBatch(b *testing.B, size int, matchEvery int) vector.Batch {
	b.Helper()

	values := make([]int64, size)
	for row := range values {
		if row%matchEvery == 0 {
			values[row] = 7
		} else {
			values[row] = int64(row) + 100
		}
	}

	batch, err := vector.NewBatch(
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)},
	)
	if err != nil {
		b.Fatal(err)
	}
	return batch
}
