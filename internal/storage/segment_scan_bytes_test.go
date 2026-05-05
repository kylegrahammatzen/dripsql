package storage

import (
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSelectSegmentInt64EqualBytes(t *testing.T) {
	data := selectSegmentData(t)
	runSelectSegmentInt64Equal(t, func(column string, value int64, scratch []uint32) ([]uint32, bool, error) {
		return SelectSegmentInt64EqualBytes(data, column, value, scratch)
	})
}

func TestCountSegmentInt64EqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)

	count, ok, err := CountSegmentInt64EqualBytes(data, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	count, ok, err = CountSegmentInt64EqualBytes(data, "tenant_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	_, ok, err = CountSegmentInt64EqualBytes(data, "missing", 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = CountSegmentInt64EqualBytes(data, "event_type", 7)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected event_type column to exist")
	}
}

func TestCountSegmentStringEqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)

	count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	count, ok, err = CountSegmentStringEqualBytes(data, "event_type", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	_, ok, err = CountSegmentStringEqualBytes(data, "missing", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = CountSegmentStringEqualBytes(data, "tenant_id", "checkout")
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected tenant_id column to exist")
	}
}

func TestSelectSegmentStringEqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)
	scratch := make([]uint32, 0, 8)

	selected, ok, err := SelectSegmentStringEqualBytes(data, "event_type", "checkout", scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if !slices.Equal(selected, []uint32{1, 4}) {
		t.Fatalf("selection = %v, want [1 4]", selected)
	}
	if cap(selected) != cap(scratch) {
		t.Fatalf("scratch cap = %d, want %d", cap(selected), cap(scratch))
	}
}

func TestCountSegmentStringEqualBytesPlain(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"alpha", "bravo", "charlie", "delta"})},
	)

	count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "charlie")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func BenchmarkSelectSegmentInt64EqualBytesWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	scratch := make([]uint32, 0, 100_000)
	b.ResetTimer()

	for b.Loop() {
		selected, ok, err := SelectSegmentInt64EqualBytes(data, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing tenant_id column")
		}
		if len(selected) != 98 {
			b.Fatalf("selection len = %d, want 98", len(selected))
		}
		scratch = selected
	}
}

func BenchmarkCountSegmentInt64EqualBytesWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		count, ok, err := CountSegmentInt64EqualBytes(data, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing tenant_id column")
		}
		if count != 98 {
			b.Fatalf("count = %d, want 98", count)
		}
	}
}

func BenchmarkCountSegmentStringEqualBytes(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	data := writeSegmentBytes(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)
	b.ResetTimer()

	for b.Loop() {
		count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing event_type column")
		}
		if count != 25_000 {
			b.Fatalf("count = %d, want 25000", count)
		}
	}
}
