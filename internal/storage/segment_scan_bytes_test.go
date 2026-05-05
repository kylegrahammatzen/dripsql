package storage

import (
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
