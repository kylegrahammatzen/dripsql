package storage

import (
	"bytes"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSelectSegmentInt64Equal(t *testing.T) {
	data := selectSegmentData(t)
	runSelectSegmentInt64Equal(t, func(column string, value int64, scratch []uint32) ([]uint32, bool, error) {
		return SelectSegmentInt64Equal(bytes.NewReader(data), column, value, scratch)
	})
}

func selectSegmentData(t testing.TB) []byte {
	t.Helper()

	return writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
}

func TestSelectSegmentInt64EqualConsumesFullSegment(t *testing.T) {
	first := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
	)
	second := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 12})},
	)

	reader := bytes.NewReader(append(first, second...))
	selected, ok, err := SelectSegmentInt64Equal(reader, "tenant_id", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if !slices.Equal(selected, []uint32{0}) {
		t.Fatalf("selection = %v, want [0]", selected)
	}

	next, _, err := ReadSegment(reader)
	if err != nil {
		t.Fatal(err)
	}
	nextCol, ok := next.Column("tenant_id")
	if !ok {
		t.Fatal("missing second tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{11, 12}), nextCol.Vector)
}

func TestCanSkipInt64Equal(t *testing.T) {
	stats := ColumnStats{
		Name:      "tenant_id",
		Kind:      vector.KindInt64,
		HasMinMax: true,
		MinInt64:  7,
		MaxInt64:  42,
	}

	for _, value := range []int64{6, 43} {
		if !canSkipInt64Equal(stats, value) {
			t.Fatalf("expected value %d to skip", value)
		}
	}
	for _, value := range []int64{7, 20, 42} {
		if canSkipInt64Equal(stats, value) {
			t.Fatalf("did not expect value %d to skip", value)
		}
	}

	stats.HasMinMax = false
	if canSkipInt64Equal(stats, 6) {
		t.Fatal("did not expect skip without min/max")
	}

	stats.HasMinMax = true
	stats.Kind = vector.KindString
	if canSkipInt64Equal(stats, 6) {
		t.Fatal("did not expect skip for non-int64 stats")
	}
}

func BenchmarkSelectSegmentInt64EqualWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	scratch := make([]uint32, 0, 100_000)
	b.ResetTimer()

	for b.Loop() {
		selected, ok, err := SelectSegmentInt64Equal(bytes.NewReader(data), "tenant_id", 7, scratch)
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

func runSelectSegmentInt64Equal(t testing.TB, selectFn func(column string, value int64, scratch []uint32) ([]uint32, bool, error)) {
	t.Helper()

	scratch := make([]uint32, 0, 8)

	selected, ok, err := selectFn("tenant_id", 7, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if !slices.Equal(selected, []uint32{0, 2}) {
		t.Fatalf("selection = %v, want [0 2]", selected)
	}
	if cap(selected) != cap(scratch) {
		t.Fatalf("scratch cap = %d, want %d", cap(selected), cap(scratch))
	}

	selected, ok, err = selectFn("tenant_id", 99, selected)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if len(selected) != 0 {
		t.Fatalf("selection len = %d, want 0", len(selected))
	}

	_, ok, err = selectFn("missing", 7, selected)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = selectFn("event_type", 7, selected)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected event_type column to exist")
	}
}
