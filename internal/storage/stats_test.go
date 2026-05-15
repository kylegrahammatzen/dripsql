// Stats tests: 16B wire round-trip + Update/Merge correctness for each kind family.
package storage

import (
	"math"
	"testing"
)

func TestNumericStats_Int64_UpdateAndMerge(t *testing.T) {
	var a, b NumericStats[int64]
	for _, v := range []int64{5, 3, 9, 1} {
		a.Update(v)
	}
	for _, v := range []int64{0, 100} {
		b.Update(v)
	}
	got := a.Merge(b)
	if got.Min != 0 || got.Max != 100 || !got.HasNonNull {
		t.Fatalf("merge got %+v", got)
	}
}

func TestNumericStats_Int64_WireRoundTrip(t *testing.T) {
	s := NumericStats[int64]{Min: -42, Max: 1 << 40, HasNonNull: true}
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalNumericStats[int64](buf[:], true)
	if got != s {
		t.Fatalf("round-trip got %+v want %+v", got, s)
	}
}

func TestNumericStats_Int32_WireRoundTrip(t *testing.T) {
	s := NumericStats[int32]{Min: -1000, Max: 2_000_000, HasNonNull: true}
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalNumericStats[int32](buf[:], true)
	if got != s {
		t.Fatalf("round-trip got %+v want %+v", got, s)
	}
}

func TestNumericStats_AllNull_MarshalUnmarshal(t *testing.T) {
	var s NumericStats[int64]
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalNumericStats[int64](buf[:], false)
	if got.HasNonNull {
		t.Fatal("all-null stats must round-trip with HasNonNull=false")
	}
}

func TestFloatStats_WireRoundTrip(t *testing.T) {
	var s FloatStats
	for _, v := range []float64{1.5, math.NaN(), -3.25, 100.0} {
		s.Update(v)
	}
	if s.Min != -3.25 || s.Max != 100.0 || !s.HasNonNull {
		t.Fatalf("Update got %+v", s)
	}
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalFloatStats(buf[:], true)
	if got != s {
		t.Fatalf("round-trip got %+v want %+v", got, s)
	}
}

func TestBoolStats_WireRoundTrip(t *testing.T) {
	s := BoolStats{TrueCount: 7, FalseCount: 13, NullCount: 4}
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalBoolStats(buf[:])
	if got != s {
		t.Fatalf("round-trip got %+v want %+v", got, s)
	}
}

func TestVarBytesStats_UpdateAndWire(t *testing.T) {
	var s VarBytesStats
	for _, n := range []int{12, 4, 30, 100} {
		s.Update(n)
	}
	if s.MinLen != 4 || s.MaxLen != 100 || s.TotalBytes != 146 || !s.HasNonNull {
		t.Fatalf("Update got %+v", s)
	}
	var buf [StatsWireSize]byte
	s.MarshalWire(buf[:])
	got := UnmarshalVarBytesStats(buf[:], true)
	if got != s {
		t.Fatalf("round-trip got %+v want %+v", got, s)
	}
}
