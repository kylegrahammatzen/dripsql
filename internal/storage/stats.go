// Stats live in the column directory at 16B each. Readers cast to the right type via Kind.
// NumericStats is generic over int32 | int64. FloatStats, BoolStats, VarBytesStats use
// kind-specific layouts that also fit in 16B.
package storage

import (
	"encoding/binary"
	"math"
)

const StatsWireSize = 16

type NumericStats[T int32 | int64] struct {
	Min        T
	Max        T
	HasNonNull bool
}

func (s *NumericStats[T]) Update(v T) {
	if !s.HasNonNull {
		s.Min, s.Max, s.HasNonNull = v, v, true
		return
	}
	if v < s.Min {
		s.Min = v
	}
	if v > s.Max {
		s.Max = v
	}
}

func (s NumericStats[T]) Merge(other NumericStats[T]) NumericStats[T] {
	if !other.HasNonNull {
		return s
	}
	if !s.HasNonNull {
		return other
	}
	out := s
	if other.Min < out.Min {
		out.Min = other.Min
	}
	if other.Max > out.Max {
		out.Max = other.Max
	}
	return out
}

func (s NumericStats[T]) MarshalWire(dst []byte) {
	_ = dst[StatsWireSize-1]
	clear(dst[:StatsWireSize])
	if !s.HasNonNull {
		return
	}
	switch any(s.Min).(type) {
	case int32:
		binary.LittleEndian.PutUint32(dst[0:4], uint32(s.Min))
		binary.LittleEndian.PutUint32(dst[4:8], uint32(s.Max))
	case int64:
		binary.LittleEndian.PutUint64(dst[0:8], uint64(s.Min))
		binary.LittleEndian.PutUint64(dst[8:16], uint64(s.Max))
	}
}

func UnmarshalNumericStats[T int32 | int64](src []byte, hasNonNull bool) NumericStats[T] {
	_ = src[StatsWireSize-1]
	var s NumericStats[T]
	if !hasNonNull {
		return s
	}
	s.HasNonNull = true
	var zero T
	switch any(zero).(type) {
	case int32:
		min := int32(binary.LittleEndian.Uint32(src[0:4]))
		max := int32(binary.LittleEndian.Uint32(src[4:8]))
		s.Min = T(min)
		s.Max = T(max)
	case int64:
		min := int64(binary.LittleEndian.Uint64(src[0:8]))
		max := int64(binary.LittleEndian.Uint64(src[8:16]))
		s.Min = T(min)
		s.Max = T(max)
	}
	return s
}

// Float32 values widen to f64 on encode and narrow on decode.
type FloatStats struct {
	Min        float64
	Max        float64
	HasNonNull bool
}

func (s *FloatStats) Update(v float64) {
	if math.IsNaN(v) {
		return
	}
	if !s.HasNonNull {
		s.Min, s.Max, s.HasNonNull = v, v, true
		return
	}
	if v < s.Min {
		s.Min = v
	}
	if v > s.Max {
		s.Max = v
	}
}

func (s FloatStats) MarshalWire(dst []byte) {
	_ = dst[StatsWireSize-1]
	clear(dst[:StatsWireSize])
	if !s.HasNonNull {
		return
	}
	binary.LittleEndian.PutUint64(dst[0:8], math.Float64bits(s.Min))
	binary.LittleEndian.PutUint64(dst[8:16], math.Float64bits(s.Max))
}

func UnmarshalFloatStats(src []byte, hasNonNull bool) FloatStats {
	_ = src[StatsWireSize-1]
	if !hasNonNull {
		return FloatStats{}
	}
	return FloatStats{
		Min:        math.Float64frombits(binary.LittleEndian.Uint64(src[0:8])),
		Max:        math.Float64frombits(binary.LittleEndian.Uint64(src[8:16])),
		HasNonNull: true,
	}
}

type BoolStats struct {
	TrueCount  uint32
	FalseCount uint32
	NullCount  uint32
}

func (s BoolStats) MarshalWire(dst []byte) {
	_ = dst[StatsWireSize-1]
	binary.LittleEndian.PutUint32(dst[0:4], s.TrueCount)
	binary.LittleEndian.PutUint32(dst[4:8], s.FalseCount)
	binary.LittleEndian.PutUint32(dst[8:12], s.NullCount)
	binary.LittleEndian.PutUint32(dst[12:16], 0)
}

func UnmarshalBoolStats(src []byte) BoolStats {
	_ = src[StatsWireSize-1]
	return BoolStats{
		TrueCount:  binary.LittleEndian.Uint32(src[0:4]),
		FalseCount: binary.LittleEndian.Uint32(src[4:8]),
		NullCount:  binary.LittleEndian.Uint32(src[8:12]),
	}
}

type VarBytesStats struct {
	MinLen     uint32
	MaxLen     uint32
	TotalBytes uint64
	HasNonNull bool
}

func (s *VarBytesStats) Update(length int) {
	l := uint32(length)
	if !s.HasNonNull {
		s.MinLen, s.MaxLen, s.HasNonNull = l, l, true
	} else {
		if l < s.MinLen {
			s.MinLen = l
		}
		if l > s.MaxLen {
			s.MaxLen = l
		}
	}
	s.TotalBytes += uint64(length)
}

func (s VarBytesStats) MarshalWire(dst []byte) {
	_ = dst[StatsWireSize-1]
	clear(dst[:StatsWireSize])
	if !s.HasNonNull {
		return
	}
	binary.LittleEndian.PutUint32(dst[0:4], s.MinLen)
	binary.LittleEndian.PutUint32(dst[4:8], s.MaxLen)
	binary.LittleEndian.PutUint64(dst[8:16], s.TotalBytes)
}

func UnmarshalVarBytesStats(src []byte, hasNonNull bool) VarBytesStats {
	_ = src[StatsWireSize-1]
	if !hasNonNull {
		return VarBytesStats{}
	}
	return VarBytesStats{
		MinLen:     binary.LittleEndian.Uint32(src[0:4]),
		MaxLen:     binary.LittleEndian.Uint32(src[4:8]),
		TotalBytes: binary.LittleEndian.Uint64(src[8:16]),
		HasNonNull: true,
	}
}
