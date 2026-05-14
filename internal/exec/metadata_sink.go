package exec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// MetadataSink is an AggregateSink that can answer its question from segment
// metadata alone, without touching page payloads. Engine fast paths call
// TryAbsorbSegment for every segment; if all calls succeed, Result is final
// and no scan is needed. Merge folds the state of a peer sink (typically a
// per-worker partial) into the receiver.
type MetadataSink interface {
	AggregateSink
	TryAbsorbSegment(meta storage.SegmentMeta) bool
	Merge(other AggregateSink) error
}

// numericStats unifies Int32Stats and Int64Stats into one shape so each
// numeric sink doesn't switch on column type. set=false means the stats
// were absent entirely; set=true with sumValid=false means rows exist but
// the running sum overflowed during stats build.
type numericStats struct {
	min, max, sum int64
	sumValid      bool
	set           bool
}

func numericColumnMeta(meta storage.SegmentMeta, name string) (storage.ColumnMeta, numericStats, bool) {
	for _, col := range meta.Columns {
		if col.Name != name {
			continue
		}
		switch {
		case col.Stats.Int64 != nil:
			return col, numericStats{min: col.Stats.Int64.Min, max: col.Stats.Int64.Max, sum: col.Stats.Int64.Sum, sumValid: col.Stats.Int64.SumValid, set: true}, true
		case col.Stats.Int32 != nil:
			return col, numericStats{min: int64(col.Stats.Int32.Min), max: int64(col.Stats.Int32.Max), sum: col.Stats.Int32.Sum, sumValid: col.Stats.Int32.SumValid, set: true}, true
		case col.AllNull:
			return col, numericStats{sumValid: true}, true
		default:
			return col, numericStats{}, false
		}
	}
	return storage.ColumnMeta{}, numericStats{}, false
}

func columnMetaByName(meta storage.SegmentMeta, name string) (storage.ColumnMeta, bool) {
	for _, col := range meta.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return storage.ColumnMeta{}, false
}

// CountSink

func (s *CountSink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	s.N += int64(meta.Rows)
	return true
}

func (s *CountSink) Merge(other AggregateSink) error {
	o, ok := other.(*CountSink)
	if !ok {
		return fmt.Errorf("CountSink.Merge: got %T", other)
	}
	s.N += o.N
	return nil
}

// CountNonNullSink

func (s *CountNonNullSink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	col, ok := columnMetaByName(meta, s.Column)
	if !ok {
		return false
	}
	s.N += int64(col.Rows - col.NullCount)
	return true
}

func (s *CountNonNullSink) Merge(other AggregateSink) error {
	o, ok := other.(*CountNonNullSink)
	if !ok {
		return fmt.Errorf("CountNonNullSink.Merge: got %T", other)
	}
	s.N += o.N
	return nil
}

// SumInt64Sink

func (s *SumInt64Sink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	col, num, ok := numericColumnMeta(meta, s.Column)
	if !ok || !num.sumValid {
		return false
	}
	next, ok := AddInt64(s.Sum, num.sum)
	if !ok {
		return false
	}
	s.Sum = next
	s.Count += int64(col.Rows - col.NullCount)
	return true
}

func (s *SumInt64Sink) Merge(other AggregateSink) error {
	o, ok := other.(*SumInt64Sink)
	if !ok {
		return fmt.Errorf("SumInt64Sink.Merge: got %T", other)
	}
	next, ok := AddInt64(s.Sum, o.Sum)
	if !ok {
		return ErrSumOverflow
	}
	s.Sum = next
	s.Count += o.Count
	return nil
}

// SumInt32Sink

func (s *SumInt32Sink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	col, num, ok := numericColumnMeta(meta, s.Column)
	if !ok || !num.sumValid {
		return false
	}
	next, ok := AddInt64(s.Sum, num.sum)
	if !ok {
		return false
	}
	s.Sum = next
	s.Count += int64(col.Rows - col.NullCount)
	return true
}

func (s *SumInt32Sink) Merge(other AggregateSink) error {
	o, ok := other.(*SumInt32Sink)
	if !ok {
		return fmt.Errorf("SumInt32Sink.Merge: got %T", other)
	}
	next, ok := AddInt64(s.Sum, o.Sum)
	if !ok {
		return ErrSumOverflow
	}
	s.Sum = next
	s.Count += o.Count
	return nil
}

// MinInt64Sink

func (s *MinInt64Sink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	_, num, ok := numericColumnMeta(meta, s.Column)
	if !ok {
		return false
	}
	if !num.set {
		// All-null page: nothing to fold but we can still answer from metadata.
		return true
	}
	if !s.Set || num.min < s.Value {
		s.Value = num.min
		s.Set = true
	}
	return true
}

func (s *MinInt64Sink) Merge(other AggregateSink) error {
	o, ok := other.(*MinInt64Sink)
	if !ok {
		return fmt.Errorf("MinInt64Sink.Merge: got %T", other)
	}
	if !o.Set {
		return nil
	}
	if !s.Set || o.Value < s.Value {
		s.Value = o.Value
		s.Set = true
	}
	return nil
}

// MaxInt64Sink

func (s *MaxInt64Sink) TryAbsorbSegment(meta storage.SegmentMeta) bool {
	_, num, ok := numericColumnMeta(meta, s.Column)
	if !ok {
		return false
	}
	if !num.set {
		return true
	}
	if !s.Set || num.max > s.Value {
		s.Value = num.max
		s.Set = true
	}
	return true
}

func (s *MaxInt64Sink) Merge(other AggregateSink) error {
	o, ok := other.(*MaxInt64Sink)
	if !ok {
		return fmt.Errorf("MaxInt64Sink.Merge: got %T", other)
	}
	if !o.Set {
		return nil
	}
	if !s.Set || o.Value > s.Value {
		s.Value = o.Value
		s.Set = true
	}
	return nil
}
