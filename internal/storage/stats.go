package storage

import "github.com/kylegrahammatzen/dripsql/internal/types"

const (
	DefaultPageRows     = types.StandardBatchRows
	DefaultSegmentRows  = 64 * types.StandardBatchRows
	TextStatsMaxValues  = 64
	ValueStatsMaxValues = 64
)

type Int32Stats struct {
	Min      int32
	Max      int32
	Sum      int64
	SumValid bool
}

type Int64Stats struct {
	Min      int64
	Max      int64
	Sum      int64
	SumValid bool
}

type BoolStats struct {
	HasTrue  bool
	HasFalse bool
}

type Int32ValueStats struct {
	Values    []int32
	Truncated bool
}

type Int64ValueStats struct {
	Values    []int64
	Truncated bool
}

type TextStats struct {
	Values    []string
	Counts    []uint32
	Hashes    []uint16
	Truncated bool
}

type ExecStats struct {
	SegmentsTotal     int64
	SegmentsCandidate int64
	PagesTotal        int64
	PagesCandidate    int64
	RowsTotal         int64
	RowsCandidate     int64
	RowsMatched       int64
	PayloadBytesRead  int64
}

func (s *ExecStats) ObserveSegment(candidate bool) {
	if s == nil {
		return
	}
	s.SegmentsTotal++
	if candidate {
		s.SegmentsCandidate++
	}
}

func (s *ExecStats) ObservePage(rows int, payloadBytes int, matched int, candidate bool) {
	if s == nil {
		return
	}
	s.PagesTotal++
	s.RowsTotal += int64(rows)
	if candidate {
		s.PagesCandidate++
		s.RowsCandidate += int64(rows)
	}
	s.RowsMatched += int64(matched)
	s.PayloadBytesRead += int64(payloadBytes)
}
