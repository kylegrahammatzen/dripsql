package table

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// ColumnStorageStats aggregates physical codec statistics for one table column.
type ColumnStorageStats struct {
	Name                 string
	Kind                 vector.Kind
	Rows                 int64
	Segments             int
	EncodedBytes         int64
	PlainBytes           int64
	Codec                string
	PlainSegments        int
	DictionarySegments   int
	DictionaryValues     int64
	MinDictionaryValues  int
	MaxDictionaryValues  int
	FixedIDSegments      int
	PackedIDSegments     int
	MinDictionaryIDWidth int
	MaxDictionaryIDWidth int
	MinPackedIDBitWidth  int
	MaxPackedIDBitWidth  int
	MinMaxSegments       int
	MinInt64             int64
	MaxInt64             int64
	BloomSegments        int
	BloomBytes           int64
	FilterPath           string
	GroupPath            string
}

// CompressionRatio returns plain payload bytes divided by encoded payload bytes.
func (s ColumnStorageStats) CompressionRatio() float64 {
	if s.EncodedBytes == 0 {
		return 0
	}
	return float64(s.PlainBytes) / float64(s.EncodedBytes)
}

// AverageDictionaryValues returns the average dictionary cardinality across dictionary-encoded segments.
func (s ColumnStorageStats) AverageDictionaryValues() float64 {
	if s.DictionarySegments == 0 {
		return 0
	}
	return float64(s.DictionaryValues) / float64(s.DictionarySegments)
}

// ColumnStorageStats returns per-column physical codec statistics aggregated from segment metadata.
func (t *Table) ColumnStorageStats() ([]ColumnStorageStats, error) {
	schema := t.Schema()
	if len(schema) == 0 {
		return nil, fmt.Errorf("table schema is not available")
	}
	stats := make([]ColumnStorageStats, len(schema))
	for i, col := range schema {
		stats[i] = ColumnStorageStats{Name: col.Name, Kind: col.Kind, Codec: "none"}
	}

	for _, segment := range t.manifest.Segments {
		for i, col := range schema {
			colStats, ok := segment.Stats.Column(col.Name)
			if !ok {
				return nil, errMissingSegmentColumn(segment, col.Name)
			}
			if colStats.Kind != col.Kind {
				return nil, errWrongSegmentColumnKind(segment, col.Name, colStats.Kind, col.Kind.String())
			}
			stats[i].addSegment(colStats)
		}
	}
	return stats, nil
}

func (s *ColumnStorageStats) addSegment(stats storage.Column) {
	codec := stats.Codec.String()
	if codec == "codec(0)" {
		codec = "unknown"
	}
	s.Codec = mergeStatsLabel(s.Codec, codec, s.Segments)
	s.FilterPath = mergeStatsLabel(s.FilterPath, stats.FilterPath, s.Segments)
	s.GroupPath = mergeStatsLabel(s.GroupPath, stats.GroupPath, s.Segments)
	s.Rows += int64(stats.Count)
	s.Segments++
	s.EncodedBytes += stats.Payload.Bytes
	s.PlainBytes += int64(stats.PlainBytes)
	if stats.Int64Bloom != nil {
		s.BloomSegments++
		s.BloomBytes += int64(len(stats.Int64Bloom.Data))
	}
	if stats.StringBloom != nil {
		s.BloomSegments++
		s.BloomBytes += int64(len(stats.StringBloom.Data))
	}
	if stats.Filters.Bytes > 0 {
		s.BloomSegments++
		s.BloomBytes += stats.Filters.Bytes
	}

	switch stats.Codec {
	case storage.CodecPlain:
		s.PlainSegments++
	case storage.CodecDictionary:
		s.addDictionarySegment(stats)
	}

	if stats.HasMinMax {
		if s.MinMaxSegments == 0 {
			s.MinInt64 = stats.MinInt64
			s.MaxInt64 = stats.MaxInt64
		} else {
			if stats.MinInt64 < s.MinInt64 {
				s.MinInt64 = stats.MinInt64
			}
			if stats.MaxInt64 > s.MaxInt64 {
				s.MaxInt64 = stats.MaxInt64
			}
		}
		s.MinMaxSegments++
	}
}

func (s *ColumnStorageStats) addDictionarySegment(stats storage.Column) {
	if s.DictionarySegments == 0 {
		s.MinDictionaryValues = stats.DictionaryValues
		s.MaxDictionaryValues = stats.DictionaryValues
	} else {
		if stats.DictionaryValues < s.MinDictionaryValues {
			s.MinDictionaryValues = stats.DictionaryValues
		}
		if stats.DictionaryValues > s.MaxDictionaryValues {
			s.MaxDictionaryValues = stats.DictionaryValues
		}
	}
	s.DictionarySegments++
	s.DictionaryValues += int64(stats.DictionaryValues)

	if stats.DictionaryPacked {
		if s.PackedIDSegments == 0 {
			s.MinPackedIDBitWidth = stats.DictionaryPackedBitWidth
			s.MaxPackedIDBitWidth = stats.DictionaryPackedBitWidth
		} else {
			if stats.DictionaryPackedBitWidth < s.MinPackedIDBitWidth {
				s.MinPackedIDBitWidth = stats.DictionaryPackedBitWidth
			}
			if stats.DictionaryPackedBitWidth > s.MaxPackedIDBitWidth {
				s.MaxPackedIDBitWidth = stats.DictionaryPackedBitWidth
			}
		}
		s.PackedIDSegments++
		return
	}

	if s.FixedIDSegments == 0 {
		s.MinDictionaryIDWidth = stats.DictionaryIDWidth
		s.MaxDictionaryIDWidth = stats.DictionaryIDWidth
	} else {
		if stats.DictionaryIDWidth < s.MinDictionaryIDWidth {
			s.MinDictionaryIDWidth = stats.DictionaryIDWidth
		}
		if stats.DictionaryIDWidth > s.MaxDictionaryIDWidth {
			s.MaxDictionaryIDWidth = stats.DictionaryIDWidth
		}
	}
	s.FixedIDSegments++
}

func mergeStatsLabel(current string, next string, segments int) string {
	if next == "" {
		next = "-"
	}
	if segments == 0 || current == "" || current == "none" {
		return next
	}
	if current == next {
		return current
	}
	return "mixed"
}
