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

func (s *ColumnStorageStats) addSegment(stats storage.ColumnStats) {
	encoding := stats.Encoding
	if encoding.Codec == "" {
		encoding.Codec = "unknown"
	}
	s.Codec = mergeStatsLabel(s.Codec, encoding.Codec, s.Segments)
	s.FilterPath = mergeStatsLabel(s.FilterPath, encoding.FilterPath, s.Segments)
	s.GroupPath = mergeStatsLabel(s.GroupPath, encoding.GroupPath, s.Segments)
	s.Rows += int64(stats.Count)
	s.Segments++
	s.EncodedBytes += int64(stats.EncodedLen)
	s.PlainBytes += int64(encoding.PlainBytes)

	switch encoding.Codec {
	case "plain":
		s.PlainSegments++
	case "dictionary":
		s.addDictionarySegment(encoding)
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

func (s *ColumnStorageStats) addDictionarySegment(encoding storage.ColumnEncoding) {
	if s.DictionarySegments == 0 {
		s.MinDictionaryValues = encoding.DictionaryValues
		s.MaxDictionaryValues = encoding.DictionaryValues
	} else {
		if encoding.DictionaryValues < s.MinDictionaryValues {
			s.MinDictionaryValues = encoding.DictionaryValues
		}
		if encoding.DictionaryValues > s.MaxDictionaryValues {
			s.MaxDictionaryValues = encoding.DictionaryValues
		}
	}
	s.DictionarySegments++
	s.DictionaryValues += int64(encoding.DictionaryValues)

	if encoding.DictionaryPacked {
		if s.PackedIDSegments == 0 {
			s.MinPackedIDBitWidth = encoding.DictionaryPackedBitWidth
			s.MaxPackedIDBitWidth = encoding.DictionaryPackedBitWidth
		} else {
			if encoding.DictionaryPackedBitWidth < s.MinPackedIDBitWidth {
				s.MinPackedIDBitWidth = encoding.DictionaryPackedBitWidth
			}
			if encoding.DictionaryPackedBitWidth > s.MaxPackedIDBitWidth {
				s.MaxPackedIDBitWidth = encoding.DictionaryPackedBitWidth
			}
		}
		s.PackedIDSegments++
		return
	}

	if s.FixedIDSegments == 0 {
		s.MinDictionaryIDWidth = encoding.DictionaryIDWidth
		s.MaxDictionaryIDWidth = encoding.DictionaryIDWidth
	} else {
		if encoding.DictionaryIDWidth < s.MinDictionaryIDWidth {
			s.MinDictionaryIDWidth = encoding.DictionaryIDWidth
		}
		if encoding.DictionaryIDWidth > s.MaxDictionaryIDWidth {
			s.MaxDictionaryIDWidth = encoding.DictionaryIDWidth
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
