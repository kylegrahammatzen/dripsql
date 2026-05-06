// Package storage contains DripSQL's immutable columnar segment format.
package storage

import (
	"fmt"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	segmentMagic    = "DRIPV2S1"
	footerMagic     = "DRIPV2F1"
	formatVersion   = uint16(1)
	maxInt          = int(^uint(0) >> 1)
	maxSegmentSize  = int64(^uint64(0) >> 1)
	maxColumnName   = 1<<16 - 1
	headerLen       = len(segmentMagic) + 2 + 8 + 4 + 8
	footerTailLen   = 8 + 4 + len(footerMagic)
	headerLen64     = int64(headerLen)
	footerTailLen64 = int64(footerTailLen)
	minSegmentSize  = headerLen64 + footerTailLen64
	defaultPageRows = 4096
)

// Codec identifies a column payload encoding.
type Codec uint8

const (
	CodecPlain Codec = iota + 1
	CodecDictionary
	CodecInt64Sequence
	CodecStringPrefix
	CodecStringTemplate
)

func (c Codec) String() string {
	switch c {
	case CodecPlain:
		return "plain"
	case CodecDictionary:
		return "dictionary"
	case CodecInt64Sequence:
		return "sequence"
	case CodecStringPrefix:
		return "prefix"
	case CodecStringTemplate:
		return "template"
	default:
		return fmt.Sprintf("codec(%d)", c)
	}
}

// Range identifies a segment-relative byte range.
type Range struct {
	Offset int64
	Bytes  int64
}

// End returns the exclusive end offset for the range.
func (r Range) End() (int64, error) {
	return checkedAddInt64("range end", r.Offset, r.Bytes)
}

// Column describes one physical column chunk in a segment.
type Column struct {
	Name                     string
	Kind                     vector.Kind
	Codec                    Codec
	Count                    int
	Payload                  Range
	Pages                    Range
	Filters                  Range
	Dictionary               Range
	HasMinMax                bool
	MinInt64                 int64
	MaxInt64                 int64
	PlainBytes               int
	DictionaryValues         int
	DictionaryIDWidth        int
	DictionaryPacked         bool
	DictionaryPackedBitWidth int
	FilterPath               string
	GroupPath                string
	Int64Bloom               *Int64BloomFilter  `json:",omitempty"`
	StringBloom              *StringBloomFilter `json:",omitempty"`
}

// Page describes one row range and its column-local byte ranges.
type Page struct {
	StartRow  int
	Count     int
	Payload   Range
	Values    Range
	HasMinMax bool
	MinInt64  int64
	MaxInt64  int64
}

// Directory is the parsed footer directory for a segment.
type Directory struct {
	Rows    int
	Columns []Column
}

// Column returns directory metadata for name.
func (d Directory) Column(name string) (Column, bool) {
	return columnByName(d.Columns, name)
}

// SegmentStats describes a written or opened segment.
type SegmentStats struct {
	Rows    int
	Columns []Column
}

// Column returns segment column stats by name.
func (s SegmentStats) Column(name string) (Column, bool) {
	return columnByName(s.Columns, name)
}

func columnByName(cols []Column, name string) (Column, bool) {
	for i := range cols {
		if cols[i].Name == name {
			return cols[i], true
		}
	}
	return Column{}, false
}

func cloneColumns(cols []Column) []Column {
	return slices.Clone(cols)
}

func checkedInt(label string, n uint64) (int, error) {
	if n > uint64(maxInt) {
		return 0, fmt.Errorf("%s %d overflows int", label, n)
	}
	return int(n), nil
}

func checkedAddInt64(label string, a, b int64) (int64, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("negative %s component", label)
	}
	if b > maxSegmentSize-a {
		return 0, fmt.Errorf("%s overflows int64", label)
	}
	return a + b, nil
}

func checkedMulInt(label string, a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("negative %s component", label)
	}
	if a != 0 && b > maxInt/a {
		return 0, fmt.Errorf("%s overflows int", label)
	}
	return a * b, nil
}

func checkedAddInt(label string, a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("negative %s component", label)
	}
	if b > maxInt-a {
		return 0, fmt.Errorf("%s overflows int", label)
	}
	return a + b, nil
}
