package schema

import "fmt"

type StorageKind uint8

const (
	StorageDefault StorageKind = iota
	StorageColumnar
	StorageRow
	StorageHybrid
)

type TableProfile uint8

const (
	ProfileDefault TableProfile = iota
	ProfileEventAnalytics
	ProfileTimeSeries
	ProfileDimensionTable
	ProfileLogAnalytics
)

type CompressionPolicy uint8

const (
	CompressionDefault CompressionPolicy = iota
	CompressionAuto
	CompressionNone
	CompressionFast
	CompressionBest
)

type SegmentRowsOption struct {
	Auto bool
	Rows int
}

var AutoSegmentRows = SegmentRowsOption{Auto: true}

func SegmentRows(rows int) SegmentRowsOption {
	return SegmentRowsOption{Rows: rows}
}

func (o SegmentRowsOption) Validate() error {
	if o.Auto && o.Rows != 0 {
		return fmt.Errorf("segment_rows cannot be both auto and %d", o.Rows)
	}
	if o.Rows < 0 {
		return fmt.Errorf("segment_rows cannot be negative")
	}
	return nil
}

func (k StorageKind) String() string {
	switch k {
	case StorageDefault:
		return "default"
	case StorageColumnar:
		return "columnar"
	case StorageRow:
		return "row"
	case StorageHybrid:
		return "hybrid"
	default:
		return fmt.Sprintf("storage(%d)", k)
	}
}

func (p TableProfile) String() string {
	switch p {
	case ProfileDefault:
		return "default"
	case ProfileEventAnalytics:
		return "event_analytics"
	case ProfileTimeSeries:
		return "time_series"
	case ProfileDimensionTable:
		return "dimension_table"
	case ProfileLogAnalytics:
		return "log_analytics"
	default:
		return fmt.Sprintf("profile(%d)", p)
	}
}

func (p CompressionPolicy) String() string {
	switch p {
	case CompressionDefault:
		return "default"
	case CompressionAuto:
		return "auto"
	case CompressionNone:
		return "none"
	case CompressionFast:
		return "fast"
	case CompressionBest:
		return "best"
	default:
		return fmt.Sprintf("compression(%d)", p)
	}
}
