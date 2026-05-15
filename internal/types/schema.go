// TypeSpec, TableSpec, ColumnSpec, and TableOptions are catalog-level shapes.
// Validate methods catch malformed specs before the binder/storage layers see them.
package types

import (
	"fmt"
	"strings"
)

type TypeSpec struct {
	Name        string
	IfNotExists bool
	EnumLabels  []string
}

type TableSpec struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnSpec
	Options     TableOptions
}

type ColumnSpec struct {
	Name     string
	Type     Type
	Nullable bool
}

type TableOptions struct {
	Storage     StorageKind
	Profile     TableProfile
	SegmentRows SegmentRowsOption
	SortBy      []string
	Compression CompressionPolicy
	TimeColumn  string
}

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

func (s TypeSpec) Validate() error {
	if err := requireTrimmedName("type", s.Name); err != nil {
		return err
	}
	if len(s.EnumLabels) == 0 {
		return fmt.Errorf("type %q requires at least one enum label", s.Name)
	}
	seen := make(map[string]struct{}, len(s.EnumLabels))
	for _, label := range s.EnumLabels {
		if label == "" {
			return fmt.Errorf("type %q has empty enum label", s.Name)
		}
		if _, ok := seen[label]; ok {
			return fmt.Errorf("type %q has duplicate enum label %q", s.Name, label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func (s TableSpec) Validate() error {
	if err := requireTrimmedName("table", s.Name); err != nil {
		return err
	}
	if len(s.Columns) == 0 {
		return fmt.Errorf("table %q requires at least one column", s.Name)
	}
	if err := s.Options.Validate(); err != nil {
		return fmt.Errorf("table %q options: %w", s.Name, err)
	}
	seen := make(map[string]struct{}, len(s.Columns))
	for _, col := range s.Columns {
		if err := col.Validate(); err != nil {
			return fmt.Errorf("table %q: %w", s.Name, err)
		}
		name := NormalizeName(col.Name)
		if _, ok := seen[name]; ok {
			return fmt.Errorf("table %q has duplicate column %q", s.Name, col.Name)
		}
		seen[name] = struct{}{}
	}
	for _, name := range s.Options.SortBy {
		if _, ok := seen[NormalizeName(name)]; !ok {
			return fmt.Errorf("table %q sort_by references missing column %q", s.Name, name)
		}
	}
	if s.Options.TimeColumn != "" {
		if _, ok := seen[NormalizeName(s.Options.TimeColumn)]; !ok {
			return fmt.Errorf("table %q time_column references missing column %q", s.Name, s.Options.TimeColumn)
		}
	}
	return nil
}

func (s ColumnSpec) Validate() error {
	if err := requireTrimmedName("column", s.Name); err != nil {
		return err
	}
	if !s.Type.Valid() {
		return fmt.Errorf("column %q has invalid type %s", s.Name, s.Type)
	}
	return nil
}

func (o TableOptions) Validate() error {
	if o.Storage > StorageHybrid {
		return fmt.Errorf("invalid storage %s", o.Storage)
	}
	if o.Profile > ProfileLogAnalytics {
		return fmt.Errorf("invalid profile %s", o.Profile)
	}
	if err := o.SegmentRows.Validate(); err != nil {
		return err
	}
	if o.Compression > CompressionBest {
		return fmt.Errorf("invalid compression %s", o.Compression)
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
	}
	return fmt.Sprintf("storage(%d)", k)
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
	}
	return fmt.Sprintf("profile(%d)", p)
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
	}
	return fmt.Sprintf("compression(%d)", p)
}

func (p CompressionPolicy) AllowsFlate() bool {
	return p != CompressionNone
}

func (p CompressionPolicy) AllowsZstd() bool {
	return p != CompressionNone && p != CompressionFast
}

func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func requireTrimmedName(kind, name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return fmt.Errorf("%s name is required", kind)
	}
	if trimmed != name {
		return fmt.Errorf("%s name %q has leading or trailing whitespace", kind, name)
	}
	return nil
}
