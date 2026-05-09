// Package schema contains typed DDL specs shared by SQL and Go APIs.
package schema

import (
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

// TypeSpec describes a catalog type definition.
type TypeSpec struct {
	Name        string
	IfNotExists bool
	EnumLabels  []string
}

// TableSpec describes a table definition before catalog IDs are assigned.
type TableSpec struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnSpec
	Options     TableOptions
}

// ColumnSpec describes one table column before catalog IDs are assigned.
type ColumnSpec struct {
	Name     string
	Type     sqltype.Type
	Nullable bool
}

// TableOptions describes DripSQL-owned physical table preferences.
type TableOptions struct {
	Storage     StorageKind
	Profile     TableProfile
	SegmentRows SegmentRowsOption
	SortBy      []string
	Compression CompressionPolicy
	TimeColumn  string
}

func (s TypeSpec) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("type name is required")
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
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("table name is required")
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
		name := normalizeName(col.Name)
		if _, ok := seen[name]; ok {
			return fmt.Errorf("table %q has duplicate column %q", s.Name, col.Name)
		}
		seen[name] = struct{}{}
	}
	for _, name := range s.Options.SortBy {
		norm := normalizeName(name)
		if _, ok := seen[norm]; !ok {
			return fmt.Errorf("table %q sort_by references missing column %q", s.Name, name)
		}
	}
	if s.Options.TimeColumn != "" {
		norm := normalizeName(s.Options.TimeColumn)
		if _, ok := seen[norm]; !ok {
			return fmt.Errorf("table %q time_column references missing column %q", s.Name, s.Options.TimeColumn)
		}
	}
	return nil
}

func (s ColumnSpec) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("column name is required")
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

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
