// Package table contains durable table layout and multi-segment scan primitives.
package table

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	manifestVersion = 2
	manifestFile    = "manifest.json"
	dataFile        = "table.dripdata"
)

// Column describes one table column's physical storage type.
type Column struct {
	Name string
	Kind vector.Kind
}

// ColumnRange describes one encoded column payload range inside a segment.
type ColumnRange = storage.ColumnPayloadRange

// Segment describes one immutable segment recorded in a table manifest.
type Segment struct {
	ID      uint64               `json:"id"`
	Offset  int64                `json:"offset"`
	Rows    int                  `json:"rows"`
	Bytes   int64                `json:"bytes"`
	Stats   storage.SegmentStats `json:"stats"`
	Columns []ColumnRange        `json:"columns,omitempty"`
}

// Table is a directory-backed collection of immutable columnar segments.
type Table struct {
	dir      string
	manifest manifest
}

// Create initializes a new table directory with a manifest and data file.
func Create(dir string, schema []Column) (*Table, error) {
	manifestSchema, err := encodeSchema(schema)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, manifestFile)); err == nil {
		return nil, fmt.Errorf("table manifest already exists: %s", filepath.Join(dir, manifestFile))
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, dataFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}

	t := &Table{
		dir: dir,
		manifest: manifest{
			Version:       manifestVersion,
			Schema:        manifestSchema,
			NextSegmentID: 1,
		},
	}
	if err := t.writeManifest(); err != nil {
		_ = os.Remove(filepath.Join(dir, dataFile))
		return nil, err
	}
	return t, nil
}

// Rows returns the total row count recorded in the manifest.
func (t *Table) Rows() int {
	rows := 0
	for _, segment := range t.manifest.Segments {
		rows += segment.Rows
	}
	return rows
}

// Bytes returns the total encoded segment bytes recorded in the manifest.
func (t *Table) Bytes() int64 {
	bytes := int64(0)
	for _, segment := range t.manifest.Segments {
		bytes += segment.Bytes
	}
	return bytes
}

// Segments returns the number of immutable segments recorded in the manifest.
func (t *Table) Segments() int {
	return len(t.manifest.Segments)
}

// Schema returns the table schema in storage order.
func (t *Table) Schema() []Column {
	schema, err := decodeSchema(t.manifest.Schema)
	if err != nil {
		return nil
	}
	return schema
}

// NewScanner creates a reusable scanner for repeated table scans.
func (t *Table) NewScanner() *Scanner {
	return &Scanner{table: t}
}
