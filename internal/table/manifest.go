package table

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type manifest struct {
	Version       int              `json:"version"`
	Schema        []manifestColumn `json:"schema"`
	NextSegmentID uint64           `json:"next_segment_id"`
	Segments      []Segment        `json:"segments"`
}

type manifestColumn struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// Open loads an existing table manifest from dir.
func Open(dir string) (*Table, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestFile))
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	return &Table{dir: dir, manifest: m}, nil
}

func (t *Table) validateBatch(batch vector.Batch) error {
	if batch.HasSelection() {
		return fmt.Errorf("table append does not support selected batches")
	}
	schema := t.manifest.Schema
	if len(batch.Columns) != len(schema) {
		return fmt.Errorf("batch column count %d does not match table column count %d", len(batch.Columns), len(schema))
	}
	for i, want := range schema {
		got := batch.Columns[i]
		if got.Name != want.Name {
			return fmt.Errorf("batch column %d is %q, want %q", i, got.Name, want.Name)
		}
		if got.Vector == nil {
			return fmt.Errorf("batch column %q has no vector", got.Name)
		}
		wantKind, ok := parseKind(want.Kind)
		if !ok {
			return fmt.Errorf("column %q has unsupported kind %q", want.Name, want.Kind)
		}
		if got.Vector.Kind() != wantKind {
			return fmt.Errorf("batch column %q is %s, want %s", got.Name, got.Vector.Kind(), wantKind)
		}
	}
	return nil
}

func (t *Table) requireColumnKind(column string, kind vector.Kind) error {
	for _, col := range t.manifest.Schema {
		if col.Name != column {
			continue
		}
		got, ok := parseKind(col.Kind)
		if !ok {
			return fmt.Errorf("column %q has unsupported kind %q", col.Name, col.Kind)
		}
		if got != kind {
			return fmt.Errorf("column %q is %s, want %s", column, got, kind)
		}
		return nil
	}
	return fmt.Errorf("missing column %q", column)
}

func (t *Table) writeManifest() error {
	data, err := json.MarshalIndent(t.manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmpPath := filepath.Join(t.dir, manifestFile+".tmp")
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, filepath.Join(t.dir, manifestFile))
}

func validateManifest(m manifest) error {
	if m.Version < minSupportedManifestVersion || m.Version > manifestVersion {
		return fmt.Errorf("unsupported table manifest version %d", m.Version)
	}
	if _, err := decodeSchema(m.Schema); err != nil {
		return err
	}
	if m.NextSegmentID == 0 {
		return fmt.Errorf("next segment id is required")
	}
	for _, segment := range m.Segments {
		if segment.ID == 0 {
			return fmt.Errorf("segment id is required")
		}
		if segment.Offset < 0 {
			return fmt.Errorf("segment %d has negative offset %d", segment.ID, segment.Offset)
		}
		if segment.Rows < 0 {
			return fmt.Errorf("segment %d has negative row count %d", segment.ID, segment.Rows)
		}
		if segment.Bytes < 0 {
			return fmt.Errorf("segment %d has negative byte length %d", segment.ID, segment.Bytes)
		}
	}
	return nil
}

func encodeSchema(schema []Column) ([]manifestColumn, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("table schema requires at least one column")
	}
	out := make([]manifestColumn, 0, len(schema))
	for i, col := range schema {
		if col.Name == "" {
			return nil, fmt.Errorf("column name is required")
		}
		for _, previous := range schema[:i] {
			if previous.Name == col.Name {
				return nil, fmt.Errorf("duplicate column %q", col.Name)
			}
		}
		kind, ok := kindName(col.Kind)
		if !ok {
			return nil, fmt.Errorf("unsupported column %q kind %s", col.Name, col.Kind)
		}
		out = append(out, manifestColumn{Name: col.Name, Kind: kind})
	}
	return out, nil
}

func decodeSchema(schema []manifestColumn) ([]Column, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("table schema requires at least one column")
	}
	out := make([]Column, 0, len(schema))
	for i, col := range schema {
		if col.Name == "" {
			return nil, fmt.Errorf("column name is required")
		}
		for _, previous := range schema[:i] {
			if previous.Name == col.Name {
				return nil, fmt.Errorf("duplicate column %q", col.Name)
			}
		}
		kind, ok := parseKind(col.Kind)
		if !ok {
			return nil, fmt.Errorf("unsupported column %q kind %q", col.Name, col.Kind)
		}
		out = append(out, Column{Name: col.Name, Kind: kind})
	}
	return out, nil
}

func kindName(kind vector.Kind) (string, bool) {
	switch kind {
	case vector.KindInt64:
		return "int64", true
	case vector.KindString:
		return "string", true
	default:
		return "", false
	}
}

func parseKind(kind string) (vector.Kind, bool) {
	switch kind {
	case "int64":
		return vector.KindInt64, true
	case "string":
		return vector.KindString, true
	default:
		return 0, false
	}
}
