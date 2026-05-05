// Package table contains durable table layout and multi-segment scan primitives.
package table

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	manifestVersion = 1
	manifestFile    = "manifest.json"
	segmentsDir     = "segments"
	segmentExt      = ".dripseg"
)

// Column describes one table column's physical storage type.
type Column struct {
	Name string
	Kind vector.Kind
}

// Segment describes one immutable segment recorded in a table manifest.
type Segment struct {
	ID    uint64               `json:"id"`
	File  string               `json:"file"`
	Rows  int                  `json:"rows"`
	Bytes int64                `json:"bytes"`
	Stats storage.SegmentStats `json:"stats"`
}

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

// Table is a directory-backed collection of immutable columnar segments.
type Table struct {
	dir      string
	manifest manifest
}

// Create initializes a new table directory with a manifest and segment directory.
func Create(dir string, schema []Column) (*Table, error) {
	manifestSchema, err := encodeSchema(schema)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, manifestFile)); err == nil {
		return nil, fmt.Errorf("table manifest already exists: %s", filepath.Join(dir, manifestFile))
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, segmentsDir), 0o755); err != nil {
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
		return nil, err
	}
	return t, nil
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

// Schema returns the table schema.
func (t *Table) Schema() []Column {
	schema, err := decodeSchema(t.manifest.Schema)
	if err != nil {
		return nil
	}
	return schema
}

// Rows returns the total row count recorded in the manifest.
func (t *Table) Rows() int {
	rows := 0
	for _, segment := range t.manifest.Segments {
		rows += segment.Rows
	}
	return rows
}

// Segments returns the number of immutable segments recorded in the manifest.
func (t *Table) Segments() int {
	return len(t.manifest.Segments)
}

// Append writes batch as one immutable table segment and records it in the manifest.
func (t *Table) Append(batch vector.Batch) error {
	if err := t.validateBatch(batch); err != nil {
		return err
	}

	id := t.manifest.NextSegmentID
	file := segmentFile(id)
	path := filepath.Join(t.dir, filepath.FromSlash(file))

	var buf bytes.Buffer
	stats, err := storage.WriteSegment(&buf, batch)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	segmentFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if n, err := segmentFile.Write(buf.Bytes()); err != nil {
		_ = segmentFile.Close()
		_ = os.Remove(path)
		return err
	} else if n != buf.Len() {
		_ = segmentFile.Close()
		_ = os.Remove(path)
		return io.ErrShortWrite
	}
	if err := segmentFile.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}

	entry := Segment{ID: id, File: file, Rows: stats.Rows, Bytes: int64(buf.Len()), Stats: stats}
	oldNext := t.manifest.NextSegmentID
	oldLen := len(t.manifest.Segments)
	t.manifest.NextSegmentID++
	t.manifest.Segments = append(t.manifest.Segments, entry)
	if err := t.writeManifest(); err != nil {
		t.manifest.NextSegmentID = oldNext
		t.manifest.Segments = t.manifest.Segments[:oldLen]
		_ = os.Remove(path)
		return err
	}
	return nil
}

// CountInt64Equal counts rows across all table segments where column equals value.
func (t *Table) CountInt64Equal(column string, value int64) (int, error) {
	if err := t.requireColumnKind(column, vector.KindInt64); err != nil {
		return 0, err
	}

	count := 0
	var buf []byte
	for _, segment := range t.manifest.Segments {
		stats, ok := segment.Stats.Column(column)
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		if stats.Kind != vector.KindInt64 {
			return 0, fmt.Errorf("segment %d column %q is %s, want int64", segment.ID, column, stats.Kind)
		}
		if canSkipInt64(stats, value) {
			continue
		}

		data, err := t.readSegmentInto(segment, buf)
		if err != nil {
			return 0, err
		}
		buf = data
		segmentCount, ok, err := storage.CountSegmentInt64EqualBytes(data, column, value)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		count, err = checkedAddCount(count, segmentCount)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// CountStringEqual counts rows across all table segments where column equals value.
func (t *Table) CountStringEqual(column string, value string) (int, error) {
	if err := t.requireColumnKind(column, vector.KindString); err != nil {
		return 0, err
	}

	count := 0
	var buf []byte
	for _, segment := range t.manifest.Segments {
		stats, ok := segment.Stats.Column(column)
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		if stats.Kind != vector.KindString {
			return 0, fmt.Errorf("segment %d column %q is %s, want string", segment.ID, column, stats.Kind)
		}

		data, err := t.readSegmentInto(segment, buf)
		if err != nil {
			return 0, err
		}
		buf = data
		segmentCount, ok, err := storage.CountSegmentStringEqualBytes(data, column, value)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		count, err = checkedAddCount(count, segmentCount)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (t *Table) validateBatch(batch vector.Batch) error {
	schema, err := decodeSchema(t.manifest.Schema)
	if err != nil {
		return err
	}
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
		if got.Vector.Kind() != want.Kind {
			return fmt.Errorf("batch column %q is %s, want %s", got.Name, got.Vector.Kind(), want.Kind)
		}
	}
	return nil
}

func (t *Table) requireColumnKind(column string, kind vector.Kind) error {
	schema, err := decodeSchema(t.manifest.Schema)
	if err != nil {
		return err
	}
	for _, col := range schema {
		if col.Name != column {
			continue
		}
		if col.Kind != kind {
			return fmt.Errorf("column %q is %s, want %s", column, col.Kind, kind)
		}
		return nil
	}
	return fmt.Errorf("missing column %q", column)
}

func (t *Table) readSegmentInto(segment Segment, buf []byte) ([]byte, error) {
	if segment.Bytes < 0 {
		return nil, fmt.Errorf("segment %d has negative byte length %d", segment.ID, segment.Bytes)
	}
	if uint64(segment.Bytes) > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("segment %d byte length %d overflows int", segment.ID, segment.Bytes)
	}
	size := int(segment.Bytes)
	if cap(buf) < size {
		buf = make([]byte, size)
	}
	buf = buf[:size]

	file, err := os.Open(filepath.Join(t.dir, filepath.FromSlash(segment.File)))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := io.ReadFull(file, buf); err != nil {
		return nil, err
	}
	var extraBuf [1]byte
	if extra, err := file.Read(extraBuf[:]); err != nil && err != io.EOF {
		return nil, err
	} else if extra != 0 {
		return nil, fmt.Errorf("segment %d byte length exceeds manifest length %d", segment.ID, segment.Bytes)
	}
	return buf, nil
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
	if m.Version != manifestVersion {
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
		if segment.File == "" {
			return fmt.Errorf("segment %d file is required", segment.ID)
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
	seen := make(map[string]struct{}, len(schema))
	for _, col := range schema {
		if col.Name == "" {
			return nil, fmt.Errorf("column name is required")
		}
		if _, ok := seen[col.Name]; ok {
			return nil, fmt.Errorf("duplicate column %q", col.Name)
		}
		kind, ok := kindName(col.Kind)
		if !ok {
			return nil, fmt.Errorf("unsupported column %q kind %s", col.Name, col.Kind)
		}
		seen[col.Name] = struct{}{}
		out = append(out, manifestColumn{Name: col.Name, Kind: kind})
	}
	return out, nil
}

func decodeSchema(schema []manifestColumn) ([]Column, error) {
	if len(schema) == 0 {
		return nil, fmt.Errorf("table schema requires at least one column")
	}
	out := make([]Column, 0, len(schema))
	seen := make(map[string]struct{}, len(schema))
	for _, col := range schema {
		if col.Name == "" {
			return nil, fmt.Errorf("column name is required")
		}
		if _, ok := seen[col.Name]; ok {
			return nil, fmt.Errorf("duplicate column %q", col.Name)
		}
		kind, ok := parseKind(col.Kind)
		if !ok {
			return nil, fmt.Errorf("unsupported column %q kind %q", col.Name, col.Kind)
		}
		seen[col.Name] = struct{}{}
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

func segmentFile(id uint64) string {
	return filepath.ToSlash(filepath.Join(segmentsDir, fmt.Sprintf("%020d%s", id, segmentExt)))
}

func canSkipInt64(stats storage.ColumnStats, value int64) bool {
	return stats.HasMinMax && (value < stats.MinInt64 || value > stats.MaxInt64)
}

func checkedAddCount(a, b int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if b > maxInt-a {
		return 0, fmt.Errorf("row count overflows int")
	}
	return a + b, nil
}
