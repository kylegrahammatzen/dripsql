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
	manifestVersion = 2
	manifestFile    = "manifest.json"
	dataFile        = "table.dripdata"
)

// Column describes one table column's physical storage type.
type Column struct {
	Name string
	Kind vector.Kind
}

// Segment describes one immutable segment recorded in a table manifest.
type Segment struct {
	ID     uint64               `json:"id"`
	Offset int64                `json:"offset"`
	Rows   int                  `json:"rows"`
	Bytes  int64                `json:"bytes"`
	Stats  storage.SegmentStats `json:"stats"`
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

// Appender batches segment writes and persists the manifest once on Close.
// It is not safe for concurrent use.
type Appender struct {
	table         *Table
	file          *os.File
	dataPath      string
	offset        int64
	initialOffset int64
	initialNext   uint64
	initialLen    int
	dirty         bool
	closed        bool
}

// Scanner reuses scratch buffers across table scans. It is not safe for concurrent use.
type Scanner struct {
	table *Table
	file  *os.File
	buf   []byte
	stats ScanStats
}

// seekReaderAt avoids Windows os.File.ReadAt allocations because Scanner owns the file offset.
type seekReaderAt struct {
	file *os.File
}

// ScanStats describes the amount of table data considered and read by the last scan.
type ScanStats struct {
	SegmentsTotal   int
	SegmentsScanned int
	SegmentsSkipped int
	RowsTotal       int64
	RowsScanned     int64
	RowsSkipped     int64
	BytesTotal      int64
	BytesScanned    int64
	BytesSkipped    int64
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

// NewScanner creates a reusable scanner for repeated table scans.
func (t *Table) NewScanner() *Scanner {
	return &Scanner{table: t}
}

// Append writes batch as one immutable table segment and records it in the manifest.
func (t *Table) Append(batch vector.Batch) error {
	appender, err := t.NewAppender()
	if err != nil {
		return err
	}
	if err := appender.Append(batch); err != nil {
		_ = appender.Close()
		return err
	}
	return appender.Close()
}

// NewAppender opens a bulk append session. Call Close to persist the manifest.
func (t *Table) NewAppender() (*Appender, error) {
	dataPath := filepath.Join(t.dir, dataFile)
	file, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	offset := t.dataEnd()
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() < offset {
		_ = file.Close()
		return nil, fmt.Errorf("table data file is %d bytes, manifest requires at least %d", info.Size(), offset)
	}
	if info.Size() > offset {
		if err := file.Truncate(offset); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &Appender{
		table:         t,
		file:          file,
		dataPath:      dataPath,
		offset:        offset,
		initialOffset: offset,
		initialNext:   t.manifest.NextSegmentID,
		initialLen:    len(t.manifest.Segments),
	}, nil
}

func (t *Table) dataEnd() int64 {
	var offset int64
	for _, segment := range t.manifest.Segments {
		end := segment.Offset + segment.Bytes
		if end > offset {
			offset = end
		}
	}
	return offset
}

// Append writes batch as one immutable segment in this append session.
func (a *Appender) Append(batch vector.Batch) error {
	if a == nil || a.table == nil || a.file == nil || a.closed {
		return fmt.Errorf("table appender is not open")
	}
	t := a.table
	if err := t.validateBatch(batch); err != nil {
		return err
	}

	id := t.manifest.NextSegmentID

	var buf bytes.Buffer
	stats, err := storage.WriteSegment(&buf, batch)
	if err != nil {
		return err
	}
	if n, err := a.file.Write(buf.Bytes()); err != nil {
		_ = a.file.Truncate(a.offset)
		_, _ = a.file.Seek(a.offset, io.SeekStart)
		return err
	} else if n != buf.Len() {
		_ = a.file.Truncate(a.offset)
		_, _ = a.file.Seek(a.offset, io.SeekStart)
		return io.ErrShortWrite
	}

	entry := Segment{ID: id, Offset: a.offset, Rows: stats.Rows, Bytes: int64(buf.Len()), Stats: stats}
	t.manifest.NextSegmentID++
	t.manifest.Segments = append(t.manifest.Segments, entry)
	a.offset += int64(buf.Len())
	a.dirty = true
	return nil
}

// Close persists the manifest and closes the append session.
func (a *Appender) Close() error {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	t := a.table
	file := a.file
	a.file = nil
	if file == nil {
		return nil
	}
	if t == nil {
		return file.Close()
	}

	if err := file.Close(); err != nil {
		if rollbackErr := a.rollback(); rollbackErr != nil {
			return fmt.Errorf("close data file: %w; rollback data file: %v", err, rollbackErr)
		}
		return err
	}
	if !a.dirty {
		return nil
	}

	if err := t.writeManifest(); err != nil {
		if rollbackErr := a.rollback(); rollbackErr != nil {
			return fmt.Errorf("write manifest: %w; rollback data file: %v", err, rollbackErr)
		}
		return err
	}
	return nil
}

func (a *Appender) rollback() error {
	if a == nil || a.table == nil {
		return nil
	}
	t := a.table
	if a.initialLen <= len(t.manifest.Segments) {
		t.manifest.NextSegmentID = a.initialNext
		t.manifest.Segments = t.manifest.Segments[:a.initialLen]
	}
	if a.dataPath == "" {
		return nil
	}
	return os.Truncate(a.dataPath, a.initialOffset)
}

// CountInt64Equal counts rows across all table segments where column equals value.
func (t *Table) CountInt64Equal(column string, value int64) (int, error) {
	scanner := t.NewScanner()
	defer scanner.Close()
	return scanner.CountInt64Equal(column, value)
}

// CountInt64Equal counts rows across all table segments where column equals value.
func (s *Scanner) CountInt64Equal(column string, value int64) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	t := s.table
	if err := t.requireColumnKind(column, vector.KindInt64); err != nil {
		return 0, err
	}
	s.beginScan()

	count := 0
	for _, segment := range t.manifest.Segments {
		stats, ok := segment.Stats.Column(column)
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		if stats.Kind != vector.KindInt64 {
			return 0, fmt.Errorf("segment %d column %q is %s, want int64", segment.ID, column, stats.Kind)
		}
		if canSkipInt64(stats, value) {
			s.markSkipped(segment)
			continue
		}

		file, err := s.dataFile()
		if err != nil {
			return 0, err
		}
		segmentCount, ok, buf, bytesRead, err := storage.CountSegmentInt64EqualAt(seekReaderAt{file: file}, segment.Offset, segment.Bytes, column, value, s.buf)
		s.buf = buf
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		s.markScanned(segment, bytesRead)
		count, err = checkedAddCount(count, segmentCount)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// CountStringEqual counts rows across all table segments where column equals value.
func (t *Table) CountStringEqual(column string, value string) (int, error) {
	scanner := t.NewScanner()
	defer scanner.Close()
	return scanner.CountStringEqual(column, value)
}

// CountStringEqual counts rows across all table segments where column equals value.
func (s *Scanner) CountStringEqual(column string, value string) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	t := s.table
	if err := t.requireColumnKind(column, vector.KindString); err != nil {
		return 0, err
	}
	s.beginScan()

	count := 0
	for _, segment := range t.manifest.Segments {
		stats, ok := segment.Stats.Column(column)
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		if stats.Kind != vector.KindString {
			return 0, fmt.Errorf("segment %d column %q is %s, want string", segment.ID, column, stats.Kind)
		}

		file, err := s.dataFile()
		if err != nil {
			return 0, err
		}
		segmentCount, ok, buf, bytesRead, err := storage.CountSegmentStringEqualAt(seekReaderAt{file: file}, segment.Offset, segment.Bytes, column, value, s.buf)
		s.buf = buf
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		s.markScanned(segment, bytesRead)
		count, err = checkedAddCount(count, segmentCount)
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

// GroupStringCounts returns per-value row counts for one string column.
func (t *Table) GroupStringCounts(column string) (map[string]int, error) {
	scanner := t.NewScanner()
	defer scanner.Close()
	return scanner.GroupStringCounts(column)
}

// GroupStringCounts returns per-value row counts for one string column.
func (s *Scanner) GroupStringCounts(column string) (map[string]int, error) {
	return s.GroupStringCountsInto(column, nil)
}

// GroupStringCountsInto adds per-value row counts for one string column into counts.
func (s *Scanner) GroupStringCountsInto(column string, counts map[string]int) (map[string]int, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if counts == nil {
		counts = make(map[string]int)
	}
	t := s.table
	if err := t.requireColumnKind(column, vector.KindString); err != nil {
		return nil, err
	}
	s.beginScan()
	for _, segment := range t.manifest.Segments {
		stats, ok := segment.Stats.Column(column)
		if !ok {
			return nil, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		if stats.Kind != vector.KindString {
			return nil, fmt.Errorf("segment %d column %q is %s, want string", segment.ID, column, stats.Kind)
		}

		file, err := s.dataFile()
		if err != nil {
			return nil, err
		}
		var buf []byte
		var bytesRead int64
		ok, buf, bytesRead, err = storage.GroupSegmentStringCountsAt(seekReaderAt{file: file}, segment.Offset, segment.Bytes, column, counts, s.buf)
		s.buf = buf
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("segment %d missing column %q", segment.ID, column)
		}
		s.markScanned(segment, bytesRead)
	}
	return counts, nil
}

// Reset releases scanner scratch buffers.
func (s *Scanner) Reset() {
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file = nil
	s.buf = nil
	s.stats = ScanStats{}
}

// Close releases scanner file handles and scratch buffers.
func (s *Scanner) Close() error {
	if s == nil {
		return nil
	}
	file := s.file
	s.file = nil
	s.buf = nil
	s.stats = ScanStats{}
	if file == nil {
		return nil
	}
	return file.Close()
}

// Stats returns statistics for the scanner's most recent scan.
func (s *Scanner) Stats() ScanStats {
	if s == nil {
		return ScanStats{}
	}
	return s.stats
}

func (s *Scanner) validate() error {
	if s == nil || s.table == nil {
		return fmt.Errorf("table scanner is not initialized")
	}
	return nil
}

func (s *Scanner) readSegment(segment Segment) ([]byte, error) {
	if segment.Bytes < 0 {
		return nil, fmt.Errorf("segment %d has negative byte length %d", segment.ID, segment.Bytes)
	}
	if uint64(segment.Bytes) > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("segment %d byte length %d overflows int", segment.ID, segment.Bytes)
	}
	size := int(segment.Bytes)
	if cap(s.buf) < size {
		s.buf = make([]byte, size)
	}
	s.buf = s.buf[:size]

	file, err := s.dataFile()
	if err != nil {
		return nil, err
	}
	if _, err := file.ReadAt(s.buf, segment.Offset); err != nil {
		return nil, err
	}
	return s.buf, nil
}

func (s *Scanner) dataFile() (*os.File, error) {
	if s.file != nil {
		return s.file, nil
	}
	file, err := os.Open(filepath.Join(s.table.dir, dataFile))
	if err != nil {
		return nil, err
	}
	s.file = file
	return file, nil
}

func (r seekReaderAt) ReadAt(buf []byte, offset int64) (int, error) {
	if _, err := r.file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	return io.ReadFull(r.file, buf)
}

func (s *Scanner) beginScan() {
	s.stats = ScanStats{SegmentsTotal: len(s.table.manifest.Segments)}
	for _, segment := range s.table.manifest.Segments {
		s.stats.RowsTotal += int64(segment.Rows)
		s.stats.BytesTotal += segment.Bytes
	}
}

func (s *Scanner) markScanned(segment Segment, bytesRead int64) {
	s.stats.SegmentsScanned++
	s.stats.RowsScanned += int64(segment.Rows)
	s.stats.BytesScanned += bytesRead
}

func (s *Scanner) markSkipped(segment Segment) {
	s.stats.SegmentsSkipped++
	s.stats.RowsSkipped += int64(segment.Rows)
	s.stats.BytesSkipped += segment.Bytes
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
