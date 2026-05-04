// Package storage contains DripSQL's durable columnar storage primitives.
package storage

import (
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	segmentMagic               = "DRIPSEG1"
	segmentVersion      uint16 = 1
	maxEncodedColumnLen uint64 = 1 << 30
)

// ColumnStats describes one encoded column in a segment.
type ColumnStats struct {
	Name       string
	Kind       vector.Kind
	Count      int
	HasMinMax  bool
	MinInt64   int64
	MaxInt64   int64
	EncodedLen int
}

// SegmentStats describes a written segment.
type SegmentStats struct {
	Rows    int
	Columns []ColumnStats
}

// Column returns segment column stats by name.
func (s SegmentStats) Column(name string) (ColumnStats, bool) {
	for _, col := range s.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnStats{}, false
}

// WriteSegment writes a small immutable columnar segment.
func WriteSegment(w io.Writer, batch vector.Batch) (SegmentStats, error) {
	sw := newSegmentWriter(w)
	if err := sw.WriteHeader(batch.Count, len(batch.Columns)); err != nil {
		return SegmentStats{}, err
	}

	stats := SegmentStats{Rows: batch.Count, Columns: make([]ColumnStats, 0, len(batch.Columns))}
	for _, col := range batch.Columns {
		colStats, err := sw.writeVectorColumn(col)
		if err != nil {
			return SegmentStats{}, err
		}
		stats.Columns = append(stats.Columns, colStats)
	}

	return stats, nil
}

// ReadSegment reads a segment written by WriteSegment.
func ReadSegment(r io.Reader) (vector.Batch, SegmentStats, error) {
	sr := newSegmentReader(r)
	rows, cols, err := sr.readHeaderChecked()
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}

	columns := make([]vector.Column, 0, cols)
	stats := SegmentStats{Rows: rows, Columns: make([]ColumnStats, 0, cols)}
	for range cols {
		col, colStats, err := sr.ReadColumn()
		if err != nil {
			return vector.Batch{}, SegmentStats{}, err
		}
		columns = append(columns, col)
		stats.Columns = append(stats.Columns, colStats)
	}

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}
	if batch.Count != rows {
		return vector.Batch{}, SegmentStats{}, fmt.Errorf("segment row count %d does not match decoded batch count %d", rows, batch.Count)
	}

	return batch, stats, nil
}

// ReadSegmentColumns reads only the requested columns from a segment.
func ReadSegmentColumns(r io.Reader, names ...string) (vector.Batch, SegmentStats, error) {
	wanted, err := requestedColumns(names)
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}

	sr := newSegmentReader(r)
	rows, cols, err := sr.readHeaderChecked()
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}

	columns := make([]vector.Column, 0, len(wanted))
	stats := SegmentStats{Rows: rows, Columns: make([]ColumnStats, 0, len(wanted))}
	for range cols {
		colStats, err := sr.readColumnHeader()
		if err != nil {
			return vector.Batch{}, SegmentStats{}, err
		}

		if _, ok := wanted[colStats.Name]; ok {
			col, err := sr.readColumnPayload(colStats)
			if err != nil {
				return vector.Batch{}, SegmentStats{}, err
			}
			columns = append(columns, col)
			stats.Columns = append(stats.Columns, colStats)
			delete(wanted, colStats.Name)
			continue
		}

		if err := sr.skip(colStats.EncodedLen); err != nil {
			return vector.Batch{}, SegmentStats{}, err
		}
	}
	if name, ok := missingRequestedColumn(names, wanted); ok {
		return vector.Batch{}, SegmentStats{}, fmt.Errorf("missing column %q", name)
	}

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		return vector.Batch{}, SegmentStats{}, err
	}
	if batch.Count != rows {
		return vector.Batch{}, SegmentStats{}, fmt.Errorf("segment row count %d does not match decoded batch count %d", rows, batch.Count)
	}

	return batch, stats, nil
}

// ReadSegmentStats reads segment metadata and skips encoded column payloads without decoding vectors.
func ReadSegmentStats(r io.Reader) (SegmentStats, error) {
	sr := newSegmentReader(r)
	rows, cols, err := sr.readHeaderChecked()
	if err != nil {
		return SegmentStats{}, err
	}

	stats := SegmentStats{Rows: rows, Columns: make([]ColumnStats, 0, cols)}
	for range cols {
		colStats, err := sr.ReadColumnStats()
		if err != nil {
			return SegmentStats{}, err
		}
		stats.Columns = append(stats.Columns, colStats)
	}

	return stats, nil
}

func missingRequestedColumn(names []string, wanted map[string]struct{}) (string, bool) {
	for _, name := range names {
		if _, ok := wanted[name]; ok {
			return name, true
		}
	}
	return "", false
}

func requestedColumns(names []string) (map[string]struct{}, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("at least one column is required")
	}

	wanted := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf("column name is required")
		}
		if _, ok := wanted[name]; ok {
			return nil, fmt.Errorf("duplicate requested column %q", name)
		}
		wanted[name] = struct{}{}
	}
	return wanted, nil
}
