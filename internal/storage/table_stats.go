package storage

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

// TableStats summarizes the on-disk size of one table for the bench/EXPLAIN
// Storage section. All byte counts are int64 to match os.FileInfo.Size().
type TableStats struct {
	Table              string
	Rows               uint64
	Segments           int
	TableBytes         int64
	ColumnPayloadBytes int64
	StorageOverhead    int64
	PlainEstimate      int64
	Columns            []ColumnStats
}

// ColumnStats is one row of the per-column report. PlainEstimate is computed
// from the type and row count assuming an uncompressed layout; Stored is the
// real on-disk page bytes summed across segments.
type ColumnStats struct {
	Name           string
	Type           sqltype.Type
	Codec          Codec
	Rows           uint64
	NullCount      uint64
	Pages          int
	StoredBytes    int64
	PlainEstimate  int64
	HasMinMax      bool
	HasTextSummary bool
}

// TableStats walks every segment in the table, sums on-disk file sizes, sums
// per-column page payload bytes, and folds in a per-type plain-estimate so a
// compression ratio can be computed. The buffered ingest layer is excluded
// because it's not on disk yet; callers that want the fully-published view
// should publish or wait for the buffer to drain first.
func (s *Store) TableStats(ctx context.Context, table catalog.TableDef) (TableStats, error) {
	if err := ctx.Err(); err != nil {
		return TableStats{}, err
	}
	if err := s.beginOp(); err != nil {
		return TableStats{}, err
	}
	defer s.endOp()
	state, err := s.ensureTableState(table)
	if err != nil {
		return TableStats{}, err
	}
	state.mu.RLock()
	dir := state.dir
	segments := slices.Clone(state.segments)
	state.mu.RUnlock()

	stats := TableStats{Table: table.Name, Segments: len(segments)}
	stats.Columns = make([]ColumnStats, len(table.Columns))
	for i, col := range table.Columns {
		stats.Columns[i] = ColumnStats{Name: col.Name, Type: col.Type}
	}
	colByID := make(map[catalog.ColumnID]int, len(table.Columns))
	for i, col := range table.Columns {
		colByID[col.ID] = i
	}

	for _, seg := range segments {
		if err := ctx.Err(); err != nil {
			return TableStats{}, err
		}
		info, err := os.Stat(seg.AbsPath(dir))
		if err != nil {
			return TableStats{}, fmt.Errorf("segment %d stat: %w", seg.ID, err)
		}
		stats.TableBytes += info.Size()
		stats.Rows += uint64(seg.Rows)

		for _, colMeta := range seg.Columns {
			idx, ok := colByID[colMeta.ColumnID]
			if !ok {
				continue
			}
			cs := &stats.Columns[idx]
			cs.Codec = colMeta.Codec
			cs.Rows += uint64(colMeta.Rows)
			cs.NullCount += uint64(colMeta.NullCount)
			cs.Pages += len(colMeta.Pages)
			for _, page := range colMeta.Pages {
				cs.StoredBytes += int64(page.Length)
			}
			if colMeta.Int32 != nil || colMeta.Int64 != nil {
				cs.HasMinMax = true
			}
			if colMeta.Text != nil {
				cs.HasTextSummary = true
			}
		}
	}

	for i := range stats.Columns {
		cs := &stats.Columns[i]
		cs.PlainEstimate = plainEstimate(cs.Type, cs.Rows)
		stats.ColumnPayloadBytes += cs.StoredBytes
		stats.PlainEstimate += cs.PlainEstimate
	}
	stats.StorageOverhead = stats.TableBytes - stats.ColumnPayloadBytes
	if stats.StorageOverhead < 0 {
		stats.StorageOverhead = 0
	}
	return stats, nil
}

// plainEstimate returns a deterministic uncompressed-size estimate for one
// column. For variable-length types (text, bytes) it assumes a 16-byte
// average payload + 4-byte offset; this is rough but stable across runs.
func plainEstimate(t sqltype.Type, rows uint64) int64 {
	switch t.Kind {
	case sqltype.KindBool:
		// 1 byte per row (we don't currently bitpack bool storage).
		return int64(rows)
	case sqltype.KindInt16:
		return int64(rows) * 2
	case sqltype.KindInt32, sqltype.KindFloat32, sqltype.KindDate, sqltype.KindNamed:
		return int64(rows) * 4
	case sqltype.KindInt64, sqltype.KindFloat64, sqltype.KindTimestamp, sqltype.KindTime:
		return int64(rows) * 8
	case sqltype.KindUUID:
		return int64(rows) * 16
	case sqltype.KindText, sqltype.KindBytes, sqltype.KindJSON:
		return int64(rows) * 20
	}
	return int64(rows) * 8
}
