package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (s *Store) CountNonNull(ctx context.Context, table catalog.TableDef, colID catalog.ColumnID, pred Predicate, scratch *QueryScratch) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.beginOp(); err != nil {
		return 0, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return 0, err
	}

	if err := waitBufferIdle(state); err != nil {
		return 0, err
	}
	_, colIndex, err := requireColumn(state, table, colID, "count")
	if err != nil {
		return 0, err
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return 0, err
	}

	stats := scratchStats(scratch)
	count, err := countNonNullFromBuffer(state.buffer, colIndex, predIndexes, pred)
	if err != nil {
		state.bufferMu.Unlock()
		return 0, err
	}
	if stats != nil {
		stats.RowsMatched += count
	}
	dir, segments := snapshotSegments(state)

	if stats != nil {
		if pred.Op == PredicateNone {
			stats.SetAccess(colID, AccessMetadataNonNullCount)
		} else {
			stats.SetAccess(colID, AccessCountedAfterRowFilter)
		}
	}

	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		colMeta, _, ok := columnMetaByID(segment, colID)
		if !ok {
			return 0, fmt.Errorf("segment %d missing count column ID %d", segment.ID, colID)
		}
		if pred.Op == PredicateNone {
			n := uint64(colMeta.Rows - colMeta.NullCount)
			count += n
			if stats != nil {
				stats.SegmentsTotal++
				stats.SegmentsCandidate++
				stats.RowsTotal += uint64(colMeta.Rows)
				stats.RowsMatched += n
				stats.MetadataAnswered++
			}
			continue
		}
		predMetas, err := predicateColumnMetas(segment, pred)
		if err != nil {
			return 0, err
		}
		n, err := s.countNonNullFromSegment(segment.AbsPath(dir), segment.ID, colMeta, predMetas, pred, stats)
		if err != nil {
			return 0, err
		}
		count += n
	}
	return count, nil
}

func countNonNullFromBuffer(buffer *IngestBuffer, colIndex int, predIndexes []int, pred Predicate) (uint64, error) {
	var count uint64
	err := forEachBufferBatch(buffer, func(batch vector.Batch, rows int) error {
		n, err := countNonNullFromBatch(batch, colIndex, predIndexes, pred, rows)
		if err != nil {
			return err
		}
		count += n
		return nil
	})
	return count, err
}

func countNonNullFromBatch(batch vector.Batch, colIndex int, predIndexes []int, pred Predicate, rows int) (uint64, error) {
	if colIndex >= len(batch.Columns) {
		return 0, fmt.Errorf("missing buffered count column index %d", colIndex)
	}
	if rows > batch.Len {
		return 0, fmt.Errorf("count buffer rows %d exceed batch len %d", rows, batch.Len)
	}
	col := batch.Columns[colIndex]
	predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
	if err != nil {
		return 0, err
	}
	if pred.Op != PredicateNone && len(predCols) == 0 {
		return 0, fmt.Errorf("missing buffered predicate column")
	}
	var count uint64
	for row := 0; row < rows; row++ {
		if pred.Op != PredicateNone {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return 0, err
			}
			if !matched {
				continue
			}
		}
		if vector.IsValid(col.V.Valid, row) {
			count++
		}
	}
	return count, nil
}

func (s *Store) countNonNullFromSegment(path string, segmentID SegmentID, colMeta ColumnMeta, predMetas []ColumnMeta, pred Predicate, stats *ExecStats) (uint64, error) {
	var count uint64
	err := s.walkSegmentPages(path, segmentID, colMeta, predMetas, stats, func(file *os.File, colPage PageMeta, pageIndex int) error {
		col, err := readColumnPageFromFile(file, colMeta, colPage)
		if err != nil {
			return err
		}
		predCols, err := readPredicateColumns(file, predMetas, pageIndex)
		if err != nil {
			return err
		}
		for _, predCol := range predCols {
			if col.V.Len != predCol.V.Len {
				return fmt.Errorf("segment %d count page row mismatch", segmentID)
			}
		}
		var pageMatched uint64
		for row := 0; row < col.V.Len; row++ {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if matched && vector.IsValid(col.V.Valid, row) {
				pageMatched++
			}
		}
		count += pageMatched
		if stats != nil {
			stats.RowsMatched += pageMatched
		}
		return nil
	})
	return count, err
}
