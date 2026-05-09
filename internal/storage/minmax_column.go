package storage

import (
	"context"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (s *Store) MinMaxInt(ctx context.Context, table catalog.TableDef, colID catalog.ColumnID, pred Predicate, max bool, scratch *QueryScratch) (int64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if err := s.beginOp(); err != nil {
		return 0, false, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return 0, false, err
	}

	if err := waitBufferIdle(state); err != nil {
		return 0, false, err
	}
	_, colIndex, err := requireIntColumn(state, table, colID, "aggregate")
	if err != nil {
		return 0, false, err
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return 0, false, err
	}

	stats := scratchStats(scratch)
	if stats != nil {
		stats.SetAccess(colID, AccessRawIntMinMaxLoop)
	}
	value, found, err := minMaxIntFromBuffer(state.buffer, colIndex, predIndexes, pred, max)
	if err != nil {
		state.bufferMu.Unlock()
		return 0, false, err
	}
	dir, segments := snapshotSegments(state)

	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		colMeta, _, ok := columnMetaByID(segment, colID)
		if !ok {
			return 0, false, fmt.Errorf("segment %d missing aggregate column ID %d", segment.ID, colID)
		}
		var n int64
		var nFound bool
		if pred.Op == PredicateNone {
			n, nFound, err = s.minMaxIntColumnFromSegment(segment.AbsPath(dir), segment.ID, colMeta, max, stats)
		} else {
			predMetas, err := predicateColumnMetas(segment, pred)
			if err != nil {
				return 0, false, err
			}
			n, nFound, err = s.minMaxIntFromSegment(segment.AbsPath(dir), segment.ID, colMeta, predMetas, pred, max, stats)
		}
		if err != nil {
			return 0, false, err
		}
		if nFound {
			value, found = mergeMinMax(value, found, n, max), true
		}
	}
	return value, found, nil
}

func minMaxIntFromBuffer(buffer *IngestBuffer, colIndex int, predIndexes []int, pred Predicate, max bool) (int64, bool, error) {
	var value int64
	found := false
	err := forEachBufferBatch(buffer, func(batch vector.Batch, rows int) error {
		n, ok, err := minMaxIntFromBatch(batch, colIndex, predIndexes, pred, rows, max)
		if err != nil {
			return err
		}
		if ok {
			value, found = mergeMinMax(value, found, n, max), true
		}
		return nil
	})
	return value, found, err
}

func minMaxIntFromBatch(batch vector.Batch, colIndex int, predIndexes []int, pred Predicate, rows int, max bool) (int64, bool, error) {
	if colIndex >= len(batch.Columns) {
		return 0, false, fmt.Errorf("missing buffered aggregate column index %d", colIndex)
	}
	if rows > batch.Len {
		return 0, false, fmt.Errorf("aggregate buffer rows %d exceed batch len %d", rows, batch.Len)
	}
	col := batch.Columns[colIndex]
	predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
	if err != nil {
		return 0, false, err
	}
	var value int64
	found := false
	for row := 0; row < rows; row++ {
		if pred.Op != PredicateNone {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return 0, false, err
			}
			if !matched {
				continue
			}
		}
		if !vector.IsValid(col.V.Valid, row) {
			continue
		}
		n, err := intValueAt(col, row)
		if err != nil {
			return 0, false, err
		}
		value, found = mergeMinMax(value, found, n, max), true
	}
	return value, found, nil
}

func (s *Store) minMaxIntColumnFromSegment(path string, segmentID SegmentID, colMeta ColumnMeta, max bool, stats *ExecStats) (int64, bool, error) {
	var value int64
	found := false
	err := s.walkSegmentPages(path, segmentID, colMeta, nil, stats, func(file *os.File, colPage PageMeta, _ int) error {
		col, err := readColumnPageFromFile(file, colMeta, colPage)
		if err != nil {
			return err
		}
		n, ok, err := minMaxIntColumn(col, max)
		if err != nil {
			return err
		}
		if ok {
			value, found = mergeMinMax(value, found, n, max), true
		}
		if stats != nil {
			stats.RowsMatched += uint64(colPage.Rows - colPage.NullCount)
		}
		return nil
	})
	return value, found, err
}

func (s *Store) minMaxIntFromSegment(path string, segmentID SegmentID, colMeta ColumnMeta, predMetas []ColumnMeta, pred Predicate, max bool, stats *ExecStats) (int64, bool, error) {
	var value int64
	found := false
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
				return fmt.Errorf("segment %d aggregate page row mismatch", segmentID)
			}
		}
		var pageMatched uint64
		for row := 0; row < col.V.Len; row++ {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if !matched || !vector.IsValid(col.V.Valid, row) {
				continue
			}
			n, err := intValueAt(col, row)
			if err != nil {
				return err
			}
			value, found = mergeMinMax(value, found, n, max), true
			pageMatched++
		}
		if stats != nil {
			stats.RowsMatched += pageMatched
		}
		return nil
	})
	return value, found, err
}

func minMaxIntColumn(col vector.Column, max bool) (int64, bool, error) {
	var value int64
	found := false
	for row := 0; row < col.V.Len; row++ {
		if !vector.IsValid(col.V.Valid, row) {
			continue
		}
		n, err := intValueAt(col, row)
		if err != nil {
			return 0, false, err
		}
		value, found = mergeMinMax(value, found, n, max), true
	}
	return value, found, nil
}

func mergeMinMax(current int64, found bool, next int64, max bool) int64 {
	if !found || (max && next > current) || (!max && next < current) {
		return next
	}
	return current
}
