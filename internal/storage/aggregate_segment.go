package storage

import (
	"fmt"
	"os"
)

// pageVisitor is the per-page callback for walkSegmentPages. The callback
// receives the open segment file, the aggregate column's page metadata, and
// the current page index; it runs once per page so per-row dispatch overhead
// stays amortized.
type pageVisitor func(file *os.File, page PageMeta, pageIndex int) error

// walkSegmentPages opens the segment file, validates that each predicate
// column's pages align with colMeta's pages, and invokes visit for each page.
// The file is acquired and released around the loop. Predicate columns need
// not be passed when there is no WHERE clause; predMetas may be nil. When
// stats is non-nil the walker attributes per-page payload bytes to AggPayload
// (colMeta) and PredPayload (predMetas); the per-row work is left to visit.
//
// This is the canonical sealed-segment iteration shape across aggregate
// paths (count(non-null), sum, min/max, group counts, group sums, group any).
func (s *Store) walkSegmentPages(path string, segmentID SegmentID, colMeta ColumnMeta, predMetas []ColumnMeta, stats *ExecStats, visit pageVisitor) error {
	for _, predMeta := range predMetas {
		if len(colMeta.Pages) != len(predMeta.Pages) {
			return fmt.Errorf("segment %d page count mismatch for aggregate and WHERE columns", segmentID)
		}
	}
	file, err := s.files.Acquire(path)
	if err != nil {
		return err
	}
	defer func() { _ = s.files.Release(path) }()
	if stats != nil {
		stats.SegmentsTotal++
		stats.SegmentsCandidate++
	}
	for i := range colMeta.Pages {
		colPage := colMeta.Pages[i]
		for _, predMeta := range predMetas {
			predPage := predMeta.Pages[i]
			if colPage.RowStart != predPage.RowStart || colPage.Rows != predPage.Rows {
				return fmt.Errorf("segment %d page mismatch for aggregate and WHERE columns", segmentID)
			}
		}
		if stats != nil {
			stats.PagesTotal++
			stats.PagesCandidate++
			stats.RowsTotal += uint64(colPage.Rows)
			stats.RowsCandidate += uint64(colPage.Rows)
			stats.AggPayloadBytes += colPage.Length
			stats.PayloadBytesRead += colPage.Length
			for _, predMeta := range predMetas {
				predPage := predMeta.Pages[i]
				stats.PredPayloadBytes += predPage.Length
				stats.PayloadBytesRead += predPage.Length
			}
		}
		if err := visit(file, colPage, i); err != nil {
			return err
		}
	}
	return nil
}

// walkSegmentPagesPaired walks a segment whose row layout is shared between
// two aligned columns (e.g. GROUP BY column + aggregate column), plus any
// number of predicate columns. The visitor receives both primary pages.
//
// This is the GROUP BY analogue of walkSegmentPages.
func (s *Store) walkSegmentPagesPaired(
	path string,
	segmentID SegmentID,
	groupMeta ColumnMeta,
	aggMeta ColumnMeta,
	predMetas []ColumnMeta,
	visit func(file *os.File, groupPage PageMeta, aggPage PageMeta, pageIndex int) error,
) error {
	if len(groupMeta.Pages) != len(aggMeta.Pages) {
		return fmt.Errorf("segment %d page count mismatch for GROUP BY and aggregate columns", segmentID)
	}
	for _, predMeta := range predMetas {
		if len(groupMeta.Pages) != len(predMeta.Pages) {
			return fmt.Errorf("segment %d page count mismatch for GROUP BY and WHERE columns", segmentID)
		}
	}
	file, err := s.files.Acquire(path)
	if err != nil {
		return err
	}
	defer func() { _ = s.files.Release(path) }()
	for i := range groupMeta.Pages {
		groupPage := groupMeta.Pages[i]
		aggPage := aggMeta.Pages[i]
		if groupPage.RowStart != aggPage.RowStart || groupPage.Rows != aggPage.Rows {
			return fmt.Errorf("segment %d page mismatch for GROUP BY and aggregate columns", segmentID)
		}
		for _, predMeta := range predMetas {
			predPage := predMeta.Pages[i]
			if groupPage.RowStart != predPage.RowStart || groupPage.Rows != predPage.Rows {
				return fmt.Errorf("segment %d page mismatch for GROUP BY and WHERE columns", segmentID)
			}
		}
		if err := visit(file, groupPage, aggPage, i); err != nil {
			return err
		}
	}
	return nil
}
