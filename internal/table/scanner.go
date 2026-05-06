package table

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Scanner reuses scratch buffers across table scans. It is not safe for concurrent use.
type Scanner struct {
	table          *Table
	file           *os.File
	buf            []byte
	stringKeys     []string
	storageReaders []storage.Reader
	storageOK      []bool
	parallelFiles  []*os.File
	parallelBufs   [][]byte
	stats          ScanStats
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
	for segmentIndex, segment := range t.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, vector.KindInt64)
		if err != nil {
			return 0, err
		}
		if storage.CanSkipInt64Equal(stats, value) {
			s.markSkipped(segment)
			continue
		}

		file, err := s.dataFile()
		if err != nil {
			return 0, err
		}
		segmentCount, scanStats, err := s.scanStorageInt64Equal(file, segmentIndex, segment, column, value)
		if err != nil {
			return 0, err
		}
		s.markStorageScanned(segment, scanStats)
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
	for segmentIndex, segment := range t.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, vector.KindString)
		if err != nil {
			return 0, err
		}
		if storage.CanSkipStringEqual(stats, value) {
			s.markSkipped(segment)
			continue
		}

		file, err := s.dataFile()
		if err != nil {
			return 0, err
		}
		segmentCount, scanStats, err := s.scanStorageStringEqual(file, segmentIndex, segment, column, value)
		if err != nil {
			return 0, err
		}
		s.markStorageScanned(segment, scanStats)
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
	for segmentIndex, segment := range t.manifest.Segments {
		if _, err := segmentColumnStats(segment, column, vector.KindString); err != nil {
			return nil, err
		}

		file, err := s.dataFile()
		if err != nil {
			return nil, err
		}
		scanStats, err := s.scanStorageStringGroups(file, segmentIndex, segment, column, counts)
		if err != nil {
			return nil, err
		}
		s.markStorageScanned(segment, scanStats)
	}
	return counts, nil
}

func segmentColumnStats(segment Segment, column string, kind vector.Kind) (storage.Column, error) {
	stats, ok := segment.Stats.Column(column)
	if !ok {
		return storage.Column{}, errMissingSegmentColumn(segment, column)
	}
	if stats.Kind != kind {
		return storage.Column{}, errWrongSegmentColumnKind(segment, column, stats.Kind, kind.String())
	}
	return stats, nil
}

func (s *Scanner) scanStorageInt64Equal(file *os.File, segmentIndex int, segment Segment, column string, value int64) (int, storage.ScanStats, error) {
	reader, err := s.openStorageSegment(file, segmentIndex, segment)
	if err != nil {
		return 0, storage.ScanStats{}, err
	}
	count, scratch, stats, err := storage.CountInt64Equal(reader, column, value, s.buf)
	s.buf = scratch
	if err != nil {
		return 0, storage.ScanStats{}, fmt.Errorf("scan storage segment %d: %w", segment.ID, err)
	}
	return count, stats, nil
}

func (s *Scanner) scanStorageStringEqual(file *os.File, segmentIndex int, segment Segment, column string, value string) (int, storage.ScanStats, error) {
	reader, err := s.openStorageSegment(file, segmentIndex, segment)
	if err != nil {
		return 0, storage.ScanStats{}, err
	}
	count, scratch, stats, err := storage.CountStringEqual(reader, column, value, s.buf)
	s.buf = scratch
	if err != nil {
		return 0, storage.ScanStats{}, fmt.Errorf("scan storage segment %d: %w", segment.ID, err)
	}
	return count, stats, nil
}

func (s *Scanner) scanStorageStringGroups(file *os.File, segmentIndex int, segment Segment, column string, counts map[string]int) (storage.ScanStats, error) {
	reader, err := s.openStorageSegment(file, segmentIndex, segment)
	if err != nil {
		return storage.ScanStats{}, err
	}
	_, scratch, stringKeys, stats, err := storage.GroupStringCounts(reader, column, counts, s.buf, s.stringKeys)
	s.buf = scratch
	s.stringKeys = stringKeys
	if err != nil {
		return storage.ScanStats{}, fmt.Errorf("group storage segment %d: %w", segment.ID, err)
	}
	return stats, nil
}

func (s *Scanner) openStorageSegment(file *os.File, segmentIndex int, segment Segment) (storage.Reader, error) {
	if segmentIndex >= 0 {
		for len(s.storageReaders) <= segmentIndex {
			s.storageReaders = append(s.storageReaders, storage.Reader{})
			s.storageOK = append(s.storageOK, false)
		}
		if s.storageOK[segmentIndex] {
			return s.storageReaders[segmentIndex], nil
		}
	}
	reader, scratch, err := storage.OpenSegment(seekReaderAt{file: file}, segment.Offset, segment.Bytes, s.buf)
	s.buf = scratch
	if err != nil {
		return storage.Reader{}, fmt.Errorf("open storage segment %d: %w", segment.ID, err)
	}
	if segmentIndex >= 0 {
		s.storageReaders[segmentIndex] = reader
		s.storageOK[segmentIndex] = true
	}
	return reader, nil
}

// Reset releases scanner scratch buffers.
func (s *Scanner) Reset() {
	if s.file != nil {
		_ = s.file.Close()
	}
	_ = s.closeParallelFiles()
	s.file = nil
	s.buf = nil
	s.stringKeys = nil
	s.storageReaders = nil
	s.storageOK = nil
	s.parallelBufs = nil
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
	s.stringKeys = nil
	s.storageReaders = nil
	s.storageOK = nil
	s.parallelBufs = nil
	s.stats = ScanStats{}
	parallelErr := s.closeParallelFiles()
	if file == nil {
		return parallelErr
	}
	if err := file.Close(); err != nil {
		return err
	}
	return parallelErr
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
	markStatsScanned(&s.stats, segment, bytesRead)
}

func (s *Scanner) markStorageScanned(segment Segment, scanStats storage.ScanStats) {
	markStatsStorageScanned(&s.stats, segment, scanStats)
}

func markStatsScanned(stats *ScanStats, segment Segment, bytesRead int64) {
	if bytesRead < 0 {
		bytesRead = 0
	}
	if bytesRead > segment.Bytes {
		bytesRead = segment.Bytes
	}
	stats.SegmentsScanned++
	stats.RowsScanned += int64(segment.Rows)
	stats.BytesScanned += bytesRead
	stats.BytesSkipped += segment.Bytes - bytesRead
}

func markStatsStorageScanned(stats *ScanStats, segment Segment, scanStats storage.ScanStats) {
	bytesScanned := scanStats.BytesScanned
	if bytesScanned < 0 {
		bytesScanned = 0
	}
	if bytesScanned > segment.Bytes {
		bytesScanned = segment.Bytes
	}

	rowsTotal := int64(segment.Rows)
	rowsScanned := int64(scanStats.RowsScanned)
	rowsSkipped := int64(scanStats.RowsSkipped)
	if rowsScanned < 0 {
		rowsScanned = 0
	}
	if rowsSkipped < 0 {
		rowsSkipped = 0
	}
	if rowsSkipped > rowsTotal {
		rowsSkipped = rowsTotal
	}
	if rowsScanned > rowsTotal-rowsSkipped {
		rowsScanned = rowsTotal - rowsSkipped
	}
	if rowsScanned == 0 && rowsSkipped == 0 {
		rowsScanned = rowsTotal
	}

	stats.SegmentsScanned++
	stats.RowsScanned += rowsScanned
	stats.RowsSkipped += rowsSkipped
	stats.BytesScanned += bytesScanned
	stats.BytesSkipped += segment.Bytes - bytesScanned
}

func (s *Scanner) markSkipped(segment Segment) {
	s.stats.SegmentsSkipped++
	s.stats.RowsSkipped += int64(segment.Rows)
	s.stats.BytesSkipped += segment.Bytes
}

func checkedAddCount(a, b int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if b > maxInt-a {
		return 0, fmt.Errorf("row count overflows int")
	}
	return a + b, nil
}
