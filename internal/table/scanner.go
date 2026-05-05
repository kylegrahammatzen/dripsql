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
	table         *Table
	file          *os.File
	buf           []byte
	stringKeys    []string
	parallelFiles []*os.File
	parallelBufs  [][]byte
	stats         ScanStats
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
	for _, segment := range t.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, vector.KindInt64)
		if err != nil {
			return 0, err
		}
		if stats.HasMinMax && (value < stats.MinInt64 || value > stats.MaxInt64) {
			s.markSkipped(segment)
			continue
		}

		file, err := s.dataFile()
		if err != nil {
			return 0, err
		}
		var segmentCount int
		var bytesRead int64
		segmentCount, s.buf, bytesRead, err = scanSegmentInt64Equal(seekReaderAt{file: file}, segment, stats, value, s.buf)
		if err != nil {
			return 0, err
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
		var segmentCount int
		var bytesRead int64
		segmentCount, s.buf, bytesRead, err = scanSegmentStringEqual(seekReaderAt{file: file}, segment, stats, value, s.buf)
		if err != nil {
			return 0, err
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
		stats, err := segmentColumnStats(segment, column, vector.KindString)
		if err != nil {
			return nil, err
		}

		file, err := s.dataFile()
		if err != nil {
			return nil, err
		}
		var bytesRead int64
		reader := seekReaderAt{file: file}
		if columnRange, ok := segmentColumnRange(segment, column); ok {
			s.buf, s.stringKeys, bytesRead, err = storage.GroupColumnStringCountsAtCached(reader, segment.Offset+columnRange.Offset, columnRange.Bytes, stats, counts, s.buf, s.stringKeys)
		} else {
			var found bool
			found, s.buf, s.stringKeys, bytesRead, err = storage.GroupSegmentStringCountsAtCached(reader, segment.Offset, segment.Bytes, column, counts, s.buf, s.stringKeys)
			if err == nil && !found {
				return nil, fmt.Errorf("segment %d missing column %q", segment.ID, column)
			}
		}
		if err != nil {
			return nil, err
		}
		s.markScanned(segment, bytesRead)
	}
	return counts, nil
}

func segmentColumnRange(segment Segment, column string) (ColumnRange, bool) {
	for _, columnRange := range segment.Columns {
		if columnRange.Name == column {
			return columnRange, true
		}
	}
	return ColumnRange{}, false
}

func segmentColumnStats(segment Segment, column string, kind vector.Kind) (storage.ColumnStats, error) {
	stats, ok := segment.Stats.Column(column)
	if !ok {
		return storage.ColumnStats{}, errMissingSegmentColumn(segment, column)
	}
	if stats.Kind != kind {
		return storage.ColumnStats{}, errWrongSegmentColumnKind(segment, column, stats.Kind, kind.String())
	}
	return stats, nil
}

func scanSegmentInt64Equal(reader io.ReaderAt, segment Segment, stats storage.ColumnStats, value int64, scratch []byte) (int, []byte, int64, error) {
	if columnRange, ok := segmentColumnRange(segment, stats.Name); ok {
		return storage.CountColumnInt64EqualAt(reader, segment.Offset+columnRange.Offset, columnRange.Bytes, stats, value, scratch)
	}
	count, found, scratch, bytesRead, err := storage.CountSegmentInt64EqualAt(reader, segment.Offset, segment.Bytes, stats.Name, value, scratch)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	if !found {
		return 0, scratch, bytesRead, fmt.Errorf("segment %d missing column %q", segment.ID, stats.Name)
	}
	return count, scratch, bytesRead, nil
}

func scanSegmentStringEqual(reader io.ReaderAt, segment Segment, stats storage.ColumnStats, value string, scratch []byte) (int, []byte, int64, error) {
	if columnRange, ok := segmentColumnRange(segment, stats.Name); ok {
		return storage.CountColumnStringEqualAt(reader, segment.Offset+columnRange.Offset, columnRange.Bytes, stats, value, scratch)
	}
	count, found, scratch, bytesRead, err := storage.CountSegmentStringEqualAt(reader, segment.Offset, segment.Bytes, stats.Name, value, scratch)
	if err != nil {
		return 0, scratch, bytesRead, err
	}
	if !found {
		return 0, scratch, bytesRead, fmt.Errorf("segment %d missing column %q", segment.ID, stats.Name)
	}
	return count, scratch, bytesRead, nil
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
