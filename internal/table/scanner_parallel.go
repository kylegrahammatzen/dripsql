package table

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type countScanJob struct {
	segment Segment
	stats   storage.Column
}

type countScanResult struct {
	count int
	buf   []byte
	stats ScanStats
	err   error
}

// CountInt64EqualParallel counts rows across table segments using up to workers file readers.
func (s *Scanner) CountInt64EqualParallel(column string, value int64, workers int) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	t := s.table
	if err := t.requireColumnKind(column, vector.KindInt64); err != nil {
		return 0, err
	}
	shouldAttempt, err := s.shouldAttemptStorageParallel(column, vector.KindInt64, workers)
	if err != nil {
		return 0, err
	}
	if !shouldAttempt {
		return s.CountInt64Equal(column, value)
	}
	s.beginScan()
	jobs, err := s.countScanJobs(column, vector.KindInt64, func(stats storage.Column) bool {
		return storage.CanSkipInt64Equal(stats, value)
	})
	if err != nil {
		return 0, err
	}
	if !shouldUseStorageParallel(jobs, workers) {
		return s.CountInt64Equal(column, value)
	}
	return s.countStorageInt64JobsParallel(jobs, column, value, workers)
}

// CountStringEqualParallel counts rows across table segments using up to workers file readers.
func (s *Scanner) CountStringEqualParallel(column string, value string, workers int) (int, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	t := s.table
	if err := t.requireColumnKind(column, vector.KindString); err != nil {
		return 0, err
	}
	shouldAttempt, err := s.shouldAttemptStorageParallel(column, vector.KindString, workers)
	if err != nil {
		return 0, err
	}
	if !shouldAttempt {
		return s.CountStringEqual(column, value)
	}
	s.beginScan()
	jobs, err := s.countScanJobs(column, vector.KindString, func(stats storage.Column) bool {
		return storage.CanSkipStringEqual(stats, value)
	})
	if err != nil {
		return 0, err
	}
	if !shouldUseStorageParallel(jobs, workers) {
		return s.CountStringEqual(column, value)
	}
	return s.countStorageStringJobsParallel(jobs, column, value, workers)
}

func (s *Scanner) countScanJobs(column string, kind vector.Kind, canSkip func(storage.Column) bool) ([]countScanJob, error) {
	jobs := make([]countScanJob, 0, len(s.table.manifest.Segments))
	for _, segment := range s.table.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, kind)
		if err != nil {
			return nil, err
		}
		if canSkip(stats) {
			s.markSkipped(segment)
			continue
		}
		jobs = append(jobs, countScanJob{segment: segment, stats: stats})
	}
	return jobs, nil
}

func (s *Scanner) shouldAttemptStorageParallel(column string, kind vector.Kind, workers int) (bool, error) {
	if parallelWorkerCount(workers, len(s.table.manifest.Segments)) < 2 || len(s.table.manifest.Segments) < 4 {
		return false, nil
	}
	for _, segment := range s.table.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, kind)
		if err != nil {
			return false, err
		}
		if stats.Codec != storage.CodecPlain {
			return false, nil
		}
	}
	return true, nil
}

func shouldUseStorageParallel(jobs []countScanJob, workers int) bool {
	workerCount := parallelWorkerCount(workers, len(jobs))
	if workerCount < 2 || len(jobs) < 4 {
		return false
	}
	for _, job := range jobs {
		if job.stats.Codec != storage.CodecPlain {
			return false
		}
	}
	return true
}

func (s *Scanner) countStorageInt64JobsParallel(jobs []countScanJob, column string, value int64, workers int) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	workerCount := parallelWorkerCount(workers, len(jobs))
	if err := s.ensureParallelFiles(workerCount); err != nil {
		return 0, err
	}
	jobCh := make(chan countScanJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	results := make([]countScanResult, workerCount)
	var wg sync.WaitGroup
	for worker := range workerCount {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			results[worker] = s.countStorageInt64Worker(s.parallelFiles[worker], s.parallelBufs[worker], jobCh, column, value)
		}(worker)
	}
	wg.Wait()
	return s.mergeCountResults(results)
}

func (s *Scanner) countStorageStringJobsParallel(jobs []countScanJob, column string, value string, workers int) (int, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	workerCount := parallelWorkerCount(workers, len(jobs))
	if err := s.ensureParallelFiles(workerCount); err != nil {
		return 0, err
	}
	jobCh := make(chan countScanJob, len(jobs))
	for _, job := range jobs {
		jobCh <- job
	}
	close(jobCh)

	results := make([]countScanResult, workerCount)
	var wg sync.WaitGroup
	for worker := range workerCount {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			results[worker] = s.countStorageStringWorker(s.parallelFiles[worker], s.parallelBufs[worker], jobCh, column, value)
		}(worker)
	}
	wg.Wait()
	return s.mergeCountResults(results)
}

func (s *Scanner) countStorageInt64Worker(file *os.File, buf []byte, jobs <-chan countScanJob, column string, value int64) countScanResult {
	readerAt := seekReaderAt{file: file}
	var result countScanResult
	for job := range jobs {
		reader, scratch, err := storage.OpenSegment(readerAt, job.segment.Offset, job.segment.Bytes, buf)
		buf = scratch
		if err != nil {
			result.buf = buf
			result.err = fmt.Errorf("open storage segment %d: %w", job.segment.ID, err)
			return result
		}
		count, scratch, scanStats, err := storage.CountInt64Equal(reader, column, value, buf)
		buf = scratch
		if err != nil {
			result.buf = buf
			result.err = fmt.Errorf("scan storage segment %d: %w", job.segment.ID, err)
			return result
		}
		result.count, err = checkedAddCount(result.count, count)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		markStatsStorageScanned(&result.stats, job.segment, scanStats)
	}
	result.buf = buf
	return result
}

func (s *Scanner) countStorageStringWorker(file *os.File, buf []byte, jobs <-chan countScanJob, column string, value string) countScanResult {
	readerAt := seekReaderAt{file: file}
	var result countScanResult
	for job := range jobs {
		reader, scratch, err := storage.OpenSegment(readerAt, job.segment.Offset, job.segment.Bytes, buf)
		buf = scratch
		if err != nil {
			result.buf = buf
			result.err = fmt.Errorf("open storage segment %d: %w", job.segment.ID, err)
			return result
		}
		count, scratch, scanStats, err := storage.CountStringEqual(reader, column, value, buf)
		buf = scratch
		if err != nil {
			result.buf = buf
			result.err = fmt.Errorf("scan storage segment %d: %w", job.segment.ID, err)
			return result
		}
		result.count, err = checkedAddCount(result.count, count)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		markStatsStorageScanned(&result.stats, job.segment, scanStats)
	}
	result.buf = buf
	return result
}

func (s *Scanner) mergeCountResults(results []countScanResult) (int, error) {
	count := 0
	var firstErr error
	for i, result := range results {
		if i < len(s.parallelBufs) {
			s.parallelBufs[i] = result.buf
		}
		if firstErr == nil && result.err != nil {
			firstErr = result.err
		}
		var err error
		count, err = checkedAddCount(count, result.count)
		if firstErr == nil && err != nil {
			firstErr = err
		}
		s.stats.SegmentsScanned += result.stats.SegmentsScanned
		s.stats.SegmentsSkipped += result.stats.SegmentsSkipped
		s.stats.RowsScanned += result.stats.RowsScanned
		s.stats.RowsSkipped += result.stats.RowsSkipped
		s.stats.BytesScanned += result.stats.BytesScanned
		s.stats.BytesSkipped += result.stats.BytesSkipped
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return count, nil
}

func (s *Scanner) ensureParallelFiles(workers int) error {
	for len(s.parallelFiles) < workers {
		file, err := os.Open(filepath.Join(s.table.dir, dataFile))
		if err != nil {
			return err
		}
		s.parallelFiles = append(s.parallelFiles, file)
	}
	for len(s.parallelBufs) < workers {
		s.parallelBufs = append(s.parallelBufs, nil)
	}
	return nil
}

func (s *Scanner) closeParallelFiles() error {
	var firstErr error
	for _, file := range s.parallelFiles {
		if file == nil {
			continue
		}
		if err := file.Close(); firstErr == nil && err != nil {
			firstErr = err
		}
	}
	s.parallelFiles = nil
	return firstErr
}

func parallelWorkerCount(workers int, jobs int) int {
	if jobs <= 0 {
		return 0
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if workers > jobs {
		workers = jobs
	}
	if workers < 1 {
		return 1
	}
	return workers
}

func errMissingSegmentColumn(segment Segment, column string) error {
	return fmt.Errorf("segment %d missing column %q", segment.ID, column)
}

func errWrongSegmentColumnKind(segment Segment, column string, got vector.Kind, want string) error {
	return fmt.Errorf("segment %d column %q is %s, want %s", segment.ID, column, got, want)
}
