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
	stats   storage.ColumnStats
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
	s.beginScan()

	jobs := make([]countScanJob, 0, len(t.manifest.Segments))
	for _, segment := range t.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, vector.KindInt64)
		if err != nil {
			return 0, err
		}
		if stats.HasMinMax && (value < stats.MinInt64 || value > stats.MaxInt64) {
			s.markSkipped(segment)
			continue
		}
		jobs = append(jobs, countScanJob{segment: segment, stats: stats})
	}

	return s.countInt64JobsParallel(jobs, value, workers)
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
	s.beginScan()

	jobs := make([]countScanJob, 0, len(t.manifest.Segments))
	for _, segment := range t.manifest.Segments {
		stats, err := segmentColumnStats(segment, column, vector.KindString)
		if err != nil {
			return 0, err
		}
		if storage.CanSkipStringEqual(stats, value) {
			s.markSkipped(segment)
			continue
		}
		jobs = append(jobs, countScanJob{segment: segment, stats: stats})
	}

	return s.countStringJobsParallel(jobs, value, workers)
}

func (s *Scanner) countInt64JobsParallel(jobs []countScanJob, value int64, workers int) (int, error) {
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
			results[worker] = s.countInt64Worker(s.parallelFiles[worker], s.parallelBufs[worker], jobCh, value)
		}(worker)
	}
	wg.Wait()
	return s.mergeCountResults(results)
}

func (s *Scanner) countStringJobsParallel(jobs []countScanJob, value string, workers int) (int, error) {
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
			results[worker] = s.countStringWorker(s.parallelFiles[worker], s.parallelBufs[worker], jobCh, value)
		}(worker)
	}
	wg.Wait()
	return s.mergeCountResults(results)
}

func (s *Scanner) countInt64Worker(file *os.File, buf []byte, jobs <-chan countScanJob, value int64) countScanResult {
	reader := seekReaderAt{file: file}
	var result countScanResult
	for job := range jobs {
		var count int
		var bytesRead int64
		var err error
		count, buf, bytesRead, err = scanSegmentInt64Equal(reader, job.segment, job.stats, value, buf)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		result.count, err = checkedAddCount(result.count, count)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		markStatsScanned(&result.stats, job.segment, bytesRead)
	}
	result.buf = buf
	return result
}

func (s *Scanner) countStringWorker(file *os.File, buf []byte, jobs <-chan countScanJob, value string) countScanResult {
	reader := seekReaderAt{file: file}
	var result countScanResult
	for job := range jobs {
		var count int
		var bytesRead int64
		var err error
		count, buf, bytesRead, err = scanSegmentStringEqual(reader, job.segment, job.stats, value, buf)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		result.count, err = checkedAddCount(result.count, count)
		if err != nil {
			result.buf = buf
			result.err = err
			return result
		}
		markStatsScanned(&result.stats, job.segment, bytesRead)
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
		s.stats.RowsScanned += result.stats.RowsScanned
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
