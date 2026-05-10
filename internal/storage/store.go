package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Store struct {
	root    string
	closeMu sync.RWMutex
	mu      sync.RWMutex
	closed  bool
	tables  map[string]*tableState
	files   *segmentFileCache
}

type tableState struct {
	spec          types.TableSpec
	dir           string
	appendMu      sync.Mutex
	mu            sync.RWMutex
	segments      []storedSegment
	nextSegmentID SegmentID
	buffer        *IngestBuffer
	sealMu        sync.Mutex
	sealCond      *sync.Cond
	sealQueue     []sealJob
	sealPending   int
	sealRunning   bool
	sealClosed    bool
	sealErr       error
}

type sealJob struct {
	batches []types.Batch
}

type storedSegment struct {
	path      string
	meta      SegmentMeta
	size      int64
	pageInfos []SegmentPageInfo
}

func Open(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("storage root is required")
	}
	rootExisted, err := dirExists(root)
	if err != nil {
		return nil, err
	}
	tablesDir := filepath.Join(root, "tables")
	tablesExisted, err := dirExists(tablesDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tablesDir, 0o755); err != nil {
		return nil, err
	}
	if !rootExisted {
		if err := syncDir(filepath.Dir(root)); err != nil {
			return nil, err
		}
	}
	if !tablesExisted {
		if err := syncDir(root); err != nil {
			return nil, err
		}
	}
	return &Store{root: root, tables: make(map[string]*tableState), files: newSegmentFileCache()}, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil
	}
	flushErr := s.flushAllBufferedLocked(context.Background())
	sealerErr := s.stopAllAsyncSealers()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	closeErr := s.files.Close()
	if flushErr != nil {
		return flushErr
	}
	if sealerErr != nil {
		return sealerErr
	}
	return closeErr
}

func (s *Store) AppendBatch(ctx context.Context, table types.TableSpec, batch types.Batch) (SegmentMeta, error) {
	return s.AppendBatches(ctx, table, []types.Batch{batch})
}

func (s *Store) AppendBatches(ctx context.Context, table types.TableSpec, batches []types.Batch) (SegmentMeta, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, err
	}
	if err := validateStoreBatches(table, batches); err != nil {
		return SegmentMeta{}, err
	}
	if err := s.beginOp(); err != nil {
		return SegmentMeta{}, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table, true)
	if err != nil {
		return SegmentMeta{}, err
	}
	state.appendMu.Lock()
	defer state.appendMu.Unlock()
	if err := s.waitForAsyncSeals(ctx, state); err != nil {
		return SegmentMeta{}, err
	}
	return s.appendBatchesToState(ctx, state, batches)
}

func (s *Store) appendBatchesToState(ctx context.Context, state *tableState, batches []types.Batch) (SegmentMeta, error) {
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, err
	}
	state.mu.Lock()
	id := state.nextSegmentID
	state.nextSegmentID++
	state.mu.Unlock()

	relPath := filepath.Join("segments", fmt.Sprintf("%016d.dsv3", id))
	finalPath := filepath.Join(state.dir, relPath)
	tmpPath := finalPath + ".tmp"
	meta, err := WriteSegment(tmpPath, id, batches)
	if err != nil {
		return SegmentMeta{}, err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return SegmentMeta{}, err
	}
	if err := syncDir(filepath.Dir(finalPath)); err != nil {
		return SegmentMeta{}, err
	}

	segment := storedSegment{path: finalPath, meta: meta}
	if err := appendManifest(state.dir, segment); err != nil {
		return SegmentMeta{}, err
	}
	if err := populateStoredSegmentCache(&segment, s.files); err != nil {
		return SegmentMeta{}, err
	}
	state.mu.Lock()
	state.segments = append(state.segments, segment)
	state.mu.Unlock()
	return meta, nil
}

// populateStoredSegmentCache fills size + pageInfos so ScanSegments can return
// without per-query os.Stat or buildSegmentPageInfos work. Segments are
// immutable once written, so the cache is set once and read forever.
// Also prewarms the file cache so the first query doesn't pay os.Open.
func populateStoredSegmentCache(s *storedSegment, fileCache *segmentFileCache) error {
	if s.size == 0 {
		info, err := os.Stat(s.path)
		if err != nil {
			return err
		}
		s.size = info.Size()
	}
	if s.pageInfos == nil {
		infos, err := buildSegmentPageInfos(s.meta, nil)
		if err != nil {
			return err
		}
		s.pageInfos = infos
	}
	fileCache.Prewarm(s.path)
	return nil
}

// AppendBuffered clones the batch into the buffer; the caller may continue
// to use batch after this call. Use AppendBufferedOwned for the no-copy
// fast path when the caller is done with the batch.
func (s *Store) AppendBuffered(ctx context.Context, table types.TableSpec, batch types.Batch) error {
	return s.appendBuffered(ctx, table, batch, true)
}

// AppendBufferedOwned takes ownership of batch — the caller MUST NOT mutate
// or reuse it after this call. Skips the per-Vec slice clone that
// AppendBuffered performs.
func (s *Store) AppendBufferedOwned(ctx context.Context, table types.TableSpec, batch types.Batch) error {
	return s.appendBuffered(ctx, table, batch, false)
}

func (s *Store) appendBuffered(ctx context.Context, table types.TableSpec, batch types.Batch, clone bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()
	state, err := s.ensureTableState(table, true)
	if err != nil {
		return err
	}
	state.appendMu.Lock()
	defer state.appendMu.Unlock()
	if err := s.asyncSealError(state); err != nil {
		return err
	}
	if state.buffer == nil {
		buffer, err := s.NewIngestBuffer(table, IngestBufferOptions{})
		if err != nil {
			return err
		}
		state.buffer = buffer
	}
	if clone {
		if err := state.buffer.appendCloned(batch); err != nil {
			return err
		}
	} else {
		if err := state.buffer.appendBorrowed(batch); err != nil {
			return err
		}
	}
	if state.buffer.BufferedRows() < state.buffer.targetRows {
		return nil
	}
	return s.queueStateBufferSeal(ctx, state)
}

func (s *Store) FlushBuffered(ctx context.Context, table types.TableSpec) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()
	state, err := s.ensureTableState(table, true)
	if err != nil {
		return err
	}
	state.appendMu.Lock()
	defer state.appendMu.Unlock()
	_, _, err = s.flushStateBuffer(ctx, state)
	return err
}

func (s *Store) FlushAllBuffered(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.beginOp(); err != nil {
		return err
	}
	defer s.endOp()
	return s.flushAllBufferedLocked(ctx)
}

func (s *Store) flushAllBufferedLocked(ctx context.Context) error {
	s.mu.RLock()
	states := make([]*tableState, 0, len(s.tables))
	for _, state := range s.tables {
		states = append(states, state)
	}
	s.mu.RUnlock()
	for _, state := range states {
		if err := ctx.Err(); err != nil {
			return err
		}
		state.appendMu.Lock()
		_, _, err := s.flushStateBuffer(ctx, state)
		state.appendMu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) flushStateBuffer(ctx context.Context, state *tableState) (SegmentMeta, bool, error) {
	flushed := false
	if state.buffer != nil && state.buffer.BufferedRows() != 0 {
		if err := s.queueStateBufferSeal(ctx, state); err != nil {
			return SegmentMeta{}, false, err
		}
		flushed = true
	}
	if err := s.waitForAsyncSeals(ctx, state); err != nil {
		return SegmentMeta{}, false, err
	}
	return SegmentMeta{}, flushed, nil
}

func (s *Store) ScanSegments(ctx context.Context, table types.TableSpec) ([]ScanSegment, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table, true)
	if err != nil {
		return nil, err
	}
	state.appendMu.Lock()
	if err := s.waitForAsyncSeals(ctx, state); err != nil {
		state.appendMu.Unlock()
		return nil, err
	}
	state.mu.RLock()
	out := make([]ScanSegment, len(state.segments), len(state.segments)+1)
	for i, segment := range state.segments {
		out[i] = ScanSegment{Path: segment.path, Size: segment.size, Meta: segment.meta, PageInfos: segment.pageInfos}
	}
	state.mu.RUnlock()
	if buffered := bufferedScanSegment(state.buffer); buffered != nil {
		out = append(out, *buffered)
	}
	state.appendMu.Unlock()
	return out, nil
}

// bufferedScanSegment exposes any unsealed buffer rows as an in-memory
// ScanSegment so queries can read fresh data without paying for a segment
// seal+fsync. Returns nil when the buffer is empty. Caller must hold
// state.appendMu.
func bufferedScanSegment(buffer *IngestBuffer) *ScanSegment {
	if buffer == nil || buffer.rows == 0 {
		return nil
	}
	pages := make([]ScanPage, len(buffer.batches))
	for i, batch := range buffer.batches {
		pages[i] = ScanPage{Batch: batch}
	}
	return &ScanSegment{Pages: pages}
}

func (s *Store) ScanIterator(ctx context.Context, table types.TableSpec, pred PredicateEvaluator, stats *ExecStats) (SegmentScanIterator, error) {
	segments, err := s.ScanSegments(ctx, table)
	if err != nil {
		return SegmentScanIterator{}, err
	}
	return SegmentScanIterator{Context: ctx, Segments: segments, Predicate: pred, Stats: stats, fileCache: s.files}, nil
}

func (s *Store) ScanIteratorForPredicate(ctx context.Context, table types.TableSpec, pred Predicate, stats *ExecStats) (SegmentScanIterator, error) {
	segments, err := s.ScanSegments(ctx, table)
	if err != nil {
		return SegmentScanIterator{}, err
	}
	return SegmentScanIterator{Context: ctx, Segments: segments, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats, fileCache: s.files}, nil
}

func (s *Store) ensureTableState(table types.TableSpec, create bool) (*tableState, error) {
	if err := table.Validate(); err != nil {
		return nil, err
	}
	key, err := tableKey(table.Name)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	state := s.tables[key]
	s.mu.RUnlock()
	if state != nil {
		if err := ensureCompatibleTable(state.spec, table); err != nil {
			return nil, err
		}
		return state, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.tables[key]; state != nil {
		if err := ensureCompatibleTable(state.spec, table); err != nil {
			return nil, err
		}
		return state, nil
	}
	if s.closed {
		return nil, fmt.Errorf("storage store is closed")
	}
	dir := tableDir(s.root, key)
	if create {
		if err := ensureTableDirs(s.root, key); err != nil {
			return nil, err
		}
	}
	segments, err := loadTableSegments(dir, s.files)
	if err != nil {
		return nil, err
	}
	state = &tableState{spec: table, dir: dir, segments: segments, nextSegmentID: nextSegmentID(segments)}
	state.sealCond = sync.NewCond(&state.sealMu)
	s.tables[key] = state
	return state, nil
}

func (s *Store) queueStateBufferSeal(ctx context.Context, state *tableState) error {
	if state.buffer == nil || state.buffer.BufferedRows() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state.sealMu.Lock()
	defer state.sealMu.Unlock()
	if state.sealErr != nil {
		return state.sealErr
	}
	if state.sealClosed {
		return fmt.Errorf("storage table sealer is closed")
	}
	if !state.sealRunning {
		state.sealRunning = true
		go s.runAsyncSealer(state)
	}
	state.sealQueue = append(state.sealQueue, sealJob{batches: state.buffer.detach()})
	state.sealPending++
	state.sealCond.Signal()
	return nil
}

func (s *Store) runAsyncSealer(state *tableState) {
	for {
		state.sealMu.Lock()
		for len(state.sealQueue) == 0 && !state.sealClosed && state.sealErr == nil {
			state.sealCond.Wait()
		}
		if len(state.sealQueue) == 0 || state.sealErr != nil {
			state.sealRunning = false
			state.sealCond.Broadcast()
			state.sealMu.Unlock()
			return
		}
		job := state.sealQueue[0]
		copy(state.sealQueue, state.sealQueue[1:])
		state.sealQueue[len(state.sealQueue)-1] = sealJob{}
		state.sealQueue = state.sealQueue[:len(state.sealQueue)-1]
		state.sealMu.Unlock()

		_, err := s.appendBatchesToState(context.Background(), state, job.batches)

		state.sealMu.Lock()
		if err != nil && state.sealErr == nil {
			state.sealErr = err
			state.sealPending -= len(state.sealQueue)
			clear(state.sealQueue)
			state.sealQueue = nil
		}
		state.sealPending--
		state.sealCond.Broadcast()
		state.sealMu.Unlock()
	}
}

func (s *Store) waitForAsyncSeals(ctx context.Context, state *tableState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state.sealMu.Lock()
	defer state.sealMu.Unlock()
	for state.sealPending > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		state.sealCond.Wait()
	}
	return state.sealErr
}

func (s *Store) asyncSealError(state *tableState) error {
	state.sealMu.Lock()
	defer state.sealMu.Unlock()
	return state.sealErr
}

func (s *Store) stopAllAsyncSealers() error {
	s.mu.RLock()
	states := make([]*tableState, 0, len(s.tables))
	for _, state := range s.tables {
		states = append(states, state)
	}
	s.mu.RUnlock()
	var firstErr error
	for _, state := range states {
		if err := stopAsyncSealer(state); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func stopAsyncSealer(state *tableState) error {
	state.sealMu.Lock()
	defer state.sealMu.Unlock()
	state.sealClosed = true
	state.sealCond.Broadcast()
	for state.sealRunning {
		state.sealCond.Wait()
	}
	return state.sealErr
}

func (s *Store) beginOp() error {
	if s == nil {
		return fmt.Errorf("storage store is nil")
	}
	s.closeMu.RLock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		s.closeMu.RUnlock()
		return fmt.Errorf("storage store is closed")
	}
	return nil
}

func (s *Store) endOp() {
	s.closeMu.RUnlock()
}

func validateStoreBatches(table types.TableSpec, batches []types.Batch) error {
	if err := table.Validate(); err != nil {
		return err
	}
	if len(batches) == 0 {
		return fmt.Errorf("storage append requires at least one batch")
	}
	var rows uint64
	for i, batch := range batches {
		if err := validateStoreBatch(table, batch); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		rows += uint64(batch.Len)
		if rows > uint64(^uint32(0)) {
			return fmt.Errorf("segment row count exceeds uint32")
		}
	}
	return nil
}

func validateStoreBatch(table types.TableSpec, batch types.Batch) error {
	if batch.Sel != nil {
		return fmt.Errorf("storage append requires an unselected batch")
	}
	if len(batch.Columns) != len(table.Columns) {
		return fmt.Errorf("batch has %d columns, table has %d", len(batch.Columns), len(table.Columns))
	}
	for i, spec := range table.Columns {
		col := batch.Columns[i]
		if col.Name != spec.Name {
			return fmt.Errorf("batch column %d is %q, want %q", i, col.Name, spec.Name)
		}
		if col.Type != spec.Type {
			return fmt.Errorf("batch column %q type is %s, want %s", spec.Name, col.Type, spec.Type)
		}
		kind, err := types.VecKindOf(spec.Type)
		if err != nil {
			return err
		}
		if col.V.Kind != kind {
			return fmt.Errorf("batch column %q vector kind is %s, want %s", spec.Name, col.V.Kind, kind)
		}
		if !spec.Nullable && types.NullCount(col.V.Valid, batch.Len) != 0 {
			return fmt.Errorf("batch column %q has nulls but table column is not nullable", spec.Name)
		}
	}
	return nil
}

func loadTableSegments(dir string, fileCache *segmentFileCache) ([]storedSegment, error) {
	segments, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	for i := range segments {
		if err := populateStoredSegmentCache(&segments[i], fileCache); err != nil {
			return nil, err
		}
	}
	return segments, nil
}

func nextSegmentID(segments []storedSegment) SegmentID {
	var max SegmentID
	for _, segment := range segments {
		if segment.meta.ID > max {
			max = segment.meta.ID
		}
	}
	return max + 1
}

func ensureCompatibleTable(existing types.TableSpec, next types.TableSpec) error {
	if existing.Name != next.Name || len(existing.Columns) != len(next.Columns) {
		return fmt.Errorf("table %q does not match existing table", next.Name)
	}
	for i, col := range existing.Columns {
		nextCol := next.Columns[i]
		if col.Name != nextCol.Name || col.Type != nextCol.Type || col.Nullable != nextCol.Nullable {
			return fmt.Errorf("table %q column %d does not match existing table", next.Name, i)
		}
	}
	return nil
}

func tableKey(name string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return "", fmt.Errorf("table name is required")
	}
	if key == "." || key == ".." || strings.ContainsAny(key, `/\`) {
		return "", fmt.Errorf("table name %q is not safe for storage path", name)
	}
	return key, nil
}

func tableDir(root string, key string) string {
	return filepath.Join(root, "tables", key)
}

func ensureTableDirs(root string, key string) error {
	tablesDir := filepath.Join(root, "tables")
	dir := tableDir(root, key)
	segmentsDir := filepath.Join(dir, "segments")
	tableExisted, err := dirExists(dir)
	if err != nil {
		return err
	}
	segmentsExisted, err := dirExists(segmentsDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(segmentsDir, 0o755); err != nil {
		return err
	}
	if !tableExisted {
		if err := syncDir(tablesDir); err != nil {
			return err
		}
	}
	if !segmentsExisted {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%q exists and is not a directory", path)
		}
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
