package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

import (
	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
	"github.com/kylegrahammatzen/dripsql/internal/wal"
)

type Store struct {
	root    string
	closeMu sync.RWMutex
	mu      sync.RWMutex
	closed  bool
	tables  map[catalog.TableID]*tableState
	files   *segmentFileCache
}

type tableState struct {
	table         catalog.TableDef
	dir           string
	appendMu      sync.Mutex
	mu            sync.RWMutex
	segments      []SegmentMeta
	nextSegmentID SegmentID
	bufferMu      sync.Mutex
	bufferCond    *sync.Cond
	buffer        *IngestBuffer
	nextLSN       wal.LSN
	publishing    bool
	pendingRuns   []SealedRun
	publishErr    error
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
	return &Store{root: root, tables: make(map[catalog.TableID]*tableState), files: newSegmentFileCache()}, nil
}

func (s *Store) Close() error {
	if s == nil || s.files == nil {
		return nil
	}
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	flushErr := s.flushAllBuffered(context.Background())
	closeErr := s.files.Close()
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

func (s *Store) AppendBatch(ctx context.Context, table catalog.TableDef, batch vector.Batch) (SegmentMeta, error) {
	return s.AppendBatches(ctx, table, []vector.Batch{batch})
}

func (s *Store) AppendBatches(ctx context.Context, table catalog.TableDef, batches []vector.Batch) (SegmentMeta, error) {
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, err
	}
	if err := validateBatches(table, batches); err != nil {
		return SegmentMeta{}, err
	}
	if err := s.beginOp(); err != nil {
		return SegmentMeta{}, err
	}
	defer s.endOp()

	state, err := s.appendTableState(table)
	if err != nil {
		return SegmentMeta{}, err
	}
	state.appendMu.Lock()
	defer state.appendMu.Unlock()
	return s.appendBatchesToState(ctx, table, state, batches)
}

func (s *Store) appendBatchesToState(ctx context.Context, table catalog.TableDef, state *tableState, batches []vector.Batch) (SegmentMeta, error) {
	state.mu.Lock()
	id := state.nextSegmentID
	state.nextSegmentID++
	lsn := state.nextLSN
	if lsn == 0 {
		lsn = 1
	}
	state.nextLSN = lsn + 1
	state.mu.Unlock()

	meta, err := writeSegmentBatches(ctx, s.root, table, batches, id)
	if err != nil {
		return SegmentMeta{}, err
	}
	if err := appendWALInsert(state.dir, table, lsn, meta); err != nil {
		return SegmentMeta{}, err
	}
	if err := wal.Append(filepath.Join(state.dir, walLogFileName), wal.Record{LSN: lsn, Type: wal.RecordCommit, Payload: nil}); err != nil {
		return SegmentMeta{}, err
	}
	if err := appendManifest(state.dir, meta); err != nil {
		return SegmentMeta{}, err
	}
	state.mu.Lock()
	state.segments = append(state.segments, meta)
	state.mu.Unlock()
	return meta, nil
}

func (s *Store) Segments(ctx context.Context, table catalog.TableDef) ([]SegmentMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()
	state, err := s.ensureTableState(table)
	if err != nil {
		return nil, err
	}
	state.mu.RLock()
	dir := state.dir
	segments := slices.Clone(state.segments)
	state.mu.RUnlock()
	out := make([]SegmentMeta, 0, len(segments))
	for _, manifestMeta := range segments {
		footerMeta, err := readSegmentFooter(manifestMeta.AbsPath(dir))
		if err != nil {
			return nil, err
		}
		if footerMeta.ID != manifestMeta.ID || footerMeta.TableID != table.ID || footerMeta.Rows != manifestMeta.Rows {
			return nil, fmt.Errorf("segment %d footer does not match manifest", manifestMeta.ID)
		}
		out = append(out, footerMeta)
	}
	return out, nil
}

func (s *Store) ensureTableState(table catalog.TableDef) (*tableState, error) {
	if s == nil {
		return nil, fmt.Errorf("storage store is nil")
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, fmt.Errorf("storage store is closed")
	}
	state := s.tables[table.ID]
	s.mu.RUnlock()
	if state != nil {
		return state, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tableStateLocked(table)
}

func (s *Store) appendTableState(table catalog.TableDef) (*tableState, error) {
	s.mu.RLock()
	state := s.tables[table.ID]
	s.mu.RUnlock()
	if state != nil {
		return state, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tables[table.ID] == nil {
		if err := ensureTableDirs(s.root, table); err != nil {
			return nil, err
		}
	}
	return s.tableStateLocked(table)
}

func (s *Store) tableStateLocked(table catalog.TableDef) (*tableState, error) {
	if s.closed {
		return nil, fmt.Errorf("storage store is closed")
	}
	if table.ID == 0 {
		return nil, fmt.Errorf("table ID is required")
	}
	if s.tables == nil {
		s.tables = make(map[catalog.TableID]*tableState)
	}
	if state := s.tables[table.ID]; state != nil {
		return state, nil
	}
	dir := tableDir(s.root, table)
	segments, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	state := &tableState{table: table, dir: dir, segments: segments, nextSegmentID: nextSegmentID(segments)}
	if err := recoverTableFromWAL(state); err != nil {
		return nil, err
	}
	state.bufferCond = sync.NewCond(&state.bufferMu)
	s.tables[table.ID] = state
	return state, nil
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

func validateBatch(table catalog.TableDef, batch vector.Batch) error {
	if table.ID == 0 {
		return fmt.Errorf("table ID is required")
	}
	if batch.Sel != nil {
		return fmt.Errorf("storage append requires an unselected batch")
	}
	if len(batch.Columns) != len(table.Columns) {
		return fmt.Errorf("batch has %d columns, table has %d", len(batch.Columns), len(table.Columns))
	}
	for i, col := range table.Columns {
		if !supportedType(col.Type) {
			return fmt.Errorf("column %q has unsupported storage type %s", col.Name, col.Type)
		}
		batchCol := batch.Columns[i]
		if batchCol.Name != col.Name {
			return fmt.Errorf("batch column %d is %q, want %q", i, batchCol.Name, col.Name)
		}
		if batchCol.Type != col.Type {
			return fmt.Errorf("batch column %q type is %s, want %s", col.Name, batchCol.Type, col.Type)
		}
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return err
		}
		if batchCol.V.Kind != kind {
			return fmt.Errorf("batch column %q vector kind is %s, want %s", col.Name, batchCol.V.Kind, kind)
		}
		if col.Type.Kind == sqltype.KindNamed {
			if err := validateEnumColumnValues(col, batchCol); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateEnumColumnValues(col catalog.ColumnDef, batchCol vector.Column) error {
	if len(col.Labels) == 0 {
		return fmt.Errorf("column %q enum type %q has no labels", col.Name, col.Type.Name)
	}
	for row := 0; row < batchCol.V.Len; row++ {
		if !vector.IsValid(batchCol.V.Valid, row) {
			continue
		}
		code := batchCol.V.U32[row]
		if code == 0 || int(code) > len(col.Labels) {
			return fmt.Errorf("column %q enum code %d is out of range", col.Name, code)
		}
	}
	return nil
}

func validateBatches(table catalog.TableDef, batches []vector.Batch) error {
	if len(batches) == 0 {
		return fmt.Errorf("storage append requires at least one batch")
	}
	var rows uint64
	for i, batch := range batches {
		if err := validateBatch(table, batch); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		rows += uint64(batch.Len)
		if rows > uint64(^uint32(0)) {
			return fmt.Errorf("segment row count exceeds uint32")
		}
	}
	return nil
}

func totalRows(batches []vector.Batch) uint32 {
	var rows uint32
	for _, batch := range batches {
		rows += uint32(batch.Len)
	}
	return rows
}

func totalPages(batches []vector.Batch) int {
	pages := 0
	for _, batch := range batches {
		pages += (batch.Len + DefaultPageRows - 1) / DefaultPageRows
	}
	return pages
}

func tableDir(root string, table catalog.TableDef) string {
	return filepath.Join(root, "tables", fmt.Sprintf("%016d", table.ID))
}

func ensureTableDirs(root string, table catalog.TableDef) error {
	tablesDir := filepath.Join(root, "tables")
	dir := tableDir(root, table)
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

func nextSegmentID(segments []SegmentMeta) SegmentID {
	var max SegmentID
	for _, segment := range segments {
		if segment.ID > max {
			max = segment.ID
		}
	}
	return max + 1
}

func tableColumnByID(table catalog.TableDef, id catalog.ColumnID) (catalog.ColumnDef, int, bool) {
	for i, col := range table.Columns {
		if col.ID == id {
			return col, i, true
		}
	}
	return catalog.ColumnDef{}, 0, false
}

func columnMetaByID(segment SegmentMeta, id catalog.ColumnID) (ColumnMeta, int, bool) {
	for i, col := range segment.Columns {
		if col.ColumnID == id {
			return col, i, true
		}
	}
	return ColumnMeta{}, 0, false
}

func projectionColumns(table catalog.TableDef, ids []catalog.ColumnID) ([]catalog.ColumnDef, error) {
	if len(ids) == 0 {
		return slices.Clone(table.Columns), nil
	}
	cols := make([]catalog.ColumnDef, 0, len(ids))
	seen := make(map[catalog.ColumnID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate projected column ID %d", id)
		}
		seen[id] = struct{}{}
		col, _, ok := tableColumnByID(table, id)
		if !ok {
			return nil, fmt.Errorf("missing projected column ID %d", id)
		}
		cols = append(cols, col)
	}
	return cols, nil
}
