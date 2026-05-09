package storage

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/kernel"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type ScanRequest struct {
	Table     catalog.TableDef
	Columns   []catalog.ColumnID
	Predicate Predicate
}

type Scanner struct {
	store      *Store
	table      catalog.TableDef
	dir        string
	segments   []SegmentMeta
	projection []catalog.ColumnDef
	predicate  Predicate
	segIndex   int
	pageIndex  int
	err        error
	closed     bool
	scratch    vector.Sel
	filePath   string
	file       *os.File
}

func (s *Store) Scan(ctx context.Context, req ScanRequest) (*Scanner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateScanPredicate(req.Table, req.Predicate); err != nil {
		return nil, err
	}
	projection, err := projectionColumns(req.Table, req.Columns)
	if err != nil {
		return nil, err
	}
	state, err := s.ensureTableState(req.Table)
	if err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()
	state.mu.RLock()
	dir := state.dir
	segments := slices.Clone(state.segments)
	state.mu.RUnlock()
	return &Scanner{store: s, table: req.Table, dir: dir, segments: segments, projection: projection, predicate: req.Predicate, scratch: make(vector.Sel, 0, DefaultPageRows)}, nil
}

func (s *Scanner) Next(dst *vector.Batch) bool {
	if s.closed || s.err != nil {
		return false
	}
	for s.segIndex < len(s.segments) {
		segment := s.segments[s.segIndex]
		if s.predicate.Op != PredicateNone {
			predMeta, _, ok := columnMetaByID(segment, s.predicate.ColumnID)
			if !ok {
				s.setErr(fmt.Errorf("segment %d missing predicate column ID %d", segment.ID, s.predicate.ColumnID))
				return false
			}
			if !maySegmentMatch(predMeta, s.predicate) {
				if err := s.releaseSegmentFile(); err != nil {
					s.setErr(err)
					return false
				}
				s.segIndex++
				s.pageIndex = 0
				continue
			}
		}
		pages := segmentPageCount(segment)
		for s.pageIndex < pages {
			pageIndex := s.pageIndex
			s.pageIndex++
			batch, ok := s.readPage(segment, pageIndex)
			if s.err != nil {
				return false
			}
			if !ok {
				continue
			}
			*dst = batch
			return true
		}
		if err := s.releaseSegmentFile(); err != nil {
			s.setErr(err)
			return false
		}
		s.segIndex++
		s.pageIndex = 0
	}
	return false
}

func (s *Scanner) Err() error {
	return s.err
}

func (s *Scanner) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.releaseSegmentFile()
}

func (s *Scanner) readPage(segment SegmentMeta, pageIndex int) (vector.Batch, bool) {
	var predSel vector.Sel
	if s.predicate.Op != PredicateNone {
		predMeta, _, _ := columnMetaByID(segment, s.predicate.ColumnID)
		page := predMeta.Pages[pageIndex]
		if !mayPageMatch(predMeta, page, s.predicate) {
			return vector.Batch{}, false
		}
		if err := s.ensureSegmentFile(segment); err != nil {
			s.setErr(err)
			return vector.Batch{}, false
		}
		predCol, err := readColumnPageFromFile(s.file, predMeta, page)
		if err != nil {
			s.setErr(err)
			return vector.Batch{}, false
		}
		switch predMeta.Type.Kind {
		case sqltype.KindInt64, sqltype.KindTimestamp:
			switch s.predicate.Op {
			case PredicateOpEq:
				predSel = kernel.EqInt64(predCol.V.I64, predCol.V.Valid, nil, s.scratch, s.predicate.Int64)
			case PredicateOpBetween:
				predSel = kernel.BetweenInt64(predCol.V.I64, predCol.V.Valid, nil, s.scratch, s.predicate.Lo, s.predicate.Hi)
			default:
				s.setErr(fmt.Errorf("unsupported scan predicate op %d on int64", s.predicate.Op))
				return vector.Batch{}, false
			}
		default:
			predSel = s.scratch[:0]
			for row := 0; row < predCol.V.Len; row++ {
				matched, err := matchPredicateRow(predCol, row, s.predicate)
				if err != nil {
					s.setErr(err)
					return vector.Batch{}, false
				}
				if matched {
					predSel = append(predSel, vector.Row(row))
				}
			}
		}
		if len(predSel) == 0 {
			return vector.Batch{}, false
		}
	}
	if err := s.ensureSegmentFile(segment); err != nil {
		s.setErr(err)
		return vector.Batch{}, false
	}

	cols := make([]vector.Column, 0, len(s.projection))
	for _, projected := range s.projection {
		colMeta, _, ok := columnMetaByID(segment, projected.ID)
		if !ok {
			s.setErr(fmt.Errorf("segment %d missing projected column ID %d", segment.ID, projected.ID))
			return vector.Batch{}, false
		}
		col, err := readColumnPageFromFile(s.file, colMeta, colMeta.Pages[pageIndex])
		if err != nil {
			s.setErr(err)
			return vector.Batch{}, false
		}
		cols = append(cols, col)
	}
	batch, err := vector.NewBatch(cols)
	if err != nil {
		s.setErr(err)
		return vector.Batch{}, false
	}
	if predSel != nil {
		selCopy := append(vector.Sel(nil), predSel...)
		if err := batch.SetSel(selCopy); err != nil {
			s.setErr(err)
			return vector.Batch{}, false
		}
	}
	return batch, true
}

func (s *Scanner) ensureSegmentFile(segment SegmentMeta) error {
	path := segment.AbsPath(s.dir)
	if s.file != nil && s.filePath == path {
		return nil
	}
	if err := s.releaseSegmentFile(); err != nil {
		return err
	}
	file, err := s.store.files.Acquire(path)
	if err != nil {
		return err
	}
	s.file = file
	s.filePath = path
	return nil
}

func (s *Scanner) releaseSegmentFile() error {
	if s.file == nil || s.filePath == "" {
		s.file = nil
		s.filePath = ""
		return nil
	}
	path := s.filePath
	s.file = nil
	s.filePath = ""
	return s.store.files.Release(path)
}

func (s *Scanner) setErr(err error) {
	s.err = err
	_ = s.releaseSegmentFile()
}

func segmentPageCount(segment SegmentMeta) int {
	if len(segment.Columns) == 0 {
		return 0
	}
	return len(segment.Columns[0].Pages)
}

func validateScanPredicate(table catalog.TableDef, pred Predicate) error {
	if pred.Op == PredicateNone {
		return nil
	}
	col, _, ok := tableColumnByID(table, pred.ColumnID)
	if !ok {
		return fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
	}
	if err := validatePredicateColumn(col, pred); err != nil {
		return err
	}
	if !isLeafComparison(pred.Op) {
		return fmt.Errorf("scan predicate op %d is not supported yet", pred.Op)
	}
	switch col.Type.Kind {
	case sqltype.KindInt64, sqltype.KindTimestamp:
		if pred.Op != PredicateOpEq && pred.Op != PredicateOpBetween {
			return fmt.Errorf("scan predicate op %d on column %q is not supported yet", pred.Op, col.Name)
		}
		return nil
	case sqltype.KindFloat32, sqltype.KindFloat64,
		sqltype.KindUUID, sqltype.KindBytes, sqltype.KindNamed:
		return nil
	default:
		return fmt.Errorf("scan predicate op %d on column %q (%s) is not supported yet", pred.Op, col.Name, col.Type)
	}
}
