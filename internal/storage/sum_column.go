package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// sumOrderedColumn iterates a typed int slice, gating each row by pred and
// validity, and returns the int64 sum (with overflow detection) plus the
// count of rows that contributed to the sum.
func sumOrderedColumn[T int32 | int64](values []T, valid vector.Validity, rows int, predCols []vector.Column, pred Predicate) (int64, uint64, error) {
	var sum int64
	var matched uint64
	for row := 0; row < rows; row++ {
		if pred.Op != PredicateNone {
			ok, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return 0, 0, err
			}
			if !ok {
				continue
			}
		}
		if !vector.IsValid(valid, row) {
			continue
		}
		var err error
		sum, err = addInt64(sum, int64(values[row]))
		if err != nil {
			return 0, 0, err
		}
		matched++
	}
	return sum, matched, nil
}

// sumColumnRows lifts the int32/int64 kind switch to once per call, then
// delegates the row loop to sumOrderedColumn.
func sumColumnRows(col vector.Column, rows int, predCols []vector.Column, pred Predicate) (int64, uint64, error) {
	switch col.Type.Kind {
	case sqltype.KindInt32:
		return sumOrderedColumn(col.V.I32, col.V.Valid, rows, predCols, pred)
	case sqltype.KindInt64:
		return sumOrderedColumn(col.V.I64, col.V.Valid, rows, predCols, pred)
	default:
		return 0, 0, fmt.Errorf("sum column %q is %s, want int32 or int64", col.Name, col.Type)
	}
}

// ErrSumOverflow is returned when a SUM aggregate would overflow int64;
// goals.md non-negotiable: "no silent approximate answers".
var ErrSumOverflow = errors.New("sum int64 overflow")

// addInt64 returns a+b or ErrSumOverflow on signed overflow (same-sign
// operands whose sum flips sign).
func addInt64(a, b int64) (int64, error) {
	s := a + b
	if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
		return 0, ErrSumOverflow
	}
	return s, nil
}

// SumInt returns sum(int_col) over rows matching pred. scratch may be nil;
// when non-nil it is reused across page reads to avoid per-call allocations.
func (s *Store) SumInt(ctx context.Context, table catalog.TableDef, colID catalog.ColumnID, pred Predicate, scratch *QueryScratch) (int64, error) {
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
	_, colIndex, err := requireIntColumn(state, table, colID, "sum")
	if err != nil {
		return 0, err
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return 0, err
	}

	sum, err := sumIntFromBuffer(state.buffer, colIndex, predIndexes, pred)
	if err != nil {
		state.bufferMu.Unlock()
		return 0, err
	}
	state.mu.RLock()
	dir := state.dir
	segments := make([]SegmentMeta, len(state.segments))
	if scratch != nil {
		segments = scratch.page.segmentSnapshot(state.segments)
	} else {
		copy(segments, state.segments)
	}
	state.mu.RUnlock()
	state.bufferMu.Unlock()

	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		colMeta, _, ok := columnMetaByID(segment, colID)
		if !ok {
			return 0, fmt.Errorf("segment %d missing sum column ID %d", segment.ID, colID)
		}
		if pred.Op == PredicateNone {
			n, err := s.sumIntColumnFromSegment(segment.AbsPath(dir), segment.ID, colMeta, scratch)
			if err != nil {
				return 0, err
			}
			sum, err = addInt64(sum, n)
			if err != nil {
				return 0, err
			}
			continue
		}
		predMetas, err := predicateColumnMetas(segment, pred)
		if err != nil {
			return 0, err
		}
		n, err := s.sumIntFromSegment(segment.AbsPath(dir), segment.ID, colMeta, predMetas, pred, scratch)
		if err != nil {
			return 0, err
		}
		sum, err = addInt64(sum, n)
		if err != nil {
			return 0, err
		}
	}
	return sum, nil
}

func sumIntFromBuffer(buffer *IngestBuffer, colIndex int, predIndexes []int, pred Predicate) (int64, error) {
	var sum int64
	err := forEachBufferBatch(buffer, func(batch vector.Batch, rows int) error {
		n, err := sumIntFromBatch(batch, colIndex, predIndexes, pred, rows)
		if err != nil {
			return err
		}
		sum, err = addInt64(sum, n)
		return err
	})
	return sum, err
}

func sumIntFromBatch(batch vector.Batch, colIndex int, predIndexes []int, pred Predicate, rows int) (int64, error) {
	if colIndex >= len(batch.Columns) {
		return 0, fmt.Errorf("missing buffered sum column index %d", colIndex)
	}
	if rows > batch.Len {
		return 0, fmt.Errorf("sum buffer rows %d exceed batch len %d", rows, batch.Len)
	}
	predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
	if err != nil {
		return 0, err
	}
	sum, _, err := sumColumnRows(batch.Columns[colIndex], rows, predCols, pred)
	return sum, err
}

func (s *Store) sumIntColumnFromSegment(path string, segmentID SegmentID, colMeta ColumnMeta, scratch *QueryScratch) (int64, error) {
	var sum int64
	stats := scratchStats(scratch)
	if stats != nil {
		stats.SetAccess(colMeta.ColumnID, AccessRawIntSumLoop)
	}
	err := s.walkSegmentPages(path, segmentID, colMeta, nil, stats, func(file *os.File, page PageMeta, _ int) error {
		if scratch != nil {
			n, matched, err := sumIntPagePayload(file, colMeta, page, &scratch.page, Predicate{})
			if err != nil {
				return err
			}
			sum, err = addInt64(sum, n)
			if err != nil {
				return err
			}
			if stats != nil {
				stats.RowsMatched += matched
			}
			return nil
		}
		col, err := readColumnPageFromFile(file, colMeta, page)
		if err != nil {
			return err
		}
		n, matched, err := sumColumnRows(col, col.V.Len, nil, Predicate{})
		if err != nil {
			return err
		}
		sum, err = addInt64(sum, n)
		if err != nil {
			return err
		}
		if stats != nil {
			stats.RowsMatched += matched
		}
		return nil
	})
	return sum, err
}

func (s *Store) sumIntFromSegment(path string, segmentID SegmentID, colMeta ColumnMeta, predMetas []ColumnMeta, pred Predicate, scratch *QueryScratch) (int64, error) {
	var sum int64
	stats := scratchStats(scratch)
	if stats != nil {
		stats.SetAccess(colMeta.ColumnID, AccessRawIntSumLoop)
		for _, p := range predMetas {
			stats.SetAccess(p.ColumnID, AccessPayloadForExpr)
		}
	}
	err := s.walkSegmentPages(path, segmentID, colMeta, predMetas, stats, func(file *os.File, colPage PageMeta, pageIndex int) error {
		if scratch != nil && pred.Op != PredicateAnd && len(predMetas) == 1 && colMeta.ColumnID == predMetas[0].ColumnID {
			n, matched, err := sumIntPagePayload(file, colMeta, colPage, &scratch.page, pred)
			if err != nil {
				return err
			}
			sum, err = addInt64(sum, n)
			if err != nil {
				return err
			}
			if stats != nil {
				stats.RowsMatched += matched
			}
			return nil
		}
		if scratch != nil && pred.Op != PredicateAnd && len(predMetas) == 1 {
			predMeta := predMetas[0]
			predPage := predMeta.Pages[pageIndex]
			n, matched, ok, err := sumIntPagePayloadWithPredicate(file, colMeta, colPage, predMeta, predPage, pred, &scratch.page)
			if err != nil {
				return err
			}
			if ok {
				sum, err = addInt64(sum, n)
				if err != nil {
					return err
				}
				if stats != nil {
					stats.RowsMatched += matched
				}
				return nil
			}
		}
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
				return fmt.Errorf("segment %d sum page row mismatch", segmentID)
			}
		}
		n, matched, err := sumColumnRows(col, col.V.Len, predCols, pred)
		if err != nil {
			return err
		}
		sum, err = addInt64(sum, n)
		if err != nil {
			return err
		}
		if stats != nil {
			stats.RowsMatched += matched
		}
		return nil
	})
	return sum, err
}

func sumIntPagePayloadWithPredicate(file *os.File, col ColumnMeta, page PageMeta, predCol ColumnMeta, predPage PageMeta, pred Predicate, scratch *pageScratch) (int64, uint64, bool, error) {
	if pred.Op != PredicateOpEq || predCol.Type.Kind != sqltype.KindText {
		return 0, 0, false, nil
	}
	if predPage.Codec == CodecDictionary {
		return 0, 0, false, nil
	}
	predPayload := scratch.predicatePagePayload(int(predPage.Length))
	if _, err := file.ReadAt(predPayload, int64(predPage.Offset)); err != nil {
		return 0, 0, true, err
	}
	predValid, predPos, err := decodeValidityBytes(predPayload, predPage, predCol.Name)
	if err != nil {
		return 0, 0, true, err
	}
	predRows := int(predPage.Rows)
	offsetLen := (predRows + 1) * 4
	if len(predPayload)-predPos < offsetLen {
		return 0, 0, true, fmt.Errorf("column %q page payload is too short for text offsets", predCol.Name)
	}
	offsets := predPayload[predPos : predPos+offsetLen]
	data := predPayload[predPos+offsetLen:]

	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, 0, true, err
	}
	rows := int(page.Rows)
	if rows != predRows {
		return 0, 0, true, fmt.Errorf("sum page row mismatch")
	}
	valid, pos, err := decodeValidityBytes(payload, page, col.Name)
	if err != nil {
		return 0, 0, true, err
	}
	switch col.Type.Kind {
	case sqltype.KindInt32:
		valueLen := rows * 4
		if len(payload)-pos < valueLen {
			return 0, 0, true, fmt.Errorf("column %q page payload is too short for int32 values", col.Name)
		}
		sum, matched, err := sumInt32PayloadWithTextEq(payload[pos:pos+valueLen], valid, rows, offsets, data, predValid, pred.Text)
		return sum, matched, true, err
	case sqltype.KindInt64:
		valueLen := rows * 8
		if len(payload)-pos < valueLen {
			return 0, 0, true, fmt.Errorf("column %q page payload is too short for int64 values", col.Name)
		}
		sum, matched, err := sumInt64PayloadWithTextEq(payload[pos:pos+valueLen], valid, rows, offsets, data, predValid, pred.Text)
		return sum, matched, true, err
	default:
		return 0, 0, true, fmt.Errorf("sum column %q is %s, want int32 or int64", col.Name, col.Type)
	}
}

func sumInt64PayloadWithTextEq(values []byte, valid []byte, rows int, offsets []byte, data []byte, predValid []byte, rhs string) (int64, uint64, error) {
	var sum int64
	var matched uint64
	for row := 0; row < rows; row++ {
		if (len(valid) != 0 && !validByte(valid, row)) || !textEqPayloadRow(offsets, data, predValid, row, rhs) {
			continue
		}
		pos := row * 8
		var err error
		sum, err = addInt64(sum, int64(binary.LittleEndian.Uint64(values[pos:pos+8])))
		if err != nil {
			return 0, 0, err
		}
		matched++
	}
	return sum, matched, nil
}

func sumInt32PayloadWithTextEq(values []byte, valid []byte, rows int, offsets []byte, data []byte, predValid []byte, rhs string) (int64, uint64, error) {
	var sum int64
	var matched uint64
	for row := 0; row < rows; row++ {
		if (len(valid) != 0 && !validByte(valid, row)) || !textEqPayloadRow(offsets, data, predValid, row, rhs) {
			continue
		}
		pos := row * 4
		var err error
		sum, err = addInt64(sum, int64(int32(binary.LittleEndian.Uint32(values[pos:pos+4]))))
		if err != nil {
			return 0, 0, err
		}
		matched++
	}
	return sum, matched, nil
}

func textEqPayloadRow(offsets []byte, data []byte, valid []byte, row int, rhs string) bool {
	if len(valid) != 0 && !validByte(valid, row) {
		return false
	}
	start := int(binary.LittleEndian.Uint32(offsets[row*4:]))
	end := int(binary.LittleEndian.Uint32(offsets[(row+1)*4:]))
	if start > end || end > len(data) {
		return false
	}
	return bytesEqualString(data[start:end], rhs)
}

func sumIntPagePayload(file *os.File, col ColumnMeta, page PageMeta, scratch *pageScratch, pred Predicate) (int64, uint64, error) {
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, 0, err
	}
	valid, pos, err := decodeValidityBytes(payload, page, col.Name)
	if err != nil {
		return 0, 0, err
	}
	rows := int(page.Rows)
	switch col.Type.Kind {
	case sqltype.KindInt32:
		valueLen := rows * 4
		if len(payload)-pos < valueLen {
			return 0, 0, fmt.Errorf("column %q page payload is too short for int32 values", col.Name)
		}
		return sumInt32Payload(payload[pos:pos+valueLen], valid, rows, pred)
	case sqltype.KindInt64:
		valueLen := rows * 8
		if len(payload)-pos < valueLen {
			return 0, 0, fmt.Errorf("column %q page payload is too short for int64 values", col.Name)
		}
		return sumInt64Payload(payload[pos:pos+valueLen], valid, rows, pred)
	default:
		return 0, 0, fmt.Errorf("sum column %q is %s, want int32 or int64", col.Name, col.Type)
	}
}

func sumInt64Payload(values []byte, valid []byte, rows int, pred Predicate) (int64, uint64, error) {
	if rows == 0 {
		return 0, 0, nil
	}
	var sum int64
	var matched uint64
	for row := 0; row < rows; row++ {
		if len(valid) != 0 && !validByte(valid, row) {
			continue
		}
		pos := row * 8
		value := int64(binary.LittleEndian.Uint64(values[pos : pos+8]))
		if pred.Op != PredicateNone && !matchInt64Value(value, pred) {
			continue
		}
		var err error
		sum, err = addInt64(sum, value)
		if err != nil {
			return 0, 0, err
		}
		matched++
	}
	return sum, matched, nil
}

func sumInt32Payload(values []byte, valid []byte, rows int, pred Predicate) (int64, uint64, error) {
	if rows == 0 {
		return 0, 0, nil
	}
	var sum int64
	var matched uint64
	for row := 0; row < rows; row++ {
		if len(valid) != 0 && !validByte(valid, row) {
			continue
		}
		pos := row * 4
		value := int32(binary.LittleEndian.Uint32(values[pos : pos+4]))
		if pred.Op != PredicateNone && !matchInt32Value(value, pred) {
			continue
		}
		var err error
		sum, err = addInt64(sum, int64(value))
		if err != nil {
			return 0, 0, err
		}
		matched++
	}
	return sum, matched, nil
}

func matchInt64Value(value int64, pred Predicate) bool {
	matched, _ := matchOrdered(value, pred.Int64, pred.Lo, pred.Hi, pred.Int64s, pred.Op)
	return matched
}

func matchInt32Value(value int32, pred Predicate) bool {
	matched, _ := matchOrdered(value, pred.Int32, pred.Lo32, pred.Hi32, pred.Int32s, pred.Op)
	return matched
}

