package storage

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// countBufferedRows returns the number of buffered rows matching pred.
// PredicateNone takes the BufferedRows fast path; everything else iterates.
func countBufferedRows(table catalog.TableDef, buffer *IngestBuffer, pred Predicate) (uint64, error) {
	if buffer == nil {
		return 0, nil
	}
	if pred.Op == PredicateNone {
		return uint64(buffer.BufferedRows()), nil
	}
	var count uint64
	err := forEachBufferBatch(buffer, func(batch vector.Batch, rows int) error {
		n, err := countColumnsPredicate(table, batch.Columns, rows, pred)
		if err != nil {
			return err
		}
		count += uint64(n)
		return nil
	})
	return count, err
}

// countColumnsPredicate counts rows in cols matching pred. Compound predicates
// fan out via matchPredicateColumns; single-column predicates first try a
// hand-tuned fast path and fall back to the generic matchPredicateRow loop.
func countColumnsPredicate(table catalog.TableDef, cols []vector.Column, rows int, pred Predicate) (int, error) {
	if pred.Op == PredicateAnd || pred.Op == PredicateOr || pred.Op == PredicateNot {
		if err := validatePredicate(table, pred); err != nil {
			return 0, err
		}
		indexes, err := predicateColumnIndexes(table, pred)
		if err != nil {
			return 0, err
		}
		predCols, err := predicateColumnsFromBatch(vector.Batch{Columns: cols, Len: rows}, indexes, pred)
		if err != nil {
			return 0, err
		}
		count := 0
		for row := 0; row < rows; row++ {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return 0, err
			}
			if matched {
				count++
			}
		}
		return count, nil
	}

	colDef, colIndex, ok := tableColumnByID(table, pred.ColumnID)
	if !ok {
		return 0, fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
	}
	if err := validatePredicateColumn(colDef, pred); err != nil {
		return 0, err
	}
	if colIndex >= len(cols) {
		return 0, fmt.Errorf("missing buffered predicate column ID %d", pred.ColumnID)
	}
	col := cols[colIndex]
	valid := col.V.Valid

	// Fast paths for the common shapes; everything else falls through to the
	// generic matchPredicateRow loop below.
	if pred.Op == PredicateOpEq {
		switch col.Type.Kind {
		case sqltype.KindBool:
			count := 0
			for row := 0; row < rows; row++ {
				if vector.IsValid(valid, row) && boolAt(col.V.BoolBits, row) == pred.Bool {
					count++
				}
			}
			return count, nil
		case sqltype.KindInt32, sqltype.KindDate:
			count := 0
			for row := 0; row < rows; row++ {
				if !vector.IsValid(valid, row) {
					continue
				}
				value, err := predicateInt32Value(col, row)
				if err != nil {
					return 0, err
				}
				if value == pred.Int32 {
					count++
				}
			}
			return count, nil
		case sqltype.KindInt64, sqltype.KindTimestamp:
			count := 0
			for row := 0; row < rows; row++ {
				if vector.IsValid(valid, row) && col.V.I64[row] == pred.Int64 {
					count++
				}
			}
			return count, nil
		case sqltype.KindText:
			count := 0
			for row := 0; row < rows; row++ {
				if vector.IsValid(valid, row) && bytesEqualString(col.V.Var.Bytes(row), pred.Text) {
					count++
				}
			}
			return count, nil
		}
	}
	if pred.Op == PredicateOpBetween {
		switch col.Type.Kind {
		case sqltype.KindInt32, sqltype.KindDate:
			count := 0
			for row := 0; row < rows; row++ {
				if !vector.IsValid(valid, row) {
					continue
				}
				value, err := predicateInt32Value(col, row)
				if err != nil {
					return 0, err
				}
				if pred.Lo32 <= value && value <= pred.Hi32 {
					count++
				}
			}
			return count, nil
		case sqltype.KindInt64, sqltype.KindTimestamp:
			count := 0
			for row := 0; row < rows; row++ {
				if !vector.IsValid(valid, row) {
					continue
				}
				value := col.V.I64[row]
				if pred.Lo <= value && value <= pred.Hi {
					count++
				}
			}
			return count, nil
		}
	}

	count := 0
	for row := 0; row < rows; row++ {
		matched, err := matchPredicateRow(col, row, pred)
		if err != nil {
			return 0, err
		}
		if matched {
			count++
		}
	}
	return count, nil
}
