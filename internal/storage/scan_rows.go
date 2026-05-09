package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const secondsPerDay = int64(24 * time.Hour / time.Second)

const timestampLayout = "2006-01-02T15:04:05.000000000Z"

func (s *Store) ScanRows(ctx context.Context, table catalog.TableDef, columnIDs []catalog.ColumnID, pred Predicate) ([][]any, error) {
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

	if err := waitBufferIdle(state); err != nil {
		return nil, err
	}
	projectIndexes := make([]int, 0, len(columnIDs))
	for _, id := range columnIDs {
		_, index, err := requireColumn(state, table, id, "projected")
		if err != nil {
			return nil, err
		}
		projectIndexes = append(projectIndexes, index)
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return nil, err
	}
	bufferRows := make([][]any, 0)
	if err := appendScanRowsFromBuffer(state.buffer, projectIndexes, predIndexes, pred, &bufferRows); err != nil {
		state.bufferMu.Unlock()
		return nil, err
	}
	dir, segments := snapshotSegments(state)

	rows := make([][]any, 0)
	for _, segment := range segments {
		path := segment.AbsPath(dir)
		file, err := s.files.Acquire(path)
		if err != nil {
			return nil, err
		}
		for pageIndex := 0; pageIndex < segmentPageCount(segment); pageIndex++ {
			projectCols := make([]vector.Column, 0, len(columnIDs))
			for _, id := range columnIDs {
				meta, _, ok := columnMetaByID(segment, id)
				if !ok {
					_ = s.files.Release(path)
					return nil, fmt.Errorf("segment %d missing projected column ID %d", segment.ID, id)
				}
				col, err := readColumnPageFromFile(file, meta, meta.Pages[pageIndex])
				if err != nil {
					_ = s.files.Release(path)
					return nil, err
				}
				projectCols = append(projectCols, col)
			}
			predMetas, err := predicateColumnMetas(segment, pred)
			if err != nil {
				_ = s.files.Release(path)
				return nil, err
			}
			predCols, err := readPredicateColumns(file, predMetas, pageIndex)
			if err != nil {
				_ = s.files.Release(path)
				return nil, err
			}
			if err := appendScanRowsFromColumns(projectCols, predCols, pred, &rows, projectCols[0].V.Len); err != nil {
				_ = s.files.Release(path)
				return nil, err
			}
		}
		if err := s.files.Release(path); err != nil {
			return nil, err
		}
	}
	rows = append(rows, bufferRows...)
	return rows, nil
}

func appendScanRowsFromBuffer(buffer *IngestBuffer, projectIndexes []int, predIndexes []int, pred Predicate, rows *[][]any) error {
	return forEachBufferBatch(buffer, func(batch vector.Batch, rowCount int) error {
		return appendScanRowsFromBatch(batch, projectIndexes, predIndexes, pred, rows, rowCount)
	})
}

func appendScanRowsFromBatch(batch vector.Batch, projectIndexes []int, predIndexes []int, pred Predicate, rows *[][]any, rowCount int) error {
	projectCols := make([]vector.Column, 0, len(projectIndexes))
	for _, index := range projectIndexes {
		if index >= len(batch.Columns) {
			return fmt.Errorf("missing projected column index %d", index)
		}
		projectCols = append(projectCols, batch.Columns[index])
	}
	predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
	if err != nil {
		return err
	}
	return appendScanRowsFromColumns(projectCols, predCols, pred, rows, rowCount)
}

func appendScanRowsFromColumns(projectCols []vector.Column, predCols []vector.Column, pred Predicate, rows *[][]any, rowCount int) error {
	for row := 0; row < rowCount; row++ {
		if pred.Op != PredicateNone {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if !matched {
				continue
			}
		}
		out := make([]any, len(projectCols))
		for i, col := range projectCols {
			value, err := scanValueAt(col, row)
			if err != nil {
				return err
			}
			out[i] = value
		}
		*rows = append(*rows, out)
	}
	return nil
}

func scanValueAt(col vector.Column, row int) (any, error) {
	if !vector.IsValid(col.V.Valid, row) {
		return nil, nil
	}
	switch col.Type.Kind {
	case sqltype.KindBool:
		return boolAt(col.V.BoolBits, row), nil
	case sqltype.KindInt16:
		return col.V.I16[row], nil
	case sqltype.KindInt32:
		return col.V.I32[row], nil
	case sqltype.KindDate:
		return formatDateDays(col.V.I32[row]), nil
	case sqltype.KindInt64:
		return col.V.I64[row], nil
	case sqltype.KindFloat32:
		return col.V.F32[row], nil
	case sqltype.KindFloat64:
		return col.V.F64[row], nil
	case sqltype.KindTimestamp:
		return formatTimestampNanos(col.V.I64[row]), nil
	case sqltype.KindNamed:
		label, ok := enumLabelForCode(col.V.U32[row], col.EnumLabels)
		if !ok {
			return nil, fmt.Errorf("scan column %q has invalid enum code %d", col.Name, col.V.U32[row])
		}
		return label, nil
	case sqltype.KindUUID:
		return vector.FormatUUID(col.V.UUID[row]), nil
	case sqltype.KindBytes:
		return string(col.V.Var.Bytes(row)), nil
	case sqltype.KindText:
		return col.V.Var.String(row), nil
	default:
		return nil, fmt.Errorf("scan column %q is %s, want bool, int16, int32, int64, float32, float64, date, timestamp, enum, uuid, bytes, or text", col.Name, col.Type)
	}
}

func enumLabelForCode(code uint32, labels []string) (string, bool) {
	if code == 0 || int(code) > len(labels) {
		return "", false
	}
	return labels[code-1], true
}

func formatDateDays(days int32) string {
	return time.Unix(int64(days)*secondsPerDay, 0).UTC().Format("2006-01-02")
}

func formatTimestampNanos(nanos int64) string {
	return time.Unix(0, nanos).UTC().Format(timestampLayout)
}
