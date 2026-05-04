// Package exec contains vectorized query execution operators.
package exec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Count returns the visible row count for a batch.
func Count(batch vector.Batch) int {
	return batch.VisibleCount()
}

// FilterInt64Equal returns a batch with a selection vector for rows where column equals value.
func FilterInt64Equal(batch vector.Batch, column string, value int64) (vector.Batch, error) {
	selected := make([]uint32, 0, min(batch.VisibleCount(), 1024))
	batch, _, err := FilterInt64EqualInto(batch, column, value, selected)
	return batch, err
}

// FilterInt64EqualInto filters rows into scratch and returns it for reuse by the caller.
func FilterInt64EqualInto(batch vector.Batch, column string, value int64, scratch []uint32) (vector.Batch, []uint32, error) {
	columnIndex, ok := batch.ColumnIndex(column)
	if !ok {
		return vector.Batch{}, scratch, fmt.Errorf("missing column %q", column)
	}
	return FilterInt64EqualAtInto(batch, columnIndex, value, scratch)
}

// FilterInt64EqualAtInto filters rows by column index into scratch and returns it for reuse by the caller.
func FilterInt64EqualAtInto(batch vector.Batch, columnIndex int, value int64, scratch []uint32) (vector.Batch, []uint32, error) {
	if columnIndex < 0 || columnIndex >= len(batch.Columns) {
		return vector.Batch{}, scratch, fmt.Errorf("column index %d out of range", columnIndex)
	}

	col := batch.Columns[columnIndex]

	values, ok := col.Vector.(vector.Int64)
	if !ok {
		return vector.Batch{}, scratch, fmt.Errorf("column %q is %s, want int64", col.Name, col.Vector.Kind())
	}
	src := values.Values
	scratch = scratch[:0]

	if batch.Sel != nil {
		for _, row := range batch.Sel {
			if int(row) >= len(src) {
				return vector.Batch{}, scratch, fmt.Errorf("selection row %d out of range for column %q", row, col.Name)
			}
			if src[row] == value {
				scratch = append(scratch, row)
			}
		}

		batch.Sel = scratch
		return batch, scratch, nil
	}

	for row, current := range src {
		if current == value {
			scratch = append(scratch, uint32(row))
		}
	}

	batch.Sel = scratch
	return batch, scratch, nil
}
