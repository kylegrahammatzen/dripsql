// Package exec contains vectorized query execution operators.
package exec

import (
	"fmt"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Count returns the visible row count for a batch.
func Count(batch vector.Batch) int {
	return batch.VisibleCount()
}

// FilterInt64Equal returns a batch with a selection vector for rows where column equals value.
func FilterInt64Equal(batch vector.Batch, column string, value int64) (vector.Batch, error) {
	columnIndex, ok := batch.ColumnIndex(column)
	if !ok {
		return vector.Batch{}, fmt.Errorf("missing column %q", column)
	}

	col := batch.Columns[columnIndex]
	values, ok := col.Vector.(vector.Int64)
	if !ok {
		return vector.Batch{}, fmt.Errorf("column %q is %s, want int64", col.Name, col.Vector.Kind())
	}
	src := values.Values
	maxSelected := batch.VisibleCount()
	selected := make([]uint32, 0, min(maxSelected, 1024))

	if batch.Sel != nil {
		if err := validateSelection(batch.Sel, len(src), col.Name); err != nil {
			return vector.Batch{}, err
		}
		for _, row := range batch.Sel {
			if src[row] == value {
				selected = appendSelected(selected, row, maxSelected)
			}
		}
		batch.Sel = selected
		return batch, nil
	}

	for row, current := range src {
		if current == value {
			selected = appendSelected(selected, uint32(row), maxSelected)
		}
	}
	batch.Sel = selected
	return batch, nil
}

// FilterInt64EqualInto filters rows into scratch and returns it for reuse by the caller.
// For repeated filters on the same column, resolve the column index once and call FilterInt64EqualAtInto.
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
		if err := validateSelection(batch.Sel, len(src), col.Name); err != nil {
			return vector.Batch{}, scratch, err
		}

		for _, row := range batch.Sel {
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

func validateSelection(sel []uint32, valuesLen int, columnName string) error {
	if len(sel) == 0 {
		return nil
	}
	maxRow := slices.Max(sel)
	if uint64(maxRow) >= uint64(valuesLen) {
		return fmt.Errorf("selection row %d out of range for column %q", maxRow, columnName)
	}
	return nil
}

func appendSelected(selected []uint32, row uint32, maxCap int) []uint32 {
	if len(selected) == cap(selected) && cap(selected) < maxCap {
		grown := make([]uint32, len(selected), maxCap)
		copy(grown, selected)
		selected = grown
	}
	return append(selected, row)
}
