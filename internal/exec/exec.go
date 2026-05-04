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
	col, ok := batch.Column(column)
	if !ok {
		return vector.Batch{}, fmt.Errorf("missing column %q", column)
	}

	values, ok := col.Vector.(vector.Int64)
	if !ok {
		return vector.Batch{}, fmt.Errorf("column %q is %s, want int64", column, col.Vector.Kind())
	}

	if batch.Sel != nil {
		selected := make([]uint32, 0, len(batch.Sel))
		for _, row := range batch.Sel {
			if int(row) >= len(values.Values) {
				return vector.Batch{}, fmt.Errorf("selection row %d out of range for column %q", row, column)
			}
			if values.Values[row] == value {
				selected = append(selected, row)
			}
		}

		batch.Sel = selected
		return batch, nil
	}

	selected := make([]uint32, 0, min(len(values.Values), 1024))
	for row, current := range values.Values {
		if current == value {
			selected = append(selected, uint32(row))
		}
	}

	batch.Sel = selected
	return batch, nil
}
