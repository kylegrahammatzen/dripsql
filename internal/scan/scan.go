// Package scan ties storage segments to vectorized execution operators.
package scan

import (
	"bytes"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// CountInt64EqualSegment counts rows in one encoded segment where column equals value.
func CountInt64EqualSegment(data []byte, column string, value int64, scratch []uint32) (int, []uint32, error) {
	colStats, ok, err := storage.ReadSegmentColumnStats(bytes.NewReader(data), column)
	if err != nil {
		return 0, scratch, err
	}
	if !ok {
		return 0, scratch, fmt.Errorf("missing column %q", column)
	}
	if storage.CanSkipInt64Equal(colStats, value) {
		return 0, scratch[:0], nil
	}

	batch, _, err := storage.ReadSegment(bytes.NewReader(data))
	if err != nil {
		return 0, scratch, err
	}
	columnIndex, ok := batch.ColumnIndex(column)
	if !ok {
		return 0, scratch, fmt.Errorf("missing column %q", column)
	}

	filtered, scratch, err := exec.FilterInt64EqualAtInto(batch, columnIndex, value, scratch)
	if err != nil {
		return 0, scratch, err
	}
	return exec.Count(filtered), scratch, nil
}
