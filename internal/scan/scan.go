// Package scan ties storage segments to vectorized execution operators.
package scan

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// CountInt64EqualSegment counts rows in one encoded segment where column equals value.
func CountInt64EqualSegment(data []byte, column string, value int64) (int, error) {
	count, ok, err := storage.CountSegmentInt64EqualBytes(data, column, value)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing column %q", column)
	}
	return count, nil
}
