package storage

import (
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// forEachBufferBatch invokes visit for every non-empty batch in buffer (the
// sealed pages plus the active page). Stops on the first error returned by
// visit. The callback receives the batch along with the row count to process —
// for sealed pages this equals batch.Len, but the active page must be capped
// to its current row count, which is below the batch capacity.
//
// This is the canonical buffer iteration shape across all aggregate paths
// (count, sum, min/max, group counts/sums, scan_rows). Each caller's per-row
// work runs inside the closure, so dispatch overhead is amortized at batch
// granularity (StandardBatchRows = 65536 rows per call) rather than per row.
func forEachBufferBatch(buffer *IngestBuffer, visit func(batch vector.Batch, rows int) error) error {
	if buffer == nil {
		return nil
	}
	for _, batch := range buffer.pages {
		if batch.Len == 0 {
			continue
		}
		if err := visit(batch, batch.Len); err != nil {
			return err
		}
	}
	if buffer.active != nil && buffer.active.rows != 0 {
		batch := vector.Batch{Columns: buffer.active.cols, Len: buffer.active.rows}
		if err := visit(batch, buffer.active.rows); err != nil {
			return err
		}
	}
	return nil
}
