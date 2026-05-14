package storage

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type IngestBufferOptions struct {
	TargetRows int
}

type IngestBuffer struct {
	store      *Store
	table      types.TableSpec
	targetRows int
	batches    []types.Batch
	rows       int
}

func (s *Store) NewIngestBuffer(table types.TableSpec, opts IngestBufferOptions) (*IngestBuffer, error) {
	if s == nil {
		return nil, fmt.Errorf("storage store is nil")
	}
	if err := table.Validate(); err != nil {
		return nil, err
	}
	targetRows := opts.TargetRows
	if targetRows <= 0 {
		targetRows = targetRowsForTable(table)
	}
	return &IngestBuffer{store: s, table: table, targetRows: targetRows, batches: make([]types.Batch, 0, (targetRows+DefaultPageRows-1)/DefaultPageRows)}, nil
}

// Append clones the batch into the buffer; the caller may continue to
// mutate or reuse batch after this call returns. Use AppendOwned when
// the caller can transfer ownership (no copy — much faster).
func (b *IngestBuffer) Append(ctx context.Context, batch types.Batch) ([]SegmentMeta, error) {
	return b.append(ctx, batch, true)
}

// AppendOwned takes ownership of batch — the caller MUST NOT mutate or
// reuse the batch (or any slice it points into) after this call. Skips
// the per-Vec slice clone that Append performs, which is the dominant
// cost on the ingest hot path.
func (b *IngestBuffer) AppendOwned(ctx context.Context, batch types.Batch) ([]SegmentMeta, error) {
	return b.append(ctx, batch, false)
}

func (b *IngestBuffer) append(ctx context.Context, batch types.Batch, clone bool) ([]SegmentMeta, error) {
	ctx, err := b.startOp(ctx)
	if err != nil {
		return nil, err
	}
	if err := b.appendInternal(batch, clone); err != nil {
		return nil, err
	}
	if b.rows < b.targetRows {
		return nil, nil
	}
	meta, ok, err := b.Flush(ctx)
	if err != nil || !ok {
		return nil, err
	}
	return []SegmentMeta{meta}, nil
}

func (b *IngestBuffer) Flush(ctx context.Context) (SegmentMeta, bool, error) {
	ctx, err := b.startOp(ctx)
	if err != nil {
		return SegmentMeta{}, false, err
	}
	if b.rows == 0 {
		return SegmentMeta{}, false, nil
	}
	meta, err := b.store.AppendBatches(ctx, b.table, b.batches)
	if err != nil {
		return SegmentMeta{}, false, err
	}
	b.clear()
	return meta, true, nil
}

func (b *IngestBuffer) BufferedRows() int {
	if b == nil {
		return 0
	}
	return b.rows
}

func (b *IngestBuffer) startOp(ctx context.Context) (context.Context, error) {
	if b == nil || b.store == nil {
		return nil, fmt.Errorf("ingest buffer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return ctx, nil
}

func (b *IngestBuffer) appendInternal(batch types.Batch, clone bool) error {
	if err := validateStoreBatch(b.table, batch); err != nil {
		return err
	}
	owned := batch
	if clone {
		var err error
		owned, err = cloneBatch(batch)
		if err != nil {
			return err
		}
	}
	b.batches = append(b.batches, owned)
	b.rows += owned.Len
	return nil
}

func (b *IngestBuffer) clear() {
	clear(b.batches)
	b.batches = b.batches[:0]
	b.rows = 0
}

func (b *IngestBuffer) detach() []types.Batch {
	batches := append([]types.Batch(nil), b.batches...)
	b.clear()
	return batches
}

func targetRowsForTable(table types.TableSpec) int {
	if table.Options.SegmentRows.Rows > 0 {
		return table.Options.SegmentRows.Rows
	}
	return DefaultSegmentRows
}

func cloneBatch(batch types.Batch) (types.Batch, error) {
	cols := make([]types.Column, len(batch.Columns))
	for i, col := range batch.Columns {
		cols[i] = types.Column{Name: col.Name, Type: col.Type, EnumLabels: col.EnumLabels, V: col.V.Clone()}
	}
	out, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, err
	}
	return out, nil
}
