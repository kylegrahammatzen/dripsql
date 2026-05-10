package storage

import (
	"context"
	"fmt"
	"slices"

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
	if b == nil || b.store == nil {
		return nil, fmt.Errorf("ingest buffer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if clone {
		if err := b.appendCloned(batch); err != nil {
			return nil, err
		}
	} else {
		if err := b.appendBorrowed(batch); err != nil {
			return nil, err
		}
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
	if b == nil || b.store == nil {
		return SegmentMeta{}, false, fmt.Errorf("ingest buffer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
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

func (b *IngestBuffer) appendCloned(batch types.Batch) error {
	if err := validateStoreBatch(b.table, batch); err != nil {
		return err
	}
	owned, err := cloneBatch(batch)
	if err != nil {
		return err
	}
	b.batches = append(b.batches, owned)
	b.rows += owned.Len
	return nil
}

func (b *IngestBuffer) appendBorrowed(batch types.Batch) error {
	if err := validateStoreBatch(b.table, batch); err != nil {
		return err
	}
	b.batches = append(b.batches, batch)
	b.rows += batch.Len
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
		cols[i] = types.Column{Name: col.Name, Type: col.Type, EnumLabels: slices.Clone(col.EnumLabels), V: cloneVec(col.V)}
	}
	out, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, err
	}
	return out, nil
}

func cloneVec(v types.Vec) types.Vec {
	out := v
	out.Valid = slices.Clone(v.Valid)
	out.BoolBits = slices.Clone(v.BoolBits)
	out.I16 = slices.Clone(v.I16)
	out.I32 = slices.Clone(v.I32)
	out.I64 = slices.Clone(v.I64)
	out.F32 = slices.Clone(v.F32)
	out.F64 = slices.Clone(v.F64)
	out.UUID = slices.Clone(v.UUID)
	out.U32 = slices.Clone(v.U32)
	out.Var = cloneVarBytes(v.Var)
	out.DictIDs = slices.Clone(v.DictIDs)
	out.DictValues = cloneVarBytes(v.DictValues)
	out.ConstantBytes = slices.Clone(v.ConstantBytes)
	out.FORData = slices.Clone(v.FORData)
	out.Runs = cloneRuns(v.Runs)
	return out
}

func cloneVarBytes(v types.VarBytes) types.VarBytes {
	return types.VarBytes{Offsets: slices.Clone(v.Offsets), Data: slices.Clone(v.Data)}
}

func cloneRuns(runs []types.Run) []types.Run {
	if len(runs) == 0 {
		return nil
	}
	out := make([]types.Run, len(runs))
	for i, run := range runs {
		out[i] = run
		out[i].Bytes = slices.Clone(run.Bytes)
	}
	return out
}
