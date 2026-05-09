package storage

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type IngestBufferOptions struct {
	TargetRows int
}

// IngestBuffer owns mutable page-sized batches until they are flushed as one segment.
// It is intentionally not concurrency-safe; callers should serialize appends per table.
type IngestBuffer struct {
	store      *Store
	table      catalog.TableDef
	targetRows int
	pages      []vector.Batch
	freePages  []vector.Batch
	active     *mutablePage
	sealedRows int
}

type SealedRun struct {
	Batches []vector.Batch
	Rows    int
}

func (s *Store) NewIngestBuffer(table catalog.TableDef, opts IngestBufferOptions) (*IngestBuffer, error) {
	if s == nil {
		return nil, fmt.Errorf("storage store is nil")
	}
	if table.ID == 0 {
		return nil, fmt.Errorf("table ID is required")
	}
	for _, col := range table.Columns {
		if !supportedType(col.Type) {
			return nil, fmt.Errorf("column %q has unsupported storage type %s", col.Name, col.Type)
		}
	}
	targetRows := opts.TargetRows
	if targetRows <= 0 {
		targetRows = DefaultSegmentRows
	}
	return &IngestBuffer{store: s, table: table, targetRows: targetRows, pages: make([]vector.Batch, 0, (targetRows+DefaultPageRows-1)/DefaultPageRows)}, nil
}

func (b *IngestBuffer) Append(ctx context.Context, batch vector.Batch) ([]SegmentMeta, error) {
	runs, err := b.AppendSealed(ctx, batch)
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, nil
	}
	published := make([]SegmentMeta, 0, len(runs))
	for _, run := range runs {
		meta, err := b.store.AppendBatches(ctx, b.table, run.Batches)
		if err != nil {
			return nil, err
		}
		published = append(published, meta)
	}
	return published, nil
}

func (b *IngestBuffer) AppendSealed(ctx context.Context, batch vector.Batch) ([]SealedRun, error) {
	if b == nil || b.store == nil {
		return nil, fmt.Errorf("ingest buffer is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateBatch(b.table, batch); err != nil {
		return nil, err
	}
	// A full input page can be owned directly only when there is no active partial page.
	if b.active == nil && batch.Len == DefaultPageRows {
		owned, err := b.ownBatch(batch)
		if err != nil {
			return nil, err
		}
		b.pages = append(b.pages, owned)
		b.sealedRows += owned.Len
		if b.BufferedRows() >= b.targetRows {
			return []SealedRun{b.detachSealed()}, nil
		}
		return nil, nil
	}
	var runs []SealedRun
	for srcRow := 0; srcRow < batch.Len; {
		if b.active == nil {
			page, err := b.newMutablePage()
			if err != nil {
				return nil, err
			}
			b.active = page
		}
		rows := min(batch.Len-srcRow, DefaultPageRows-b.active.rows)
		if err := b.active.appendFrom(batch, srcRow, rows); err != nil {
			return nil, err
		}
		srcRow += rows
		if b.active.rows == DefaultPageRows {
			if err := b.sealActive(); err != nil {
				return nil, err
			}
		}
		if b.BufferedRows() >= b.targetRows {
			if b.active != nil && b.active.rows != 0 {
				if err := b.sealActive(); err != nil {
					return nil, err
				}
			}
			if len(b.pages) != 0 {
				runs = append(runs, b.detachSealed())
			}
		}
	}
	return runs, nil
}

func (b *IngestBuffer) detachSealed() SealedRun {
	run := SealedRun{Batches: b.pages, Rows: b.sealedRows}
	b.pages = make([]vector.Batch, 0, (b.targetRows+DefaultPageRows-1)/DefaultPageRows)
	b.sealedRows = 0
	return run
}

func (b *IngestBuffer) RecycleRun(run SealedRun) {
	if b == nil || len(run.Batches) == 0 {
		return
	}
	limit := (b.targetRows + DefaultPageRows - 1) / DefaultPageRows
	for _, batch := range run.Batches {
		if len(b.freePages) >= limit {
			return
		}
		b.freePages = append(b.freePages, batch)
	}
}

func (b *IngestBuffer) Flush(ctx context.Context) (SegmentMeta, bool, error) {
	if b == nil || b.store == nil {
		return SegmentMeta{}, false, fmt.Errorf("ingest buffer is nil")
	}
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, false, err
	}
	if b.active != nil && b.active.rows != 0 {
		if err := b.sealActive(); err != nil {
			return SegmentMeta{}, false, err
		}
	}
	if len(b.pages) == 0 {
		return SegmentMeta{}, false, nil
	}
	meta, err := b.store.AppendBatches(ctx, b.table, b.pages)
	if err != nil {
		return SegmentMeta{}, false, err
	}
	b.pages = b.pages[:0]
	b.sealedRows = 0
	return meta, true, nil
}

func (b *IngestBuffer) FlushSealed(ctx context.Context) (SealedRun, bool, error) {
	if b == nil || b.store == nil {
		return SealedRun{}, false, fmt.Errorf("ingest buffer is nil")
	}
	if err := ctx.Err(); err != nil {
		return SealedRun{}, false, err
	}
	if b.active != nil && b.active.rows != 0 {
		if err := b.sealActive(); err != nil {
			return SealedRun{}, false, err
		}
	}
	if len(b.pages) == 0 {
		return SealedRun{}, false, nil
	}
	return b.detachSealed(), true, nil
}

func (b *IngestBuffer) BufferedRows() int {
	if b == nil {
		return 0
	}
	rows := b.sealedRows
	if b.active != nil {
		rows += b.active.rows
	}
	return rows
}

func (b *IngestBuffer) sealActive() error {
	if b.active == nil || b.active.rows == 0 {
		return nil
	}
	batch, err := b.active.seal()
	if err != nil {
		return err
	}
	b.pages = append(b.pages, batch)
	b.sealedRows += batch.Len
	b.active = nil
	return nil
}

func (b *IngestBuffer) ownBatch(batch vector.Batch) (vector.Batch, error) {
	if len(b.freePages) == 0 {
		return cloneBatchForIngest(batch)
	}
	last := len(b.freePages) - 1
	page := b.freePages[last]
	b.freePages[last] = vector.Batch{}
	b.freePages = b.freePages[:last]
	return copyBatchForIngest(page, batch)
}

func (b *IngestBuffer) newMutablePage() (*mutablePage, error) {
	if len(b.freePages) == 0 {
		return newMutablePage(b.table)
	}
	last := len(b.freePages) - 1
	page := b.freePages[last]
	b.freePages[last] = vector.Batch{}
	b.freePages = b.freePages[:last]
	return mutablePageFromBatch(b.table, page)
}

type mutablePage struct {
	cols []vector.Column
	rows int
}

func newMutablePage(table catalog.TableDef) (*mutablePage, error) {
	cols := make([]vector.Column, len(table.Columns))
	for i, col := range table.Columns {
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return nil, err
		}
		v := vector.Vec{Kind: kind, Len: DefaultPageRows}
		switch kind {
		case vector.Bool:
			v.BoolBits = make([]uint64, vector.ValidityWords(DefaultPageRows))
		case vector.Int16:
			v.I16 = make([]int16, DefaultPageRows)
		case vector.Int32, vector.Date:
			v.I32 = make([]int32, DefaultPageRows)
		case vector.Int64, vector.Timestamp:
			v.I64 = make([]int64, DefaultPageRows)
		case vector.Float32:
			v.F32 = make([]float32, DefaultPageRows)
		case vector.Float64:
			v.F64 = make([]float64, DefaultPageRows)
		case vector.UUID:
			v.UUID = make([]vector.UUID16, DefaultPageRows)
		case vector.Enum32:
			v.U32 = make([]uint32, DefaultPageRows)
		case vector.Text, vector.Bytes:
			v.Var = vector.NewVarBytes(DefaultPageRows, DefaultPageRows*8)
		default:
			return nil, fmt.Errorf("unsupported ingest buffer type %s", col.Type)
		}
		cols[i] = vector.Column{Name: col.Name, Type: col.Type, EnumLabels: append([]string(nil), col.Labels...), V: v}
	}
	return &mutablePage{cols: cols}, nil
}

func mutablePageFromBatch(table catalog.TableDef, batch vector.Batch) (*mutablePage, error) {
	if len(batch.Columns) != len(table.Columns) {
		return newMutablePage(table)
	}
	cols := batch.Columns[:len(table.Columns)]
	for i, col := range table.Columns {
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return nil, err
		}
		v := &cols[i].V
		cols[i].Name = col.Name
		cols[i].Type = col.Type
		cols[i].EnumLabels = append(cols[i].EnumLabels[:0], col.Labels...)
		v.Kind = kind
		v.Len = DefaultPageRows
		v.Valid = nil
		switch kind {
		case vector.Bool:
			// Bool pages set bits with OR during append, so recycled words must start clear.
			v.BoolBits = ensureLen(v.BoolBits, vector.ValidityWords(DefaultPageRows))
			clear(v.BoolBits)
		case vector.Int16:
			v.I16 = ensureLen(v.I16, DefaultPageRows)
		case vector.Int32, vector.Date:
			v.I32 = ensureLen(v.I32, DefaultPageRows)
		case vector.Int64, vector.Timestamp:
			v.I64 = ensureLen(v.I64, DefaultPageRows)
		case vector.Float32:
			v.F32 = ensureLen(v.F32, DefaultPageRows)
		case vector.Float64:
			v.F64 = ensureLen(v.F64, DefaultPageRows)
		case vector.UUID:
			v.UUID = ensureLen(v.UUID, DefaultPageRows)
		case vector.Enum32:
			v.U32 = ensureLen(v.U32, DefaultPageRows)
		case vector.Text, vector.Bytes:
			v.Var.Offsets = ensureLen(v.Var.Offsets, DefaultPageRows+1)
			v.Var.Offsets[0] = 0
			v.Var.Data = v.Var.Data[:0]
		default:
			return nil, fmt.Errorf("unsupported ingest buffer type %s", col.Type)
		}
	}
	return &mutablePage{cols: cols}, nil
}

func (p *mutablePage) appendFrom(batch vector.Batch, srcStart int, rows int) error {
	if rows < 0 || p.rows+rows > DefaultPageRows || srcStart+rows > batch.Len {
		return fmt.Errorf("invalid ingest buffer append range")
	}
	for colIndex := range p.cols {
		dst := &p.cols[colIndex]
		src := batch.Columns[colIndex]
		copyValidity(&dst.V, src.V.Valid, p.rows, srcStart, rows)
		switch dst.V.Kind {
		case vector.Bool:
			for row := 0; row < rows; row++ {
				if boolAt(src.V.BoolBits, srcStart+row) {
					dstRow := p.rows + row
					dst.V.BoolBits[dstRow>>6] |= uint64(1) << uint(dstRow&63)
				}
			}
		case vector.Int16:
			copy(dst.V.I16[p.rows:p.rows+rows], src.V.I16[srcStart:srcStart+rows])
		case vector.Int32, vector.Date:
			copy(dst.V.I32[p.rows:p.rows+rows], src.V.I32[srcStart:srcStart+rows])
		case vector.Int64, vector.Timestamp:
			copy(dst.V.I64[p.rows:p.rows+rows], src.V.I64[srcStart:srcStart+rows])
		case vector.Float32:
			copy(dst.V.F32[p.rows:p.rows+rows], src.V.F32[srcStart:srcStart+rows])
		case vector.Float64:
			copy(dst.V.F64[p.rows:p.rows+rows], src.V.F64[srcStart:srcStart+rows])
		case vector.UUID:
			copy(dst.V.UUID[p.rows:p.rows+rows], src.V.UUID[srcStart:srcStart+rows])
		case vector.Enum32:
			copy(dst.V.U32[p.rows:p.rows+rows], src.V.U32[srcStart:srcStart+rows])
		case vector.Text, vector.Bytes:
			appendVarBytes(&dst.V.Var, src.V.Var, src.V.Valid, p.rows, srcStart, rows)
		default:
			return fmt.Errorf("unsupported ingest buffer vector kind %s", dst.V.Kind)
		}
	}
	p.rows += rows
	return nil
}

func (p *mutablePage) seal() (vector.Batch, error) {
	cols := make([]vector.Column, len(p.cols))
	for i, col := range p.cols {
		v := col.V
		v.Len = p.rows
		if v.Valid != nil {
			v.Valid = v.Valid[:vector.ValidityWords(p.rows)]
		}
		switch v.Kind {
		case vector.Bool:
			v.BoolBits = v.BoolBits[:vector.ValidityWords(p.rows)]
		case vector.Int16:
			v.I16 = v.I16[:p.rows]
		case vector.Int32, vector.Date:
			v.I32 = v.I32[:p.rows]
		case vector.Int64, vector.Timestamp:
			v.I64 = v.I64[:p.rows]
		case vector.Float32:
			v.F32 = v.F32[:p.rows]
		case vector.Float64:
			v.F64 = v.F64[:p.rows]
		case vector.UUID:
			v.UUID = v.UUID[:p.rows]
		case vector.Enum32:
			v.U32 = v.U32[:p.rows]
		case vector.Text, vector.Bytes:
			v.Var.Offsets = v.Var.Offsets[:p.rows+1]
		}
		cols[i] = vector.Column{Name: col.Name, Type: col.Type, EnumLabels: append([]string(nil), col.EnumLabels...), V: v}
	}
	return vector.NewBatch(cols)
}
