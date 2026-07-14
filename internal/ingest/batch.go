// BatchBuilder helps construct vector.Batch instances from columnar data.
// Callers provide a BoundTableDef once, then set columns by name and Build().
package ingest

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// BatchBuilder builds a single vector.Batch (one page) matching a table schema.
type BatchBuilder struct {
	def    sql.BoundTableDef
	cols   map[string]*vector.Column
	rowCnt int
}

// NewBatchBuilder creates a builder for the given table schema.
func NewBatchBuilder(def sql.BoundTableDef) *BatchBuilder {
	cols := make(map[string]*vector.Column, len(def.Columns))
	for _, c := range def.Columns {
		v, _ := vector.NewVecForKind(vector.VecKindOfMust(c.Type), 0)
		cols[schema.NormalizeName(c.Name)] = &vector.Column{
			Name:       c.Name,
			Type:       c.Type,
			EnumLabels: c.Labels,
			V:          v,
		}
	}
	return &BatchBuilder{
		def:  def,
		cols: cols,
	}
}

// Reset prepares the builder for a new batch of the given row count.
func (b *BatchBuilder) Reset(rows int) {
	b.rowCnt = rows
	for _, col := range b.cols {
		vk := vector.VecKindOfMust(col.Type)
		col.V, _ = vector.NewVecForKind(vk, rows)
		if col.V.Valid != nil {
			col.V.Valid = nil
		}
	}
}

// SetRowCount sets the expected row count (must call before adding data).
func (b *BatchBuilder) SetRowCount(rows int) {
	b.rowCnt = rows
}

// Int64 sets int64/timestamp/time/decimal64 column data from a slice.
// slice len must match row count.
func (b *BatchBuilder) Int64(name string, vals []int64) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.I64()
	copy(dst, vals)
	return b
}

// Int32 sets int32/date column data.
func (b *BatchBuilder) Int32(name string, vals []int32) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.I32()
	copy(dst, vals)
	return b
}

// Int16 sets int16 column data.
func (b *BatchBuilder) Int16(name string, vals []int16) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.I16()
	copy(dst, vals)
	return b
}

// Float64 sets float64 column data.
func (b *BatchBuilder) Float64(name string, vals []float64) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.F64()
	copy(dst, vals)
	return b
}

// Float32 sets float32 column data.
func (b *BatchBuilder) Float32(name string, vals []float32) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.F32()
	copy(dst, vals)
	return b
}

// Bool sets boolean column data.
func (b *BatchBuilder) Bool(name string, vals []bool) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.BoolBits()
	for i, v := range vals {
		if v {
			dst[i>>3] |= 1 << (i & 7)
		}
	}
	return b
}

// Text sets text/bytes/JSON column data from strings.
func (b *BatchBuilder) Text(name string, vals []string) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	vb := col.V.Var()
	for i, s := range vals {
		vb.AppendString(i, s)
	}
	return b
}

// Enum32 sets enum column data from code slices.
func (b *BatchBuilder) Enum32(name string, vals []uint32) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.U32()
	copy(dst, vals)
	return b
}

// UUID sets UUID column data from UUID16 slices.
func (b *BatchBuilder) UUID(name string, vals []vector.UUID16) *BatchBuilder {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if len(vals) != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q: got %d values for %d rows", name, len(vals), b.rowCnt))
	}
	dst := col.V.UUID()
	copy(dst, vals)
	return b
}

// Build validates and returns the batch.
func (b *BatchBuilder) Build() (vector.Batch, error) {
	cols := make([]vector.Column, 0, len(b.def.Columns))
	for _, defCol := range b.def.Columns {
		col, ok := b.cols[schema.NormalizeName(defCol.Name)]
		if !ok {
			return vector.Batch{}, fmt.Errorf("ingest: missing column %q", defCol.Name)
		}
		if int(col.V.Len) != b.rowCnt {
			return vector.Batch{}, fmt.Errorf("ingest: column %q vec len %d != row count %d", defCol.Name, col.V.Len, b.rowCnt)
		}
		cols = append(cols, *col)
	}
	return vector.NewBatch(cols)
}