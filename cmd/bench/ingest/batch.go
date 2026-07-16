// BatchBuilder constructs vector.Batch pages matching a table schema for the bulk ingest path.
// Callers provide a BoundTableDef once, then Reset per page, set columns by name, and Build.
package ingest

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type BatchBuilder struct {
	def    sql.BoundTableDef
	cols   map[string]*vector.Column
	rowCnt int
}

func NewBatchBuilder(def sql.BoundTableDef) *BatchBuilder {
	cols := make(map[string]*vector.Column, len(def.Columns))
	for _, c := range def.Columns {
		cols[schema.NormalizeName(c.Name)] = &vector.Column{
			Name:       c.Name,
			Type:       c.Type,
			EnumLabels: c.Labels,
		}
	}
	return &BatchBuilder{def: def, cols: cols}
}

// Reset allocates fresh vectors because previously built batches keep their backing arrays until ingest.
func (b *BatchBuilder) Reset(rows int) {
	b.rowCnt = rows
	for _, col := range b.cols {
		col.V, _ = vector.NewVecForKind(vector.VecKindOfMust(col.Type), rows)
		col.V.Valid = nil
	}
}

func (b *BatchBuilder) col(name string, n int) *vector.Column {
	col, ok := b.cols[schema.NormalizeName(name)]
	if !ok {
		panic(fmt.Sprintf("ingest: column %q not in table %q", name, b.def.Name))
	}
	if n != b.rowCnt {
		panic(fmt.Sprintf("ingest: column %q got %d values for %d rows", name, n, b.rowCnt))
	}
	return col
}

func (b *BatchBuilder) Int64(name string, vals []int64) *BatchBuilder {
	copy(b.col(name, len(vals)).V.I64(), vals)
	return b
}

func (b *BatchBuilder) Int32(name string, vals []int32) *BatchBuilder {
	copy(b.col(name, len(vals)).V.I32(), vals)
	return b
}

func (b *BatchBuilder) Int16(name string, vals []int16) *BatchBuilder {
	copy(b.col(name, len(vals)).V.I16(), vals)
	return b
}

func (b *BatchBuilder) Float64(name string, vals []float64) *BatchBuilder {
	copy(b.col(name, len(vals)).V.F64(), vals)
	return b
}

func (b *BatchBuilder) Float32(name string, vals []float32) *BatchBuilder {
	copy(b.col(name, len(vals)).V.F32(), vals)
	return b
}

func (b *BatchBuilder) Bool(name string, vals []bool) *BatchBuilder {
	dst := b.col(name, len(vals)).V.BoolBits()
	for i, v := range vals {
		if v {
			dst[i>>3] |= 1 << (i & 7)
		}
	}
	return b
}

func (b *BatchBuilder) Text(name string, vals []string) *BatchBuilder {
	vb := b.col(name, len(vals)).V.Var()
	for i, s := range vals {
		vb.AppendString(i, s)
	}
	return b
}

func (b *BatchBuilder) UUID(name string, vals []vector.UUID16) *BatchBuilder {
	copy(b.col(name, len(vals)).V.UUID(), vals)
	return b
}

func (b *BatchBuilder) Build() (vector.Batch, error) {
	cols := make([]vector.Column, 0, len(b.def.Columns))
	for _, defCol := range b.def.Columns {
		col := b.cols[schema.NormalizeName(defCol.Name)]
		if int(col.V.Len) != b.rowCnt {
			return vector.Batch{}, fmt.Errorf("ingest: column %q vec len %d, row count %d", defCol.Name, col.V.Len, b.rowCnt)
		}
		cols = append(cols, *col)
	}
	return vector.NewBatch(cols)
}
