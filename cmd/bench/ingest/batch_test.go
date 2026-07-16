// BatchBuilder contract, built batches match the table schema and keep their data across Reset.
package ingest

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
)

func testDef() sql.BoundTableDef {
	return sql.BoundTableDef{
		Name: "t",
		Columns: []sql.BoundColumnDef{
			{Name: "id", Type: schema.Int64},
			{Name: "name", Type: schema.Text},
			{Name: "price", Type: schema.Float64},
		},
	}
}

func TestBatchBuilder_BuildMatchesSchema(t *testing.T) {
	b := NewBatchBuilder(testDef())
	b.Reset(3)
	b.Int64("id", []int64{1, 2, 3}).Text("name", []string{"a", "b", "c"}).Float64("price", []float64{1.5, 2.5, 3.5})
	batch, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if batch.Len != 3 || len(batch.Columns) != 3 {
		t.Fatalf("batch shape %d rows %d cols", batch.Len, len(batch.Columns))
	}
	if got := batch.Columns[0].V.I64(); got[0] != 1 || got[2] != 3 {
		t.Errorf("id column = %v", got)
	}
	if got := batch.Columns[1].V.Var().String(1); got != "b" {
		t.Errorf("name[1] = %q", got)
	}
}

// A built batch must keep its backing arrays when the builder is reset for the next page.
func TestBatchBuilder_ResetDoesNotAliasBuiltBatch(t *testing.T) {
	b := NewBatchBuilder(testDef())
	b.Reset(1)
	b.Int64("id", []int64{7}).Text("name", []string{"x"}).Float64("price", []float64{9.9})
	first, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	b.Reset(1)
	b.Int64("id", []int64{8}).Text("name", []string{"y"}).Float64("price", []float64{0.1})
	if _, err := b.Build(); err != nil {
		t.Fatalf("Build second: %v", err)
	}
	if got := first.Columns[0].V.I64()[0]; got != 7 {
		t.Errorf("first batch id mutated to %d", got)
	}
}

func TestBatchBuilder_LengthMismatchPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic for wrong value count")
		}
	}()
	b := NewBatchBuilder(testDef())
	b.Reset(2)
	b.Int64("id", []int64{1})
}
