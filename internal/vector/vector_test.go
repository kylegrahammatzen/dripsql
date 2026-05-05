package vector

import (
	"math"
	"slices"
	"testing"
)

func TestInt64MinMax(t *testing.T) {
	v := NewInt64([]int64{9, -2, 12, 4})

	min, max, ok := v.MinMax()
	if !ok {
		t.Fatal("expected min/max for non-empty vector")
	}
	if min != -2 || max != 12 {
		t.Fatalf("min/max = %d/%d, want -2/12", min, max)
	}
}

func TestInt64Take(t *testing.T) {
	v := NewInt64([]int64{10, 20, 30, 40})

	got, err := v.Take([]uint32{3, 1})
	if err != nil {
		t.Fatal(err)
	}

	gotInt64, ok := got.(Int64)
	if !ok {
		t.Fatalf("got %T, want Int64", got)
	}

	want := []int64{40, 20}
	if !slices.Equal(gotInt64.Values, want) {
		t.Fatalf("values = %v, want %v", gotInt64.Values, want)
	}
}

func TestInt64TakeRejectsOutOfRange(t *testing.T) {
	v := NewInt64([]int64{10, 20})

	_, err := v.Take([]uint32{2})
	if err == nil {
		t.Fatal("expected out of range error")
	}
}

func TestFloat64Take(t *testing.T) {
	v := NewFloat64([]float64{1.5, 2.5, 3.5})

	got, err := v.Take([]uint32{2, 0})
	if err != nil {
		t.Fatal(err)
	}

	gotFloat64, ok := got.(Float64)
	if !ok {
		t.Fatalf("got %T, want Float64", got)
	}

	want := []float64{3.5, 1.5}
	if !slices.Equal(gotFloat64.Values, want) {
		t.Fatalf("values = %v, want %v", gotFloat64.Values, want)
	}
}

func TestFloat64MinMax(t *testing.T) {
	v := NewFloat64([]float64{9.5, -2.25, 12.75, 4})

	min, max, ok := v.MinMax()
	if !ok {
		t.Fatal("expected min/max for non-empty vector")
	}
	if min != -2.25 || max != 12.75 {
		t.Fatalf("min/max = %f/%f, want -2.25/12.75", min, max)
	}
}

func TestFloat64MinMaxRejectsNaN(t *testing.T) {
	v := NewFloat64([]float64{1, math.NaN(), 2})

	_, _, ok := v.MinMax()
	if ok {
		t.Fatal("did not expect min/max when values contain NaN")
	}
}

func TestStringData(t *testing.T) {
	data := []byte("alphabeta")
	v, err := NewStringData(data, []uint64{
		StringRange(0, 5),
		StringRange(5, 0),
		StringRange(5, 4),
	})
	if err != nil {
		t.Fatal(err)
	}

	if v.Len() != 3 {
		t.Fatalf("len = %d, want 3", v.Len())
	}
	if got := v.Value(0); got != "alpha" {
		t.Fatalf("value 0 = %q, want alpha", got)
	}
	if got := v.Value(1); got != "" {
		t.Fatalf("value 1 = %q, want empty", got)
	}
	if got := v.Value(2); got != "beta" {
		t.Fatalf("value 2 = %q, want beta", got)
	}

	taken, err := v.Take([]uint32{2, 0})
	if err != nil {
		t.Fatal(err)
	}
	takenStrings, ok := taken.(String)
	if !ok {
		t.Fatalf("got %T, want String", taken)
	}
	if takenStrings.Data == nil || len(takenStrings.Ranges) != 2 {
		t.Fatalf("expected compact taken string vector: %+v", takenStrings)
	}
	if takenStrings.Value(0) != "beta" || takenStrings.Value(1) != "alpha" {
		t.Fatalf("taken = [%q %q], want [beta alpha]", takenStrings.Value(0), takenStrings.Value(1))
	}
}

func TestNewStringDataRejectsInvalidRanges(t *testing.T) {
	_, err := NewStringData([]byte("alpha"), []uint64{StringRange(4, 2)})
	if err == nil {
		t.Fatal("expected invalid range error")
	}
}

func TestVectorConstructorsCloneInput(t *testing.T) {
	ints := []int64{1, 2}
	intVec := NewInt64(ints)
	ints[0] = 99
	if intVec.Values[0] != 1 {
		t.Fatalf("int vector aliased input: %v", intVec.Values)
	}

	floats := []float64{1.5, 2.5}
	floatVec := NewFloat64(floats)
	floats[0] = 99
	if floatVec.Values[0] != 1.5 {
		t.Fatalf("float vector aliased input: %v", floatVec.Values)
	}

	strings := []string{"a", "b"}
	stringVec := NewString(strings)
	strings[0] = "z"
	if stringVec.Values[0] != "a" {
		t.Fatalf("string vector aliased input: %v", stringVec.Values)
	}
}

func TestNewBatchRejectsInvalidColumns(t *testing.T) {
	tests := []struct {
		name    string
		columns []Column
	}{
		{
			name: "mismatched lengths",
			columns: []Column{
				{Name: "tenant_id", Vector: NewInt64([]int64{1, 2})},
				{Name: "event_type", Vector: NewString([]string{"signup"})},
			},
		},
		{
			name: "duplicate names",
			columns: []Column{
				{Name: "tenant_id", Vector: NewInt64([]int64{1})},
				{Name: "tenant_id", Vector: NewInt64([]int64{2})},
			},
		},
		{
			name: "empty name",
			columns: []Column{
				{Name: "", Vector: NewInt64([]int64{1})},
			},
		},
		{
			name: "nil vector",
			columns: []Column{
				{Name: "tenant_id"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBatch(tt.columns...)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestNewBatchClonesColumns(t *testing.T) {
	columns := []Column{
		{Name: "tenant_id", Vector: NewInt64([]int64{1})},
	}

	batch, err := NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}

	columns[0].Name = "changed"
	if batch.Columns[0].Name != "tenant_id" {
		t.Fatalf("batch column name = %q, want tenant_id", batch.Columns[0].Name)
	}
}

func TestBatchVisibleCount(t *testing.T) {
	batch, err := NewBatch(
		Column{Name: "tenant_id", Vector: NewInt64([]int64{1, 2, 3})},
	)
	if err != nil {
		t.Fatal(err)
	}

	if got := batch.VisibleCount(); got != 3 {
		t.Fatalf("visible count = %d, want 3", got)
	}
	if batch.HasSelection() {
		t.Fatal("did not expect selection")
	}

	batch.Sel = []uint32{0, 2}
	if got := batch.VisibleCount(); got != 2 {
		t.Fatalf("selected visible count = %d, want 2", got)
	}
	if !batch.HasSelection() {
		t.Fatal("expected selection")
	}
}

func TestBatchColumn(t *testing.T) {
	batch, err := NewBatch(
		Column{Name: "tenant_id", Vector: NewInt64([]int64{1, 2})},
		Column{Name: "event_type", Vector: NewString([]string{"signup", "checkout"})},
	)
	if err != nil {
		t.Fatal(err)
	}

	col, ok := batch.Column("event_type")
	if !ok {
		t.Fatal("missing event_type column")
	}
	if col.Vector.Kind() != KindString {
		t.Fatalf("kind = %s, want string", col.Vector.Kind())
	}

	index, ok := batch.ColumnIndex("event_type")
	if !ok {
		t.Fatal("missing event_type column index")
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}

	if _, ok := batch.ColumnIndex("missing"); ok {
		t.Fatal("did not expect missing column index")
	}
}
