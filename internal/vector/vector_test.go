package vector

import (
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
}
