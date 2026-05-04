package vector

import "testing"

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

	want := []int64{40, 20}
	if len(got.Values) != len(want) {
		t.Fatalf("len = %d, want %d", len(got.Values), len(want))
	}
	for i := range want {
		if got.Values[i] != want[i] {
			t.Fatalf("value[%d] = %d, want %d", i, got.Values[i], want[i])
		}
	}
}

func TestNewBatchRejectsMismatchedLengths(t *testing.T) {
	_, err := NewBatch(
		Column{Name: "tenant_id", Values: NewInt64([]int64{1, 2})},
		Column{Name: "event_type", Values: NewString([]string{"signup"})},
	)
	if err == nil {
		t.Fatal("expected length mismatch error")
	}
}

func TestBatchColumn(t *testing.T) {
	batch, err := NewBatch(
		Column{Name: "tenant_id", Values: NewInt64([]int64{1, 2})},
		Column{Name: "event_type", Values: NewString([]string{"signup", "checkout"})},
	)
	if err != nil {
		t.Fatal(err)
	}

	col, ok := batch.Column("event_type")
	if !ok {
		t.Fatal("missing event_type column")
	}
	if col.Values.Kind() != KindString {
		t.Fatalf("kind = %s, want string", col.Values.Kind())
	}
}
