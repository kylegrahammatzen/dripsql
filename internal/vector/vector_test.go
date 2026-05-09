package vector

import (
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestKindOf(t *testing.T) {
	cases := []struct {
		typ  sqltype.Type
		want Kind
	}{
		{sqltype.Bool, Bool},
		{sqltype.Int16, Int16},
		{sqltype.Int32, Int32},
		{sqltype.Int64, Int64},
		{sqltype.Float32, Float32},
		{sqltype.Float64, Float64},
		{sqltype.Decimal, Decimal64},
		{sqltype.Text, Text},
		{sqltype.Bytes, Bytes},
		{sqltype.UUID, UUID},
		{sqltype.Timestamp, Timestamp},
		{sqltype.Time, Time},
		{sqltype.Date, Date},
		{sqltype.JSON, JSON},
		{sqltype.Named("status"), Enum32},
	}
	for _, tc := range cases {
		got, err := KindOf(tc.typ)
		if err != nil || got != tc.want {
			t.Fatalf("KindOf(%s) = %s, %v; want %s", tc.typ, got, err, tc.want)
		}
	}
}

func TestValidity(t *testing.T) {
	valid := NewValidity(130)
	if NullCount(valid, 130) != 0 {
		t.Fatalf("new validity has nulls")
	}
	SetInvalid(valid, 0)
	SetInvalid(valid, 64)
	SetInvalid(valid, 129)
	if IsValid(valid, 0) || IsValid(valid, 64) || IsValid(valid, 129) {
		t.Fatalf("invalid bits still reported valid")
	}
	if got := NullCount(valid, 130); got != 3 {
		t.Fatalf("NullCount = %d, want 3", got)
	}
	SetValid(valid, 64)
	if got := NullCount(valid, 130); got != 2 {
		t.Fatalf("NullCount after SetValid = %d, want 2", got)
	}
	if NullCount(nil, 130) != 0 || !Validity(nil).IsAllValid() {
		t.Fatalf("nil validity should mean all valid")
	}
}

func TestFillValid(t *testing.T) {
	valid := make(Validity, ValidityWords(70))
	FillValid(valid, 70)
	if got := NullCount(valid, 70); got != 0 {
		t.Fatalf("NullCount after FillValid = %d, want 0", got)
	}
	if valid[1] != (uint64(1)<<6)-1 {
		t.Fatalf("last validity word = %#x, want lower 6 bits", valid[1])
	}
	if got := ValidityWords(-1); got != 0 {
		t.Fatalf("ValidityWords(-1) = %d, want 0", got)
	}
}

func TestVarBytes(t *testing.T) {
	newVar := NewVarBytes(2, -1)
	if len(newVar.Offsets) != 3 || cap(newVar.Data) != 0 {
		t.Fatalf("NewVarBytes = offsets %d data cap %d, want 3/0", len(newVar.Offsets), cap(newVar.Data))
	}
	newVar.Data = append(newVar.Data, 'a', 'b')
	newVar.Offsets[1] = 0
	newVar.Offsets[2] = 2
	if got := string(newVar.Bytes(0)); got != "" {
		t.Fatalf("Bytes(0) = %q, want empty", got)
	}
	if got := newVar.String(1); got != "ab" {
		t.Fatalf("String(1) = %q, want ab", got)
	}

	v := VarBytes{Offsets: []uint32{0, 3, 3, 6}, Data: []byte("foobin")}
	if got := string(v.Bytes(0)); got != "foo" {
		t.Fatalf("Bytes(0) = %q", got)
	}
	if got := v.String(1); got != "" {
		t.Fatalf("String(1) = %q", got)
	}
	if got := v.String(2); got != "bin" {
		t.Fatalf("String(2) = %q", got)
	}
}

func TestNewBatch(t *testing.T) {
	batch, err := NewBatch([]Column{
		{Name: " id ", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 3, I64: []int64{1, 2, 3}}},
		{Name: "status", Type: sqltype.Named("status"), V: Vec{Kind: Enum32, Len: 3, U32: []uint32{1, 2, 1}}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if batch.Len != 3 || batch.VisibleLen() != 3 {
		t.Fatalf("batch lengths = %d/%d", batch.Len, batch.VisibleLen())
	}
	if batch.Columns[0].Name != "id" {
		t.Fatalf("trimmed column name = %q, want id", batch.Columns[0].Name)
	}
	if err := batch.SetSel(Sel{2, 0}); err != nil {
		t.Fatalf("SetSel: %v", err)
	}
	if batch.VisibleLen() != 2 || !slices.Equal(batch.Sel, Sel{2, 0}) {
		t.Fatalf("selection = %#v", batch.Sel)
	}
}

func TestNewBatchRejectsBadColumn(t *testing.T) {
	tests := []struct {
		name string
		cols []Column
	}{
		{
			name: "empty name",
			cols: []Column{{Name: " ", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 1, I64: []int64{1}}}},
		},
		{
			name: "duplicate trimmed name",
			cols: []Column{
				{Name: " id", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 1, I64: []int64{1}}},
				{Name: "id ", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 1, I64: []int64{2}}},
			},
		},
		{
			name: "length mismatch",
			cols: []Column{
				{Name: "id", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 1, I64: []int64{1}}},
				{Name: "other", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 2, I64: []int64{1, 2}}},
			},
		},
		{
			name: "kind mismatch",
			cols: []Column{{Name: "id", Type: sqltype.Int64, V: Vec{Kind: Float64, Len: 1, F64: []float64{1}}}},
		},
		{
			name: "batch length exceeded",
			cols: []Column{{Name: "id", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: StandardBatchRows + 1, I64: make([]int64, StandardBatchRows+1)}}},
		},
		{
			name: "inactive field",
			cols: []Column{{Name: "id", Type: sqltype.Int64, V: Vec{Kind: Int64, Len: 1, I64: []int64{1}, F64: []float64{1}}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewBatch(tt.cols); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestValidateSelRejectsOutOfRange(t *testing.T) {
	if err := ValidateSel(Sel{0, 2}, 2); err == nil {
		t.Fatalf("expected out-of-range selection error")
	}
}
