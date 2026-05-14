package types

import "testing"

func TestVecZeroValueValidatesAsInvalid(t *testing.T) {
	v := Vec{}
	if err := v.validate(); err == nil {
		t.Fatal("zero-value Vec should fail validation")
	}
}

func TestVecInt64FlatConstruction(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 3, I64: []int64{1, 2, 3}}
	if err := v.validate(); err != nil {
		t.Fatalf("Int64 flat vector failed validation: %v", err)
	}
}

func TestVecTextFlatConstruction(t *testing.T) {
	vb := NewVarBytes(2, 8)
	vb.AppendString(0, "a")
	vb.AppendString(1, "bb")

	v := Vec{Kind: VecText, Encoding: EncodingFlat, Len: 2, Var: vb}
	if err := v.validate(); err != nil {
		t.Fatalf("Text flat vector failed validation: %v", err)
	}
}

func TestVecBoolBitsLengthMatchesValidityWords(t *testing.T) {
	v := Vec{Kind: VecBool, Encoding: EncodingFlat, Len: 65, BoolBits: make([]uint64, 2)}
	if err := v.validate(); err != nil {
		t.Fatalf("Bool vector failed validation: %v", err)
	}
}

func TestVecValidityWordsCheck(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 100, Valid: make(Validity, 1), I64: make([]int64, 100)}
	if err := v.validate(); err == nil {
		t.Fatal("expected validity-too-short error")
	}
}

func TestVecKindMustMatchTypedSlice(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 1, I32: []int32{1}}
	if err := v.validate(); err == nil {
		t.Fatal("expected inactive-field error")
	}
}

func TestVecRejectsLengthOverBatchSize(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: StandardBatchRows + 1, I64: make([]int64, StandardBatchRows+1)}
	if err := v.validate(); err == nil {
		t.Fatal("expected length-exceeds-batch error")
	}
}

func TestVecRejectsNegativeLength(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: -1}
	if err := v.validate(); err == nil {
		t.Fatal("expected negative-length error")
	}
}

func TestVecRejectsInvalidKind(t *testing.T) {
	v := Vec{Kind: VecKind(255), Encoding: EncodingFlat, Len: 1, I64: []int64{1}}
	if err := v.validate(); err == nil {
		t.Fatal("expected invalid-kind error")
	}
}

func TestVecRejectsInvalidKindForNonFlatEncoding(t *testing.T) {
	v := Vec{Kind: VecKind(255), Encoding: EncodingDictionary, Len: 0}

	if err := v.validate(); err == nil {
		t.Fatal("expected invalid-kind error for non-flat vector")
	}
}

func TestVecAllowsOverAllocatedTypedBuffer(t *testing.T) {
	v := Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 3, I64: make([]int64, 100)}
	if err := v.validate(); err != nil {
		t.Fatalf("over-allocated typed buffer should be allowed: %v", err)
	}
}

func TestVecEncodingFlatIsDefault(t *testing.T) {
	v := Vec{Kind: VecInt64, Len: 3, I64: []int64{1, 2, 3}}
	if v.Encoding != EncodingFlat {
		t.Fatalf("zero-value Encoding = %v, want EncodingFlat", v.Encoding)
	}
}

func TestVecEncodingNonFlatPermissivelyValid(t *testing.T) {
	for _, enc := range []Encoding{EncodingDictionary, EncodingConstant, EncodingSequence, EncodingFORBitPack, EncodingFlate} {
		v := Vec{Kind: VecText, Encoding: enc, Len: 0}
		if err := v.validate(); err != nil {
			t.Errorf("non-flat encoding %v should validate empty Vec: %v", enc, err)
		}
	}
}

func TestBatchNewBatchRejectsDuplicateColumns(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 0, I64: nil}},
		{Name: "id", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 0, I64: nil}},
	}
	if _, err := NewBatch(cols); err == nil {
		t.Fatal("expected duplicate-column error")
	}
}

func TestBatchNewBatchRejectsLengthMismatch(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 1, I64: []int64{1}}},
		{Name: "b", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 2, I64: []int64{1, 2}}},
	}
	if _, err := NewBatch(cols); err == nil {
		t.Fatal("expected length-mismatch error")
	}
}

func TestBatchNewBatchNoCloneUsesProvidedColumns(t *testing.T) {
	cols := []Column{{Name: "a", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 1, I64: []int64{1}}}}
	b, err := NewBatchNoClone(cols)
	if err != nil {
		t.Fatalf("NewBatchNoClone: %v", err)
	}
	cols[0].Name = "changed"
	if b.Columns[0].Name != "changed" {
		t.Fatalf("batch column name = %q, want shared column slice", b.Columns[0].Name)
	}
}

func TestBatchNewBatchNoCloneRejectsMalformedBatch(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 1, I64: []int64{1}}},
		{Name: "a", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 1, I64: []int64{2}}},
	}
	if _, err := NewBatchNoClone(cols); err == nil {
		t.Fatal("expected duplicate-column error")
	}
}

func TestBatchEmpty(t *testing.T) {
	b, err := NewBatch(nil)
	if err != nil {
		t.Fatalf("NewBatch(nil): %v", err)
	}
	if b.Len != 0 {
		t.Errorf("empty batch Len = %d, want 0", b.Len)
	}
}

func TestBatchVisibleLen(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: Int64, V: Vec{Kind: VecInt64, Encoding: EncodingFlat, Len: 3, I64: []int64{1, 2, 3}}},
	}
	b, err := NewBatch(cols)
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if b.VisibleLen() != 3 {
		t.Fatalf("VisibleLen with no Sel = %d, want 3", b.VisibleLen())
	}
	if err := b.SetSel(Sel{0, 2}); err != nil {
		t.Fatalf("SetSel: %v", err)
	}
	if b.VisibleLen() != 2 {
		t.Fatalf("VisibleLen with Sel = %d, want 2", b.VisibleLen())
	}
}
