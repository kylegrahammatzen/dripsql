// Batch validation tests: shape consistency, dup name (case-insensitive) detection,
// malformed-Vec rejection, EnumLabels defensive clone, SelectionMask shape match.
package vector

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func TestBatch_Empty(t *testing.T) {
	b, err := NewBatch(nil)
	if err != nil {
		t.Fatalf("empty batch should not error: %v", err)
	}
	if b.Len != 0 || len(b.Columns) != 0 || b.VisibleLen() != 0 {
		t.Fatal("empty batch must be all zero")
	}
}

func TestBatch_ValidatesShape(t *testing.T) {
	c1 := Column{Name: "id", Type: schema.Int64, V: NewVec(VecInt64, 4)}
	c2 := Column{Name: "name", Type: schema.Text, V: NewVarVec(VecText, 4, 0)}
	b, err := NewBatch([]Column{c1, c2})
	if err != nil {
		t.Fatalf("good batch: %v", err)
	}
	if b.Len != 4 || len(b.Columns) != 2 {
		t.Fatalf("len=%d cols=%d", b.Len, len(b.Columns))
	}
	if got, ok := b.ColumnByName("id"); !ok || got.Type != schema.Int64 {
		t.Fatal("ColumnByName lookup failed")
	}
}

func TestBatch_ColumnByNameCaseInsensitive(t *testing.T) {
	c := Column{Name: "User_ID", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	b, err := NewBatch([]Column{c})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	for _, q := range []string{"user_id", "USER_ID", "  User_ID  "} {
		if _, ok := b.ColumnByName(q); !ok {
			t.Fatalf("ColumnByName(%q) must find User_ID", q)
		}
	}
}

func TestBatch_RejectsMismatchedLen(t *testing.T) {
	c1 := Column{Name: "a", Type: schema.Int64, V: NewVec(VecInt64, 4)}
	c2 := Column{Name: "b", Type: schema.Int64, V: NewVec(VecInt64, 5)}
	if _, err := NewBatch([]Column{c1, c2}); err == nil {
		t.Fatal("mismatched column length must error")
	}
}

func TestBatch_RejectsDupAndEmptyNames(t *testing.T) {
	c1 := Column{Name: "x", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	c2 := Column{Name: "x", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	if _, err := NewBatch([]Column{c1, c2}); err == nil {
		t.Fatal("duplicate names must error")
	}
	c3 := Column{Name: "  ", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	if _, err := NewBatch([]Column{c3}); err == nil {
		t.Fatal("whitespace-only names must error")
	}
}

func TestBatch_RejectsKindMismatch(t *testing.T) {
	c := Column{Name: "x", Type: schema.Int64, V: NewVec(VecInt32, 1)}
	if _, err := NewBatch([]Column{c}); err == nil {
		t.Fatal("type/veckind mismatch must error")
	}
}

func TestBatch_RejectsMalformedVec(t *testing.T) {
	c := Column{Name: "x", Type: schema.Int64, V: Vec{Kind: VecInt64, Len: 1}}
	if _, err := NewBatch([]Column{c}); err == nil {
		t.Fatal("Vec with Len>0 but nil data must error")
	}
	c2 := Column{Name: "y", Type: schema.Int64, V: Vec{Kind: VecInt64, Len: 5, Cap: 2}}
	if _, err := NewBatch([]Column{c2}); err == nil {
		t.Fatal("Vec with Len>Cap must error")
	}
}

func TestBatch_DupNamesCaseInsensitive(t *testing.T) {
	c1 := Column{Name: "id", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	c2 := Column{Name: "ID", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	if _, err := NewBatch([]Column{c1, c2}); err == nil {
		t.Fatal("case-different duplicate names must error")
	}
}

func TestBatch_RejectsOverflow(t *testing.T) {
	c := Column{Name: "x", Type: schema.Int64, V: Vec{Kind: VecInt64, Len: StandardBatchRows + 1}}
	if _, err := NewBatch([]Column{c}); err == nil {
		t.Fatal("batch exceeding StandardBatchRows must error")
	}
}

func TestBatch_TrimsName(t *testing.T) {
	c := Column{Name: "  id  ", Type: schema.Int64, V: NewVec(VecInt64, 1)}
	b, err := NewBatch([]Column{c})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if b.Columns[0].Name != "id" {
		t.Fatalf("name not trimmed: %q", b.Columns[0].Name)
	}
}

func TestBatch_EnumLabelsCloned(t *testing.T) {
	labels := []string{"a", "b"}
	c := Column{Name: "s", Type: schema.Named("status"), EnumLabels: labels, V: NewVec(VecEnum32, 1)}
	b, err := NewBatch([]Column{c})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	labels[0] = "MUTATED"
	if b.Columns[0].EnumLabels[0] != "a" {
		t.Fatal("EnumLabels must be cloned, not aliased")
	}
}

func TestBatch_SetSelValidatesShape(t *testing.T) {
	c := Column{Name: "x", Type: schema.Int64, V: NewVec(VecInt64, 8)}
	b, err := NewBatch([]Column{c})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	sel := NewSelectionMask(8)
	sel.Set(2)
	sel.Set(5)
	if err := b.SetSel(&sel); err != nil {
		t.Fatalf("SetSel: %v", err)
	}
	if b.VisibleLen() != 2 {
		t.Fatalf("VisibleLen=%d want 2", b.VisibleLen())
	}
	bad := NewSelectionMask(4)
	if err := b.SetSel(&bad); err == nil {
		t.Fatal("SetSel with mismatched shape must error")
	}
	if err := b.SetSel(nil); err != nil {
		t.Fatalf("clearing sel: %v", err)
	}
	if b.VisibleLen() != 8 {
		t.Fatalf("after clear VisibleLen=%d want 8", b.VisibleLen())
	}
}
