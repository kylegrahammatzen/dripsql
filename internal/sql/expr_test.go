// Scalar BindExpr tests: column resolution, literal typing, arithmetic result type,
// JSON path typing, scalar function arity/typing, NOT/AND/OR shape, aggregate rejection.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func sampleSchema() []BoundColumnDef {
	return []BoundColumnDef{
		{ID: 1, Name: "id", Type: types.Int64},
		{ID: 2, Name: "name", Type: types.Text},
		{ID: 3, Name: "rating", Type: types.Float64},
		{ID: 4, Name: "payload", Type: types.JSON},
	}
}

func bindExprFromSQL(t *testing.T, sql string) (BoundExpr, error) {
	t.Helper()
	stmt, err := ParseOne("SELECT " + sql + " AS x FROM t")
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel := stmt.(*SelectStmt)
	return BindExpr(sampleSchema(), sel.Select[0].Expr)
}

func TestBindExpr_ColumnAndLiteral(t *testing.T) {
	be, err := bindExprFromSQL(t, "id")
	if err != nil {
		t.Fatalf("BindExpr: %v", err)
	}
	if be.Op != ExprColumn || be.Type.Kind != types.KindInt64 || be.ColumnID != 1 {
		t.Fatalf("col bind = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "42")
	if be.Op != ExprLiteral || be.Type.Kind != types.KindInt64 || be.Literal.(int64) != 42 {
		t.Fatalf("int lit = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "2.5")
	if be.Type.Kind != types.KindFloat64 {
		t.Fatalf("float lit type = %v", be.Type.Kind)
	}
	be, _ = bindExprFromSQL(t, "'hi'")
	if be.Type.Kind != types.KindText {
		t.Fatalf("text lit type = %v", be.Type.Kind)
	}
	be, _ = bindExprFromSQL(t, "true")
	if be.Type.Kind != types.KindBool {
		t.Fatalf("bool lit type = %v", be.Type.Kind)
	}
}

func TestBindExpr_MissingColumn(t *testing.T) {
	if _, err := bindExprFromSQL(t, "missing"); err == nil {
		t.Fatal("missing column must error")
	}
}

func TestBindExpr_ArithmeticResultType(t *testing.T) {
	be, err := bindExprFromSQL(t, "id + 1")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if be.Op != ExprAdd || be.Type.Kind != types.KindInt64 {
		t.Fatalf("int+int = %+v", be)
	}
	be, err = bindExprFromSQL(t, "id + 1.5")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if be.Type.Kind != types.KindFloat64 {
		t.Fatalf("int+float should widen to float64, got %v", be.Type.Kind)
	}
	if _, err := bindExprFromSQL(t, "name + 1"); err == nil {
		t.Fatal("text + int must error")
	}
	if _, err := bindExprFromSQL(t, "id mod 2.5"); err == nil {
		t.Fatal("MOD with float must error")
	}
}

func TestBindExpr_Concat(t *testing.T) {
	be, err := bindExprFromSQL(t, "name || '!'")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if be.Op != ExprConcat || be.Type.Kind != types.KindText {
		t.Fatalf("concat = %+v", be)
	}
	if _, err := bindExprFromSQL(t, "name || 1"); err == nil {
		t.Fatal("text || int must error")
	}
}

func TestBindExpr_JSONPath(t *testing.T) {
	be, err := bindExprFromSQL(t, "payload -> 'k'")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if be.Op != ExprJSONGet || be.Type.Kind != types.KindJSON {
		t.Fatalf("-> = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "payload ->> 'k'")
	if be.Op != ExprJSONGetText || be.Type.Kind != types.KindText {
		t.Fatalf("->> = %+v", be)
	}
	if _, err := bindExprFromSQL(t, "id -> 'k'"); err == nil {
		t.Fatal("-> on int must error")
	}
}

func TestBindExpr_ScalarFuncs(t *testing.T) {
	for _, src := range []string{"lower(name)", "upper(name)", "length(name)", "coalesce(name, 'x')", "concat(name, '!')", "substring(name, 1, 3)"} {
		if _, err := bindExprFromSQL(t, src); err != nil {
			t.Fatalf("%q: %v", src, err)
		}
	}
	if _, err := bindExprFromSQL(t, "lower(id)"); err == nil {
		t.Fatal("lower(int) must error")
	}
	if _, err := bindExprFromSQL(t, "length(id)"); err == nil {
		t.Fatal("length(int) must error")
	}
	if _, err := bindExprFromSQL(t, "substring(name, 'x', 3)"); err == nil {
		t.Fatal("substring with non-int start must error")
	}
	if _, err := bindExprFromSQL(t, "coalesce(name, 1)"); err == nil {
		t.Fatal("coalesce with mixed types must error")
	}
	if _, err := bindExprFromSQL(t, "bogus(name)"); err == nil {
		t.Fatal("unknown function must error")
	}
}

func TestBindExpr_AggregateRejectedInScalarContext(t *testing.T) {
	if _, err := bindExprFromSQL(t, "sum(id)"); err == nil {
		t.Fatal("aggregate in scalar context must error")
	}
}

func TestBindExpr_UUIDColumn_RejectsIncompatibleColumn(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "u", Type: types.UUID},
		{ID: 2, Name: "n", Type: types.Int64},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "u"}, Op: BinaryEqual, Right: &ColumnRef{Name: "n"}}
	if _, err := BindExpr(schema, cmp); err == nil {
		t.Fatal("uuid_col = int_col must error")
	}
}

func TestBindExpr_BytesColumn_RejectsIncompatibleColumn(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "b", Type: types.Bytes},
		{ID: 2, Name: "f", Type: types.Bool},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "b"}, Op: BinaryEqual, Right: &ColumnRef{Name: "f"}}
	if _, err := BindExpr(schema, cmp); err == nil {
		t.Fatal("bytes_col = bool_col must error")
	}
}

func TestBindExpr_EnumColumn_RejectsIncompatibleColumn(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "e", Type: types.Named("event"), Labels: []string{"x"}},
		{ID: 2, Name: "n", Type: types.Int64},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "e"}, Op: BinaryEqual, Right: &ColumnRef{Name: "n"}}
	if _, err := BindExpr(schema, cmp); err == nil {
		t.Fatal("enum_col = int_col must error")
	}
}

func TestBindExpr_DifferentEnums_Reject(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "a", Type: types.Named("event"), Labels: []string{"x"}},
		{ID: 2, Name: "b", Type: types.Named("status"), Labels: []string{"y"}},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "a"}, Op: BinaryEqual, Right: &ColumnRef{Name: "b"}}
	if _, err := BindExpr(schema, cmp); err == nil {
		t.Fatal("comparing different enum types must error")
	}
	coalesce := &FuncCall{Name: "coalesce", Args: []Expr{&ColumnRef{Name: "a"}, &ColumnRef{Name: "b"}}}
	if _, err := BindExpr(schema, coalesce); err == nil {
		t.Fatal("coalescing different enum types must error")
	}
}

func TestBindExpr_NotAndOr(t *testing.T) {
	// NOT/AND/OR are WHERE-level operators in the grammar, so test via a hand-built AST.
	not := &NotExpr{Expr: &BinaryExpr{Left: &ColumnRef{Name: "name"}, Op: BinaryEqual, Right: &Literal{Value: Value{Kind: ValueString, String: "x"}}}}
	be, err := BindExpr(sampleSchema(), not)
	if err != nil {
		t.Fatalf("BindExpr: %v", err)
	}
	if be.Op != ExprNot {
		t.Fatalf("NOT bind = %+v", be)
	}
}
