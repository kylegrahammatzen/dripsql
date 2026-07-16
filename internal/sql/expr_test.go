// Scalar bindExpr tests covering column resolution, literal and arithmetic typing, JSON path typing,
// scalar function arity, NOT/AND/OR shape, and aggregate rejection.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func sampleSchema() []BoundColumnDef {
	return []BoundColumnDef{
		{ID: 1, Name: "id", Type: schema.Int64},
		{ID: 2, Name: "name", Type: schema.Text},
		{ID: 3, Name: "rating", Type: schema.Float64},
		{ID: 4, Name: "payload", Type: schema.JSON},
	}
}

func bindExprFromSQL(t *testing.T, sql string) (BoundExpr, error) {
	t.Helper()
	stmt, err := ParseOne("SELECT " + sql + " AS x FROM t")
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel := stmt.(*SelectStmt)
	return bindExpr(buildColumnIndex(sampleSchema()), sel.Select[0].Expr)
}

func TestBindExpr_ColumnAndLiteral(t *testing.T) {
	be, err := bindExprFromSQL(t, "id")
	if err != nil {
		t.Fatalf("BindExpr: %v", err)
	}
	if be.Op != ExprColumn || be.Type.Kind != schema.KindInt64 || be.ColumnID != 1 {
		t.Fatalf("col bind = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "42")
	if be.Op != ExprLiteral || be.Type.Kind != schema.KindInt64 || be.Literal.(int64) != 42 {
		t.Fatalf("int lit = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "2.5")
	if be.Type.Kind != schema.KindFloat64 {
		t.Fatalf("float lit type = %v", be.Type.Kind)
	}
	be, _ = bindExprFromSQL(t, "'hi'")
	if be.Type.Kind != schema.KindText {
		t.Fatalf("text lit type = %v", be.Type.Kind)
	}
	be, _ = bindExprFromSQL(t, "true")
	if be.Type.Kind != schema.KindBool {
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
	if be.Op != ExprAdd || be.Type.Kind != schema.KindInt64 {
		t.Fatalf("int+int = %+v", be)
	}
	be, err = bindExprFromSQL(t, "id + 1.5")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	if be.Type.Kind != schema.KindFloat64 {
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
	if be.Op != ExprConcat || be.Type.Kind != schema.KindText {
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
	if be.Op != ExprJSONGet || be.Type.Kind != schema.KindJSON {
		t.Fatalf("-> = %+v", be)
	}
	be, _ = bindExprFromSQL(t, "payload ->> 'k'")
	if be.Op != ExprJSONGetText || be.Type.Kind != schema.KindText {
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
		{ID: 1, Name: "u", Type: schema.UUID},
		{ID: 2, Name: "n", Type: schema.Int64},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "u"}, Op: BinaryEqual, Right: &ColumnRef{Name: "n"}}
	if _, err := bindExpr(buildColumnIndex(schema), cmp); err == nil {
		t.Fatal("uuid_col = int_col must error")
	}
}

func TestBindExpr_BytesColumn_RejectsIncompatibleColumn(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "b", Type: schema.Bytes},
		{ID: 2, Name: "f", Type: schema.Bool},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "b"}, Op: BinaryEqual, Right: &ColumnRef{Name: "f"}}
	if _, err := bindExpr(buildColumnIndex(schema), cmp); err == nil {
		t.Fatal("bytes_col = bool_col must error")
	}
}

func TestBindExpr_EnumColumn_RejectsIncompatibleColumn(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "e", Type: schema.Named("event"), Labels: []string{"x"}},
		{ID: 2, Name: "n", Type: schema.Int64},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "e"}, Op: BinaryEqual, Right: &ColumnRef{Name: "n"}}
	if _, err := bindExpr(buildColumnIndex(schema), cmp); err == nil {
		t.Fatal("enum_col = int_col must error")
	}
}

func TestBindExpr_DifferentEnums_Reject(t *testing.T) {
	schema := []BoundColumnDef{
		{ID: 1, Name: "a", Type: schema.Named("event"), Labels: []string{"x"}},
		{ID: 2, Name: "b", Type: schema.Named("status"), Labels: []string{"y"}},
	}
	cmp := &BinaryExpr{Left: &ColumnRef{Name: "a"}, Op: BinaryEqual, Right: &ColumnRef{Name: "b"}}
	if _, err := bindExpr(buildColumnIndex(schema), cmp); err == nil {
		t.Fatal("comparing different enum types must error")
	}
	coalesce := &FuncCall{Name: "coalesce", Args: []Expr{&ColumnRef{Name: "a"}, &ColumnRef{Name: "b"}}}
	if _, err := bindExpr(buildColumnIndex(schema), coalesce); err == nil {
		t.Fatal("coalescing different enum types must error")
	}
}

func TestBindExpr_NotAndOr(t *testing.T) {
	// NOT/AND/OR are WHERE-level operators in the grammar, so test via a hand-built AST.
	not := &NotExpr{Expr: &BinaryExpr{Left: &ColumnRef{Name: "name"}, Op: BinaryEqual, Right: &Literal{Value: Value{Kind: ValueString, String: "x"}}}}
	be, err := bindExpr(buildColumnIndex(sampleSchema()), not)
	if err != nil {
		t.Fatalf("BindExpr: %v", err)
	}
	if be.Op != ExprNot {
		t.Fatalf("NOT bind = %+v", be)
	}
}
