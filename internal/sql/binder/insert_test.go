package binder

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestBindInsertValues(t *testing.T) {
	bound, err := BindInsertValues(&ast.InsertStmt{
		Table:   "events",
		Columns: []string{"event_type", "tenant_id"},
		Values: [][]ast.Value{
			{{Kind: ast.ValueString, String: "signup"}, {Kind: ast.ValueInt, Int: 1}},
			{{Kind: ast.ValueString, String: "checkout"}, {Kind: ast.ValueInt, Int: 2}},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindInsertValues: %v", err)
	}
	if bound.RowCount != 2 || len(bound.Columns) != 2 {
		t.Fatalf("bound insert = %#v", bound)
	}
	tenants := bound.Columns[0].Values
	events := bound.Columns[1].Values
	if tenants[0].Int != 1 || tenants[1].Int != 2 {
		t.Fatalf("tenant values = %#v", tenants)
	}
	if events[0].String+","+events[1].String != "signup,checkout" {
		t.Fatalf("event values = %#v", events)
	}
}

func TestBindInsertValuesRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		stmt *ast.InsertStmt
		want string
	}{
		{
			name: "wrong row length",
			stmt: &ast.InsertStmt{Table: "events", Values: [][]ast.Value{{{Kind: ast.ValueInt, Int: 1}}}},
			want: "has 1 values",
		},
		{
			name: "partial column list",
			stmt: &ast.InsertStmt{Table: "events", Columns: []string{"tenant_id"}, Values: [][]ast.Value{{{Kind: ast.ValueInt, Int: 1}}}},
			want: "column count",
		},
		{
			name: "unknown column",
			stmt: &ast.InsertStmt{Table: "events", Columns: []string{"tenant_id", "missing"}, Values: [][]ast.Value{{{Kind: ast.ValueInt, Int: 1}, {Kind: ast.ValueString, String: "signup"}}}},
			want: "unknown INSERT column",
		},
		{
			name: "type mismatch",
			stmt: &ast.InsertStmt{Table: "events", Values: [][]ast.Value{{{Kind: ast.ValueString, String: "bad"}, {Kind: ast.ValueString, String: "signup"}}}},
			want: "expects int64 literal",
		},
		{
			name: "null",
			stmt: &ast.InsertStmt{Table: "events", Values: [][]ast.Value{{{Kind: ast.ValueNull}, {Kind: ast.ValueString, String: "signup"}}}},
			want: "NOT NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BindInsertValues(tt.stmt, eventsDef())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindInsertValues error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindInsertValuesRejectsWrongRowLengthAtSecondRow(t *testing.T) {
	_, err := BindInsertValues(&ast.InsertStmt{
		Table:   "events",
		Columns: []string{"tenant_id", "event_type"},
		Values: [][]ast.Value{
			{{Kind: ast.ValueInt, Int: 1}, {Kind: ast.ValueString, String: "signup"}},
			{{Kind: ast.ValueInt, Int: 2}},
		},
	}, eventsDef())
	if err == nil {
		t.Fatal("expected row width error")
	}
	if !strings.Contains(err.Error(), "INSERT row 2 has 1 values, want 2") {
		t.Fatalf("unexpected error = %v", err)
	}
}

func TestBindInsertValuesAllowsNullsForNullableColumns(t *testing.T) {
	def := eventsDef()
	def.Columns[1].Nullable = true
	bound, err := BindInsertValues(&ast.InsertStmt{Table: "events", Values: [][]ast.Value{{{Kind: ast.ValueInt, Int: 1}, {Kind: ast.ValueNull}}}}, def)
	if err != nil {
		t.Fatalf("BindInsertValues: %v", err)
	}
	if bound.Columns[1].NullCount != 1 || bound.Columns[1].Values[0].Kind != ast.ValueNull {
		t.Fatalf("nulls = %#v, want row 0 null", bound.Columns[1])
	}
}

func TestBindInsertValuesValidatesAllBuiltins(t *testing.T) {
	def := catalog.TableDef{
		Name: "all_types",
		Columns: []catalog.ColumnDef{
			{Name: "b", Type: sqltype.Bool},
			{Name: "i16", Type: sqltype.Int16},
			{Name: "i32", Type: sqltype.Int32},
			{Name: "i64", Type: sqltype.Int64},
			{Name: "f32", Type: sqltype.Float32},
			{Name: "f64", Type: sqltype.Float64},
			{Name: "dec", Type: sqltype.Decimal},
			{Name: "txt", Type: sqltype.Text},
			{Name: "bin", Type: sqltype.Bytes},
			{Name: "id", Type: sqltype.UUID},
			{Name: "ts", Type: sqltype.Timestamp},
			{Name: "tm", Type: sqltype.Time},
			{Name: "d", Type: sqltype.Date},
			{Name: "j", Type: sqltype.JSON},
			{Name: "status", Type: sqltype.Named("event_status"), Labels: []string{"new", "done"}},
		},
	}
	_, err := BindInsertValues(&ast.InsertStmt{
		Table: "all_types",
		Values: [][]ast.Value{{
			{Kind: ast.ValueBool, Bool: true},
			{Kind: ast.ValueInt, Int: 7},
			{Kind: ast.ValueInt, Int: 8},
			{Kind: ast.ValueInt, Int: 9},
			{Kind: ast.ValueFloat, Float: 10.5},
			{Kind: ast.ValueFloat, Float: 11.75},
			{Kind: ast.ValueString, String: "12.25"},
			{Kind: ast.ValueString, String: "hello"},
			{Kind: ast.ValueString, String: "deadbeef"},
			{Kind: ast.ValueString, String: "550e8400-e29b-41d4-a716-446655440000"},
			{Kind: ast.ValueString, String: "2026-05-07T00:00:00Z"},
			{Kind: ast.ValueString, String: "12:34:56"},
			{Kind: ast.ValueString, String: "2026-05-07"},
			{Kind: ast.ValueString, String: `{"ok":true}`},
			{Kind: ast.ValueString, String: "new"},
		}},
	}, def)
	if err != nil {
		t.Fatalf("BindInsertValues all builtins: %v", err)
	}
}

func TestBindInsertValuesRejectsBytesNonString(t *testing.T) {
	def := catalog.TableDef{
		Name: "files",
		Columns: []catalog.ColumnDef{
			{Name: "payload", Type: sqltype.Bytes},
		},
	}
	_, err := BindInsertValues(&ast.InsertStmt{
		Table:  "files",
		Values: [][]ast.Value{{{Kind: ast.ValueInt, Int: 7}}},
	}, def)
	if err == nil || !strings.Contains(err.Error(), "expects string literal") {
		t.Fatalf("BindInsertValues bytes error = %v", err)
	}
}

func TestBindInsertValuesValidatesNamedEnumLabel(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "status", Type: sqltype.Named("event_status"), Labels: []string{"new", "done"}},
		},
	}
	_, err := BindInsertValues(&ast.InsertStmt{
		Table:  "events",
		Values: [][]ast.Value{{{Kind: ast.ValueString, String: "missing"}}},
	}, def)
	if err == nil || !strings.Contains(err.Error(), "invalid enum label") {
		t.Fatalf("BindInsertValues enum error = %v", err)
	}
}

func eventsDef() catalog.TableDef {
	return catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "tenant_id", Type: sqltype.Int64},
			{Name: "event_type", Type: sqltype.Text},
		},
	}
}
