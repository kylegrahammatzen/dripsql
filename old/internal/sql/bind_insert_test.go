package sql

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestBindInsertValues(t *testing.T) {
	bound, err := BindInsertValues(&InsertStmt{
		Table:   "events",
		Columns: []string{"event_type", "tenant_id"},
		Values: [][]Value{
			{{Kind: ValueString, String: "signup"}, {Kind: ValueInt, Int: 1}},
			{{Kind: ValueString, String: "checkout"}, {Kind: ValueInt, Int: 2}},
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

func TestBindInsertPlan(t *testing.T) {
	plan, err := BindInsertPlan(&InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueInt, Int: 1}, {Kind: ValueString, String: "signup"}}}}, eventsDef())
	if err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
	if plan.Table.Name != "events" || len(plan.Columns) != 2 || plan.Columns[0] != 1 || plan.Columns[1] != 2 {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBindInsertValuesRejectsBadInput(t *testing.T) {
	tests := []struct {
		name string
		stmt *InsertStmt
		want string
	}{
		{
			name: "wrong row length",
			stmt: &InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueInt, Int: 1}}}},
			want: "has 1 values",
		},
		{
			name: "partial column list",
			stmt: &InsertStmt{Table: "events", Columns: []string{"tenant_id"}, Values: [][]Value{{{Kind: ValueInt, Int: 1}}}},
			want: "column count",
		},
		{
			name: "unknown column",
			stmt: &InsertStmt{Table: "events", Columns: []string{"tenant_id", "missing"}, Values: [][]Value{{{Kind: ValueInt, Int: 1}, {Kind: ValueString, String: "signup"}}}},
			want: "unknown INSERT column",
		},
		{
			name: "type mismatch",
			stmt: &InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueString, String: "bad"}, {Kind: ValueString, String: "signup"}}}},
			want: "expects int64 literal",
		},
		{
			name: "null",
			stmt: &InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueNull}, {Kind: ValueString, String: "signup"}}}},
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
	_, err := BindInsertValues(&InsertStmt{
		Table:   "events",
		Columns: []string{"tenant_id", "event_type"},
		Values: [][]Value{
			{{Kind: ValueInt, Int: 1}, {Kind: ValueString, String: "signup"}},
			{{Kind: ValueInt, Int: 2}},
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
	bound, err := BindInsertValues(&InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueInt, Int: 1}, {Kind: ValueNull}}}}, def)
	if err != nil {
		t.Fatalf("BindInsertValues: %v", err)
	}
	if bound.Columns[1].NullCount != 1 || bound.Columns[1].Values[0].Kind != ValueNull {
		t.Fatalf("nulls = %#v, want row 0 null", bound.Columns[1])
	}
}

func TestBindInsertValuesValidatesAllBuiltins(t *testing.T) {
	def := BoundTableDef{
		Name: "all_types",
		Columns: []BoundColumnDef{
			{Name: "b", Type: types.Bool},
			{Name: "i16", Type: types.Int16},
			{Name: "i32", Type: types.Int32},
			{Name: "i64", Type: types.Int64},
			{Name: "f32", Type: types.Float32},
			{Name: "f64", Type: types.Float64},
			{Name: "dec", Type: types.Decimal},
			{Name: "txt", Type: types.Text},
			{Name: "bin", Type: types.Bytes},
			{Name: "id", Type: types.UUID},
			{Name: "ts", Type: types.Timestamp},
			{Name: "tm", Type: types.Time},
			{Name: "d", Type: types.Date},
			{Name: "j", Type: types.JSON},
			{Name: "status", Type: types.Named("event_status"), Labels: []string{"new", "done"}},
		},
	}
	_, err := BindInsertValues(&InsertStmt{
		Table: "all_types",
		Values: [][]Value{{
			{Kind: ValueBool, Bool: true},
			{Kind: ValueInt, Int: 7},
			{Kind: ValueInt, Int: 8},
			{Kind: ValueInt, Int: 9},
			{Kind: ValueFloat, Float: 10.5},
			{Kind: ValueFloat, Float: 11.75},
			{Kind: ValueString, String: "12.25"},
			{Kind: ValueString, String: "hello"},
			{Kind: ValueString, String: "deadbeef"},
			{Kind: ValueString, String: "550e8400-e29b-41d4-a716-446655440000"},
			{Kind: ValueString, String: "2026-05-07T00:00:00Z"},
			{Kind: ValueString, String: "12:34:56"},
			{Kind: ValueString, String: "2026-05-07"},
			{Kind: ValueString, String: `{"ok":true}`},
			{Kind: ValueString, String: "new"},
		}},
	}, def)
	if err != nil {
		t.Fatalf("BindInsertValues all builtins: %v", err)
	}
}

func TestBindInsertValuesRejectsBytesNonString(t *testing.T) {
	def := BoundTableDef{Name: "files", Columns: []BoundColumnDef{{Name: "payload", Type: types.Bytes}}}
	_, err := BindInsertValues(&InsertStmt{Table: "files", Values: [][]Value{{{Kind: ValueInt, Int: 7}}}}, def)
	if err == nil || !strings.Contains(err.Error(), "expects string literal") {
		t.Fatalf("BindInsertValues bytes error = %v", err)
	}
}

func TestBindInsertValuesValidatesNamedEnumLabel(t *testing.T) {
	def := BoundTableDef{Name: "events", Columns: []BoundColumnDef{{Name: "status", Type: types.Named("event_status"), Labels: []string{"new", "done"}}}}
	_, err := BindInsertValues(&InsertStmt{Table: "events", Values: [][]Value{{{Kind: ValueString, String: "missing"}}}}, def)
	if err == nil || !strings.Contains(err.Error(), "invalid enum label") {
		t.Fatalf("BindInsertValues enum error = %v", err)
	}
}

func eventsDef() BoundTableDef {
	return BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "tenant_id", Type: types.Int64},
			{ID: 2, Name: "event_type", Type: types.Text},
		},
	}
}
