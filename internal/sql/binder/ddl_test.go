package binder

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestBindCreateTable(t *testing.T) {
	spec, err := BindCreateTable(&ast.CreateTableStmt{
		Name:        "events",
		IfNotExists: true,
		Columns: []ast.ColumnDef{
			{Name: "tenant_id", Type: "int64", NotNull: true},
			{Name: "event_type", Type: "text", NotNull: true},
			{Name: "status", Type: "event_status"},
		},
		Options: []ast.TableOption{
			{Name: "storage", Value: ast.Value{Kind: ast.ValueIdent, String: "columnar"}},
			{Name: "profile", Value: ast.Value{Kind: ast.ValueIdent, String: "event_analytics"}},
			{Name: "segment_rows", Value: ast.Value{Kind: ast.ValueInt, Int: 100000}},
			{Name: "compression", Value: ast.Value{Kind: ast.ValueString, String: "auto"}},
			{Name: "sort_by", Value: ast.Value{Kind: ast.ValueString, String: "tenant_id, event_type"}},
			{Name: "time_column", Value: ast.Value{Kind: ast.ValueIdent, String: "tenant_id"}},
		},
	})
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if spec.Name != "events" || !spec.IfNotExists {
		t.Fatalf("unexpected table header: %#v", spec)
	}
	if len(spec.Columns) != 3 {
		t.Fatalf("columns = %#v", spec.Columns)
	}
	assertColumn(t, spec.Columns[0], "tenant_id", sqltype.Int64, false)
	assertColumn(t, spec.Columns[1], "event_type", sqltype.Text, false)
	assertColumn(t, spec.Columns[2], "status", sqltype.Named("event_status"), true)

	if spec.Options.Storage != schema.StorageColumnar || spec.Options.Profile != schema.ProfileEventAnalytics {
		t.Fatalf("options = %#v", spec.Options)
	}
	if spec.Options.SegmentRows.Rows != 100000 || spec.Options.SegmentRows.Auto {
		t.Fatalf("segment rows = %#v", spec.Options.SegmentRows)
	}
	if spec.Options.Compression != schema.CompressionAuto {
		t.Fatalf("compression = %#v", spec.Options.Compression)
	}
	if strings.Join(spec.Options.SortBy, ",") != "tenant_id,event_type" {
		t.Fatalf("sort_by = %#v", spec.Options.SortBy)
	}
	if spec.Options.TimeColumn != "tenant_id" {
		t.Fatalf("time_column = %q", spec.Options.TimeColumn)
	}
}

func TestBindCreateTableRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  ast.TableOption
		want string
	}{
		{name: "unknown", opt: ast.TableOption{Name: "column_store", Value: ast.Value{Kind: ast.ValueBool, Bool: true}}, want: "unknown table option"},
		{name: "bad storage", opt: ast.TableOption{Name: "storage", Value: ast.Value{Kind: ast.ValueIdent, String: "heap"}}, want: "unsupported storage"},
		{name: "bad segment rows", opt: ast.TableOption{Name: "segment_rows", Value: ast.Value{Kind: ast.ValueInt, Int: 0}}, want: "segment_rows must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BindCreateTable(&ast.CreateTableStmt{
				Name:    "events",
				Columns: []ast.ColumnDef{{Name: "id", Type: "int64"}},
				Options: []ast.TableOption{tt.opt},
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindCreateTable error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindCreateTableRejectsDuplicateOption(t *testing.T) {
	_, err := BindCreateTable(&ast.CreateTableStmt{
		Name:    "events",
		Columns: []ast.ColumnDef{{Name: "id", Type: "int64"}},
		Options: []ast.TableOption{
			{Name: "storage", Value: ast.Value{Kind: ast.ValueIdent, String: "columnar"}},
			{Name: "storage", Value: ast.Value{Kind: ast.ValueIdent, String: "row"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate table option") {
		t.Fatalf("BindCreateTable duplicate option error = %v", err)
	}
}

func TestBindCreateTypeCopiesEnumLabels(t *testing.T) {
	labels := []string{"new", "old"}
	spec, err := BindCreateType(&ast.CreateTypeStmt{Name: "status", EnumLabels: labels})
	if err != nil {
		t.Fatalf("BindCreateType: %v", err)
	}

	labels[0] = "changed"
	if spec.EnumLabels[0] != "new" {
		t.Fatalf("enum labels should be copied, got %q", spec.EnumLabels[0])
	}
}

func assertColumn(t *testing.T, got schema.ColumnSpec, name string, typ sqltype.Type, nullable bool) {
	t.Helper()
	if got.Name != name || got.Type != typ || got.Nullable != nullable {
		t.Fatalf("column = %#v, want name=%q type=%s nullable=%v", got, name, typ, nullable)
	}
}
