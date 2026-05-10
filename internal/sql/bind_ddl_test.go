package sql

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestBindCreateTableSpec(t *testing.T) {
	spec, err := BindCreateTableSpec(&CreateTableStmt{
		Name:        "events",
		IfNotExists: true,
		Columns: []ColumnDef{
			{Name: "tenant_id", Type: "int64", NotNull: true},
			{Name: "event_type", Type: "text", NotNull: true},
			{Name: "status", Type: "event_status"},
		},
		Options: []TableOption{
			{Name: "storage", Value: Value{Kind: ValueIdent, String: "columnar"}},
			{Name: "profile", Value: Value{Kind: ValueIdent, String: "event_analytics"}},
			{Name: "segment_rows", Value: Value{Kind: ValueInt, Int: 100000}},
			{Name: "compression", Value: Value{Kind: ValueString, String: "auto"}},
			{Name: "sort_by", Value: Value{Kind: ValueString, String: "tenant_id, event_type"}},
			{Name: "time_column", Value: Value{Kind: ValueIdent, String: "tenant_id"}},
		},
	})
	if err != nil {
		t.Fatalf("BindCreateTableSpec: %v", err)
	}
	if spec.Name != "events" || !spec.IfNotExists {
		t.Fatalf("unexpected table header: %#v", spec)
	}
	if len(spec.Columns) != 3 {
		t.Fatalf("columns = %#v", spec.Columns)
	}
	assertColumn(t, spec.Columns[0], "tenant_id", types.Int64, false)
	assertColumn(t, spec.Columns[1], "event_type", types.Text, false)
	assertColumn(t, spec.Columns[2], "status", types.Named("event_status"), true)

	if spec.Options.Storage != types.StorageColumnar || spec.Options.Profile != types.ProfileEventAnalytics {
		t.Fatalf("options = %#v", spec.Options)
	}
	if spec.Options.SegmentRows.Rows != 100000 || spec.Options.SegmentRows.Auto {
		t.Fatalf("segment rows = %#v", spec.Options.SegmentRows)
	}
	if spec.Options.Compression != types.CompressionAuto {
		t.Fatalf("compression = %#v", spec.Options.Compression)
	}
	if strings.Join(spec.Options.SortBy, ",") != "tenant_id,event_type" {
		t.Fatalf("sort_by = %#v", spec.Options.SortBy)
	}
	if spec.Options.TimeColumn != "tenant_id" {
		t.Fatalf("time_column = %q", spec.Options.TimeColumn)
	}
}

func TestBindCreateTableReturnsPlan(t *testing.T) {
	plan, err := BindCreateTable(&CreateTableStmt{Name: "events", Columns: []ColumnDef{{Name: "id", Type: "int64"}}})
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.Spec.Name != "events" || len(plan.Spec.Columns) != 1 {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBindCreateTableRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name string
		opt  TableOption
		want string
	}{
		{name: "unknown", opt: TableOption{Name: "column_store", Value: Value{Kind: ValueBool, Bool: true}}, want: "unknown table option"},
		{name: "bad storage", opt: TableOption{Name: "storage", Value: Value{Kind: ValueIdent, String: "heap"}}, want: "unsupported storage"},
		{name: "bad segment rows", opt: TableOption{Name: "segment_rows", Value: Value{Kind: ValueInt, Int: 0}}, want: "segment_rows must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BindCreateTableSpec(&CreateTableStmt{
				Name:    "events",
				Columns: []ColumnDef{{Name: "id", Type: "int64"}},
				Options: []TableOption{tt.opt},
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("BindCreateTableSpec error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindCreateTableRejectsDuplicateOption(t *testing.T) {
	_, err := BindCreateTableSpec(&CreateTableStmt{
		Name:    "events",
		Columns: []ColumnDef{{Name: "id", Type: "int64"}},
		Options: []TableOption{
			{Name: "storage", Value: Value{Kind: ValueIdent, String: "columnar"}},
			{Name: "storage", Value: Value{Kind: ValueIdent, String: "row"}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate table option") {
		t.Fatalf("BindCreateTableSpec duplicate option error = %v", err)
	}
}

func TestBindCreateTypeCopiesEnumLabels(t *testing.T) {
	labels := []string{"new", "old"}
	spec, err := BindCreateTypeSpec(&CreateTypeStmt{Name: "status", EnumLabels: labels})
	if err != nil {
		t.Fatalf("BindCreateTypeSpec: %v", err)
	}

	labels[0] = "changed"
	if spec.EnumLabels[0] != "new" {
		t.Fatalf("enum labels should be copied, got %q", spec.EnumLabels[0])
	}
}

func TestBindCreateTypeReturnsPlan(t *testing.T) {
	plan, err := BindCreateType(&CreateTypeStmt{Name: "status", EnumLabels: []string{"new"}})
	if err != nil {
		t.Fatalf("BindCreateType: %v", err)
	}
	if plan.Spec.Name != "status" || len(plan.Spec.EnumLabels) != 1 {
		t.Fatalf("plan = %#v", plan)
	}
}

func assertColumn(t *testing.T, got types.ColumnSpec, name string, typ types.Type, nullable bool) {
	t.Helper()
	if got.Name != name || got.Type != typ || got.Nullable != nullable {
		t.Fatalf("column = %#v, want name=%q type=%s nullable=%v", got, name, typ, nullable)
	}
}
