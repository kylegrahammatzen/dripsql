// DDL binder tests: CREATE TYPE -> TypeSpec, CREATE TABLE -> TableSpec with options vocab.
// Validation runs through schema.Validate so option/value pairs that are syntactically valid but
// semantically wrong (unknown enum, segment_rows <= 0, etc.) are caught here.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func bindStmt[T Stmt](t *testing.T, src string) T {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	out, ok := stmt.(T)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	return out
}

func TestBindCreateType_NormalizesAndValidates(t *testing.T) {
	stmt := bindStmt[*CreateTypeStmt](t, "CREATE TYPE Event AS ENUM ('a', 'b')")
	plan, err := BindCreateType(stmt)
	if err != nil {
		t.Fatalf("BindCreateType: %v", err)
	}
	if plan.TypeSpec.Name != "event" {
		t.Fatalf("Name = %q, want lowercase", plan.TypeSpec.Name)
	}
	if len(plan.TypeSpec.EnumLabels) != 2 {
		t.Fatalf("EnumLabels = %v", plan.TypeSpec.EnumLabels)
	}
}

func TestBindCreateType_EmptyLabelsErrors(t *testing.T) {
	stmt := &CreateTypeStmt{Name: "x"}
	if _, err := BindCreateType(stmt); err == nil {
		t.Fatal("empty enum must fail Validate")
	}
}

func TestBindCreateTable_ColumnsParseTypes(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE users (id int64 NOT NULL, name text)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Name != "users" || len(plan.TableSpec.Columns) != 2 {
		t.Fatalf("spec = %+v", plan.TableSpec)
	}
	if plan.TableSpec.Columns[0].Type.Kind != schema.KindInt64 || plan.TableSpec.Columns[0].Nullable {
		t.Fatalf("col 0 = %+v", plan.TableSpec.Columns[0])
	}
	if plan.TableSpec.Columns[1].Type.Kind != schema.KindText || !plan.TableSpec.Columns[1].Nullable {
		t.Fatalf("col 1 = %+v", plan.TableSpec.Columns[1])
	}
}

func TestBindCreateTable_NamedTypeColumn(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (e EventKind)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Columns[0].Type.Kind != schema.KindNamed || plan.TableSpec.Columns[0].Type.Name != "eventkind" {
		t.Fatalf("col = %+v", plan.TableSpec.Columns[0])
	}
}

func TestBindCreateTable_OptionsVocab(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (storage = columnar, profile = event_analytics, compression = best, segment_rows = 1000, sort_by = 'id', time_column = 'id')")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	opts := plan.TableSpec.Options
	if opts.Storage != schema.StorageColumnar {
		t.Fatalf("Storage = %v", opts.Storage)
	}
	if opts.Profile != schema.ProfileEventAnalytics {
		t.Fatalf("Profile = %v", opts.Profile)
	}
	if opts.Compression != schema.CompressionBest {
		t.Fatalf("Compression = %v", opts.Compression)
	}
	if opts.SegmentRows.Auto || opts.SegmentRows.Rows != 1000 {
		t.Fatalf("SegmentRows = %+v", opts.SegmentRows)
	}
	if len(opts.SortBy) != 1 || opts.SortBy[0] != "id" {
		t.Fatalf("SortBy = %v", opts.SortBy)
	}
	if opts.TimeColumn != "id" {
		t.Fatalf("TimeColumn = %q", opts.TimeColumn)
	}
}

func TestBindCreateTable_SegmentRowsAuto(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (segment_rows = auto)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Options.SegmentRows != schema.AutoSegmentRows {
		t.Fatalf("SegmentRows = %v, want Auto", plan.TableSpec.Options.SegmentRows)
	}
}

func TestBindCreateTable_RejectsUnknownOption(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (bogus = 1)")
	if _, err := BindCreateTable(stmt); err == nil {
		t.Fatal("unknown option must error")
	}
}

func TestBindCreateTable_RejectsBadOptionValue(t *testing.T) {
	for _, src := range []string{
		"CREATE TABLE t (id int64) WITH (storage = nonexistent)",
		"CREATE TABLE t (id int64) WITH (profile = bogus)",
		"CREATE TABLE t (id int64) WITH (compression = bogus)",
		"CREATE TABLE t (id int64) WITH (segment_rows = 0)",
		"CREATE TABLE t (id int64) WITH (segment_rows = manual)",
	} {
		stmt := bindStmt[*CreateTableStmt](t, src)
		if _, err := BindCreateTable(stmt); err == nil {
			t.Fatalf("must reject %q", src)
		}
	}
}

func TestBindCreateTable_RejectsDuplicateOption(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (storage = columnar, storage = row)")
	if _, err := BindCreateTable(stmt); err == nil {
		t.Fatal("duplicate option must error")
	}
}

func TestBindCreateTable_SortByMultipleColumns(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (a int64, b int64) WITH (sort_by = 'a,b')")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if len(plan.TableSpec.Options.SortBy) != 2 || plan.TableSpec.Options.SortBy[0] != "a" || plan.TableSpec.Options.SortBy[1] != "b" {
		t.Fatalf("SortBy = %v", plan.TableSpec.Options.SortBy)
	}
}
