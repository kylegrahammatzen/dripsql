// INSERT binder tests: column reordering, NOT NULL rejection, per-kind literal validation,
// enum label validation, int range checks.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func usersDef() BoundTableDef {
	return BoundTableDef{
		Name: "users",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64, Nullable: false},
			{ID: 2, Name: "name", Type: schema.Text, Nullable: true},
			{ID: 3, Name: "age", Type: schema.Int32, Nullable: true},
		},
	}
}

func bindInsert(t *testing.T, src string, def BoundTableDef) (*Plan, error) {
	t.Helper()
	stmt := bindStmt[*InsertStmt](t, src)
	return BindInsertPlan(stmt, def)
}

func TestBindInsert_AllColumns_PositionalOrder(t *testing.T) {
	plan, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice', 30)", usersDef())
	if err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
	cols := plan.Values.Columns
	if len(cols) != 3 || cols[0].ID != 1 || cols[2].ID != 3 {
		t.Fatalf("Columns = %+v", cols)
	}
	if cols[0].Values[0].Int != 1 || cols[1].Values[0].String != "alice" || cols[2].Values[0].Int != 30 {
		t.Fatalf("row 0 = %+v", cols)
	}
}

func TestBindInsert_ColumnList_Reorders_PreservesValues(t *testing.T) {
	plan, err := bindInsert(t, "INSERT INTO users (name, id, age) VALUES ('alice', 1, 30)", usersDef())
	if err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
	cols := plan.Values.Columns
	if len(cols) != 3 {
		t.Fatalf("Columns len = %d", len(cols))
	}
	if cols[0].Name != "id" || cols[0].Values[0].Int != 1 {
		t.Fatalf("id col = %+v", cols[0])
	}
	if cols[1].Name != "name" || cols[1].Values[0].String != "alice" {
		t.Fatalf("name col = %+v", cols[1])
	}
	if cols[2].Name != "age" || cols[2].Values[0].Int != 30 {
		t.Fatalf("age col = %+v", cols[2])
	}
}

func TestBindInsert_PartialColumnList_Rejected(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, name) VALUES (1, 'alice')", usersDef()); err == nil {
		t.Fatal("partial column list must error (only all-cols or no-list supported)")
	}
}

func TestBindInsert_NotNullViolation(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (null, 'alice', 30)", usersDef()); err == nil {
		t.Fatal("NULL into NOT NULL column must error")
	}
}

func TestBindInsert_NullableAcceptsNull(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, null, null)", usersDef()); err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
}

func TestBindInsert_TypeMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES ('not-an-int', 'alice', 30)", usersDef()); err == nil {
		t.Fatal("string into int64 column must error")
	}
}

func TestBindInsert_Int32Range(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice', 99999999999)", usersDef()); err == nil {
		t.Fatal("int32 out-of-range literal must error")
	}
}

func TestBindInsert_RowCountMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice')", usersDef()); err == nil {
		t.Fatal("row arity mismatch must error")
	}
}

func TestBindInsert_DuplicateColumnInList(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, id, age) VALUES (1, 2, 30)", usersDef()); err == nil {
		t.Fatal("duplicate column in list must error")
	}
}

func TestBindInsert_UnknownColumn(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, nope, age) VALUES (1, 'x', 30)", usersDef()); err == nil {
		t.Fatal("unknown column must error")
	}
}

func TestBindInsert_TargetTableMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO other VALUES (1, 'a', 30)", usersDef()); err == nil {
		t.Fatal("table-name mismatch must error")
	}
}

func TestBindInsert_EnumLabelValidation(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "kind", Type: schema.Named("event"), Labels: []string{"view", "click"}, Nullable: false},
		},
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('view')", def); err != nil {
		t.Fatalf("known label must bind: %v", err)
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('unknown')", def); err == nil {
		t.Fatal("unknown enum label must error")
	}
}

func TestBindInsert_TemporalLiteralValidation(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.UUID},
			{ID: 2, Name: "at", Type: schema.Timestamp},
			{ID: 3, Name: "day", Type: schema.Date},
		},
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', '2026-05-14T12:00:00.000Z', '2026-05-14')", def); err != nil {
		t.Fatalf("valid temporals must bind: %v", err)
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('not-a-uuid', '2026-05-14T12:00:00.000Z', '2026-05-14')", def); err == nil {
		t.Fatal("invalid UUID must error")
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', 'tomorrow', '2026-05-14')", def); err == nil {
		t.Fatal("invalid timestamp must error")
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', '2026-05-14T12:00:00.000Z', 'yesterday')", def); err == nil {
		t.Fatal("invalid date must error")
	}
}

func TestBindInsert_RejectsEmptyValues(t *testing.T) {
	stmt := &InsertStmt{Table: "users"}
	if _, err := BindInsertPlan(stmt, usersDef()); err == nil {
		t.Fatal("empty VALUES must error")
	}
}

func TestBindInsert_NullCountTracked(t *testing.T) {
	stmt := bindStmt[*InsertStmt](t, "INSERT INTO users VALUES (1, null, 30), (2, 'b', null)")
	bound, err := BindInsertValues(stmt, usersDef())
	if err != nil {
		t.Fatalf("BindInsertValues: %v", err)
	}
	if bound.Columns[1].NullCount != 1 {
		t.Fatalf("name NullCount = %d, want 1", bound.Columns[1].NullCount)
	}
	if bound.Columns[2].NullCount != 1 {
		t.Fatalf("age NullCount = %d, want 1", bound.Columns[2].NullCount)
	}
}
