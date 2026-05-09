package catalog

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestCreateType(t *testing.T) {
	c := New()
	def, existed, err := c.CreateType(schema.TypeSpec{Name: "event_status", EnumLabels: []string{"new", "done"}})
	if err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	if existed {
		t.Fatalf("new type should not report existed")
	}
	if def.ID != 1 || def.Version != 1 || def.Name != "event_status" {
		t.Fatalf("unexpected type def: %#v", def)
	}

	got, ok := c.Type("EVENT_STATUS")
	if !ok || got.Name != def.Name {
		t.Fatalf("Type lookup = %#v, %v", got, ok)
	}
	got.Labels[0] = "mutated"
	got, _ = c.Type("event_status")
	if got.Labels[0] != "new" {
		t.Fatalf("Type lookup should return copy, got %#v", got.Labels)
	}
}

func TestCreateTypeIfNotExists(t *testing.T) {
	c := New()
	if _, _, err := c.CreateType(schema.TypeSpec{Name: "event_status", EnumLabels: []string{"new"}}); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	def, existed, err := c.CreateType(schema.TypeSpec{Name: "event_status", IfNotExists: true, EnumLabels: []string{"other"}})
	if err != nil {
		t.Fatalf("CreateType if not exists: %v", err)
	}
	if !existed || def.Labels[0] != "new" {
		t.Fatalf("CreateType if not exists = %#v existed=%v", def, existed)
	}
}

func TestCreateTable(t *testing.T) {
	c := New()
	if _, _, err := c.CreateType(schema.TypeSpec{Name: "event_status", EnumLabels: []string{"new", "done"}}); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	def, existed, err := c.CreateTable(schema.TableSpec{
		Name: "events",
		Columns: []schema.ColumnSpec{
			{Name: "tenant_id", Type: sqltype.Int64},
			{Name: "event_type", Type: sqltype.Text},
			{Name: "status", Type: sqltype.Named("event_status")},
		},
		Options: schema.TableOptions{Storage: schema.StorageColumnar, Profile: schema.ProfileEventAnalytics, Compression: schema.CompressionAuto},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if existed {
		t.Fatalf("new table should not report existed")
	}
	if def.ID != 1 || def.Version != 2 || len(def.Columns) != 3 {
		t.Fatalf("unexpected table def: %#v", def)
	}
	if def.Columns[0].ID != 1 || def.Columns[2].Type != sqltype.Named("event_status") {
		t.Fatalf("unexpected columns: %#v", def.Columns)
	}
	if def.Options.Storage != schema.StorageColumnar || def.Options.Profile != schema.ProfileEventAnalytics {
		t.Fatalf("options not preserved: %#v", def.Options)
	}

	got, ok := c.Table("EVENTS")
	if !ok || got.Name != "events" {
		t.Fatalf("Table lookup = %#v, %v", got, ok)
	}
	got.Columns[0].Name = "mutated"
	got, _ = c.Table("events")
	if got.Columns[0].Name != "tenant_id" {
		t.Fatalf("Table lookup should return copy, got %#v", got.Columns)
	}
}

func TestCreateTableAcceptsAllBuiltInTypes(t *testing.T) {
	c := New()
	def, _, err := c.CreateTable(schema.TableSpec{
		Name: "all_types",
		Columns: []schema.ColumnSpec{
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
		},
	})
	if err != nil {
		t.Fatalf("CreateTable all builtins: %v", err)
	}
	if len(def.Columns) != 14 {
		t.Fatalf("columns = %#v", def.Columns)
	}
	if def.Columns[0].Type != sqltype.Bool || def.Columns[13].Type != sqltype.JSON {
		t.Fatalf("types not preserved: %#v", def.Columns)
	}
}

func TestCreateTableRejectsMissingNamedType(t *testing.T) {
	c := New()
	_, _, err := c.CreateTable(schema.TableSpec{
		Name:    "events",
		Columns: []schema.ColumnSpec{{Name: "status", Type: sqltype.Named("event_status")}},
	})
	if err == nil || !strings.Contains(err.Error(), "missing type") {
		t.Fatalf("CreateTable missing type error = %v", err)
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	c := New()
	spec := schema.TableSpec{Name: "events", Columns: []schema.ColumnSpec{{Name: "tenant_id", Type: sqltype.Int64}}}
	if _, _, err := c.CreateTable(spec); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	spec.IfNotExists = true
	def, existed, err := c.CreateTable(spec)
	if err != nil {
		t.Fatalf("CreateTable if not exists: %v", err)
	}
	if !existed || def.ID != 1 {
		t.Fatalf("CreateTable if not exists = %#v existed=%v", def, existed)
	}
}
