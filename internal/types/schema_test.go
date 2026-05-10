package types

import "testing"

func TestSchemaTableSpecValidate(t *testing.T) {
	good := TableSpec{
		Name:    "events",
		Columns: []ColumnSpec{{Name: "id", Type: Int64}, {Name: "body", Type: Text}},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("Validate good spec: %v", err)
	}
}

func TestSchemaDuplicateColumnNamesRejected(t *testing.T) {
	spec := TableSpec{
		Name: "events",
		Columns: []ColumnSpec{
			{Name: "id", Type: Int64},
			{Name: "ID", Type: Int64},
		},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected duplicate-column error")
	}
}

func TestSchemaEmptyColumnsRejected(t *testing.T) {
	spec := TableSpec{Name: "events"}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected at-least-one-column error")
	}
}

func TestSchemaEmptyTableNameRejected(t *testing.T) {
	spec := TableSpec{Columns: []ColumnSpec{{Name: "id", Type: Int64}}}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected table-name-required error")
	}
}

func TestSchemaSortByMustReferenceExistingColumn(t *testing.T) {
	spec := TableSpec{
		Name:    "events",
		Columns: []ColumnSpec{{Name: "id", Type: Int64}},
		Options: TableOptions{SortBy: []string{"missing"}},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected sort_by-references-missing error")
	}
}

func TestSchemaTimeColumnMustReferenceExistingColumn(t *testing.T) {
	spec := TableSpec{
		Name:    "events",
		Columns: []ColumnSpec{{Name: "id", Type: Int64}},
		Options: TableOptions{TimeColumn: "missing"},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected time_column-references-missing error")
	}
}

func TestColumnSpecRejectsInvalidType(t *testing.T) {
	col := ColumnSpec{Name: "id"}
	if err := col.Validate(); err == nil {
		t.Fatal("expected invalid-type error")
	}
}

func TestColumnSpecRequiresName(t *testing.T) {
	col := ColumnSpec{Type: Int64}
	if err := col.Validate(); err == nil {
		t.Fatal("expected column-name-required error")
	}
}

func TestColumnSpecAllowsNullable(t *testing.T) {
	col := ColumnSpec{Name: "x", Type: Int64, Nullable: true}
	if err := col.Validate(); err != nil {
		t.Fatalf("Validate nullable col: %v", err)
	}
}

func TestTableOptionsZeroValueIsValid(t *testing.T) {
	if err := (TableOptions{}).Validate(); err != nil {
		t.Fatalf("zero TableOptions invalid: %v", err)
	}
}

func TestTableOptionsRejectsInvalidStorageKind(t *testing.T) {
	if err := (TableOptions{Storage: 99}).Validate(); err == nil {
		t.Fatal("expected invalid-storage error")
	}
}

func TestTableOptionsRejectsInvalidProfile(t *testing.T) {
	if err := (TableOptions{Profile: 99}).Validate(); err == nil {
		t.Fatal("expected invalid-profile error")
	}
}

func TestTableOptionsRejectsNegativeSegmentRows(t *testing.T) {
	if err := (TableOptions{SegmentRows: SegmentRowsOption{Rows: -1}}).Validate(); err == nil {
		t.Fatal("expected segment-rows-negative error")
	}
}

func TestSegmentRowsOptionAutoAndRowsConflict(t *testing.T) {
	o := SegmentRowsOption{Auto: true, Rows: 1000}
	if err := o.Validate(); err == nil {
		t.Fatal("expected auto-and-rows error")
	}
}

func TestTypeSpecRequiresAtLeastOneEnumLabel(t *testing.T) {
	spec := TypeSpec{Name: "status"}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected at-least-one-label error")
	}
}

func TestTypeSpecRejectsDuplicateLabels(t *testing.T) {
	spec := TypeSpec{Name: "status", EnumLabels: []string{"a", "a"}}
	if err := spec.Validate(); err == nil {
		t.Fatal("expected duplicate-label error")
	}
}
