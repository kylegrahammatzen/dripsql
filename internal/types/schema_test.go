// Schema validation tests: TableSpec/ColumnSpec/TableOptions/TypeSpec invariants
// and the CompressionPolicy AllowsFlate/AllowsZstd matrix.
package types

import "testing"

func TestSchema_TableSpecGood(t *testing.T) {
	spec := TableSpec{
		Name: "events",
		Columns: []ColumnSpec{
			{Name: "id", Type: Int64},
			{Name: "body", Type: Text, Nullable: true},
		},
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("good spec: %v", err)
	}
}

func TestSchema_DupColumnNamesRejected(t *testing.T) {
	spec := TableSpec{
		Name:    "events",
		Columns: []ColumnSpec{{Name: "id", Type: Int64}, {Name: "ID", Type: Int64}},
	}
	if err := spec.Validate(); err == nil {
		t.Fatal("dup column names (case-insensitive) must error")
	}
}

func TestSchema_EmptyColumnsAndName(t *testing.T) {
	if err := (TableSpec{Name: "x"}).Validate(); err == nil {
		t.Fatal("zero columns must error")
	}
	if err := (TableSpec{Columns: []ColumnSpec{{Name: "id", Type: Int64}}}).Validate(); err == nil {
		t.Fatal("empty name must error")
	}
}

func TestSchema_SortByAndTimeColumnReferences(t *testing.T) {
	base := TableSpec{
		Name:    "events",
		Columns: []ColumnSpec{{Name: "id", Type: Int64}, {Name: "ts", Type: Timestamp}},
	}
	good := base
	good.Options = TableOptions{SortBy: []string{"id"}, TimeColumn: "ts"}
	if err := good.Validate(); err != nil {
		t.Fatalf("good refs: %v", err)
	}
	bad := base
	bad.Options = TableOptions{SortBy: []string{"missing"}}
	if err := bad.Validate(); err == nil {
		t.Fatal("sort_by missing column must error")
	}
	bad2 := base
	bad2.Options = TableOptions{TimeColumn: "missing"}
	if err := bad2.Validate(); err == nil {
		t.Fatal("time_column missing column must error")
	}
}

func TestSchema_ColumnSpecValidation(t *testing.T) {
	if err := (ColumnSpec{Name: "id"}).Validate(); err == nil {
		t.Fatal("invalid type must error")
	}
	if err := (ColumnSpec{Type: Int64}).Validate(); err == nil {
		t.Fatal("empty name must error")
	}
	if err := (ColumnSpec{Name: "x", Type: Int64, Nullable: true}).Validate(); err != nil {
		t.Fatalf("nullable col: %v", err)
	}
}

func TestSchema_TableOptionsValidation(t *testing.T) {
	if err := (TableOptions{}).Validate(); err != nil {
		t.Fatalf("zero TableOptions invalid: %v", err)
	}
	if err := (TableOptions{Storage: 99}).Validate(); err == nil {
		t.Fatal("bad storage must error")
	}
	if err := (TableOptions{Profile: 99}).Validate(); err == nil {
		t.Fatal("bad profile must error")
	}
	if err := (TableOptions{Compression: 99}).Validate(); err == nil {
		t.Fatal("bad compression must error")
	}
	if err := (TableOptions{SegmentRows: SegmentRowsOption{Rows: -1}}).Validate(); err == nil {
		t.Fatal("negative segment rows must error")
	}
}

func TestSchema_SegmentRowsAutoConflict(t *testing.T) {
	if err := (SegmentRowsOption{Auto: true, Rows: 1000}).Validate(); err == nil {
		t.Fatal("auto+rows must error")
	}
	if err := AutoSegmentRows.Validate(); err != nil {
		t.Fatalf("AutoSegmentRows: %v", err)
	}
	if err := SegmentRows(4096).Validate(); err != nil {
		t.Fatalf("SegmentRows(4096): %v", err)
	}
}

func TestSchema_TypeSpecValidation(t *testing.T) {
	if err := (TypeSpec{Name: "status"}).Validate(); err == nil {
		t.Fatal("zero labels must error")
	}
	if err := (TypeSpec{Name: "status", EnumLabels: []string{"a", "a"}}).Validate(); err == nil {
		t.Fatal("duplicate label must error")
	}
	if err := (TypeSpec{Name: "status", EnumLabels: []string{"a", ""}}).Validate(); err == nil {
		t.Fatal("empty label must error")
	}
	if err := (TypeSpec{Name: "status", EnumLabels: []string{"a", "b"}}).Validate(); err != nil {
		t.Fatalf("good TypeSpec: %v", err)
	}
}

func TestSchema_CompressionPolicyAllows(t *testing.T) {
	cases := []struct {
		p     CompressionPolicy
		flate bool
		zstd  bool
	}{
		{CompressionDefault, true, true},
		{CompressionAuto, true, true},
		{CompressionNone, false, false},
		{CompressionFast, true, false},
		{CompressionBest, true, true},
	}
	for _, c := range cases {
		if c.p.AllowsFlate() != c.flate {
			t.Errorf("%s AllowsFlate=%v want %v", c.p, c.p.AllowsFlate(), c.flate)
		}
		if c.p.AllowsZstd() != c.zstd {
			t.Errorf("%s AllowsZstd=%v want %v", c.p, c.p.AllowsZstd(), c.zstd)
		}
	}
}
