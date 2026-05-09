package schema

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestTypeSpecValidate(t *testing.T) {
	valid := TypeSpec{Name: "event_status", EnumLabels: []string{"new", "done"}}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid spec: %v", err)
	}

	cases := []struct {
		name string
		spec TypeSpec
		want string
	}{
		{name: "empty name", spec: TypeSpec{Name: "", EnumLabels: []string{"new"}}, want: "name is required"},
		{name: "no labels", spec: TypeSpec{Name: "s"}, want: "at least one enum label"},
		{name: "empty label", spec: TypeSpec{Name: "s", EnumLabels: []string{""}}, want: "empty enum label"},
		{name: "duplicate label", spec: TypeSpec{Name: "s", EnumLabels: []string{"a", "a"}}, want: "duplicate enum label"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestColumnSpecValidate(t *testing.T) {
	if err := (ColumnSpec{Name: "id", Type: sqltype.Int64}).Validate(); err != nil {
		t.Fatalf("valid column: %v", err)
	}
	for _, typ := range []sqltype.Type{
		sqltype.Bool,
		sqltype.Int16,
		sqltype.Int32,
		sqltype.Int64,
		sqltype.Float32,
		sqltype.Float64,
		sqltype.Decimal,
		sqltype.Text,
		sqltype.Bytes,
		sqltype.UUID,
		sqltype.Timestamp,
		sqltype.Time,
		sqltype.Date,
		sqltype.JSON,
	} {
		if err := (ColumnSpec{Name: "value", Type: typ}).Validate(); err != nil {
			t.Fatalf("valid %s column: %v", typ, err)
		}
	}

	cases := []struct {
		name string
		spec ColumnSpec
		want string
	}{
		{name: "empty name", spec: ColumnSpec{Type: sqltype.Int64}, want: "name is required"},
		{name: "invalid type", spec: ColumnSpec{Name: "id"}, want: "invalid type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestTableSpecValidate(t *testing.T) {
	valid := TableSpec{
		Name: "events",
		Columns: []ColumnSpec{
			{Name: "tenant_id", Type: sqltype.Int64},
			{Name: "event_type", Type: sqltype.Text},
		},
		Options: TableOptions{Storage: StorageColumnar, Profile: ProfileEventAnalytics, Compression: CompressionAuto},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid spec: %v", err)
	}

	cases := []struct {
		name string
		spec TableSpec
		want string
	}{
		{name: "empty name", spec: TableSpec{Columns: valid.Columns}, want: "name is required"},
		{name: "no columns", spec: TableSpec{Name: "events"}, want: "at least one column"},
		{name: "duplicate column", spec: TableSpec{Name: "events", Columns: []ColumnSpec{
			{Name: "id", Type: sqltype.Int64}, {Name: "id", Type: sqltype.Int64},
		}}, want: "duplicate column"},
		{name: "bad options", spec: TableSpec{Name: "events", Columns: valid.Columns, Options: TableOptions{
			SegmentRows: SegmentRowsOption{Auto: true, Rows: 1024},
		}}, want: "segment_rows"},
		{name: "missing sort column", spec: TableSpec{Name: "events", Columns: valid.Columns, Options: TableOptions{
			SortBy: []string{"missing"},
		}}, want: "sort_by references missing column"},
		{name: "missing time column", spec: TableSpec{Name: "events", Columns: valid.Columns, Options: TableOptions{
			TimeColumn: "missing",
		}}, want: "time_column references missing column"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestTableOptionsValidate(t *testing.T) {
	if err := (TableOptions{}).Validate(); err != nil {
		t.Fatalf("zero options: %v", err)
	}
	if err := (TableOptions{Storage: StorageColumnar, Profile: ProfileEventAnalytics, Compression: CompressionAuto, SegmentRows: AutoSegmentRows}).Validate(); err != nil {
		t.Fatalf("full valid options: %v", err)
	}

	cases := []struct {
		name string
		opts TableOptions
		want string
	}{
		{name: "bad storage", opts: TableOptions{Storage: 99}, want: "invalid storage"},
		{name: "bad profile", opts: TableOptions{Profile: 99}, want: "invalid profile"},
		{name: "bad compression", opts: TableOptions{Compression: 99}, want: "invalid compression"},
		{name: "segment_rows conflict", opts: TableOptions{SegmentRows: SegmentRowsOption{Auto: true, Rows: 1024}}, want: "segment_rows"},
		{name: "negative segment_rows", opts: TableOptions{SegmentRows: SegmentRowsOption{Rows: -1}}, want: "segment_rows"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opts.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}
