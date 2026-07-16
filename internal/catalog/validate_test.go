package catalog

import (
	"strings"
	"testing"
)

func validBase() *File {
	return &File{
		FormatVersion: CurrentFormatVersion,
		Generation:    1,
		NextTableID:   2,
		NextTypeID:    1,
		Types:         []Type{},
		Tables: []Table{{
			TableID:             1,
			Name:                "t",
			SchemaVersion:       1,
			NextColumnID:        3,
			CreatedAtGeneration: 1,
			UpdatedAtGeneration: 1,
			Columns: []Column{
				{ColumnID: 1, Name: "id", Type: "int64", Ordinal: 0, AddedAtGeneration: 1},
				{ColumnID: 2, Name: "v", Type: "int64", Ordinal: 1, AddedAtGeneration: 1},
			},
			PrimaryKey:  []ColumnID{},
			Constraints: []Constraint{},
			StoragePolicy: StoragePolicy{
				Storage:     "default",
				Profile:     "default",
				Compression: "default",
				SegmentRows: SegmentRows{Mode: "auto"},
			},
		}},
	}
}

func mustFail(t *testing.T, f *File, want string) {
	t.Helper()
	err := Validate(f)
	if err == nil {
		t.Fatalf("expected failure containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got %v", want, err)
	}
}

func TestValidate_RejectsNewerFormatVersion(t *testing.T) {
	f := validBase()
	f.FormatVersion = CurrentFormatVersion + 1
	mustFail(t, f, "newer")
}

func TestValidate_RejectsZeroFormatVersion(t *testing.T) {
	f := validBase()
	f.FormatVersion = 0
	mustFail(t, f, "format_version is zero")
}

func TestValidate_RejectsDuplicateTableID(t *testing.T) {
	f := validBase()
	f.NextTableID = 3
	f.Tables = append(f.Tables, f.Tables[0])
	f.Tables[1].Name = "t2"
	mustFail(t, f, "duplicate table_id")
}

func TestValidate_RejectsDuplicateTableName(t *testing.T) {
	f := validBase()
	f.NextTableID = 3
	dup := f.Tables[0]
	dup.TableID = 2
	f.Tables = append(f.Tables, dup)
	mustFail(t, f, "duplicate table name")
}

func TestValidate_RejectsDuplicateColumnID(t *testing.T) {
	f := validBase()
	f.Tables[0].Columns[1].ColumnID = 1
	mustFail(t, f, "duplicate column_id")
}

func TestValidate_RejectsDuplicateActiveColumnName(t *testing.T) {
	f := validBase()
	f.Tables[0].Columns[1].Name = "id"
	mustFail(t, f, "duplicate active column name")
}

func TestValidate_RejectsNextTableIDTooLow(t *testing.T) {
	f := validBase()
	f.NextTableID = 1
	mustFail(t, f, "next_table_id")
}

func TestValidate_RejectsNextColumnIDTooLow(t *testing.T) {
	f := validBase()
	f.Tables[0].NextColumnID = 2
	mustFail(t, f, "next_column_id")
}

func TestValidate_RejectsUnknownStorage(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.Storage = "weird"
	mustFail(t, f, "unknown storage")
}

func TestValidate_RejectsUnknownCompression(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.Compression = "ultra"
	mustFail(t, f, "unknown compression")
}

func TestValidate_RejectsSortByMissingColumn(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.SortBy = []ColumnID{99}
	mustFail(t, f, "sort_by references missing")
}

func TestValidate_RejectsTimeColumnMissing(t *testing.T) {
	f := validBase()
	id := ColumnID(99)
	f.Tables[0].StoragePolicy.TimeColumnID = &id
	mustFail(t, f, "time_column_id references missing")
}

func TestValidate_RejectsPrimaryKeyMissingColumn(t *testing.T) {
	f := validBase()
	f.Tables[0].PrimaryKey = []ColumnID{99}
	mustFail(t, f, "primary_key references missing")
}

func TestValidate_RejectsPrimaryKeyDroppedColumn(t *testing.T) {
	f := validBase()
	g := Generation(1)
	f.Tables[0].Columns[0].DroppedAtGeneration = &g
	f.Tables[0].PrimaryKey = []ColumnID{1}
	mustFail(t, f, "dropped column_id")
}

func TestValidate_RejectsUnknownType(t *testing.T) {
	f := validBase()
	f.Tables[0].Columns[0].Type = "nonsense"
	mustFail(t, f, "unknown type")
}

func TestValidate_RejectsUnknownNamedType(t *testing.T) {
	f := validBase()
	f.Tables[0].Columns[0].Type = "named:nothere"
	mustFail(t, f, "unknown named type")
}

func TestValidate_RejectsUnknownColumnCodec(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.ColumnCodecs = []ColumnCodec{{ColumnID: 1, Codec: "bogus"}}
	mustFail(t, f, "unknown codec")
}

func TestValidate_RejectsDuplicateColumnCodec(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.ColumnCodecs = []ColumnCodec{
		{ColumnID: 1, Codec: "for"},
		{ColumnID: 1, Codec: "dict"},
	}
	mustFail(t, f, "duplicate column_id")
}

func TestValidate_RejectsBadSegmentRows(t *testing.T) {
	f := validBase()
	f.Tables[0].StoragePolicy.SegmentRows = SegmentRows{Mode: "fixed", Rows: 0}
	mustFail(t, f, "rows > 0")
}

func TestValidate_AcceptsValidBase(t *testing.T) {
	if err := Validate(validBase()); err != nil {
		t.Fatalf("validBase: %v", err)
	}
}
