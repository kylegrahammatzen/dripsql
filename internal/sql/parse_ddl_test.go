// DDL parser tests: CREATE TYPE AS ENUM with labels, CREATE TABLE columns+constraints+options,
// rejection of unsupported constraints (primary/unique/check/etc).
package sql

import "testing"

func parseCreateType(t *testing.T, src string) *CreateTypeStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*CreateTypeStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTypeStmt", stmt)
	}
	return s
}

func parseCreateTable(t *testing.T, src string) *CreateTableStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	return s
}

func TestCreateType_Enum_LabelsAndIfNotExists(t *testing.T) {
	s := parseCreateType(t, "CREATE TYPE event AS ENUM ('view', 'click', 'buy')")
	if s.Name != "event" || s.IfNotExists {
		t.Fatalf("Name=%q IfNotExists=%v", s.Name, s.IfNotExists)
	}
	if len(s.EnumLabels) != 3 || s.EnumLabels[0] != "view" || s.EnumLabels[2] != "buy" {
		t.Fatalf("EnumLabels = %v", s.EnumLabels)
	}
	s = parseCreateType(t, "CREATE TYPE IF NOT EXISTS event AS ENUM ('a')")
	if !s.IfNotExists {
		t.Fatal("IfNotExists not set")
	}
}

func TestCreateType_RejectsNonEnum(t *testing.T) {
	if _, err := Parse("CREATE TYPE x AS RANGE ('a', 'b')"); err == nil {
		t.Fatal("non-ENUM type must error")
	}
}

func TestCreateTable_BasicColumns(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE users (id int64, name text, age int32)")
	if s.Name != "users" {
		t.Fatalf("Name = %q", s.Name)
	}
	if len(s.Columns) != 3 {
		t.Fatalf("cols = %d", len(s.Columns))
	}
	if s.Columns[0].Type != "int64" || s.Columns[1].Type != "text" || s.Columns[2].Type != "int32" {
		t.Fatalf("col types = %+v", s.Columns)
	}
}

func TestCreateTable_NotNull(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64 NOT NULL, name text)")
	if !s.Columns[0].NotNull {
		t.Fatal("col 0 NotNull missing")
	}
	if s.Columns[1].NotNull {
		t.Fatal("col 1 NotNull should be false")
	}
}

func TestCreateTable_MultiwordType(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (ts timestamp with time zone)")
	if s.Columns[0].Type != "timestamp with time zone" {
		t.Fatalf("Type = %q", s.Columns[0].Type)
	}
}

func TestCreateTable_WithOptions(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64) WITH (segment_rows = 1000000, codec = 'zstd', mutable = true)")
	if len(s.Options) != 3 {
		t.Fatalf("Options len = %d", len(s.Options))
	}
	if s.Options[0].Value.Kind != ValueInt || s.Options[0].Value.Int != 1_000_000 {
		t.Fatalf("opt 0 = %+v", s.Options[0])
	}
	if s.Options[1].Value.Kind != ValueString || s.Options[1].Value.String != "zstd" {
		t.Fatalf("opt 1 = %+v", s.Options[1])
	}
	if s.Options[2].Value.Kind != ValueBool || !s.Options[2].Value.Bool {
		t.Fatalf("opt 2 = %+v", s.Options[2])
	}
}

func TestCreateTable_OptionIdentBareWord(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64) WITH (storage = mutable)")
	if s.Options[0].Value.Kind != ValueIdent || s.Options[0].Value.String != "mutable" {
		t.Fatalf("opt = %+v", s.Options[0])
	}
}

func TestCreateTable_RejectsUnsupportedConstraints(t *testing.T) {
	for _, src := range []string{
		"CREATE TABLE t (id int64 PRIMARY KEY)",
		"CREATE TABLE t (id int64 UNIQUE)",
		"CREATE TABLE t (id int64 CHECK (id > 0))",
		"CREATE TABLE t (id int64 REFERENCES other)",
		"CREATE TABLE t (PRIMARY KEY (id))",
		"CREATE TABLE t (id int64, FOREIGN KEY (id))",
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("must reject %q", src)
		}
	}
}

func TestCreateTable_RejectsEmptyColumnList(t *testing.T) {
	if _, err := Parse("CREATE TABLE t ()"); err == nil {
		t.Fatal("empty column list must error")
	}
}

func TestCreateTable_RejectsTrailingComma(t *testing.T) {
	if _, err := Parse("CREATE TABLE t (id int64,)"); err == nil {
		t.Fatal("trailing comma must error")
	}
}

func TestCreateTable_IfNotExists(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE IF NOT EXISTS t (id int64)")
	if !s.IfNotExists {
		t.Fatal("IfNotExists not set")
	}
}
