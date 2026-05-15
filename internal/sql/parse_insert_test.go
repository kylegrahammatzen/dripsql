// INSERT parser tests: column list optional, single + multi-row VALUES, null/bool/int/float/string literals.
package sql

import "testing"

func parseInsert(t *testing.T, src string) *InsertStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T, want *InsertStmt", stmt)
	}
	return s
}

func TestInsert_WithColumnList_SingleRow(t *testing.T) {
	s := parseInsert(t, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	if s.Table != "users" {
		t.Fatalf("Table = %q", s.Table)
	}
	if len(s.Columns) != 2 || s.Columns[0] != "id" || s.Columns[1] != "name" {
		t.Fatalf("Columns = %v", s.Columns)
	}
	if len(s.Values) != 1 || len(s.Values[0]) != 2 {
		t.Fatalf("Values shape = %+v", s.Values)
	}
	if s.Values[0][0].Kind != ValueInt || s.Values[0][0].Int != 1 {
		t.Fatalf("row 0 col 0 = %+v", s.Values[0][0])
	}
	if s.Values[0][1].Kind != ValueString || s.Values[0][1].String != "alice" {
		t.Fatalf("row 0 col 1 = %+v", s.Values[0][1])
	}
}

func TestInsert_NoColumnList(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (1)")
	if len(s.Columns) != 0 {
		t.Fatalf("Columns = %v", s.Columns)
	}
	if len(s.Values) != 1 || s.Values[0][0].Int != 1 {
		t.Fatalf("Values = %+v", s.Values)
	}
}

func TestInsert_MultiRow(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t (id) VALUES (1), (2), (3)")
	if len(s.Values) != 3 {
		t.Fatalf("rows = %d", len(s.Values))
	}
	for i, want := range []int64{1, 2, 3} {
		if s.Values[i][0].Int != want {
			t.Fatalf("row %d = %d, want %d", i, s.Values[i][0].Int, want)
		}
	}
}

func TestInsert_LiteralKinds(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (1, 2.5, 'x', true, false, null)")
	row := s.Values[0]
	want := []ValueKind{ValueInt, ValueFloat, ValueString, ValueBool, ValueBool, ValueNull}
	for i, w := range want {
		if row[i].Kind != w {
			t.Fatalf("col %d kind = %d, want %d (%+v)", i, row[i].Kind, w, row[i])
		}
	}
	if !row[3].Bool || row[4].Bool {
		t.Fatalf("bool values: %+v %+v", row[3], row[4])
	}
}

func TestInsert_NegativeLiteral(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (-1, -2.5)")
	if s.Values[0][0].Kind != ValueInt || s.Values[0][0].Int != -1 {
		t.Fatalf("col 0 = %+v", s.Values[0][0])
	}
	if s.Values[0][1].Kind != ValueFloat || s.Values[0][1].Float != -2.5 {
		t.Fatalf("col 1 = %+v", s.Values[0][1])
	}
}

func TestInsert_RejectsNonLiteralValue(t *testing.T) {
	if _, err := Parse("INSERT INTO t VALUES (id)"); err == nil {
		t.Fatal("non-literal in VALUES must error")
	}
}
