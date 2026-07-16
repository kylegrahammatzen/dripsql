// Engine round-trip smoke tests running CREATE -> INSERT -> SELECT through the Open/Exec/Query API.
// Each test gets a fresh temp dir so catalog and manifest start clean.
package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustExec(t *testing.T, db *DB, sql string) Result {
	t.Helper()
	r, err := db.Exec(context.Background(), sql)
	if err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
	return r
}

func mustQuery(t *testing.T, db *DB, sql string) *Rows {
	t.Helper()
	r, err := db.Query(context.Background(), sql)
	if err != nil {
		t.Fatalf("Query %q: %v", sql, err)
	}
	return r
}

func mustValues(t *testing.T, db *DB, sql string) [][]any {
	t.Helper()
	return mustQuery(t, db, sql).Values
}

func wantRows(t *testing.T, got [][]any, want [][]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d has %d cols, want %d: %v", i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("row %d col %d = %v (%T), want %v (%T)", i, j, got[i][j], got[i][j], want[i][j], want[i][j])
			}
		}
	}
}

func wantBag[T comparable](t *testing.T, got []T, want map[T]int) {
	t.Helper()
	seen := make(map[T]int, len(want))
	for _, v := range got {
		seen[v]++
	}
	if len(seen) != len(want) {
		t.Fatalf("got %v, want %v", seen, want)
	}
	for k, n := range want {
		if seen[k] != n {
			t.Fatalf("got %v = %d, want %d; all=%v", k, seen[k], n, seen)
		}
	}
}

func TestEngine_CreateInsertSelect(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	r := mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	if r.RowsAffected != 3 {
		t.Fatalf("inserted %d rows, want 3", r.RowsAffected)
	}
	wantRows(t, mustValues(t, db, "SELECT id, name FROM users"), [][]any{
		{int64(1), "alice"},
		{int64(2), "bob"},
		{int64(3), "carol"},
	})
}

func TestEngine_WhereAndOrder(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE items (id int64 NOT NULL, price int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO items (id, price) VALUES (1, 30), (2, 10), (3, 20)")
	wantRows(t, mustValues(t, db, "SELECT id FROM items WHERE price >= 20 ORDER BY price DESC"), [][]any{
		{int64(1)},
		{int64(3)},
	})
}

func TestEngine_GroupBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE sales (id int64 NOT NULL, category text NOT NULL, price int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO sales (id, category, price) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)")
	rows := mustValues(t, db, "SELECT category, sum(price) FROM sales GROUP BY category")
	totals := make(map[string]int64, len(rows))
	for _, r := range rows {
		totals[r[0].(string)] = r[1].(int64)
	}
	if totals["a"] != 40 || totals["b"] != 20 || len(totals) != 2 {
		t.Fatalf("totals = %v, want a=40 b=20", totals)
	}
}

func TestEngine_MultiSegmentInsert(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3), (4)")
	rows := mustQuery(t, db, "SELECT id FROM t ORDER BY id")
	if len(rows.Values) != 4 {
		t.Fatalf("got %d rows, want 4", len(rows.Values))
	}
	for i, r := range rows.Values {
		if r[0].(int64) != int64(i+1) {
			t.Fatalf("row %d = %v, want %d", i, r[0], i+1)
		}
	}
}

func TestEngine_ReopenPersistsCatalogAndData(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id) VALUES (42)"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	rows, err := db2.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if len(rows.Values) != 1 || rows.Values[0][0].(int64) != 42 {
		t.Fatalf("after reopen: rows = %v, want [[42]]", rows.Values)
	}
}

func TestEngine_DuplicateTableErrors(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err == nil {
		t.Fatal("duplicate CREATE TABLE must error")
	}
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS t (id int64 NOT NULL)")
}

func TestEngine_QueryUnknownTable(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Query(context.Background(), "SELECT id FROM bogus"); err == nil {
		t.Fatal("query on unknown table must error")
	}
}

func TestEngine_ScalarLowerUpperLength(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, 'BOB')")
	rows := mustQuery(t, db, "SELECT lower(name) AS lo, upper(name) AS up, length(name) AS len FROM t")
	if len(rows.Values) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows.Values))
	}
	if rows.Values[0][0].(string) != "alice" || rows.Values[0][1].(string) != "ALICE" || rows.Values[0][2].(int64) != 5 {
		t.Fatalf("row 0 = %v", rows.Values[0])
	}
	if rows.Values[1][2].(int64) != 3 {
		t.Fatalf("row 1 length = %v, want 3", rows.Values[1][2])
	}
}

func TestEngine_Substring(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'dripsql')")
	rows := mustQuery(t, db, "SELECT substring(name, 1, 4) AS head, substring(name, 5) AS tail FROM t")
	if rows.Values[0][0].(string) != "drip" || rows.Values[0][1].(string) != "sql" {
		t.Fatalf("substring rows = %v", rows.Values[0])
	}
}

func TestEngine_Coalesce(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, label) VALUES (1, 'a'), (2, 'b')")
	rows := mustQuery(t, db, "SELECT coalesce(label, 'fallback') AS x FROM t")
	if len(rows.Values) != 2 || rows.Values[0][0].(string) != "a" || rows.Values[1][0].(string) != "b" {
		t.Fatalf("coalesce rows = %v", rows.Values)
	}
}

func TestEngine_NullableInsertAndSelect(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'carol')")
	rows := mustQuery(t, db, "SELECT id, name FROM t")
	if len(rows.Values) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows.Values))
	}
	byID := map[int64]any{}
	for _, r := range rows.Values {
		byID[r[0].(int64)] = r[1]
	}
	if byID[1] != "alice" || byID[3] != "carol" {
		t.Fatalf("non-null rows wrong: %v", byID)
	}
	if byID[2] != nil {
		t.Fatalf("row 2 name = %v, want nil", byID[2])
	}
}

func TestEngine_NullableRejectsNullForNotNullColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id, name) VALUES (1, NULL)"); err == nil {
		t.Fatal("INSERT NULL into NOT NULL column must error")
	}
}

func TestEngine_NullableWhereFiltersNulls(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, NULL), (3, 'c')")
	rows := mustQuery(t, db, "SELECT id FROM t WHERE name = 'a'")
	if len(rows.Values) != 1 || rows.Values[0][0].(int64) != 1 {
		t.Fatalf("filter result = %v, want [[1]]", rows.Values)
	}
}

func TestEngine_DeleteWhere(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, tag text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, tag) VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c')")
	r, err := db.Exec(context.Background(), "DELETE FROM t WHERE tag = 'a'")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if r.RowsAffected != 2 {
		t.Fatalf("DELETE affected = %d, want 2", r.RowsAffected)
	}
	values := mustValues(t, db, "SELECT id FROM t")
	ids := make([]int64, len(values))
	for i, row := range values {
		ids[i] = row[0].(int64)
	}
	wantBag(t, ids, map[int64]int{2: 1, 4: 1})
}

func TestEngine_DeleteAllRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")
	r, err := db.Exec(context.Background(), "DELETE FROM t")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	if r.RowsAffected != 3 {
		t.Fatalf("DELETE affected = %d, want 3", r.RowsAffected)
	}
	rows := mustQuery(t, db, "SELECT id FROM t")
	if len(rows.Values) != 0 {
		t.Fatalf("expected empty, got %v", rows.Values)
	}
}

func TestEngine_DeleteRepeatedAccumulates(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3), (4)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	mustExec(t, db, "DELETE FROM t WHERE id = 3")
	rows := mustQuery(t, db, "SELECT id FROM t")
	if len(rows.Values) != 2 {
		t.Fatalf("after two deletes got %d rows: %v", len(rows.Values), rows.Values)
	}
}

func TestEngine_DeletePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id) VALUES (1), (2), (3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "DELETE FROM t WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	rows, err := db2.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 2 {
		t.Fatalf("after reopen got %d rows: %v", len(rows.Values), rows.Values)
	}
	for _, r := range rows.Values {
		if r[0].(int64) == 2 {
			t.Fatalf("id=2 should be deleted")
		}
	}
}

func TestEngine_InnerJoin(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (2, 50), (2, 75)")
	rows := mustQuery(t, db, "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	if len(rows.Values) != 3 {
		t.Fatalf("got %d rows, want 3 (alice 100, bob 50, bob 75): %v", len(rows.Values), rows.Values)
	}
	type pair struct {
		name  string
		total int64
	}
	got := make(map[pair]int)
	for _, r := range rows.Values {
		got[pair{r[0].(string), r[1].(int64)}]++
	}
	want := map[pair]int{{"alice", 100}: 1, {"bob", 50}: 1, {"bob", 75}: 1}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("missing/duplicate join row %v: got=%v", k, got)
		}
	}
}

func TestEngine_InnerJoinWhereAndOrder(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (2, 50), (2, 75), (3, 10)")
	rows := mustQuery(t, db, "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 40 ORDER BY o.total DESC")
	want := [][]any{
		{"alice", int64(100)},
		{"bob", int64(75)},
		{"bob", int64(50)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_InnerJoinNoMatches(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id) VALUES (1), (2)")
	mustExec(t, db, "INSERT INTO b (id) VALUES (99)")
	rows := mustQuery(t, db, "SELECT a.id, b.id FROM a JOIN b ON a.id = b.id")
	if len(rows.Values) != 0 {
		t.Fatalf("expected zero rows, got %v", rows.Values)
	}
}

func TestEngine_ThreeTableJoinChain(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (id int64 NOT NULL, user_id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE items (order_id int64 NOT NULL, sku text NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	mustExec(t, db, "INSERT INTO orders (id, user_id) VALUES (10, 1), (11, 2)")
	mustExec(t, db, "INSERT INTO items (order_id, sku) VALUES (10, 'x'), (10, 'y'), (11, 'z')")
	rows := mustQuery(t, db, "SELECT u.name, i.sku FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id ORDER BY i.sku")
	want := [][]any{
		{"alice", "x"},
		{"alice", "y"},
		{"bob", "z"},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_LeftOuterJoinNullFill(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (2, 50)")
	rows := mustQuery(t, db, "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name")
	want := [][]any{
		{"alice", int64(100)},
		{"bob", int64(50)},
		{"carol", nil},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_UpdateWhere(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, score int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
	res, err := db.Exec(context.Background(), "UPDATE t SET score = 99 WHERE id = 2")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected = %d, want 1", res.RowsAffected)
	}
	rows := mustQuery(t, db, "SELECT id, name, score FROM t ORDER BY id")
	want := [][]any{
		{int64(1), "a", int64(10)},
		{int64(2), "b", int64(99)},
		{int64(3), "c", int64(30)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		for j := range w {
			if rows.Values[i][j] != w[j] {
				t.Fatalf("row %d col %d = %v, want %v", i, j, rows.Values[i][j], w[j])
			}
		}
	}
}

func TestEngine_UpdateAllRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, score int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, score) VALUES (1, 10), (2, 20), (3, 30)")
	res, err := db.Exec(context.Background(), "UPDATE t SET score = 0")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if res.RowsAffected != 3 {
		t.Fatalf("affected = %d, want 3", res.RowsAffected)
	}
	rows := mustQuery(t, db, "SELECT score FROM t")
	for _, r := range rows.Values {
		if r[0].(int64) != 0 {
			t.Fatalf("score = %v, want 0", r[0])
		}
	}
}

func TestEngine_UpdateMultipleColumns(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, score int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name, score) VALUES (1, 'a', 10), (2, 'b', 20)")
	res, err := db.Exec(context.Background(), "UPDATE t SET name = 'B', score = 100 WHERE id = 2")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("affected = %d, want 1", res.RowsAffected)
	}
	rows := mustQuery(t, db, "SELECT id, name, score FROM t WHERE id = 2")
	if len(rows.Values) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows.Values))
	}
	r := rows.Values[0]
	if r[1] != "B" || r[2].(int64) != 100 {
		t.Fatalf("row = %v, want [2 B 100]", r)
	}
}

func TestEngine_UpdatePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, score int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, score) VALUES (1, 10), (2, 20)")
	mustExec(t, db, "UPDATE t SET score = 77 WHERE id = 1")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	rows := mustQuery(t, db, "SELECT id, score FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(77)},
		{int64(2), int64(20)},
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_JoinGroupBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (1, 50), (2, 75)")
	rows := mustQuery(t, db, "SELECT u.name, sum(o.total) AS spend FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
	want := [][]any{
		{"alice", int64(150)},
		{"bob", int64(75)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_JoinGroupByHaving(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (1, 50), (2, 75), (3, 10)")
	rows := mustQuery(t, db, "SELECT u.name, sum(o.total) AS spend FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING sum(o.total) >= 75 ORDER BY u.name")
	want := [][]any{
		{"alice", int64(150)},
		{"bob", int64(75)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_JoinCountStar(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (1, 50), (2, 75)")
	rows := mustQuery(t, db, "SELECT u.name, count(*) AS n FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
	want := [][]any{
		{"alice", int64(2)},
		{"bob", int64(1)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_HashJoinOutputPagination(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (k int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (k int64 NOT NULL, w int64 NOT NULL)")
	const n = 60
	var avals, bvals []string
	for i := range n {
		avals = append(avals, fmt.Sprintf("(1, %d)", i))
		bvals = append(bvals, fmt.Sprintf("(1, %d)", i))
	}
	mustExec(t, db, "INSERT INTO a (k, v) VALUES "+strings.Join(avals, ", "))
	mustExec(t, db, "INSERT INTO b (k, w) VALUES "+strings.Join(bvals, ", "))
	rows := mustQuery(t, db, "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k")
	if len(rows.Values) != n*n {
		t.Fatalf("got %d rows, want %d", len(rows.Values), n*n)
	}
}

func TestEngine_CompositeJoinKeys(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (region text NOT NULL, day int64 NOT NULL, label text NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (region text NOT NULL, day int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (region, day, label) VALUES ('us', 1, 'us-1'), ('us', 2, 'us-2'), ('eu', 1, 'eu-1')")
	mustExec(t, db, "INSERT INTO b (region, day, total) VALUES ('us', 1, 100), ('us', 2, 200), ('eu', 9, 50)")
	rows := mustQuery(t, db, "SELECT a.label, b.total FROM a JOIN b ON a.region = b.region AND a.day = b.day ORDER BY a.label")
	want := [][]any{
		{"us-1", int64(100)},
		{"us-2", int64(200)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_RightOuterJoinNullFill(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (2, 50), (99, 7)")
	rows := mustQuery(t, db, "SELECT u.name, o.total FROM users u RIGHT JOIN orders o ON u.id = o.user_id ORDER BY o.total")
	want := [][]any{
		{nil, int64(7)},
		{"bob", int64(50)},
		{"alice", int64(100)},
	}
	if len(rows.Values) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(rows.Values), len(want), rows.Values)
	}
	for i, w := range want {
		if rows.Values[i][0] != w[0] || rows.Values[i][1] != w[1] {
			t.Fatalf("row %d = %v, want %v", i, rows.Values[i], w)
		}
	}
}

func TestEngine_FullOuterJoin(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE orders (user_id int64 NOT NULL, total int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
	mustExec(t, db, "INSERT INTO orders (user_id, total) VALUES (1, 100), (99, 7)")
	rows := mustQuery(t, db, "SELECT u.name, o.total FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id")
	type pair struct {
		name  any
		total any
	}
	got := make(map[pair]int)
	for _, r := range rows.Values {
		got[pair{r[0], r[1]}]++
	}
	want := map[pair]int{
		{"alice", int64(100)}: 1,
		{"bob", nil}:          1,
		{"carol", nil}:        1,
		{nil, int64(7)}:       1,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("missing/duplicate full-join row %v: got=%v", k, got)
		}
	}
}

func TestEngine_JSONPath(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE events (id int64 NOT NULL, payload text NOT NULL)")
	mustExec(t, db, "INSERT INTO events (id, payload) VALUES (1, '{\"user\":{\"name\":\"alice\"},\"tags\":[\"a\",\"b\"]}')")
	rows := mustQuery(t, db, "SELECT payload ->> 'user' AS user_obj, payload ->> 'tags' AS tags FROM events")
	if len(rows.Values) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows.Values))
	}
	got := rows.Values[0][0].(string)
	if got != `{"name":"alice"}` {
		t.Fatalf("user_obj = %q", got)
	}
	if rows.Values[0][1].(string) != `["a","b"]` {
		t.Fatalf("tags = %q", rows.Values[0][1])
	}
	rows = mustQuery(t, db, "SELECT payload -> 'user' -> 'name' AS name FROM events")
	if got := rows.Values[0][0].(string); got != `"alice"` {
		t.Fatalf("nested -> name = %q, want JSON-quoted \"alice\"", got)
	}
	rows = mustQuery(t, db, "SELECT payload -> 'tags' -> 1 AS t1 FROM events")
	if got := rows.Values[0][0].(string); got != `"b"` {
		t.Fatalf("array index -> 1 = %q, want \"b\"", got)
	}
}

func TestAutoRetention_RunsInsideVacuum(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetAutoRetention(true)
	db.SetRetentionLag(1)
	t.Cleanup(func() { db.Close() })
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	// Pile up more commits so (nextCommitTs - RetentionLag) > DVCommitTs.
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (4)")

	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if n < 1 {
		t.Fatalf("AutoRetention should have retired >=1 segment, got %d", n)
	}
	rows := mustValues(t, db, "SELECT id FROM t")
	if len(rows) != 3 {
		t.Fatalf("after auto-retention got %d live rows, want 3", len(rows))
	}
}

func TestAutoRetention_DefaultDoesNotRetire(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	view := snapshotOf(t, db, "t")
	if len(view.Entries) != 1 {
		t.Fatalf("default Vacuum should not retire (entries=%d, n=%d)", len(view.Entries), n)
	}
}

func TestEngine_PushedNotExcludesNulls(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 7), (3, NULL)")

	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v != 5 ORDER BY id"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v = 5) ORDER BY id"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v NOT IN (5, 9) ORDER BY id"), [][]any{{int64(2)}})
}

func TestEngine_Int16BoundaryPushdown(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, s int16 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, s) VALUES (1, -32768), (2, 0), (3, 32767)")

	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s <= 32767 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s >= -32768 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s < 40000 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s = 40000 ORDER BY id"), [][]any{})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s > -40000 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
}

func TestEngine_FloatLiteralOnIntColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 3), (2, 4), (3, 5)")

	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v > 3.5 ORDER BY id"), [][]any{{int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v <= 4.5 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v = 4.0 ORDER BY id"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v = 4.5 ORDER BY id"), [][]any{})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v != 4.5 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v BETWEEN 3.5 AND 4.5 ORDER BY id"), [][]any{{int64(2)}})
}

func TestEngine_AggregateExpressionArgs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (cat text NOT NULL, price float64 NOT NULL, disc float64 NOT NULL, qty int64 NOT NULL, opt int64)")
	mustExec(t, db, "INSERT INTO t (cat, price, disc, qty, opt) VALUES ('a', 10.0, 0.1, 2, 5), ('a', 20.0, 0.25, 3, NULL), ('b', 30.0, 0.0, 4, 7)")

	rows := mustValues(t, db, "SELECT sum(price * (1.0 - disc)) AS rev FROM t")
	if got := rows[0][0].(float64); got != 54.0 {
		t.Errorf("sum(price*(1-disc)) = %v, want 54", got)
	}
	rows = mustValues(t, db, "SELECT sum(qty * 2) AS dq FROM t")
	if got := rows[0][0].(int64); got != 18 {
		t.Errorf("sum(qty*2) = %v, want 18", got)
	}
	wantRows(t, mustValues(t, db, "SELECT cat, sum(price * (1.0 - disc)) AS rev, count(*) AS c FROM t GROUP BY cat ORDER BY cat"), [][]any{
		{"a", 24.0, int64(2)},
		{"b", 30.0, int64(3 - 2)},
	})
	rows = mustValues(t, db, "SELECT max(qty * qty) AS mq, min(qty - 5) AS mn FROM t")
	if got := rows[0][0].(int64); got != 16 {
		t.Errorf("max(qty*qty) = %v, want 16", got)
	}
	if got := rows[0][1].(int64); got != -3 {
		t.Errorf("min(qty-5) = %v, want -3", got)
	}
	rows = mustValues(t, db, "SELECT sum(opt + 1) AS so FROM t")
	if got := rows[0][0].(int64); got != 14 {
		t.Errorf("sum(opt+1) with null = %v, want 14", got)
	}
}

func TestEngine_AggregateExpressionWithDictGroupKey(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (cat text NOT NULL, price int64 NOT NULL, qty int64 NOT NULL)")
	var ins strings.Builder
	ins.WriteString("INSERT INTO t (cat, price, qty) VALUES ")
	for i := range 300 {
		if i > 0 {
			ins.WriteString(",")
		}
		cat := []string{"x", "y", "z"}[i%3]
		fmt.Fprintf(&ins, "('%s', %d, %d)", cat, i, i%7)
	}
	mustExec(t, db, ins.String())

	got := mustValues(t, db, "SELECT cat, sum(price * qty) AS pq, count(*) AS c FROM t GROUP BY cat ORDER BY cat")
	var wantX, wantY, wantZ int64
	for i := range 300 {
		v := int64(i) * int64(i%7)
		switch i % 3 {
		case 0:
			wantX += v
		case 1:
			wantY += v
		case 2:
			wantZ += v
		}
	}
	wantRows(t, got, [][]any{
		{"x", wantX, int64(100)},
		{"y", wantY, int64(100)},
		{"z", wantZ, int64(100)},
	})
}

func TestEngine_Int32PushdownMatchesDecoded(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, d int32 NOT NULL, s int16)")
	mustExec(t, db, "INSERT INTO t (id, d, s) VALUES (1, 100, 5), (2, 200, NULL), (3, 300, -5), (4, 400, 32767)")

	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE d < 250 ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE d >= 200 ORDER BY id"), [][]any{{int64(2)}, {int64(3)}, {int64(4)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE d BETWEEN 150 AND 350 ORDER BY id"), [][]any{{int64(2)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s = 32767 ORDER BY id"), [][]any{{int64(4)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s != 5 ORDER BY id"), [][]any{{int64(3)}, {int64(4)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE s <= 0 ORDER BY id"), [][]any{{int64(3)}})
}
