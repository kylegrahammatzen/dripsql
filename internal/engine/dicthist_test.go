// Dict-histogram GROUP BY SMA: when the SELECT shape is `<col>, count(*)` over a single
// table with no WHERE / HAVING and the group-by column is varbytes, results come from
// per-segment .dh sidecars and the operator path never runs.
package engine

import (
	"context"
	"sort"
	"testing"
)

func TestEngine_DictHistogram_GroupBy_CountStar(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'a'),(5,'c'),(6,'b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (7,'a'),(8,'c')"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT label, count(*) FROM t GROUP BY label")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int64, len(rows.Values))
	for _, r := range rows.Values {
		got[r[0].(string)] = r[1].(int64)
	}
	want := map[string]int64{"a": 4, "b": 2, "c": 2}
	if len(got) != len(want) {
		t.Fatalf("got %d groups want %d (got=%v)", len(got), len(want), got)
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if got[k] != want[k] {
			t.Errorf("group %q: got %d, want %d", k, got[k], want[k])
		}
	}
}

func TestEngine_DictHistogram_GroupBy_FallsBackOnDV(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT label, count(*) FROM t GROUP BY label")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int64, len(rows.Values))
	for _, r := range rows.Values {
		got[r[0].(string)] = r[1].(int64)
	}
	want := map[string]int64{"a": 1, "b": 1}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("group %q: got %d, want %d (full=%v)", k, got[k], v, got)
		}
	}
}
