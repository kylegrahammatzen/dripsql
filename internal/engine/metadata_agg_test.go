// Metadata-only aggregate fast path tests covering count plus min and max plus sum short-circuits, DV-popcount count(*), GROUP BY dict-histogram, and the DV bail back to the scan path.
package engine

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestEngine_MetadataAggregate_CountStarAndMinMax(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE u (id int64, age int32);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO u VALUES (1,7),(2,4),(3,9),(4,2);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO u VALUES (5,11),(6,3);"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		sql  string
		want any
	}{
		{"SELECT count(*) FROM u", int64(6)},
		{"SELECT count(id) FROM u", int64(6)},
		{"SELECT min(age) FROM u", int32(2)},
		{"SELECT max(age) FROM u", int32(11)},
		{"SELECT min(id) FROM u", int64(1)},
		{"SELECT max(id) FROM u", int64(6)},
		{"SELECT sum(age) FROM u", int64(36)},
		{"SELECT sum(id) FROM u", int64(21)},
	}
	for _, c := range cases {
		rows, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		if len(rows.Values) != 1 || len(rows.Values[0]) != 1 {
			t.Fatalf("%s: shape %v", c.sql, rows.Values)
		}
		if got := rows.Values[0][0]; got != c.want {
			t.Fatalf("%s: got %v (%T), want %v (%T)", c.sql, got, got, c.want, c.want)
		}
	}
}

func TestEngine_MetadataAggregate_CountStaysFastUnderDV(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE u (id int64 NOT NULL, age int32);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO u VALUES (1,7),(2,4),(3,9),(4,2);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM u WHERE id = 2;"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "SELECT count(*) FROM u"); err != nil {
		t.Fatal(err)
	}
	storage.ResetTimings()
	rows, err := db.Query(ctx, "SELECT count(*) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.Values[0][0]; got != int64(3) {
		t.Fatalf("count(*) with DV got %v want 3", got)
	}
	ioNs, decodeNs := storage.ReadTimings()
	if ioNs != 0 || decodeNs != 0 {
		t.Fatalf("count(*) with DV should be metadata-only, got io=%d decode=%d", ioNs, decodeNs)
	}
	rows, err = db.Query(ctx, "SELECT count(id) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.Values[0][0]; got != int64(3) {
		t.Fatalf("count(id) with non-nullable id under DV got %v want 3", got)
	}
}

func TestEngine_MetadataAggregate_MinMaxSumStillBailOnDV(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE u (id int64 NOT NULL, age int32);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO u VALUES (1,7),(2,4),(3,9),(4,2);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM u WHERE id = 4;"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT min(age) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := toInt64(rows.Values[0][0]); got != 4 {
		t.Fatalf("min(age) after deleting id=4 (age=2) got %v want 4", rows.Values[0][0])
	}
	rows, err = db.Query(ctx, "SELECT sum(id) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := toInt64(rows.Values[0][0]); got != 6 {
		t.Fatalf("sum(id) after deleting id=4 got %v want 6", rows.Values[0][0])
	}
}

func BenchmarkEngine_CountStar_WithDV_10k(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE u (id int64 NOT NULL);"); err != nil {
		b.Fatal(err)
	}
	for start := 0; start < 10000; start += 2000 {
		var sb []byte
		sb = append(sb, "INSERT INTO u VALUES "...)
		for i := start; i < start+2000 && i < 10000; i++ {
			if i > start {
				sb = append(sb, ',')
			}
			sb = append(sb, '(')
			sb = strconv.AppendInt(sb, int64(i), 10)
			sb = append(sb, ')')
		}
		if _, err := db.Exec(ctx, string(sb)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := db.Exec(ctx, "DELETE FROM u WHERE id < 100;"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.Query(ctx, "SELECT count(*) FROM u")
		if err != nil {
			b.Fatal(err)
		}
		if rows.Values[0][0] != int64(9900) {
			b.Fatalf("count got %v want 9900", rows.Values[0][0])
		}
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int:
		return int64(x)
	}
	return -1
}

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
