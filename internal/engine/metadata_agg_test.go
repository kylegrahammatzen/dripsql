package engine

import (
	"context"
	"testing"
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

func TestEngine_MetadataAggregate_FallsBackOnDV(t *testing.T) {
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
	if _, err := db.Exec(ctx, "DELETE FROM u WHERE id = 2;"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT count(*) FROM u")
	if err != nil {
		t.Fatal(err)
	}
	if got := rows.Values[0][0]; got != int64(3) {
		t.Fatalf("count(*) with DV: got %v, want 3", got)
	}
}
