// QueryBatches must return exactly the rows Query returns and its buffers must
// survive later queries because pooled scan memory is copied out, never aliased.
package engine

import (
	"context"
	"fmt"
	"testing"

)

func TestQueryBatches_MatchesQuery(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, price float64 NOT NULL, opt int64)")
	var stmts []string
	for i := range 500 {
		opt := fmt.Sprintf("%d", i*2)
		if i%5 == 0 {
			opt = "NULL"
		}
		stmts = append(stmts, fmt.Sprintf("INSERT INTO t (id, name, price, opt) VALUES (%d, 'n%d', %d.5, %s)", i, i, i, opt))
	}
	if _, err := db.BulkInsert(context.Background(), stmts); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	const q = "SELECT id, name, price, opt FROM t WHERE id >= 100 ORDER BY id"
	rows := mustValues(t, db, q)
	br, err := db.QueryBatches(context.Background(), q)
	if err != nil {
		t.Fatalf("QueryBatches: %v", err)
	}
	if br.Rows != len(rows) {
		t.Fatalf("QueryBatches rows %d, Query rows %d", br.Rows, len(rows))
	}
	r := 0
	for _, b := range br.Batches {
		if b.Sel != nil {
			t.Fatal("dense batch must not carry a selection mask")
		}
		ids, prices := b.Columns[0].V.I64(), b.Columns[2].V.F64()
		names, opts := b.Columns[1].V.Var(), &b.Columns[3]
		for i := range b.Len {
			want := rows[r]
			if ids[i] != want[0].(int64) {
				t.Fatalf("row %d id %d, want %v", r, ids[i], want[0])
			}
			if names.String(i) != want[1].(string) {
				t.Fatalf("row %d name %q, want %v", r, names.String(i), want[1])
			}
			if prices[i] != want[2].(float64) {
				t.Fatalf("row %d price %v, want %v", r, prices[i], want[2])
			}
			null := opts.V.Valid != nil && !opts.V.Valid.IsValid(i)
			if null != (want[3] == nil) {
				t.Fatalf("row %d null mismatch, got %v want %v", r, null, want[3])
			}
			if !null && opts.V.I64()[i] != want[3].(int64) {
				t.Fatalf("row %d opt %d, want %v", r, opts.V.I64()[i], want[3])
			}
			r++
		}
	}
}

func TestQueryBatches_RetainedAcrossQueries(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	var stmts []string
	for i := range 300 {
		stmts = append(stmts, fmt.Sprintf("INSERT INTO t (id, name) VALUES (%d, 'v%d')", i, i))
	}
	if _, err := db.BulkInsert(context.Background(), stmts); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}
	br, err := db.QueryBatches(context.Background(), "SELECT id, name FROM t WHERE id < 100")
	if err != nil {
		t.Fatalf("QueryBatches: %v", err)
	}
	var snapshot []int64
	for _, b := range br.Batches {
		snapshot = append(snapshot, b.Columns[0].V.I64()...)
	}
	for range 5 {
		if _, err := db.Query(context.Background(), "SELECT id, name FROM t"); err != nil {
			t.Fatalf("interleaved Query: %v", err)
		}
	}
	idx := 0
	for _, b := range br.Batches {
		for _, v := range b.Columns[0].V.I64() {
			if v != snapshot[idx] {
				t.Fatalf("retained batch mutated at %d, got %d want %d", idx, v, snapshot[idx])
			}
			idx++
		}
	}
}
