// QueryBatches must return exactly the rows Query returns and its buffers must
// survive later queries because pooled scan memory is copied out, never aliased.
package engine

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestQueryBatches_MatchesQuery(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, price float64 NOT NULL, opt int64)")
	var ins strings.Builder
	ins.WriteString("INSERT INTO t (id, name, price, opt) VALUES ")
	for i := range 500 {
		if i > 0 {
			ins.WriteString(",")
		}
		opt := fmt.Sprintf("%d", i*2)
		if i%5 == 0 {
			opt = "NULL"
		}
		fmt.Fprintf(&ins, "(%d, 'n%d', %d.5, %s)", i, i, i, opt)
	}
	mustExec(t, db, ins.String())

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

// batchValues flattens a BatchRows into boxed rows for comparison against Query output.
func batchValues(t *testing.T, br *BatchRows) [][]any {
	t.Helper()
	out := make([][]any, 0, br.Rows)
	for _, b := range br.Batches {
		for r := range b.Len {
			row := make([]any, len(b.Columns))
			for ci := range b.Columns {
				v, err := b.Columns[ci].ValueAt(r)
				if err != nil {
					t.Fatalf("ValueAt: %v", err)
				}
				row[ci] = v
			}
			out = append(out, row)
		}
	}
	return out
}

// avgByKey maps group key to the last output column, ungrouped rows key on "".
func avgByKey(rows [][]any) map[string]any {
	m := make(map[string]any, len(rows))
	for _, r := range rows {
		key := ""
		if len(r) == 2 {
			key = r[0].(string)
		}
		m[key] = r[len(r)-1]
	}
	return m
}

func TestQueryBatches_MetadataAvgBitIdentity(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, k text NOT NULL, v int64 NOT NULL, f float64 NOT NULL, o int64)")
	groupSum := map[string]int64{}
	groupCnt := map[string]int64{}
	for b := range 2 {
		var sb strings.Builder
		sb.WriteString("INSERT INTO t (id, k, v, f, o) VALUES ")
		for i := range 200 {
			id := b*200 + i
			if i > 0 {
				sb.WriteString(",")
			}
			k := string(rune('a' + id%3))
			v := int64(id*id*7 + 3)
			groupSum[k] += v
			groupCnt[k]++
			fmt.Fprintf(&sb, "(%d, '%s', %d, %d.%03d, NULL)", id, k, v, id, id%997)
		}
		mustExec(t, db, sb.String())
	}
	mustExec(t, db, "CREATE TABLE e (id int64 NOT NULL, k text NOT NULL, v int64 NOT NULL)")
	// exact marks int or all-null arguments whose exact int64 sums make every path bit-identical.
	// Float arguments compare approximately because parallel scan workers accumulate in nondeterministic order.
	cases := []struct {
		meta  string
		scan  string
		exact bool
	}{
		{"SELECT avg(v) FROM t", "SELECT avg(v) FROM t WHERE id >= 0", true},
		{"SELECT avg(f) FROM t", "SELECT avg(f) FROM t WHERE id >= 0", false},
		{"SELECT k, avg(v) AS a FROM t GROUP BY k", "SELECT k, avg(v) AS a FROM t WHERE id >= 0 GROUP BY k", true},
		{"SELECT k, avg(f) AS a FROM t GROUP BY k", "SELECT k, avg(f) AS a FROM t WHERE id >= 0 GROUP BY k", false},
		{"SELECT avg(o) FROM t", "SELECT avg(o) FROM t WHERE id >= 0", true},
		{"SELECT avg(v) FROM e", "SELECT avg(v) FROM e WHERE id >= 0", true},
		{"SELECT k, avg(v) AS a FROM e GROUP BY k", "SELECT k, avg(v) AS a FROM e WHERE id >= 0 GROUP BY k", true},
	}
	for _, c := range cases {
		want := avgByKey(mustValues(t, db, c.meta))
		br, err := db.QueryBatches(ctx, c.meta)
		if err != nil {
			t.Fatalf("%s: QueryBatches: %v", c.meta, err)
		}
		paths := map[string]map[string]any{
			"QueryBatches": avgByKey(batchValues(t, br)),
			"forced scan":  avgByKey(mustValues(t, db, c.scan)),
		}
		for label, got := range paths {
			if len(got) != len(want) {
				t.Fatalf("%s via %s: %d groups, Query has %d", c.meta, label, len(got), len(want))
			}
			for k, w := range want {
				g, ok := got[k]
				if !ok {
					t.Fatalf("%s via %s: group %q missing", c.meta, label, k)
				}
				wf, wIsF := w.(float64)
				gf, gIsF := g.(float64)
				switch {
				case wIsF != gIsF:
					t.Errorf("%s via %s group %q: got %v (%T) want %v (%T)", c.meta, label, k, g, g, w, w)
				case wIsF && c.exact && math.Float64bits(gf) != math.Float64bits(wf):
					t.Errorf("%s via %s group %q: got bits %x want %x", c.meta, label, k, math.Float64bits(gf), math.Float64bits(wf))
				case wIsF && !c.exact && math.Abs(gf-wf) > 1e-9*math.Max(1, math.Abs(wf)):
					t.Errorf("%s via %s group %q: got %v want %v", c.meta, label, k, gf, wf)
				case !wIsF && g != w:
					t.Errorf("%s via %s group %q: got %v want %v", c.meta, label, k, g, w)
				}
			}
		}
	}
	// Eligible int avg must route through metadata with the exact one-division bits.
	metadataAvg := func(q string) *Rows {
		t.Helper()
		if err := db.lockOpen(); err != nil {
			t.Fatal(err)
		}
		defer db.mu.Unlock()
		plan, err := db.planForQuery(q)
		if err != nil {
			t.Fatal(err)
		}
		rows, ok, err := db.answerFromMetadata(plan)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("%s should answer from metadata", q)
		}
		return rows
	}
	var totalSum, totalCnt int64
	for k, s := range groupSum {
		totalSum += s
		totalCnt += groupCnt[k]
	}
	rows := metadataAvg("SELECT avg(v) FROM t")
	if got := rows.Values[0][0].(float64); math.Float64bits(got) != math.Float64bits(float64(totalSum)/float64(totalCnt)) {
		t.Fatalf("ungrouped metadata avg bits %x, want %x", math.Float64bits(got), math.Float64bits(float64(totalSum)/float64(totalCnt)))
	}
	// The combined query pins that sum and avg over the same column merge each segment once.
	rows = metadataAvg("SELECT k, sum(v) AS s, avg(v) AS a FROM t GROUP BY k")
	if len(rows.Values) != len(groupSum) {
		t.Fatalf("grouped metadata avg has %d groups, want %d", len(rows.Values), len(groupSum))
	}
	for _, r := range rows.Values {
		k := r[0].(string)
		if got := r[1].(int64); got != groupSum[k] {
			t.Fatalf("group %q metadata sum %d, want %d", k, got, groupSum[k])
		}
		want := float64(groupSum[k]) / float64(groupCnt[k])
		if got := r[2].(float64); math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("group %q metadata avg bits %x, want %x", k, math.Float64bits(got), math.Float64bits(want))
		}
	}
	// Metadata answers must never touch pages, the counters stay zero on both entry points.
	storage.ResetTimings()
	if _, err := db.Query(ctx, "SELECT avg(v) FROM t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryBatches(ctx, "SELECT k, avg(v) AS a FROM t GROUP BY k"); err != nil {
		t.Fatal(err)
	}
	ioNs, decodeNs := storage.ReadTimings()
	if ioNs != 0 || decodeNs != 0 {
		t.Fatalf("avg should be metadata-only after warmup, got io=%d decode=%d", ioNs, decodeNs)
	}
}

func TestQueryBatches_RetainedAcrossQueries(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	var ins strings.Builder
	ins.WriteString("INSERT INTO t (id, name) VALUES ")
	for i := range 300 {
		if i > 0 {
			ins.WriteString(",")
		}
		fmt.Fprintf(&ins, "(%d, 'v%d')", i, i)
	}
	mustExec(t, db, ins.String())
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
