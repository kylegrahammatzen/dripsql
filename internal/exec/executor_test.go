// Exec smoke tests: round-trip a written segment through Scan/Filter/Project/Limit and
// verify the result row set matches expectations.
package exec

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func writeUsersSegment(t *testing.T, dir string) *storage.Segment {
	t.Helper()
	v := vector.NewVec(vector.VecInt64, 10)
	for i := range v.I64() {
		v.I64()[i] = int64(i)
	}
	name := vector.NewVarVec(vector.VecText, 10, 0)
	for i := range 10 {
		name.Var().AppendString(i, "user")
	}
	b, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: v},
		{Name: "name", Type: schema.Text, V: name},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	path := filepath.Join(dir, "users.dsv4")
	if _, err := storage.WriteSegment(path, []vector.Batch{b}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	return seg
}

func runOperator(t *testing.T, op Operator) [][]any {
	t.Helper()
	if err := op.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer op.Close()
	var rows [][]any
	for {
		batch, ok, err := op.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		batch.Sel.IterSet(func(row int) {
			out := make([]any, len(batch.Columns))
			ctx := newEvalCtx(batch)
			for i, c := range batch.Columns {
				v, err := ctx.colValue(c.Name, row)
				if err != nil {
					t.Fatalf("colValue: %v", err)
				}
				out[i] = v
			}
			rows = append(rows, out)
		})
	}
	return rows
}

func usersDef(seg *storage.Segment) sql.BoundTableDef {
	return sql.BoundTableDef{
		Name: "users",
		Columns: []sql.BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64},
			{ID: 2, Name: "name", Type: schema.Text},
		},
	}
}

func segmentsFn(seg *storage.Segment) SegmentsFn {
	return func(def sql.BoundTableDef) ([]*storage.Segment, error) {
		return []*storage.Segment{seg}, nil
	}
}

func TestExec_SelectStar(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT * FROM users")
	if len(rows) != 10 {
		t.Fatalf("got %d rows, want 10", len(rows))
	}
	for i, r := range rows {
		if r[0].(int64) != int64(i) {
			t.Fatalf("row %d id = %v, want %d", i, r[0], i)
		}
	}
}

func TestExec_SelectColumnProjection(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT name FROM users")
	if len(rows) != 10 {
		t.Fatalf("got %d rows, want 10", len(rows))
	}
	if len(rows[0]) != 1 {
		t.Fatalf("got %d cols, want 1", len(rows[0]))
	}
	if rows[0][0].(string) != "user" {
		t.Fatalf("name = %v", rows[0][0])
	}
}

func TestExec_WhereFilter(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT id FROM users WHERE id > 5")
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want 4 (id in {6,7,8,9})", len(rows))
	}
	want := []int64{6, 7, 8, 9}
	for i, r := range rows {
		if r[0].(int64) != want[i] {
			t.Fatalf("row %d = %v, want %d", i, r[0], want[i])
		}
	}
}

func TestExec_Limit(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT id FROM users LIMIT 3")
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	for i, r := range rows {
		if r[0].(int64) != int64(i) {
			t.Fatalf("row %d = %v, want %d", i, r[0], i)
		}
	}
}

func TestExec_LimitOffset(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT id FROM users LIMIT 2 OFFSET 5")
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0][0].(int64) != 5 || rows[1][0].(int64) != 6 {
		t.Fatalf("offset rows = %v %v, want 5 6", rows[0][0], rows[1][0])
	}
}

func TestExec_FilterThenLimit(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT id FROM users WHERE id >= 3 LIMIT 2")
	if len(rows) != 2 || rows[0][0].(int64) != 3 || rows[1][0].(int64) != 4 {
		t.Fatalf("rows = %v", rows)
	}
}

func runQuery(t *testing.T, seg *storage.Segment, def sql.BoundTableDef, src string) [][]any {
	t.Helper()
	stmt, err := sql.ParseOne(src)
	if err != nil {
		t.Fatalf("Parse %q: %v", src, err)
	}
	plan, err := sql.NewPlanner(func(string) (sql.BoundTableDef, error) { return def, nil }).Plan(stmt)
	if err != nil {
		t.Fatalf("Bind %q: %v", src, err)
	}
	op, err := BuildOperator(plan, segmentsFn(seg))
	if err != nil {
		t.Fatalf("BuildOperator %q: %v", src, err)
	}
	return runOperator(t, op)
}

func TestExec_AggregateCountStar(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT count(*) FROM users")
	if len(rows) != 1 || rows[0][0].(int64) != 10 {
		t.Fatalf("count(*) rows = %v", rows)
	}
}

func TestExec_AggregateSumMinMax(t *testing.T) {
	seg := writeUsersSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, usersDef(seg), "SELECT sum(id), min(id), max(id) FROM users")
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0][0].(int64) != 45 || rows[0][1].(int64) != 0 || rows[0][2].(int64) != 9 {
		t.Fatalf("sum/min/max = %v", rows[0])
	}
}

func writeSalesSegment(t *testing.T, dir string) *storage.Segment {
	t.Helper()
	id := vector.NewVec(vector.VecInt64, 6)
	cat := vector.NewVarVec(vector.VecText, 6, 0)
	price := vector.NewVec(vector.VecInt64, 6)
	cats := []string{"a", "b", "a", "b", "a", "c"}
	prices := []int64{10, 20, 30, 40, 50, 60}
	for i := range 6 {
		id.I64()[i] = int64(i)
		cat.Var().AppendString(i, cats[i])
		price.I64()[i] = prices[i]
	}
	b, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: id},
		{Name: "category", Type: schema.Text, V: cat},
		{Name: "price", Type: schema.Int64, V: price},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	path := filepath.Join(dir, "sales.dsv4")
	if _, err := storage.WriteSegment(path, []vector.Batch{b}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	return seg
}

func salesDef() sql.BoundTableDef {
	return sql.BoundTableDef{
		Name: "sales",
		Columns: []sql.BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64},
			{ID: 2, Name: "category", Type: schema.Text},
			{ID: 3, Name: "price", Type: schema.Int64},
		},
	}
}

func collectByCategory(rows [][]any) map[string][2]int64 {
	out := make(map[string][2]int64)
	for _, r := range rows {
		out[r[0].(string)] = [2]int64{r[1].(int64), r[2].(int64)}
	}
	return out
}

func TestExec_GroupBy(t *testing.T) {
	seg := writeSalesSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, salesDef(), "SELECT category, count(price), sum(price) FROM sales GROUP BY category")
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3 (a,b,c): %v", len(rows), rows)
	}
	got := collectByCategory(rows)
	want := map[string][2]int64{"a": {3, 90}, "b": {2, 60}, "c": {1, 60}}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("group %q = %v, want %v", k, got[k], v)
		}
	}
}

func TestExec_GroupByHaving(t *testing.T) {
	seg := writeSalesSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, salesDef(), "SELECT category, sum(price) FROM sales GROUP BY category HAVING sum(price) > 70")
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1: %v", len(rows), rows)
	}
	if rows[0][0].(string) != "a" || rows[0][1].(int64) != 90 {
		t.Fatalf("having row = %v, want [a 90]", rows[0])
	}
}

func TestExec_OrderByAsc(t *testing.T) {
	seg := writeSalesSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, salesDef(), "SELECT id FROM sales ORDER BY price")
	want := []int64{0, 1, 2, 3, 4, 5}
	for i, r := range rows {
		if r[0].(int64) != want[i] {
			t.Fatalf("row %d = %v, want %d", i, r[0], want[i])
		}
	}
}

func TestExec_OrderByDesc(t *testing.T) {
	seg := writeSalesSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, salesDef(), "SELECT id FROM sales ORDER BY price DESC")
	want := []int64{5, 4, 3, 2, 1, 0}
	for i, r := range rows {
		if r[0].(int64) != want[i] {
			t.Fatalf("row %d = %v, want %d", i, r[0], want[i])
		}
	}
}

func TestExec_OrderByThenLimit(t *testing.T) {
	seg := writeSalesSegment(t, t.TempDir())
	defer seg.Close()
	rows := runQuery(t, seg, salesDef(), "SELECT id FROM sales ORDER BY price DESC LIMIT 2")
	if len(rows) != 2 || rows[0][0].(int64) != 5 || rows[1][0].(int64) != 4 {
		t.Fatalf("rows = %v, want [[5] [4]]", rows)
	}
}

func writeWideGroupSegment(t *testing.T, dir string, groups int) *storage.Segment {
	t.Helper()
	if groups > vector.StandardBatchRows {
		t.Fatalf("writeWideGroupSegment: cannot seed > StandardBatchRows in a single batch (got %d)", groups)
	}
	id := vector.NewVec(vector.VecInt64, groups)
	tag := vector.NewVarVec(vector.VecText, groups, 0)
	for i := range groups {
		id.I64()[i] = int64(i)
		tag.Var().AppendString(i, fmt.Sprintf("g%05d", i))
	}
	b, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: id},
		{Name: "tag", Type: schema.Text, V: tag},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	path := filepath.Join(dir, "wide.dsv4")
	if _, err := storage.WriteSegment(path, []vector.Batch{b}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	return seg
}

func wideGroupDef() sql.BoundTableDef {
	return sql.BoundTableDef{
		Name: "wide",
		Columns: []sql.BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64},
			{ID: 2, Name: "tag", Type: schema.Text},
		},
	}
}

func TestExec_GroupBy_PaginatesAboveStandardBatchRows(t *testing.T) {
	// Two segments of unique tags so the group count crosses StandardBatchRows
	// without violating the per-batch input cap.
	dir := t.TempDir()
	seg1 := writeWideGroupSegment(t, dir, vector.StandardBatchRows)
	defer seg1.Close()
	dir2 := t.TempDir()
	seg2 := writeWideGroupSegment(t, dir2, 1000)
	defer seg2.Close()

	stmt, _ := sql.ParseOne("SELECT tag, count(id) FROM wide GROUP BY tag")
	plan, err := sql.NewPlanner(func(string) (sql.BoundTableDef, error) { return wideGroupDef(), nil }).Plan(stmt)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	segsFn := func(def sql.BoundTableDef) ([]*storage.Segment, error) {
		return []*storage.Segment{seg1, seg2}, nil
	}
	op, err := BuildOperator(plan, segsFn)
	if err != nil {
		t.Fatalf("BuildOperator: %v", err)
	}
	rows := runOperator(t, op)
	// seg1 has 2048 unique tags (g00000..g02047), seg2 has 1000 unique tags (g00000..g00999).
	// Overlap is the first 1000. Distinct group count = 2048.
	if len(rows) != vector.StandardBatchRows {
		t.Fatalf("got %d groups, want %d", len(rows), vector.StandardBatchRows)
	}
	overlap := 0
	for _, r := range rows {
		if r[1].(int64) == 2 {
			overlap++
		}
	}
	if overlap != 1000 {
		t.Fatalf("got %d overlap groups (count=2), want 1000", overlap)
	}
}

// Grouping by dictionary codes must produce exactly what string grouping produces,
// including when some segments exceed the dict limit and fall back to decoded pages.
func TestExec_GroupBy_DictCodesMatchDecoded(t *testing.T) {
	dir := t.TempDir()
	cats := []string{"alpha", "beta", "gamma"}
	type exp struct{ count, sum, min int64 }
	want := map[string]*exp{}
	var segs []*storage.Segment
	addSeg := func(fname string, rows int, makeKey func(i int) string) {
		k := vector.NewVarVec(vector.VecText, rows, 0)
		v := vector.NewVec(vector.VecInt64, rows)
		for i := range rows {
			key := makeKey(i)
			k.Var().AppendString(i, key)
			v.I64()[i] = int64(i)
			e := want[key]
			if e == nil {
				e = &exp{min: int64(i)}
				want[key] = e
			}
			e.count++
			e.sum += int64(i)
			e.min = min(e.min, int64(i))
		}
		b, err := vector.NewBatch([]vector.Column{
			{Name: "k", Type: schema.Text, V: k},
			{Name: "v", Type: schema.Int64, V: v},
		})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		path := filepath.Join(dir, fname)
		if _, err := storage.WriteSegment(path, []vector.Batch{b}, nil); err != nil {
			t.Fatalf("WriteSegment: %v", err)
		}
		seg, err := storage.OpenSegment(path)
		if err != nil {
			t.Fatalf("OpenSegment: %v", err)
		}
		segs = append(segs, seg)
	}
	for s := range 4 {
		addSeg(fmt.Sprintf("dict%d.dsv4", s), 1500, func(i int) string { return cats[(i+s)%len(cats)] })
	}
	// 400 distinct keys defeat the 256-entry dict so these pages arrive decoded.
	addSeg("wide.dsv4", 2000, func(i int) string { return fmt.Sprintf("wide%03d", i%400) })
	defer func() {
		for _, seg := range segs {
			seg.Close()
		}
	}()

	run := func(dictCodes bool, specs []sql.AggSpec, pred *storage.Pred) [][]any {
		opts := storage.ScanOpts{Segments: segs, Columns: []string{"k", "v"}, Pred: pred}
		if dictCodes {
			opts.DictCodeColumns = []string{"k"}
		}
		agg := &AggregateOp{
			Source:     &ScanOp{Opts: opts, Parallelism: 4},
			GroupBy:    []sql.BoundExpr{{Op: sql.ExprColumn, Column: "k", Type: schema.Text}},
			Aggregates: specs,
		}
		return runOperator(t, agg)
	}
	countSum := []sql.AggSpec{
		{Func: sql.AggregateCount, Star: true, Alias: "c"},
		{Func: sql.AggregateSum, ArgName: "v", Alias: "s"},
	}
	generic := []sql.AggSpec{
		{Func: sql.AggregateCount, ArgName: "k", Alias: "c"},
		{Func: sql.AggregateSum, ArgName: "v", Alias: "s"},
		{Func: sql.AggregateMin, ArgName: "v", Alias: "m"},
	}
	for _, tc := range []struct {
		name      string
		dictCodes bool
		specs     []sql.AggSpec
		withMin   bool
	}{
		{"decoded count+sum", false, countSum, false},
		{"dict count+sum", true, countSum, false},
		{"decoded generic", false, generic, true},
		{"dict generic", true, generic, true},
	} {
		rows := run(tc.dictCodes, tc.specs, nil)
		if len(rows) != len(want) {
			t.Fatalf("%s: got %d groups, want %d", tc.name, len(rows), len(want))
		}
		for _, r := range rows {
			key := r[0].(string)
			e := want[key]
			if e == nil {
				t.Fatalf("%s: unexpected group %q", tc.name, key)
			}
			if r[1].(int64) != e.count || r[2].(int64) != e.sum {
				t.Fatalf("%s: group %q = %v, want count=%d sum=%d", tc.name, key, r[1:3], e.count, e.sum)
			}
			if tc.withMin && r[3].(int64) != e.min {
				t.Fatalf("%s: group %q min = %v, want %d", tc.name, key, r[3], e.min)
			}
		}
	}

	// A predicate on v selects a strict row subset, so dict entries whose rows are
	// all filtered out must not surface as phantom groups.
	pred := storage.Pred{Op: storage.OpGt, Col: "v", Kind: vector.VecInt64, I64: 1399}
	base := run(false, countSum, &pred)
	head := run(true, countSum, &pred)
	if len(base) != len(head) {
		t.Fatalf("predicate: got %d dict groups, want %d", len(head), len(base))
	}
	baseByKey := map[string][2]int64{}
	for _, r := range base {
		baseByKey[r[0].(string)] = [2]int64{r[1].(int64), r[2].(int64)}
	}
	for _, r := range head {
		if got, ok := baseByKey[r[0].(string)]; !ok || got != [2]int64{r[1].(int64), r[2].(int64)} {
			t.Fatalf("predicate: dict group %q = %v, decoded run had %v (present=%v)", r[0], r[1:3], got, ok)
		}
	}
}

// Composite dict grouping must match string grouping exactly, including batches
// where only one of the two keys is dict-encoded so the mixed path runs.
func TestExec_GroupBy_CompositeDictMatchesDecoded(t *testing.T) {
	dir := t.TempDir()
	k1s := []string{"red", "green", "blue"}
	type exp struct{ count, sum int64 }
	want := map[string]*exp{}
	var segs []*storage.Segment
	addSeg := func(fname string, rows int, makeK2 func(i int) string) {
		k1 := vector.NewVarVec(vector.VecText, rows, 0)
		k2 := vector.NewVarVec(vector.VecText, rows, 0)
		v := vector.NewVec(vector.VecInt64, rows)
		for i := range rows {
			a := k1s[i%len(k1s)]
			b := makeK2(i)
			k1.Var().AppendString(i, a)
			k2.Var().AppendString(i, b)
			v.I64()[i] = int64(i)
			key := a + "|" + b
			e := want[key]
			if e == nil {
				e = &exp{}
				want[key] = e
			}
			e.count++
			e.sum += int64(i)
		}
		batch, err := vector.NewBatch([]vector.Column{
			{Name: "k1", Type: schema.Text, V: k1},
			{Name: "k2", Type: schema.Text, V: k2},
			{Name: "v", Type: schema.Int64, V: v},
		})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		path := filepath.Join(dir, fname)
		if _, err := storage.WriteSegment(path, []vector.Batch{batch}, nil); err != nil {
			t.Fatalf("WriteSegment: %v", err)
		}
		seg, err := storage.OpenSegment(path)
		if err != nil {
			t.Fatalf("OpenSegment: %v", err)
		}
		segs = append(segs, seg)
	}
	for s := range 4 {
		addSeg(fmt.Sprintf("cd%d.dsv4", s), 1200, func(i int) string { return fmt.Sprintf("t%d", (i+s)%4) })
	}
	// 400 distinct k2 values defeat the dict so only k1 carries codes in this segment.
	addSeg("cwide.dsv4", 2000, func(i int) string { return fmt.Sprintf("w%03d", i%400) })
	defer func() {
		for _, seg := range segs {
			seg.Close()
		}
	}()

	run := func(dictCodes bool) [][]any {
		opts := storage.ScanOpts{Segments: segs, Columns: []string{"k1", "k2", "v"}}
		if dictCodes {
			opts.DictCodeColumns = []string{"k1", "k2"}
		}
		agg := &AggregateOp{
			Source: &ScanOp{Opts: opts, Parallelism: 4},
			GroupBy: []sql.BoundExpr{
				{Op: sql.ExprColumn, Column: "k1", Type: schema.Text},
				{Op: sql.ExprColumn, Column: "k2", Type: schema.Text},
			},
			Aggregates: []sql.AggSpec{
				{Func: sql.AggregateCount, Star: true, Alias: "c"},
				{Func: sql.AggregateSum, ArgName: "v", Alias: "s"},
			},
		}
		return runOperator(t, agg)
	}
	for _, tc := range []struct {
		name      string
		dictCodes bool
	}{{"decoded", false}, {"dict", true}} {
		rows := run(tc.dictCodes)
		if len(rows) != len(want) {
			t.Fatalf("%s: got %d groups, want %d", tc.name, len(rows), len(want))
		}
		for _, r := range rows {
			key := r[0].(string) + "|" + r[1].(string)
			e := want[key]
			if e == nil {
				t.Fatalf("%s: unexpected group %q", tc.name, key)
			}
			if r[2].(int64) != e.count || r[3].(int64) != e.sum {
				t.Fatalf("%s: group %q = %v, want count=%d sum=%d", tc.name, key, r[2:], e.count, e.sum)
			}
		}
	}
}

// A parallel scan drained by several aggregate workers and merged must produce
// exactly the groups and values a single worker produces, for every aggregate
// function over both int and float accumulators.
func TestExec_GroupBy_ParallelMergeMatchesSerial(t *testing.T) {
	const segCount = 8
	const perCat = 5
	cats := []string{"a", "b", "c"}
	type expect struct {
		count      int64
		sum        int64
		min, max   int64
		fsum       float64
		fmin, fmax float64
	}
	want := map[string]*expect{}
	segs := make([]*storage.Segment, 0, segCount)
	for s := range segCount {
		rows := len(cats) * perCat
		k := vector.NewVarVec(vector.VecText, rows, 0)
		v := vector.NewVec(vector.VecInt64, rows)
		f := vector.NewVec(vector.VecFloat64, rows)
		r := 0
		for ci, cat := range cats {
			for j := range perCat {
				val := int64(s*100 + ci*1000 + j)
				fv := float64(val) * 0.5
				k.Var().AppendString(r, cat)
				v.I64()[r] = val
				f.F64()[r] = fv
				e, ok := want[cat]
				if !ok {
					e = &expect{min: val, max: val, fmin: fv, fmax: fv}
					want[cat] = e
				}
				e.count++
				e.sum += val
				e.fsum += fv
				e.min = min(e.min, val)
				e.max = max(e.max, val)
				e.fmin = min(e.fmin, fv)
				e.fmax = max(e.fmax, fv)
				r++
			}
		}
		b, err := vector.NewBatch([]vector.Column{
			{Name: "k", Type: schema.Text, V: k},
			{Name: "v", Type: schema.Int64, V: v},
			{Name: "f", Type: schema.Float64, V: f},
		})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		path := filepath.Join(t.TempDir(), fmt.Sprintf("p%d.dsv4", s))
		if _, err := storage.WriteSegment(path, []vector.Batch{b}, nil); err != nil {
			t.Fatalf("WriteSegment: %v", err)
		}
		seg, err := storage.OpenSegment(path)
		if err != nil {
			t.Fatalf("OpenSegment: %v", err)
		}
		defer seg.Close()
		segs = append(segs, seg)
	}

	agg := &AggregateOp{
		Source: &ScanOp{
			Opts:        storage.ScanOpts{Segments: segs, Columns: []string{"k", "v", "f"}},
			Parallelism: segCount,
		},
		GroupBy: []sql.BoundExpr{{Op: sql.ExprColumn, Column: "k", Type: schema.Text}},
		Aggregates: []sql.AggSpec{
			{Func: sql.AggregateCount, Star: true, Alias: "cnt"},
			{Func: sql.AggregateSum, ArgName: "v", Alias: "sv"},
			{Func: sql.AggregateMin, ArgName: "v", Alias: "mnv"},
			{Func: sql.AggregateMax, ArgName: "v", Alias: "mxv"},
			{Func: sql.AggregateAvg, ArgName: "f", Alias: "af"},
			{Func: sql.AggregateMin, ArgName: "f", Alias: "mnf"},
			{Func: sql.AggregateMax, ArgName: "f", Alias: "mxf"},
		},
	}
	rows := runOperator(t, agg)
	if len(rows) != len(cats) {
		t.Fatalf("got %d groups, want %d: %v", len(rows), len(cats), rows)
	}
	for _, r := range rows {
		key := r[0].(string)
		e := want[key]
		if e == nil {
			t.Fatalf("unexpected group %q", key)
		}
		if r[1].(int64) != e.count || r[2].(int64) != e.sum || r[3].(int64) != e.min || r[4].(int64) != e.max {
			t.Fatalf("group %q int aggs = %v, want count=%d sum=%d min=%d max=%d", key, r[1:5], e.count, e.sum, e.min, e.max)
		}
		if r[5].(float64) != e.fsum/float64(e.count) || r[6].(float64) != e.fmin || r[7].(float64) != e.fmax {
			t.Fatalf("group %q float aggs = %v, want avg=%v min=%v max=%v", key, r[5:], e.fsum/float64(e.count), e.fmin, e.fmax)
		}
	}
}
