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
