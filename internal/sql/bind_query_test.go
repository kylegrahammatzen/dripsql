// BindSelect tests: scan-only path, aggregate path, GROUP BY coupling enforced at bind time,
// HAVING hidden aggregates, ORDER BY against outputs, LIMIT/OFFSET non-negativity, EXPLAIN.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func salesDef() BoundTableDef {
	return BoundTableDef{
		Name: "sales",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: types.Int64},
			{ID: 2, Name: "category", Type: types.Text},
			{ID: 3, Name: "price", Type: types.Int64},
			{ID: 4, Name: "qty", Type: types.Int32},
		},
	}
}

func bindSelect(t *testing.T, src string) (*Plan, error) {
	t.Helper()
	stmt := bindStmt[*SelectStmt](t, src)
	return BindSelect(stmt, salesDef())
}

func queryRoot(t *testing.T, plan *Plan) *Rel {
	t.Helper()
	if plan == nil || plan.Kind != PlanQuery || plan.Rel == nil {
		t.Fatalf("plan = %+v, want PlanQuery with Rel", plan)
	}
	return plan.Rel
}

func TestBindSelect_Scan_Star(t *testing.T) {
	plan, err := bindSelect(t, "SELECT * FROM sales")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	root := queryRoot(t, plan)
	if root.Op != RelProject {
		t.Fatalf("root op = %v, want RelProject", root.Op)
	}
	if len(root.Projection) != 4 {
		t.Fatalf("star projected %d cols, want 4", len(root.Projection))
	}
	if root.Inputs[0].Op != RelScan {
		t.Fatalf("project source op = %v, want RelScan", root.Inputs[0].Op)
	}
}

func TestBindSelect_Scan_WhereAndOrderLimit(t *testing.T) {
	plan, err := bindSelect(t, "SELECT id FROM sales WHERE price > 10 ORDER BY id DESC LIMIT 5 OFFSET 2")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	root := queryRoot(t, plan)
	if root.Op != RelProject {
		t.Fatalf("root op = %v, want RelProject", root.Op)
	}
	sort := root.Inputs[0]
	if sort.Op != RelSort || len(sort.SortKeys) != 1 || !sort.SortKeys[0].Desc {
		t.Fatalf("sort = %+v", sort)
	}
	if sort.K != 5 || sort.Offset != 2 {
		t.Fatalf("Sort K=%d Offset=%d, want K=5 Offset=2", sort.K, sort.Offset)
	}
	scan := sort.Inputs[0]
	if scan.Op != RelScan || scan.Where == nil {
		t.Fatalf("scan = %+v", scan)
	}
}

func TestBindSelect_Scan_WhereMissingColumn(t *testing.T) {
	if _, err := bindSelect(t, "SELECT id FROM sales WHERE bogus = 1"); err == nil {
		t.Fatal("missing WHERE column must error")
	}
}

func TestBindSelect_Scan_WhereInt32Range(t *testing.T) {
	if _, err := bindSelect(t, "SELECT id FROM sales WHERE qty = 9999999999"); err == nil {
		t.Fatal("int32 out-of-range literal must error")
	}
}

func TestBindSelect_GroupByWithoutAggregateIsDistinct(t *testing.T) {
	// GROUP BY without aggregates is the lowered form of SELECT DISTINCT: keep the
	// group key as the only output column and dedup. Used to reject this; now valid.
	if _, err := bindSelect(t, "SELECT category FROM sales GROUP BY category"); err != nil {
		t.Fatalf("GROUP BY without aggregate is now allowed as DISTINCT equivalent: %v", err)
	}
}

func TestBindSelect_RejectsHavingWithoutAggregate(t *testing.T) {
	if _, err := bindSelect(t, "SELECT id FROM sales HAVING id > 0"); err == nil {
		t.Fatal("HAVING without aggregate must error")
	}
}

func TestBindSelect_RejectsMixedSelectWithoutGroupBy(t *testing.T) {
	if _, err := bindSelect(t, "SELECT category, sum(price) FROM sales"); err == nil {
		t.Fatal("non-aggregate + aggregate select must require GROUP BY")
	}
}

func TestBindSelect_RejectsAggregateOverExpression(t *testing.T) {
	if _, err := bindSelect(t, "SELECT sum(price * qty) FROM sales"); err == nil {
		t.Fatal("aggregate over expression must error (column-only for now)")
	}
}

func TestBindSelect_Aggregate_CountStar(t *testing.T) {
	plan, err := bindSelect(t, "SELECT count(*) FROM sales")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	agg := queryRoot(t, plan).Inputs[0]
	if agg.Op != RelAggregate || len(agg.Aggregates) != 1 || agg.Aggregates[0].Func != AggregateCount || !agg.Aggregates[0].Star {
		t.Fatalf("aggregate = %+v", agg)
	}
}

func TestBindSelect_Aggregate_GroupBy(t *testing.T) {
	plan, err := bindSelect(t, "SELECT category, sum(price) FROM sales GROUP BY category")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	agg := queryRoot(t, plan).Inputs[0]
	if agg.Op != RelAggregate || len(agg.GroupBy) != 1 || agg.GroupBy[0].Column != "category" {
		t.Fatalf("GroupBy = %+v", agg.GroupBy)
	}
	if len(agg.Aggregates) != 1 || agg.Aggregates[0].Func != AggregateSum {
		t.Fatalf("Aggregates = %+v", agg.Aggregates)
	}
}

func TestBindSelect_Aggregate_GroupByExprMustMatch(t *testing.T) {
	if _, err := bindSelect(t, "SELECT category, sum(price) FROM sales GROUP BY id"); err == nil {
		t.Fatal("GROUP BY expression must match leading select expression")
	}
}

func TestBindSelect_Aggregate_SumRequiresIntColumn(t *testing.T) {
	if _, err := bindSelect(t, "SELECT sum(category) FROM sales"); err == nil {
		t.Fatal("SUM over text column must error")
	}
}

func TestBindSelect_Aggregate_Having(t *testing.T) {
	plan, err := bindSelect(t, "SELECT category, sum(price) FROM sales GROUP BY category HAVING sum(price) > 100")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	agg := queryRoot(t, plan).Inputs[0]
	if agg.Op != RelAggregate || agg.Having == nil {
		t.Fatal("HAVING not bound")
	}
}

func TestBindSelect_Aggregate_HiddenHavingAggregate(t *testing.T) {
	plan, err := bindSelect(t, "SELECT category, sum(price) FROM sales GROUP BY category HAVING count(*) > 5")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	agg := queryRoot(t, plan).Inputs[0]
	if len(agg.Hidden) != 1 || agg.Hidden[0].Func != AggregateCount || !agg.Hidden[0].Star {
		t.Fatalf("Hidden = %+v", agg.Hidden)
	}
}

func TestBindSelect_OrderBy_MissingNotOutputErrors(t *testing.T) {
	if _, err := bindSelect(t, "SELECT id FROM sales ORDER BY missing"); err == nil {
		t.Fatal("ORDER BY missing column must error")
	}
}

func TestBindSelect_NegativeLimit(t *testing.T) {
	stmt := bindStmt[*SelectStmt](t, "SELECT id FROM sales LIMIT 0")
	n := int64(-1)
	stmt.Limit = &n
	if _, err := BindSelect(stmt, salesDef()); err == nil {
		t.Fatal("negative LIMIT must error")
	}
}

func TestBindExplain_WrapsSelect(t *testing.T) {
	stmt := bindStmt[*ExplainStmt](t, "EXPLAIN SELECT id FROM sales")
	plan, err := BindExplain(stmt, salesDef())
	if err != nil {
		t.Fatalf("BindExplain: %v", err)
	}
	if plan.Kind != PlanExplain || plan.Inner == nil {
		t.Fatalf("plan = %+v, want PlanExplain with Inner", plan)
	}
	if plan.Inner.Kind != PlanQuery || plan.Inner.Rel == nil || plan.Inner.Rel.Op != RelProject {
		t.Fatalf("Inner = %+v", plan.Inner)
	}
}

func TestBindSelect_Aggregate_WhereColumnInScanColumns(t *testing.T) {
	plan, err := bindSelect(t, "SELECT sum(price) FROM sales WHERE category = 'a'")
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	agg := queryRoot(t, plan).Inputs[0]
	scan := agg.Inputs[0]
	if scan.Op != RelScan {
		t.Fatalf("scan source op = %v, want RelScan", scan.Op)
	}
	hasCategory := false
	hasPrice := false
	for _, id := range scan.Columns {
		switch id {
		case 2:
			hasCategory = true
		case 3:
			hasPrice = true
		}
	}
	if !hasCategory {
		t.Fatalf("scan columns missing category (WHERE column), got %v", scan.Columns)
	}
	if !hasPrice {
		t.Fatalf("scan columns missing price (aggregate arg), got %v", scan.Columns)
	}
}

func TestBindSelect_TableNameMismatch(t *testing.T) {
	if _, err := bindSelect(t, "SELECT id FROM other"); err == nil {
		t.Fatal("table-name mismatch must error")
	}
}
