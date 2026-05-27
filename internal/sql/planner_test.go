// Planner tests for every binder path. DDL maps SQL into TableSpec or TypeSpec, INSERT reorders into BoundColumns, SELECT lowers into a Rel tree.
// Validation through schema.Validate catches semantic errors that survive the parser.
package sql

import (
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func bindStmt[T Stmt](t *testing.T, src string) T {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	out, ok := stmt.(T)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	return out
}

func usersDef() BoundTableDef {
	return BoundTableDef{
		Name: "users",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64, Nullable: false},
			{ID: 2, Name: "name", Type: schema.Text, Nullable: true},
			{ID: 3, Name: "age", Type: schema.Int32, Nullable: true},
		},
	}
}

func bindInsert(t *testing.T, src string, def BoundTableDef) (*Plan, error) {
	t.Helper()
	stmt := bindStmt[*InsertStmt](t, src)
	return BindInsertPlan(stmt, def)
}

func salesDef() BoundTableDef {
	return BoundTableDef{
		Name: "sales",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64},
			{ID: 2, Name: "category", Type: schema.Text},
			{ID: 3, Name: "price", Type: schema.Int64},
			{ID: 4, Name: "qty", Type: schema.Int32},
		},
	}
}

func bindSelect(t *testing.T, src string) (*Plan, error) {
	t.Helper()
	stmt := bindStmt[*SelectStmt](t, src)
	return NewPlanner(func(name string) (BoundTableDef, error) {
		if schema.NormalizeName(name) != "sales" {
			return BoundTableDef{}, fmt.Errorf("missing table %q", name)
		}
		return salesDef(), nil
	}).Plan(stmt)
}

func queryRoot(t *testing.T, plan *Plan) *Rel {
	t.Helper()
	if plan == nil || plan.Kind != PlanQuery || plan.Rel == nil {
		t.Fatalf("plan = %+v, want PlanQuery with Rel", plan)
	}
	return plan.Rel
}

func TestBindCreateType_NormalizesAndValidates(t *testing.T) {
	stmt := bindStmt[*CreateTypeStmt](t, "CREATE TYPE Event AS ENUM ('a', 'b')")
	plan, err := BindCreateType(stmt)
	if err != nil {
		t.Fatalf("BindCreateType: %v", err)
	}
	if plan.TypeSpec.Name != "event" {
		t.Fatalf("Name = %q, want lowercase", plan.TypeSpec.Name)
	}
	if len(plan.TypeSpec.EnumLabels) != 2 {
		t.Fatalf("EnumLabels = %v", plan.TypeSpec.EnumLabels)
	}
}

func TestBindCreateType_EmptyLabelsErrors(t *testing.T) {
	stmt := &CreateTypeStmt{Name: "x"}
	if _, err := BindCreateType(stmt); err == nil {
		t.Fatal("empty enum must fail Validate")
	}
}

func TestBindCreateTable_ColumnsParseTypes(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE users (id int64 NOT NULL, name text)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Name != "users" || len(plan.TableSpec.Columns) != 2 {
		t.Fatalf("spec = %+v", plan.TableSpec)
	}
	if plan.TableSpec.Columns[0].Type.Kind != schema.KindInt64 || plan.TableSpec.Columns[0].Nullable {
		t.Fatalf("col 0 = %+v", plan.TableSpec.Columns[0])
	}
	if plan.TableSpec.Columns[1].Type.Kind != schema.KindText || !plan.TableSpec.Columns[1].Nullable {
		t.Fatalf("col 1 = %+v", plan.TableSpec.Columns[1])
	}
}

func TestBindCreateTable_NamedTypeColumn(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (e EventKind)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Columns[0].Type.Kind != schema.KindNamed || plan.TableSpec.Columns[0].Type.Name != "eventkind" {
		t.Fatalf("col = %+v", plan.TableSpec.Columns[0])
	}
}

func TestBindCreateTable_OptionsVocab(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (storage = columnar, profile = event_analytics, compression = best, segment_rows = 1000, sort_by = 'id', time_column = 'id')")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	opts := plan.TableSpec.Options
	if opts.Storage != schema.StorageColumnar {
		t.Fatalf("Storage = %v", opts.Storage)
	}
	if opts.Profile != schema.ProfileEventAnalytics {
		t.Fatalf("Profile = %v", opts.Profile)
	}
	if opts.Compression != schema.CompressionBest {
		t.Fatalf("Compression = %v", opts.Compression)
	}
	if opts.SegmentRows.Auto || opts.SegmentRows.Rows != 1000 {
		t.Fatalf("SegmentRows = %+v", opts.SegmentRows)
	}
	if len(opts.SortBy) != 1 || opts.SortBy[0] != "id" {
		t.Fatalf("SortBy = %v", opts.SortBy)
	}
	if opts.TimeColumn != "id" {
		t.Fatalf("TimeColumn = %q", opts.TimeColumn)
	}
}

func TestBindCreateTable_SegmentRowsAuto(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (segment_rows = auto)")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if plan.TableSpec.Options.SegmentRows != schema.AutoSegmentRows {
		t.Fatalf("SegmentRows = %v, want Auto", plan.TableSpec.Options.SegmentRows)
	}
}

func TestBindCreateTable_RejectsUnknownOption(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (bogus = 1)")
	if _, err := BindCreateTable(stmt); err == nil {
		t.Fatal("unknown option must error")
	}
}

func TestBindCreateTable_RejectsBadOptionValue(t *testing.T) {
	for _, src := range []string{
		"CREATE TABLE t (id int64) WITH (storage = nonexistent)",
		"CREATE TABLE t (id int64) WITH (profile = bogus)",
		"CREATE TABLE t (id int64) WITH (compression = bogus)",
		"CREATE TABLE t (id int64) WITH (segment_rows = 0)",
		"CREATE TABLE t (id int64) WITH (segment_rows = manual)",
	} {
		stmt := bindStmt[*CreateTableStmt](t, src)
		if _, err := BindCreateTable(stmt); err == nil {
			t.Fatalf("must reject %q", src)
		}
	}
}

func TestBindCreateTable_RejectsDuplicateOption(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (id int64) WITH (storage = columnar, storage = row)")
	if _, err := BindCreateTable(stmt); err == nil {
		t.Fatal("duplicate option must error")
	}
}

func TestBindCreateTable_SortByMultipleColumns(t *testing.T) {
	stmt := bindStmt[*CreateTableStmt](t, "CREATE TABLE t (a int64, b int64) WITH (sort_by = 'a,b')")
	plan, err := BindCreateTable(stmt)
	if err != nil {
		t.Fatalf("BindCreateTable: %v", err)
	}
	if len(plan.TableSpec.Options.SortBy) != 2 || plan.TableSpec.Options.SortBy[0] != "a" || plan.TableSpec.Options.SortBy[1] != "b" {
		t.Fatalf("SortBy = %v", plan.TableSpec.Options.SortBy)
	}
}

func TestBindInsert_AllColumns_PositionalOrder(t *testing.T) {
	plan, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice', 30)", usersDef())
	if err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
	cols := plan.Values.Columns
	if len(cols) != 3 || cols[0].ID != 1 || cols[2].ID != 3 {
		t.Fatalf("Columns = %+v", cols)
	}
	if cols[0].Values[0].Int != 1 || cols[1].Values[0].String != "alice" || cols[2].Values[0].Int != 30 {
		t.Fatalf("row 0 = %+v", cols)
	}
}

func TestBindInsert_ColumnList_Reorders_PreservesValues(t *testing.T) {
	plan, err := bindInsert(t, "INSERT INTO users (name, id, age) VALUES ('alice', 1, 30)", usersDef())
	if err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
	cols := plan.Values.Columns
	if len(cols) != 3 {
		t.Fatalf("Columns len = %d", len(cols))
	}
	if cols[0].Name != "id" || cols[0].Values[0].Int != 1 {
		t.Fatalf("id col = %+v", cols[0])
	}
	if cols[1].Name != "name" || cols[1].Values[0].String != "alice" {
		t.Fatalf("name col = %+v", cols[1])
	}
	if cols[2].Name != "age" || cols[2].Values[0].Int != 30 {
		t.Fatalf("age col = %+v", cols[2])
	}
}

func TestBindInsert_PartialColumnList_Rejected(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, name) VALUES (1, 'alice')", usersDef()); err == nil {
		t.Fatal("partial column list must error (only all-cols or no-list supported)")
	}
}

func TestBindInsert_NotNullViolation(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (null, 'alice', 30)", usersDef()); err == nil {
		t.Fatal("NULL into NOT NULL column must error")
	}
}

func TestBindInsert_NullableAcceptsNull(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, null, null)", usersDef()); err != nil {
		t.Fatalf("BindInsertPlan: %v", err)
	}
}

func TestBindInsert_TypeMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES ('not-an-int', 'alice', 30)", usersDef()); err == nil {
		t.Fatal("string into int64 column must error")
	}
}

func TestBindInsert_Int32Range(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice', 99999999999)", usersDef()); err == nil {
		t.Fatal("int32 out-of-range literal must error")
	}
}

func TestBindInsert_RowCountMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users VALUES (1, 'alice')", usersDef()); err == nil {
		t.Fatal("row arity mismatch must error")
	}
}

func TestBindInsert_DuplicateColumnInList(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, id, age) VALUES (1, 2, 30)", usersDef()); err == nil {
		t.Fatal("duplicate column in list must error")
	}
}

func TestBindInsert_UnknownColumn(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO users (id, nope, age) VALUES (1, 'x', 30)", usersDef()); err == nil {
		t.Fatal("unknown column must error")
	}
}

func TestBindInsert_TargetTableMismatch(t *testing.T) {
	if _, err := bindInsert(t, "INSERT INTO other VALUES (1, 'a', 30)", usersDef()); err == nil {
		t.Fatal("table-name mismatch must error")
	}
}

func TestBindInsert_EnumLabelValidation(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "kind", Type: schema.Named("event"), Labels: []string{"view", "click"}, Nullable: false},
		},
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('view')", def); err != nil {
		t.Fatalf("known label must bind: %v", err)
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('unknown')", def); err == nil {
		t.Fatal("unknown enum label must error")
	}
}

func TestBindInsert_TemporalLiteralValidation(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.UUID},
			{ID: 2, Name: "at", Type: schema.Timestamp},
			{ID: 3, Name: "day", Type: schema.Date},
		},
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', '2026-05-14T12:00:00.000Z', '2026-05-14')", def); err != nil {
		t.Fatalf("valid temporals must bind: %v", err)
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('not-a-uuid', '2026-05-14T12:00:00.000Z', '2026-05-14')", def); err == nil {
		t.Fatal("invalid UUID must error")
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', 'tomorrow', '2026-05-14')", def); err == nil {
		t.Fatal("invalid timestamp must error")
	}
	if _, err := bindInsert(t, "INSERT INTO events VALUES ('11111111-2222-3333-4444-555555555555', '2026-05-14T12:00:00.000Z', 'yesterday')", def); err == nil {
		t.Fatal("invalid date must error")
	}
}

func TestBindInsert_NullCountTracked(t *testing.T) {
	stmt := bindStmt[*InsertStmt](t, "INSERT INTO users VALUES (1, null, 30), (2, 'b', null)")
	bound, err := BindInsertValues(stmt, usersDef())
	if err != nil {
		t.Fatalf("BindInsertValues: %v", err)
	}
	if bound.Columns[1].NullCount != 1 {
		t.Fatalf("name NullCount = %d, want 1", bound.Columns[1].NullCount)
	}
	if bound.Columns[2].NullCount != 1 {
		t.Fatalf("age NullCount = %d, want 1", bound.Columns[2].NullCount)
	}
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
	// GROUP BY without aggregates is the lowered form of SELECT DISTINCT, keeping the group key as the only output column and deduping.
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

func TestBindExplain_WrapsSelect(t *testing.T) {
	stmt := bindStmt[*ExplainStmt](t, "EXPLAIN SELECT id FROM sales")
	plan, err := NewPlanner(func(string) (BoundTableDef, error) { return salesDef(), nil }).Plan(stmt)
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
