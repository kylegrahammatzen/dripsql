package engine

import (
	"context"
	"strings"
	"testing"
)

func setupExplainDB(t *testing.T) *DB {
	t.Helper()
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (
		tenant_id INT64 NOT NULL,
		event_type TEXT NOT NULL,
		amount INT64 NOT NULL
	)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'checkout', 10)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'view', 0)`)
	mustExec(t, db, `INSERT INTO events VALUES (42, 'checkout', 99)`)
	return db
}

func TestQueryExplainSelectRendersPlan(t *testing.T) {
	db := setupExplainDB(t)
	rows, err := db.Query(context.Background(), `EXPLAIN SELECT count(*) FROM events WHERE tenant_id = 42`)
	if err != nil {
		t.Fatalf("Query EXPLAIN: %v", err)
	}
	if len(rows.Columns) != 1 || rows.Columns[0] != "plan" {
		t.Errorf("columns = %v, want [plan]", rows.Columns)
	}
	out := joinPlanRows(rows)
	for _, want := range []string{
		"Count",
		"└── ReadSegments(events)",
		"Predicate:",
		"tenant_id = 42",
		"Access:",
		"min/max prune eligible",
		"raw count loop eligible",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestQueryExplainSelectFlatlandPredicateLines(t *testing.T) {
	db := setupExplainDB(t)
	rows, err := db.Query(context.Background(), `EXPLAIN SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'`)
	if err != nil {
		t.Fatalf("Query EXPLAIN: %v", err)
	}
	out := joinPlanRows(rows)
	for _, want := range []string{
		"tenant_id = 42",
		"event_type = 'checkout'",
		"text summary prune eligible",
		"metadata count: no exact count for combined predicate",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestQueryExplainAnalyzeIncludesTotalTiming(t *testing.T) {
	db := setupExplainDB(t)
	rows, err := db.Query(context.Background(), `EXPLAIN ANALYZE SELECT count(*) FROM events WHERE tenant_id = 42`)
	if err != nil {
		t.Fatalf("Query EXPLAIN ANALYZE: %v", err)
	}
	out := joinPlanRows(rows)
	if !strings.Contains(out, "Timing:") {
		t.Errorf("EXPLAIN ANALYZE missing Timing block:\n%s", out)
	}
	if !strings.Contains(out, "total: ") {
		t.Errorf("EXPLAIN ANALYZE missing total: line:\n%s", out)
	}
}

func TestExplainAPIReturnsStructuredReport(t *testing.T) {
	db := setupExplainDB(t)
	rep, err := db.Explain(context.Background(),
		`SELECT count(*) FROM events WHERE tenant_id = 42`,
		ExplainOptions{})
	if err != nil {
		t.Fatalf("db.Explain: %v", err)
	}
	if rep == nil {
		t.Fatal("Explain returned nil report")
	}
	if rep.Plan == nil || rep.Plan.Op != "Count" {
		t.Errorf("plan = %+v, want Count root", rep.Plan)
	}
	if len(rep.Plan.Children) != 1 || rep.Plan.Children[0].Op != "ReadSegments" {
		t.Errorf("plan children = %+v, want [ReadSegments(events)]", rep.Plan.Children)
	}
	if len(rep.Predicate) != 1 || rep.Predicate[0] != "tenant_id = 42" {
		t.Errorf("predicate = %#v, want [\"tenant_id = 42\"]", rep.Predicate)
	}
	if len(rep.Access) == 0 {
		t.Error("access entries empty, want at least one")
	}
	if rep.Reduction != nil || len(rep.Read) != 0 || rep.Timing != nil {
		t.Errorf("plain Explain populated execution sections: %+v", rep)
	}
}

func TestExplainAPIWithAnalyzePopulatesTiming(t *testing.T) {
	db := setupExplainDB(t)
	rep, err := db.Explain(context.Background(),
		`SELECT count(*) FROM events WHERE tenant_id = 42`,
		ExplainOptions{Analyze: true})
	if err != nil {
		t.Fatalf("db.Explain ANALYZE: %v", err)
	}
	if rep.Timing == nil {
		t.Fatal("Timing nil after ANALYZE")
	}
	if rep.Timing.TotalMs < 0 {
		t.Errorf("TotalMs = %v, want >= 0", rep.Timing.TotalMs)
	}
}

func TestExplainAPIRejectsNonSelect(t *testing.T) {
	db := setupExplainDB(t)
	_, err := db.Explain(context.Background(),
		`INSERT INTO events VALUES (1, 'x', 0)`,
		ExplainOptions{})
	if err == nil {
		t.Fatal("expected error for INSERT, got nil")
	}
}

func TestQueryExplainGroupedShowsGroupOp(t *testing.T) {
	db := setupExplainDB(t)
	rows, err := db.Query(context.Background(),
		`EXPLAIN SELECT event_type, count(*) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query EXPLAIN grouped: %v", err)
	}
	out := joinPlanRows(rows)
	if !strings.Contains(out, "Group(event_type), Count") {
		t.Errorf("missing Group(event_type), Count:\n%s", out)
	}
}

func TestExplainAnalyzePopulatesReductionAndReadOnPublishedSegments(t *testing.T) {
	db := setupExplainDB(t)
	// Force the buffered hot rows out to a sealed segment so the storage path
	// that populates ExecStats counters fires.
	if err := db.data.FlushAllBuffered(context.Background()); err != nil {
		t.Fatalf("FlushAllBuffered: %v", err)
	}
	rep, err := db.Explain(context.Background(),
		`SELECT count(*) FROM events WHERE tenant_id = 42`,
		ExplainOptions{Analyze: true})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE: %v", err)
	}
	if rep.Reduction == nil {
		t.Fatal("Reduction nil after ANALYZE on published segments")
	}
	if rep.Reduction.SegmentsTotal == 0 {
		t.Errorf("SegmentsTotal = 0, want > 0")
	}
	if rep.Reduction.RowsTotal == 0 {
		t.Errorf("RowsTotal = 0, want > 0")
	}
	if rep.Reduction.RowsMatched != 1 {
		t.Errorf("RowsMatched = %d, want 1 (one row with tenant_id=42)", rep.Reduction.RowsMatched)
	}
	if len(rep.Read) == 0 {
		t.Errorf("Read entries empty after ANALYZE; want at least one (predicate payload or metadata)")
	}
}

func joinPlanRows(rows *Rows) string {
	var lines []string
	for _, row := range rows.Values {
		if len(row) == 0 {
			continue
		}
		s, _ := row[0].(string)
		lines = append(lines, s)
	}
	return strings.Join(lines, "\n")
}
