package engine

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/binder"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sql/parser"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// ExplainOptions controls the engine's Explain entrypoint.
type ExplainOptions struct {
	// Analyze runs the inner query and captures total wall time. Reduction
	// and Read counters become available once storage counter wiring lands
	// (see PR3 in the active plan).
	Analyze bool
}

// Explain binds the SQL text and returns the structured query report. The
// inner statement must be a SELECT; other shapes are rejected with a clear
// error. Used by cmd/bench and by the SQL EXPLAIN surface (which renders the
// report as text rows).
func (db *DB) Explain(ctx context.Context, sqlText string, opts ExplainOptions) (*explain.QueryReport, error) {
	if err := db.checkReady(ctx); err != nil {
		return nil, err
	}
	stmt, err := parseOneSelectOrExplain(sqlText)
	if err != nil {
		return nil, err
	}
	return db.buildExplainReport(ctx, opts.Analyze, stmt)
}

// executeExplain handles a parsed *ast.ExplainStmt for the SQL EXPLAIN
// surface. It builds the report and renders it to a single-column Rows
// shaped like the rest of Query's output.
func (db *DB) executeExplain(ctx context.Context, stmt *ast.ExplainStmt) (*Rows, error) {
	report, err := db.buildExplainReport(ctx, stmt.Analyze, stmt.Inner)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := explain.Render(&buf, report, explain.RenderOptions{}); err != nil {
		return nil, err
	}
	rows := &Rows{Columns: []string{"plan"}}
	for line := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		rows.Values = append(rows.Values, []any{line})
	}
	return rows, nil
}

func (db *DB) buildExplainReport(ctx context.Context, analyze bool, inner ast.Stmt) (*explain.QueryReport, error) {
	sel, ok := inner.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN supports SELECT only, got %T", inner)
	}
	def, ok := db.catalog.Table(sel.Table)
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", sel.Table)
	}
	plan, err := binder.BindSelect(sel, def)
	if err != nil {
		return nil, err
	}
	report := &explain.QueryReport{
		Plan:      explain.PlanFromLogical(plan),
		Predicate: explain.PredicatesFromExpr(plan.WhereExpr),
		Access:    accessEligibility(plan),
		NotUsed:   notUsedHints(plan),
	}
	if !analyze {
		return report, nil
	}
	start := time.Now()
	var scratch storage.QueryScratch
	scratch.Stats = &storage.ExecStats{}
	switch plan.Kind {
	case logical.QueryAggregate:
		if _, err := db.executeAggregateQuery(ctx, def, plan, &scratch); err != nil {
			return nil, err
		}
	case logical.QueryScan:
		if _, err := db.executeScanQuery(ctx, def, plan); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("EXPLAIN ANALYZE supports aggregate and scan queries only")
	}
	ms := float64(time.Since(start).Nanoseconds()) / 1_000_000.0
	report.Timing = &explain.Timing{TotalMs: ms}
	foldExecStats(report, plan, scratch.Stats)
	return report, nil
}

// foldExecStats merges the storage-side counters into the report's Reduction
// and Read sections, and overwrites the Access entries with the actual paths
// taken (replacing the " eligible" labels that the EXPLAIN-only build used).
func foldExecStats(report *explain.QueryReport, plan logical.Query, stats *storage.ExecStats) {
	if stats == nil {
		return
	}
	report.Reduction = &explain.Reduction{
		SegmentsTotal:     stats.SegmentsTotal,
		SegmentsCandidate: stats.SegmentsCandidate,
		PagesTotal:        stats.PagesTotal,
		PagesCandidate:    stats.PagesCandidate,
		RowsTotal:         stats.RowsTotal,
		RowsCandidate:     stats.RowsCandidate,
		RowsMatched:       stats.RowsMatched,
	}
	if stats.PredPayloadBytes > 0 {
		report.Read = append(report.Read, explain.ReadEntry{Purpose: "predicate payload", Bytes: stats.PredPayloadBytes})
	}
	if stats.AggPayloadBytes > 0 {
		report.Read = append(report.Read, explain.ReadEntry{Purpose: "aggregate payload", Bytes: stats.AggPayloadBytes})
	}
	if stats.MetadataBytesRead > 0 {
		report.Read = append(report.Read, explain.ReadEntry{Purpose: "metadata", Bytes: stats.MetadataBytesRead})
	}
	report.Access = analyzeAccessEntries(plan, stats)
}

// analyzeAccessEntries replaces the EXPLAIN-time eligibility list with the
// actual decisions captured during execution, where available. Columns the
// storage layer didn't tag fall back to the eligibility label (without the
// " eligible" suffix) so the report stays informative.
func analyzeAccessEntries(plan logical.Query, stats *storage.ExecStats) []explain.AccessEntry {
	var out []explain.AccessEntry
	seen := make(map[string]struct{})
	add := func(name string, decision string) {
		if name == "" || decision == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, explain.AccessEntry{Column: name, Decision: decision})
	}
	for _, name := range collectWhereColumns(plan.WhereExpr) {
		col, ok := findTableColumn(plan.Table, name)
		if !ok {
			continue
		}
		if code, has := stats.PerColumn[col.ID]; has {
			add(name, accessCodeLabel(code))
			continue
		}
		add(name, columnPruneEligibility(col.Type))
	}
	if plan.GroupColumn != "" {
		col, ok := findTableColumn(plan.Table, plan.GroupColumn)
		if ok {
			if code, has := stats.PerColumn[col.ID]; has {
				add(plan.GroupColumn, accessCodeLabel(code))
			} else {
				add(plan.GroupColumn, explain.AccessTextPayloadGrouped)
			}
		}
	}
	if entry := aggregateAccessEntry(plan); entry.Column != "" {
		// During analyze the eligibility suffix becomes the actual path. For
		// aggregates we inspect PerColumn for the aggregate column; otherwise
		// we fall back to the eligibility label without the suffix.
		decision := strings.TrimSuffix(entry.Decision, " eligible")
		if plan.AggregateColumn != "" {
			if col, ok := findTableColumn(plan.Table, plan.AggregateColumn); ok {
				if code, has := stats.PerColumn[col.ID]; has {
					decision = accessCodeLabel(code)
				}
			}
		}
		add(entry.Column, decision)
	}
	return out
}

// accessCodeLabel maps a storage.AccessCode to the canonical user-facing
// label from internal/explain.
func accessCodeLabel(code storage.AccessCode) string {
	switch code {
	case storage.AccessSegmentMinMax:
		return explain.AccessSegmentMinMaxPrune
	case storage.AccessPageMinMax:
		return explain.AccessPageMinMaxPrune
	case storage.AccessTextSummary:
		return explain.AccessTextSummaryPrune
	case storage.AccessRawCountLoop:
		return explain.AccessRawCountLoop
	case storage.AccessRawIntSumLoop:
		return explain.AccessRawIntSumLoop
	case storage.AccessRawIntMinMaxLoop:
		return explain.AccessRawIntMinMaxLoop
	case storage.AccessTextPayloadGrouped:
		return explain.AccessTextPayloadGrouped
	case storage.AccessGroupedScanCount:
		return explain.AccessGroupedScanCount
	case storage.AccessPayloadForExpr:
		return explain.AccessPayloadForExpr
	case storage.AccessCountedAfterRowFilter:
		return explain.AccessCountedAfterRowFilter
	case storage.AccessMetadataAnswered:
		return explain.AccessMetadataAnswered
	case storage.AccessMetadataNonNullCount:
		return explain.AccessMetadataNonNullCount
	}
	return ""
}

// accessEligibility produces the per-column "Access" entries for an EXPLAIN
// report, before execution counters exist. Each label is suffixed with
// " eligible" because we are reporting what the engine *could* do with the
// available metadata; PR3 replaces this with the actual path taken.
func accessEligibility(plan logical.Query) []explain.AccessEntry {
	var out []explain.AccessEntry
	seen := make(map[string]struct{})
	addColumn := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		col, ok := findTableColumn(plan.Table, name)
		if !ok {
			return
		}
		out = append(out, explain.AccessEntry{
			Column:   name,
			Decision: columnPruneEligibility(col.Type) + " eligible",
		})
	}
	for _, name := range collectWhereColumns(plan.WhereExpr) {
		addColumn(name)
	}
	if plan.GroupColumn != "" {
		addColumn(plan.GroupColumn)
	}
	if entry := aggregateAccessEntry(plan); entry.Column != "" {
		out = append(out, entry)
	}
	if plan.Kind == logical.QueryScan && len(plan.SelectOutputs) > 0 {
		for _, output := range plan.SelectOutputs {
			if output.Expr.Kind == logical.ExprColumn {
				addColumn(output.Expr.Column)
			}
		}
	}
	return out
}

func aggregateAccessEntry(plan logical.Query) explain.AccessEntry {
	if plan.Kind != logical.QueryAggregate {
		return explain.AccessEntry{}
	}
	switch plan.Aggregate {
	case logical.AggregateCount:
		if plan.AggregateColumn == "" {
			return explain.AccessEntry{Column: "count(*)", Decision: explain.AccessRawCountLoop + " eligible"}
		}
		return explain.AccessEntry{
			Column:   "count(" + plan.AggregateColumn + ")",
			Decision: explain.AccessMetadataNonNullCount + " eligible",
		}
	case logical.AggregateSum:
		return explain.AccessEntry{
			Column:   "sum(" + plan.AggregateColumn + ")",
			Decision: explain.AccessRawIntSumLoop + " eligible",
		}
	case logical.AggregateMin, logical.AggregateMax:
		name := "min"
		if plan.Aggregate == logical.AggregateMax {
			name = "max"
		}
		return explain.AccessEntry{
			Column:   name + "(" + plan.AggregateColumn + ")",
			Decision: explain.AccessRawIntMinMaxLoop + " eligible",
		}
	}
	return explain.AccessEntry{}
}

func columnPruneEligibility(t sqltype.Type) string {
	switch t.Kind {
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64,
		sqltype.KindDate, sqltype.KindTimestamp,
		sqltype.KindFloat32, sqltype.KindFloat64:
		return explain.AccessMinMaxPrune
	case sqltype.KindText:
		return explain.AccessTextSummaryPrune
	case sqltype.KindBytes, sqltype.KindUUID, sqltype.KindNamed, sqltype.KindBool:
		return explain.AccessMetadataAnswered
	}
	return explain.AccessMetadataAnswered
}

// collectWhereColumns walks the WHERE tree and returns the unique column
// names referenced (including those wrapped in lower/upper).
func collectWhereColumns(expr *logical.Expr) []string {
	if expr == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	var walk func(*logical.Expr)
	walk = func(e *logical.Expr) {
		if e == nil {
			return
		}
		switch e.Kind {
		case logical.ExprColumn:
			if _, ok := seen[e.Column]; !ok {
				seen[e.Column] = struct{}{}
				out = append(out, e.Column)
			}
		case logical.ExprBinary, logical.ExprUnary:
			walk(e.Left)
			walk(e.Right)
		case logical.ExprBetween, logical.ExprIn:
			walk(e.Left)
			for i := range e.Args {
				walk(&e.Args[i])
			}
		}
	}
	walk(expr)
	return out
}

// notUsedHints documents optimizations that did not fire and why, based on
// the plan shape alone (no execution required). Storage-level "not used"
// reasons (e.g. a Bloom filter not consulted because of a wildcard) require
// counter wiring and arrive in PR3.
func notUsedHints(plan logical.Query) []explain.NotUsedEntry {
	var out []explain.NotUsedEntry
	if plan.Kind == logical.QueryAggregate && plan.Aggregate == logical.AggregateCount && plan.AggregateColumn == "" {
		if hasExpressionPredicate(plan.WhereExpr) {
			out = append(out, explain.NotUsedEntry{
				Name:   "metadata count",
				Reason: "no exact count for expression predicate",
			})
		} else if predicateColumnCount(plan.WhereExpr) > 1 {
			out = append(out, explain.NotUsedEntry{
				Name:   "metadata count",
				Reason: "no exact count for combined predicate",
			})
		}
	}
	return out
}

func hasExpressionPredicate(expr *logical.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == logical.ExprUnary && (expr.Op == logical.OpLower || expr.Op == logical.OpUpper) {
		return true
	}
	if expr.Kind == logical.ExprBinary {
		return hasExpressionPredicate(expr.Left) || hasExpressionPredicate(expr.Right)
	}
	if expr.Kind == logical.ExprBetween || expr.Kind == logical.ExprIn {
		if hasExpressionPredicate(expr.Left) {
			return true
		}
		for i := range expr.Args {
			if hasExpressionPredicate(&expr.Args[i]) {
				return true
			}
		}
	}
	return false
}

func predicateColumnCount(expr *logical.Expr) int {
	cols := collectWhereColumns(expr)
	return len(cols)
}

// parseOneSelectOrExplain parses a single statement that must be either a
// SELECT or an EXPLAIN; the caller is db.Explain so we unwrap an EXPLAIN
// down to its inner SELECT.
func parseOneSelectOrExplain(sqlText string) (ast.Stmt, error) {
	stmt, err := parser.ParseOne(sqlText)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *ast.ExplainStmt:
		return s.Inner, nil
	case *ast.SelectStmt:
		return s, nil
	default:
		return nil, fmt.Errorf("Explain only supports SELECT statements, got %T", stmt)
	}
}
