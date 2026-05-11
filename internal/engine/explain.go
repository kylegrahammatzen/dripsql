package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v3exec "github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/explain"
	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type executionTrace struct {
	scans []scanTrace
}

type scanTrace struct {
	table v3sql.BoundTableDef
	where *v3sql.BoundExpr
	stats storage.ExecStats
}

// recordScan is the nil-safe wrapper around addScan; nearly every caller
// guarded the underlying call with `if trace != nil`, which adds 2 lines
// per call site. Keeping the guard in one place is cleaner.
func (t *executionTrace) recordScan(plan *v3sql.ScanPlan, stats storage.ExecStats) {
	if t != nil {
		t.addScan(plan, stats)
	}
}

func (t *executionTrace) addScan(plan *v3sql.ScanPlan, stats storage.ExecStats) {
	if t == nil || plan == nil {
		return
	}
	t.scans = append(t.scans, scanTrace{
		table: plan.Table,
		where: plan.Where,
		stats: stats,
	})
}

func (db *DB) ExplainAnalyze(ctx context.Context, sqlText string, args ...any) (*Rows, explain.Report, error) {
	if err := db.checkReady(ctx); err != nil {
		return nil, explain.Report{}, err
	}
	if len(args) != 0 {
		return nil, explain.Report{}, fmt.Errorf("ExplainAnalyze arguments are not supported yet")
	}
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, explain.Report{}, err
	}
	if explainPlan, ok := plan.(*v3sql.ExplainPlan); ok {
		plan = explainPlan.Inner
	}
	return db.executeAnalyzed(ctx, plan)
}

func (db *DB) executeExplain(ctx context.Context, plan *v3sql.ExplainPlan) (*Rows, error) {
	if plan.Analyze {
		_, report, err := db.executeAnalyzed(ctx, plan.Inner)
		if err != nil {
			return nil, err
		}
		columns, values := report.Table()
		return &Rows{Columns: columns, Values: values}, nil
	}
	return &Rows{Columns: []string{"plan"}, Values: explainPlanRows(plan.Inner, 0)}, nil
}

func (db *DB) executeAnalyzed(ctx context.Context, plan v3sql.Plan) (*Rows, explain.Report, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	trace := &executionTrace{}
	collector := newRowCollector(planOutputColumns(plan))
	counter := &v3exec.Explain{Downstream: collector}
	start := time.Now()
	if err := db.runSource(ctx, plan, counter, trace); err != nil {
		return nil, explain.Report{}, err
	}
	report := explainReport(plan, trace, counter, time.Since(start))
	return collector.rows, report, nil
}

func explainPlanRows(plan v3sql.Plan, depth int) [][]any {
	label := strings.Repeat("  ", depth) + planNodeLabel(plan)
	rows := [][]any{{label}}
	switch plan := plan.(type) {
	case *v3sql.LimitPlan:
		rows = append(rows, explainPlanRows(plan.Source, depth+1)...)
	case *v3sql.SortPlan:
		rows = append(rows, explainPlanRows(plan.Source, depth+1)...)
	case *v3sql.ProjectPlan:
		rows = append(rows, explainPlanRows(plan.Source, depth+1)...)
	case *v3sql.AggregatePlan:
		rows = append(rows, explainPlanRows(plan.Source, depth+1)...)
	}
	return rows
}

func planNodeLabel(plan v3sql.Plan) string {
	switch plan := plan.(type) {
	case *v3sql.ScanPlan:
		if plan.Where != nil {
			return fmt.Sprintf("scan %s filter %s", plan.Table.Name, boundExprString(*plan.Where))
		}
		return fmt.Sprintf("scan %s", plan.Table.Name)
	case *v3sql.AggregatePlan:
		return "aggregate " + aggregateSpecsString(plan.Aggregates)
	case *v3sql.ProjectPlan:
		return "project " + strings.Join(planOutputColumns(plan), ", ")
	case *v3sql.SortPlan:
		return "sort " + sortKeysString(plan.Keys)
	case *v3sql.LimitPlan:
		return fmt.Sprintf("limit %d offset %d", plan.N, plan.Offset)
	default:
		return fmt.Sprintf("%T", plan)
	}
}

func explainReport(plan v3sql.Plan, trace *executionTrace, counter *v3exec.Explain, elapsed time.Duration) explain.Report {
	stats := combinedScanStats(trace)
	ns := elapsed.Nanoseconds()
	ms := float64(ns) / 1_000_000.0
	report := explain.Report{
		Plan:      planLine(plan),
		Output:    explain.Output{Batches: counter.Batches, Rows: counter.Rows, Kept: counter.Selected},
		Reduction: explain.ReductionFromStats(stats),
		Access:    explainAccess(plan, trace),
		Read:      explainRead(plan, trace, stats),
		Timing:    explain.Timing{FirstMs: ms, BestMs: ms, AvgMs: ms, FirstNs: ns, BestNs: ns, AvgNs: ns, Samples: 1},
	}
	report.Finalize()
	return report
}

func combinedScanStats(trace *executionTrace) storage.ExecStats {
	var out storage.ExecStats
	if trace == nil {
		return out
	}
	for _, scan := range trace.scans {
		out.SegmentsTotal += scan.stats.SegmentsTotal
		out.SegmentsCandidate += scan.stats.SegmentsCandidate
		out.PagesTotal += scan.stats.PagesTotal
		out.PagesCandidate += scan.stats.PagesCandidate
		out.RowsTotal += scan.stats.RowsTotal
		out.RowsCandidate += scan.stats.RowsCandidate
		out.RowsMatched += scan.stats.RowsMatched
		out.PayloadBytesRead += scan.stats.PayloadBytesRead
	}
	return out
}

func planLine(plan v3sql.Plan) string {
	switch plan := plan.(type) {
	case *v3sql.LimitPlan:
		return fmt.Sprintf("Limit(%d, offset %d) -> %s", plan.N, plan.Offset, planLine(plan.Source))
	case *v3sql.SortPlan:
		return "Sort(" + sortKeysString(plan.Keys) + ") -> " + planLine(plan.Source)
	case *v3sql.ProjectPlan:
		return "Project(" + strings.Join(planOutputColumns(plan), ", ") + ") -> " + planLine(plan.Source)
	case *v3sql.AggregatePlan:
		return "Aggregate(" + aggregateSpecsString(plan.Aggregates) + ") -> " + planLine(plan.Source)
	case *v3sql.ScanPlan:
		if plan.Where != nil {
			return "Filter(" + boundExprString(*plan.Where) + ") -> Scan(" + plan.Table.Name + ")"
		}
		return "Scan(" + plan.Table.Name + ")"
	default:
		return fmt.Sprintf("%T", plan)
	}
}

func sortKeysString(keys []v3sql.SortKey) string {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		part := key.Name
		if part == "" {
			part = boundExprString(key.Expr)
		}
		if key.Desc {
			part += " DESC"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, ", ")
}

func aggregateSpecsString(specs []v3sql.AggSpec) string {
	parts := make([]string, 0, len(specs))
	for _, spec := range specs {
		parts = append(parts, aggregateSpecString(spec))
	}
	return strings.Join(parts, ", ")
}

func aggregateSpecString(spec v3sql.AggSpec) string {
	switch spec.Func {
	case v3sql.AggregateCount:
		if spec.Star {
			return "count(*)"
		}
		return "count(" + spec.ArgName + ")"
	case v3sql.AggregateSum:
		return "sum(" + spec.ArgName + ")"
	case v3sql.AggregateMin:
		return "min(" + spec.ArgName + ")"
	case v3sql.AggregateMax:
		return "max(" + spec.ArgName + ")"
	default:
		return fmt.Sprintf("aggregate(%d)", spec.Func)
	}
}

func boundExprString(expr v3sql.BoundExpr) string {
	switch expr.Kind {
	case v3sql.BoundExprColumn:
		return expr.Column
	case v3sql.BoundExprLiteral:
		return literalString(expr.Literal)
	case v3sql.BoundExprBinary:
		left := "?"
		right := "?"
		if expr.Left != nil {
			left = boundExprString(*expr.Left)
		}
		if expr.Right != nil {
			right = boundExprString(*expr.Right)
		}
		return left + " " + boundOpString(expr.Op) + " " + right
	case v3sql.BoundExprUnary:
		if expr.Op == v3sql.BoundOpNot && expr.Left != nil {
			return "NOT (" + boundExprString(*expr.Left) + ")"
		}
		return boundOpString(expr.Op)
	case v3sql.BoundExprBetween:
		if expr.Left == nil || len(expr.Args) != 2 {
			return "BETWEEN"
		}
		return boundExprString(*expr.Left) + " BETWEEN " + boundExprString(expr.Args[0]) + " AND " + boundExprString(expr.Args[1])
	case v3sql.BoundExprIn:
		if expr.Left == nil {
			return "IN"
		}
		parts := make([]string, 0, len(expr.Args))
		for _, arg := range expr.Args {
			parts = append(parts, boundExprString(arg))
		}
		op := "IN"
		if expr.Not {
			op = "NOT IN"
		}
		return boundExprString(*expr.Left) + " " + op + " (" + strings.Join(parts, ", ") + ")"
	default:
		return "?"
	}
}

func boundOpString(op v3sql.BoundOp) string {
	switch op {
	case v3sql.BoundOpEqual:
		return "="
	case v3sql.BoundOpNotEqual:
		return "!="
	case v3sql.BoundOpLess:
		return "<"
	case v3sql.BoundOpLessEqual:
		return "<="
	case v3sql.BoundOpGreater:
		return ">"
	case v3sql.BoundOpGreaterEqual:
		return ">="
	case v3sql.BoundOpAdd:
		return "+"
	case v3sql.BoundOpSubtract:
		return "-"
	case v3sql.BoundOpMultiply:
		return "*"
	case v3sql.BoundOpDivide:
		return "/"
	case v3sql.BoundOpModulo:
		return "%"
	case v3sql.BoundOpIntDivide:
		return "//"
	case v3sql.BoundOpConcat:
		return "||"
	case v3sql.BoundOpAnd:
		return "AND"
	case v3sql.BoundOpOr:
		return "OR"
	case v3sql.BoundOpNot:
		return "NOT"
	default:
		return "?"
	}
}

func literalString(value any) string {
	switch value := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(value, "'", "''") + "'"
	case bool:
		if value {
			return "true"
		}
		return "false"
	case int16:
		return strconv.FormatInt(int64(value), 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	default:
		return fmt.Sprint(value)
	}
}

func explainAccess(plan v3sql.Plan, trace *executionTrace) []explain.Access {
	var entries []explain.Access
	seen := make(map[string]struct{})
	if trace != nil {
		for _, scan := range trace.scans {
			for _, entry := range scanAccess(scan) {
				key := entry.Name + "\x00" + entry.Strategy
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				entries = append(entries, entry)
			}
		}
	}
	entries = append(entries, aggregateAccess(plan, trace)...)
	return entries
}

func scanAccess(scan scanTrace) []explain.Access {
	if scan.where == nil {
		if scan.stats.PayloadBytesRead == 0 {
			return []explain.Access{{Name: scan.table.Name, Strategy: "metadata scan", Effect: "no payload"}}
		}
		return []explain.Access{{Name: scan.table.Name, Strategy: "full scan", Effect: "all pages"}}
	}
	leaves := predicateLeaves(*scan.where)
	entries := make([]explain.Access, 0, len(leaves))
	for _, leaf := range leaves {
		column := predicateLeafColumn(leaf)
		if column == "" {
			continue
		}
		col, ok := boundColumn(scan.table, column)
		strategy, eligible := pruneStrategy(leaf, col.Type.Kind)
		if !ok {
			strategy = "metadata prune"
			eligible = false
		}
		entries = append(entries, explain.Access{Name: column, Strategy: strategy, Effect: pruneEffect(scan.stats, eligible)})
	}
	if len(entries) == 0 {
		return []explain.Access{{Name: "predicate", Strategy: "metadata prune", Effect: "not eligible"}}
	}
	return entries
}

func predicateLeaves(expr v3sql.BoundExpr) []v3sql.BoundExpr {
	switch expr.Kind {
	case v3sql.BoundExprBinary:
		if expr.Op == v3sql.BoundOpAnd || expr.Op == v3sql.BoundOpOr {
			var out []v3sql.BoundExpr
			if expr.Left != nil {
				out = append(out, predicateLeaves(*expr.Left)...)
			}
			if expr.Right != nil {
				out = append(out, predicateLeaves(*expr.Right)...)
			}
			return out
		}
	case v3sql.BoundExprUnary:
		return []v3sql.BoundExpr{expr}
	}
	return []v3sql.BoundExpr{expr}
}

func predicateLeafColumn(expr v3sql.BoundExpr) string {
	switch expr.Kind {
	case v3sql.BoundExprBinary:
		if expr.Left != nil && expr.Left.Kind == v3sql.BoundExprColumn {
			return expr.Left.Column
		}
		if expr.Right != nil && expr.Right.Kind == v3sql.BoundExprColumn {
			return expr.Right.Column
		}
	case v3sql.BoundExprBetween, v3sql.BoundExprIn:
		if expr.Left != nil && expr.Left.Kind == v3sql.BoundExprColumn {
			return expr.Left.Column
		}
	case v3sql.BoundExprUnary:
		if expr.Left != nil {
			return predicateLeafColumn(*expr.Left)
		}
	}
	return ""
}

func pruneStrategy(expr v3sql.BoundExpr, kind types.Kind) (string, bool) {
	switch expr.Kind {
	case v3sql.BoundExprBinary:
		switch kind {
		case types.KindBool:
			return "bool summary prune", expr.Op == v3sql.BoundOpEqual || expr.Op == v3sql.BoundOpNotEqual
		case types.KindInt16, types.KindInt32, types.KindInt64, types.KindDate, types.KindTimestamp, types.KindTime:
			return "min/max prune", isComparisonOp(expr.Op)
		case types.KindText, types.KindBytes:
			return "text summary prune", expr.Op == v3sql.BoundOpEqual
		case types.KindUUID:
			return "uuid summary prune", expr.Op == v3sql.BoundOpEqual
		}
	case v3sql.BoundExprBetween:
		if intPruneKind(kind) {
			return "min/max prune", true
		}
	case v3sql.BoundExprIn:
		switch kind {
		case types.KindBool:
			return "bool summary prune", true
		case types.KindInt16, types.KindInt32, types.KindInt64, types.KindDate, types.KindTimestamp, types.KindTime:
			return "min/max prune", true
		case types.KindText, types.KindBytes:
			return "text summary prune", !expr.Not
		case types.KindUUID:
			return "uuid summary prune", !expr.Not
		}
	}
	return "metadata prune", false
}

func isComparisonOp(op v3sql.BoundOp) bool {
	switch op {
	case v3sql.BoundOpEqual, v3sql.BoundOpNotEqual, v3sql.BoundOpLess, v3sql.BoundOpLessEqual, v3sql.BoundOpGreater, v3sql.BoundOpGreaterEqual:
		return true
	default:
		return false
	}
}

func intPruneKind(kind types.Kind) bool {
	switch kind {
	case types.KindInt16, types.KindInt32, types.KindInt64, types.KindDate, types.KindTimestamp, types.KindTime:
		return true
	default:
		return false
	}
}

func pruneEffect(stats storage.ExecStats, eligible bool) string {
	if !eligible {
		return "not eligible"
	}
	if stats.SegmentsTotal == 0 && stats.PagesTotal == 0 {
		return "no data"
	}
	if stats.SegmentsCandidate < stats.SegmentsTotal || stats.PagesCandidate < stats.PagesTotal {
		return "effective"
	}
	return "no effect"
}

func boundColumn(table v3sql.BoundTableDef, name string) (v3sql.BoundColumnDef, bool) {
	for _, col := range table.Columns {
		if normalizeName(col.Name) == normalizeName(name) {
			return col, true
		}
	}
	return v3sql.BoundColumnDef{}, false
}

func aggregateAccess(plan v3sql.Plan, trace *executionTrace) []explain.Access {
	switch plan := plan.(type) {
	case *v3sql.LimitPlan:
		return aggregateAccess(plan.Source, trace)
	case *v3sql.SortPlan:
		return aggregateAccess(plan.Source, trace)
	case *v3sql.ProjectPlan:
		return aggregateAccess(plan.Source, trace)
	case *v3sql.AggregatePlan:
		entries := make([]explain.Access, 0, len(plan.GroupBy)+len(plan.Aggregates)+len(plan.Hidden))
		metadataOnly := aggregateTraceMetadataOnly(trace)
		for _, group := range plan.GroupBy {
			if metadataOnly {
				entries = append(entries, explain.Access{Name: "group by " + group.Column, Strategy: "metadata group", Effect: "exact counts"})
				continue
			}
			entries = append(entries, explain.Access{Name: "group by " + group.Column, Strategy: "hash group", Effect: "in memory"})
		}
		for _, spec := range append(append([]v3sql.AggSpec{}, plan.Aggregates...), plan.Hidden...) {
			entries = append(entries, explain.Access{Name: aggregateSpecString(spec), Strategy: aggregateStrategy(spec, metadataOnly)})
		}
		entries = append(entries, aggregateAccess(plan.Source, trace)...)
		return entries
	default:
		return nil
	}
}

func aggregateTraceMetadataOnly(trace *executionTrace) bool {
	if trace == nil || len(trace.scans) == 0 {
		return false
	}
	for _, scan := range trace.scans {
		if scan.stats.PayloadBytesRead != 0 {
			return false
		}
	}
	return true
}

func aggregateStrategy(spec v3sql.AggSpec, metadataOnly bool) string {
	switch spec.Func {
	case v3sql.AggregateCount:
		if metadataOnly {
			return "metadata count"
		}
		if spec.Star {
			return "raw count loop"
		}
		return "raw count non-null loop"
	case v3sql.AggregateSum:
		if metadataOnly {
			return "metadata sum"
		}
		return "raw sum loop"
	case v3sql.AggregateMin:
		if metadataOnly {
			return "metadata min"
		}
		return "raw min loop"
	case v3sql.AggregateMax:
		if metadataOnly {
			return "metadata max"
		}
		return "raw max loop"
	default:
		return "raw aggregate loop"
	}
}

func explainRead(plan v3sql.Plan, trace *executionTrace, stats storage.ExecStats) explain.Read {
	read := explain.Read{PayloadBytes: stats.PayloadBytesRead}
	if hasPredicateScan(trace) {
		read.PredicatePayloadBytes = stats.PayloadBytesRead
	} else if hasAggregate(plan) {
		read.AggregatePayloadBytes = stats.PayloadBytesRead
	}
	return read
}

func hasPredicateScan(trace *executionTrace) bool {
	if trace == nil {
		return false
	}
	for _, scan := range trace.scans {
		if scan.where != nil {
			return true
		}
	}
	return false
}

func hasAggregate(plan v3sql.Plan) bool {
	switch plan := plan.(type) {
	case *v3sql.LimitPlan:
		return hasAggregate(plan.Source)
	case *v3sql.SortPlan:
		return hasAggregate(plan.Source)
	case *v3sql.ProjectPlan:
		return hasAggregate(plan.Source)
	case *v3sql.AggregatePlan:
		return true
	default:
		return false
	}
}
