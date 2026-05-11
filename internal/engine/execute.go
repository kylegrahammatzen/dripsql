package engine

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	v3exec "github.com/kylegrahammatzen/dripsql/internal/exec"
	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	secondsPerDay   = int64(24 * time.Hour / time.Second)
	timestampLayout = "2006-01-02T15:04:05.000000000Z"
)

func (db *DB) Execute(ctx context.Context, plan v3sql.Plan) (*Rows, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.checkReady(ctx); err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("plan is nil")
	}
	if explain, ok := plan.(*v3sql.ExplainPlan); ok {
		return db.executeExplain(ctx, explain)
	}

	return db.executeRows(ctx, plan, nil)
}

func (db *DB) executeRows(ctx context.Context, plan v3sql.Plan, trace *executionTrace) (*Rows, error) {
	collector := newRowCollector(planOutputColumns(plan))
	if err := db.runSource(ctx, plan, collector, trace); err != nil {
		return nil, err
	}
	return collector.rows, nil
}

func (db *DB) runSource(ctx context.Context, plan v3sql.Plan, consumer v3exec.Consumer, trace *executionTrace) error {
	switch plan := plan.(type) {
	case *v3sql.ScanPlan:
		return db.runScan(ctx, plan, consumer, trace)
	case *v3sql.ProjectPlan:
		if aggregate, ok := plan.Source.(*v3sql.AggregatePlan); ok && aggregate.Having != nil {
			return db.runAggregateProject(ctx, aggregate, plan.Exprs, consumer, trace)
		}
		project := &v3exec.Project{Exprs: plan.Exprs, Downstream: consumer}
		return db.runSource(ctx, plan.Source, project, trace)
	case *v3sql.SortPlan:
		if project, ok := plan.Source.(*v3sql.ProjectPlan); ok && sortNeedsHiddenExpressions(plan.Keys) {
			return db.runProjectSort(ctx, project, plan.Keys, consumer, trace)
		}
		keys, err := execSortKeys(plan.Keys)
		if err != nil {
			return err
		}
		sortOp := &v3exec.Sort{Keys: keys, Downstream: consumer}
		return db.runSource(ctx, plan.Source, sortOp, trace)
	case *v3sql.LimitPlan:
		limit, err := checkedInt(plan.N, "LIMIT")
		if err != nil {
			return err
		}
		offset, err := checkedInt(plan.Offset, "OFFSET")
		if err != nil {
			return err
		}
		limitOp := &v3exec.Limit{Limit: limit, Offset: offset, Downstream: consumer}
		return db.runSource(ctx, plan.Source, limitOp, trace)
	case *v3sql.AggregatePlan:
		batch, err := db.aggregateBatch(ctx, plan, trace)
		if err != nil {
			return err
		}
		downstream := consumer
		if plan.Having != nil {
			downstream = &v3exec.Filter{Expr: plan.Having, Downstream: consumer}
		}
		return pushBatch(ctx, batch, downstream)
	default:
		return fmt.Errorf("unsupported plan %T", plan)
	}
}

func (db *DB) runProjectSort(ctx context.Context, project *v3sql.ProjectPlan, keys []v3sql.SortKey, consumer v3exec.Consumer, trace *executionTrace) error {
	visibleNames, err := boundOutputNames(project.Exprs)
	if err != nil {
		return err
	}
	extended, sortKeys, err := projectSortOutputs(project.Exprs, keys)
	if err != nil {
		return err
	}
	stripHidden := &v3exec.Project{Columns: visibleNames, Downstream: consumer}
	sortOp := &v3exec.Sort{Keys: sortKeys, Downstream: stripHidden}
	if aggregate, ok := project.Source.(*v3sql.AggregatePlan); ok && aggregate.Having != nil {
		return db.runAggregateProject(ctx, aggregate, extended, sortOp, trace)
	}
	extendedProject := &v3exec.Project{Exprs: extended, Downstream: sortOp}
	return db.runSource(ctx, project.Source, extendedProject, trace)
}

func (db *DB) runAggregateProject(ctx context.Context, plan *v3sql.AggregatePlan, outputs []v3sql.BoundOutput, consumer v3exec.Consumer, trace *executionTrace) error {
	batch, err := db.aggregateBatch(ctx, plan, trace)
	if err != nil {
		return err
	}
	havingBatch, err := havingEvaluationBatch(batch, outputs)
	if err != nil {
		return err
	}
	input := types.NewSelectionMask(batch.Len)
	input.FillAll()
	sel, err := filterExpressionSelection(ctx, havingBatch, input, *plan.Having)
	if err != nil {
		return err
	}
	project := &v3exec.Project{Exprs: outputs, Downstream: consumer}
	return pushSelectedBatch(ctx, batch, sel, project)
}

func (db *DB) runScan(ctx context.Context, plan *v3sql.ScanPlan, consumer v3exec.Consumer, trace *executionTrace) error {
	entry, ok := db.table(plan.Table.Name)
	if !ok {
		return fmt.Errorf("table %q does not exist", plan.Table.Name)
	}
	pushExpr, postExpr, err := splitScanWhere(plan.Where)
	if err != nil {
		return err
	}
	pred := storage.Predicate{Op: storage.PredicateNone}
	if pushExpr != nil {
		var pushed bool
		pred, pushed, err = predicateFromBoundExpr(pushExpr)
		if err != nil {
			return err
		}
		if !pushed {
			return fmt.Errorf("internal error: split WHERE produced non-pushable predicate")
		}
	}

	stats := &storage.ExecStats{}
	var it storage.SegmentScanIterator
	if pushExpr == nil {
		it, err = db.data.ScanIterator(ctx, entry.spec, nil, stats)
	} else {
		it, err = db.data.ScanIteratorForPredicate(ctx, entry.spec, pred, stats)
	}
	if err != nil {
		return err
	}
	it.OutputColumns = columnNamesForIDs(plan.Table, plan.Columns)
	if postExpr != nil {
		it.OutputColumns = appendMissingColumnNames(it.OutputColumns, boundExprColumnNames(*postExpr))
	}
	if requester, ok := consumer.(interface{ EncodedColumns() []string }); ok {
		it.EncodedOutputColumns = columnSet(requester.EncodedColumns())
		if postExpr != nil {
			removeColumnNames(it.EncodedOutputColumns, boundExprColumnNames(*postExpr))
		}
	}

	downstream := consumer
	if postExpr != nil {
		downstream = &v3exec.Filter{Expr: postExpr, Downstream: consumer}
	}
	scan := &v3exec.Scan{Iterator: it}
	if err := scan.Open(ctx); err != nil {
		return err
	}
	runErr := scan.Run(downstream)
	closeErr := scan.Close()
	if runErr != nil {
		return runErr
	}
	if closeErr == nil && trace != nil {
		trace.addScan(plan, *stats)
	}
	return closeErr
}

func splitScanWhere(expr *v3sql.BoundExpr) (*v3sql.BoundExpr, *v3sql.BoundExpr, error) {
	if expr == nil {
		return nil, nil, nil
	}
	if _, pushed, err := predicateFromBoundExpr(expr); err != nil || pushed {
		if pushed {
			return expr, nil, nil
		}
		return nil, nil, err
	}
	if expr.Kind == v3sql.BoundExprBinary && expr.Op == v3sql.BoundOpAnd && expr.Left != nil && expr.Right != nil {
		leftPush, leftPost, err := splitScanWhere(expr.Left)
		if err != nil {
			return nil, nil, err
		}
		rightPush, rightPost, err := splitScanWhere(expr.Right)
		if err != nil {
			return nil, nil, err
		}
		return andBoundExpr(leftPush, rightPush), andBoundExpr(leftPost, rightPost), nil
	}
	return nil, expr, nil
}

func andBoundExpr(left *v3sql.BoundExpr, right *v3sql.BoundExpr) *v3sql.BoundExpr {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	out := v3sql.BoundExpr{Kind: v3sql.BoundExprBinary, Type: types.Bool, Op: v3sql.BoundOpAnd, Left: left, Right: right}
	return &out
}

func pushBatch(ctx context.Context, batch types.Batch, consumer v3exec.Consumer) error {
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	return pushSelectedBatch(ctx, batch, sel, consumer)
}

func pushSelectedBatch(ctx context.Context, batch types.Batch, sel types.SelectionMask, consumer v3exec.Consumer) error {
	if err := consumer.Open(ctx); err != nil {
		return err
	}
	pushErr := consumer.Push(batch, sel)
	closeErr := consumer.Close()
	if pushErr != nil {
		return pushErr
	}
	return closeErr
}

type selectionCapture struct {
	sel types.SelectionMask
}

func (c *selectionCapture) Open(context.Context) error { return nil }

func (c *selectionCapture) Push(_ types.Batch, sel types.SelectionMask) error {
	c.sel = types.NewSelectionMask(sel.Rows)
	copy(c.sel.Words, sel.Words)
	return nil
}

func (c *selectionCapture) Close() error { return nil }

func filterExpressionSelection(ctx context.Context, batch types.Batch, input types.SelectionMask, expr v3sql.BoundExpr) (types.SelectionMask, error) {
	capture := &selectionCapture{sel: types.NewSelectionMask(batch.Len)}
	filter := &v3exec.Filter{Expr: &expr, Downstream: capture}
	if err := filter.Open(ctx); err != nil {
		return types.SelectionMask{}, err
	}
	pushErr := filter.Push(batch, input)
	closeErr := filter.Close()
	if pushErr != nil {
		return types.SelectionMask{}, pushErr
	}
	return capture.sel, closeErr
}

func havingEvaluationBatch(batch types.Batch, outputs []v3sql.BoundOutput) (types.Batch, error) {
	cols := append([]types.Column(nil), batch.Columns...)
	for _, output := range outputs {
		name, err := checkedBoundOutputName(output)
		if err != nil {
			return types.Batch{}, err
		}
		if _, ok := columnFromList(cols, name); ok {
			continue
		}
		if output.Expr.Kind != v3sql.BoundExprColumn {
			return types.Batch{}, fmt.Errorf("HAVING output alias %q requires a column expression", name)
		}
		col, ok := columnFromList(cols, output.Expr.Column)
		if !ok {
			return types.Batch{}, fmt.Errorf("missing HAVING output column %q", output.Expr.Column)
		}
		col.Name = name
		cols = append(cols, col)
	}
	return types.NewBatchNoClone(cols)
}

func checkedBoundOutputName(output v3sql.BoundOutput) (string, error) {
	if output.Alias != "" {
		return output.Alias, nil
	}
	if output.Expr.Kind == v3sql.BoundExprColumn && output.Expr.Column != "" {
		return output.Expr.Column, nil
	}
	return "", fmt.Errorf("project expression requires an alias")
}

func columnFromList(cols []types.Column, name string) (types.Column, bool) {
	for _, col := range cols {
		if normalizeName(col.Name) == normalizeName(name) {
			return col, true
		}
	}
	return types.Column{}, false
}

func execSortKeys(keys []v3sql.SortKey) ([]v3exec.SortKey, error) {
	out := make([]v3exec.SortKey, 0, len(keys))
	for _, key := range keys {
		name := key.Name
		if name == "" {
			return nil, fmt.Errorf("ORDER BY expression execution is not implemented yet")
		}
		out = append(out, v3exec.SortKey{Column: name, Desc: key.Desc})
	}
	return out, nil
}

func sortNeedsHiddenExpressions(keys []v3sql.SortKey) bool {
	for _, key := range keys {
		if key.Name == "" {
			return true
		}
	}
	return false
}

func projectSortOutputs(outputs []v3sql.BoundOutput, keys []v3sql.SortKey) ([]v3sql.BoundOutput, []v3exec.SortKey, error) {
	extended := append([]v3sql.BoundOutput(nil), outputs...)
	seen := make(map[string]struct{}, len(outputs)+len(keys))
	for _, output := range outputs {
		name, err := checkedBoundOutputName(output)
		if err != nil {
			return nil, nil, err
		}
		seen[normalizeName(name)] = struct{}{}
	}
	sortKeys := make([]v3exec.SortKey, 0, len(keys))
	for i, key := range keys {
		name := key.Name
		if name == "" {
			name = uniqueHiddenSortName(i, seen)
			extended = append(extended, v3sql.BoundOutput{Alias: name, Expr: key.Expr})
		}
		sortKeys = append(sortKeys, v3exec.SortKey{Column: name, Desc: key.Desc})
	}
	return extended, sortKeys, nil
}

func uniqueHiddenSortName(index int, seen map[string]struct{}) string {
	for suffix := index; ; suffix++ {
		name := fmt.Sprintf("__sort_%d", suffix)
		key := normalizeName(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		return name
	}
}

func boundOutputNames(outputs []v3sql.BoundOutput) ([]string, error) {
	names := make([]string, 0, len(outputs))
	for _, output := range outputs {
		name, err := checkedBoundOutputName(output)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, nil
}

func columnSet(columns []string) map[string]struct{} {
	if len(columns) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column != "" {
			out[column] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func removeColumnNames(columns map[string]struct{}, remove []string) {
	if len(columns) == 0 || len(remove) == 0 {
		return
	}
	for _, name := range remove {
		for column := range columns {
			if normalizeName(column) == normalizeName(name) {
				delete(columns, column)
			}
		}
	}
}

func appendMissingColumnNames(columns []string, extra []string) []string {
	if len(extra) == 0 {
		return columns
	}
	seen := make(map[string]struct{}, len(columns)+len(extra))
	for _, column := range columns {
		seen[normalizeName(column)] = struct{}{}
	}
	for _, column := range extra {
		key := normalizeName(column)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		columns = append(columns, column)
	}
	return columns
}

func boundExprColumnNames(expr v3sql.BoundExpr) []string {
	seen := make(map[string]struct{})
	var columns []string
	collectBoundExprColumnNames(expr, seen, &columns)
	return columns
}

func collectBoundExprColumnNames(expr v3sql.BoundExpr, seen map[string]struct{}, columns *[]string) {
	if expr.Kind == v3sql.BoundExprColumn {
		key := normalizeName(expr.Column)
		if key != "" {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				*columns = append(*columns, expr.Column)
			}
		}
	}
	if expr.Left != nil {
		collectBoundExprColumnNames(*expr.Left, seen, columns)
	}
	if expr.Right != nil {
		collectBoundExprColumnNames(*expr.Right, seen, columns)
	}
	for _, arg := range expr.Args {
		collectBoundExprColumnNames(arg, seen, columns)
	}
}

func checkedInt(value int64, label string) (int, error) {
	maxInt := int64(int(^uint(0) >> 1))
	if value > maxInt || value < -1 {
		return 0, fmt.Errorf("%s %d is out of range", label, value)
	}
	return int(value), nil
}

func predicateFromBoundExpr(expr *v3sql.BoundExpr) (storage.Predicate, bool, error) {
	if expr == nil {
		return storage.Predicate{Op: storage.PredicateNone}, true, nil
	}
	switch expr.Kind {
	case v3sql.BoundExprBinary:
		if expr.Left == nil || expr.Right == nil {
			return storage.Predicate{}, false, nil
		}
		switch expr.Op {
		case v3sql.BoundOpAnd, v3sql.BoundOpOr:
			left, leftOK, err := predicateFromBoundExpr(expr.Left)
			if err != nil || !leftOK {
				return storage.Predicate{}, leftOK, err
			}
			right, rightOK, err := predicateFromBoundExpr(expr.Right)
			if err != nil || !rightOK {
				return storage.Predicate{}, rightOK, err
			}
			op := storage.PredicateAnd
			if expr.Op == v3sql.BoundOpOr {
				op = storage.PredicateOr
			}
			return storage.Predicate{Op: op, Children: []storage.Predicate{left, right}}, true, nil
		default:
			return comparisonPredicate(*expr)
		}
	case v3sql.BoundExprUnary:
		if expr.Op != v3sql.BoundOpNot || expr.Left == nil {
			return storage.Predicate{}, false, nil
		}
		child, ok, err := predicateFromBoundExpr(expr.Left)
		if err != nil || !ok {
			return storage.Predicate{}, ok, err
		}
		return storage.Predicate{Op: storage.PredicateNot, Children: []storage.Predicate{child}}, true, nil
	case v3sql.BoundExprBetween:
		return betweenPredicate(*expr)
	case v3sql.BoundExprIn:
		return inPredicate(*expr)
	default:
		return storage.Predicate{}, false, nil
	}
}

func comparisonPredicate(expr v3sql.BoundExpr) (storage.Predicate, bool, error) {
	left, right := *expr.Left, *expr.Right
	op := expr.Op
	if left.Kind == v3sql.BoundExprLiteral && right.Kind == v3sql.BoundExprColumn {
		left, right = right, left
		op = invertComparisonOp(op)
	}
	if left.Kind != v3sql.BoundExprColumn || right.Kind != v3sql.BoundExprLiteral {
		return storage.Predicate{}, false, nil
	}
	predOp, ok := predicateComparisonOp(op)
	if !ok {
		return storage.Predicate{}, false, nil
	}
	litKind, ok := predicateLiteralKindOf(right.Literal)
	if !ok || !predicateLiteralCompatible(left.Type.Kind, litKind) || !predicateOpSupportsLiteralKind(predOp, litKind) {
		return storage.Predicate{}, false, nil
	}
	pred := storage.Predicate{Column: left.Column, Op: predOp}
	if ok := setPredicateLiteral(&pred, left.Type.Kind, right.Literal); !ok {
		return storage.Predicate{}, false, nil
	}
	return pred, true, nil
}

func betweenPredicate(expr v3sql.BoundExpr) (storage.Predicate, bool, error) {
	if expr.Left == nil || expr.Left.Kind != v3sql.BoundExprColumn || len(expr.Args) != 2 {
		return storage.Predicate{}, false, nil
	}
	if !predicateLiteralCompatible(expr.Left.Type.Kind, predicateLiteralInt) {
		return storage.Predicate{}, false, nil
	}
	lo, ok := int64Literal(expr.Args[0].Literal)
	if !ok {
		return storage.Predicate{}, false, nil
	}
	hi, ok := int64Literal(expr.Args[1].Literal)
	if !ok {
		return storage.Predicate{}, false, nil
	}
	return storage.Predicate{Column: expr.Left.Column, Op: storage.PredicateOpBetween, Lo: lo, Hi: hi}, true, nil
}

func inPredicate(expr v3sql.BoundExpr) (storage.Predicate, bool, error) {
	if expr.Left == nil || expr.Left.Kind != v3sql.BoundExprColumn || len(expr.Args) == 0 {
		return storage.Predicate{}, false, nil
	}
	var wantKind predicateLiteralKind
	op := storage.PredicateOpIn
	if expr.Not {
		op = storage.PredicateOpNotIn
	}
	pred := storage.Predicate{Column: expr.Left.Column, Op: op}
	for _, arg := range expr.Args {
		if arg.Kind != v3sql.BoundExprLiteral {
			return storage.Predicate{}, false, nil
		}
		litKind, ok := predicateLiteralKindOf(arg.Literal)
		if !ok || !predicateLiteralCompatible(expr.Left.Type.Kind, litKind) {
			return storage.Predicate{}, false, nil
		}
		if wantKind == 0 {
			wantKind = litKind
		} else if wantKind != litKind {
			return storage.Predicate{}, false, nil
		}
		switch value := arg.Literal.(type) {
		case bool:
			if len(pred.Int64s) != 0 || len(pred.Texts) != 0 || len(pred.UUIDs) != 0 {
				return storage.Predicate{}, false, nil
			}
			pred.Bools = append(pred.Bools, value)
		case int16:
			if len(pred.Bools) != 0 || len(pred.Texts) != 0 || len(pred.UUIDs) != 0 {
				return storage.Predicate{}, false, nil
			}
			pred.Int64s = append(pred.Int64s, int64(value))
		case int32:
			if len(pred.Bools) != 0 || len(pred.Texts) != 0 || len(pred.UUIDs) != 0 {
				return storage.Predicate{}, false, nil
			}
			pred.Int64s = append(pred.Int64s, int64(value))
		case int64:
			if len(pred.Bools) != 0 || len(pred.Texts) != 0 || len(pred.UUIDs) != 0 {
				return storage.Predicate{}, false, nil
			}
			pred.Int64s = append(pred.Int64s, value)
		case string:
			if len(pred.Bools) != 0 || len(pred.Int64s) != 0 {
				return storage.Predicate{}, false, nil
			}
			if expr.Left.Type.Kind == types.KindUUID {
				parsed, err := types.ParseUUID(value)
				if err != nil {
					return storage.Predicate{}, false, nil
				}
				pred.UUIDs = append(pred.UUIDs, parsed)
				continue
			}
			if len(pred.UUIDs) != 0 {
				return storage.Predicate{}, false, nil
			}
			pred.Texts = append(pred.Texts, value)
		default:
			return storage.Predicate{}, false, nil
		}
	}
	return pred, true, nil
}

func predicateComparisonOp(op v3sql.BoundOp) (storage.PredicateOp, bool) {
	switch op {
	case v3sql.BoundOpEqual:
		return storage.PredicateOpEq, true
	case v3sql.BoundOpNotEqual:
		return storage.PredicateOpNotEq, true
	case v3sql.BoundOpLess:
		return storage.PredicateOpLess, true
	case v3sql.BoundOpLessEqual:
		return storage.PredicateOpLessEqual, true
	case v3sql.BoundOpGreater:
		return storage.PredicateOpGreater, true
	case v3sql.BoundOpGreaterEqual:
		return storage.PredicateOpGreaterEqual, true
	default:
		return storage.PredicateNone, false
	}
}

func invertComparisonOp(op v3sql.BoundOp) v3sql.BoundOp {
	switch op {
	case v3sql.BoundOpLess:
		return v3sql.BoundOpGreater
	case v3sql.BoundOpLessEqual:
		return v3sql.BoundOpGreaterEqual
	case v3sql.BoundOpGreater:
		return v3sql.BoundOpLess
	case v3sql.BoundOpGreaterEqual:
		return v3sql.BoundOpLessEqual
	default:
		return op
	}
}

func setPredicateLiteral(pred *storage.Predicate, columnKind types.Kind, value any) bool {
	switch value := value.(type) {
	case bool:
		pred.Bool = value
		return true
	case int16:
		pred.Int64 = int64(value)
		return true
	case int32:
		pred.Int64 = int64(value)
		return true
	case int64:
		pred.Int64 = value
		return true
	case string:
		if columnKind == types.KindUUID {
			parsed, err := types.ParseUUID(value)
			if err != nil {
				return false
			}
			pred.UUID = parsed
			return true
		}
		pred.Text = value
		return true
	default:
		return false
	}
}

type predicateLiteralKind uint8

const (
	predicateLiteralBool predicateLiteralKind = iota + 1
	predicateLiteralInt
	predicateLiteralText
)

func predicateLiteralKindOf(value any) (predicateLiteralKind, bool) {
	switch value.(type) {
	case bool:
		return predicateLiteralBool, true
	case int16, int32, int64:
		return predicateLiteralInt, true
	case string:
		return predicateLiteralText, true
	default:
		return 0, false
	}
}

func predicateLiteralCompatible(column types.Kind, literal predicateLiteralKind) bool {
	switch literal {
	case predicateLiteralBool:
		return column == types.KindBool
	case predicateLiteralInt:
		switch column {
		case types.KindInt16, types.KindInt32, types.KindInt64, types.KindDate, types.KindTimestamp, types.KindTime:
			return true
		default:
			return false
		}
	case predicateLiteralText:
		return column == types.KindText || column == types.KindBytes || column == types.KindUUID
	default:
		return false
	}
}

func predicateOpSupportsLiteralKind(op storage.PredicateOp, literal predicateLiteralKind) bool {
	switch literal {
	case predicateLiteralBool, predicateLiteralText:
		return op == storage.PredicateOpEq || op == storage.PredicateOpNotEq
	case predicateLiteralInt:
		return true
	default:
		return false
	}
}

func int64Literal(value any) (int64, bool) {
	switch value := value.(type) {
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	default:
		return 0, false
	}
}

func (db *DB) aggregateBatch(ctx context.Context, plan *v3sql.AggregatePlan, trace *executionTrace) (types.Batch, error) {
	if len(plan.GroupBy) != 0 {
		return db.groupedAggregateBatch(ctx, plan, trace)
	}
	specs := append([]v3sql.AggSpec{}, plan.Aggregates...)
	specs = append(specs, plan.Hidden...)
	if batch, ok, err := db.scalarMetadataBatch(ctx, plan.Source, specs, trace); err != nil || ok {
		return batch, err
	}
	if batch, ok, err := db.countStarMetadataBatch(ctx, plan.Source, specs, trace); err != nil || ok {
		return batch, err
	}
	if batch, ok, err := db.sumMetadataBatch(ctx, plan.Source, specs, trace); err != nil || ok {
		return batch, err
	}
	if batch, ok, err := db.parallelScalarAggregateBatch(ctx, plan.Source, specs, trace); err != nil || ok {
		return batch, err
	}
	sinks, err := db.aggregateSinks(plan.Source, specs)
	if err != nil {
		return types.Batch{}, err
	}
	agg := &v3exec.Aggregate{Sinks: sinks}
	if err := db.runSource(ctx, plan.Source, agg, trace); err != nil {
		return types.Batch{}, err
	}
	return scalarAggregateBatch(specs, sinks)
}

type metadataAggState struct {
	count int64
	sum   int64
	value int64
	set   bool
}

// scanSegmentsForTable resolves the table entry and returns its segments.
// ScanSegments includes any unsealed buffer rows as a virtual in-memory
// segment, so callers do not need to FlushBuffered to see fresh data.
// Metadata fast-paths must bail when a returned segment has Path == ""
// (in-memory) since segment.Meta carries no per-page stats for buffered rows.
func (db *DB) scanSegmentsForTable(ctx context.Context, tableName string) ([]storage.ScanSegment, types.TableSpec, error) {
	entry, ok := db.table(tableName)
	if !ok {
		return nil, types.TableSpec{}, fmt.Errorf("table %q does not exist", tableName)
	}
	segments, err := db.data.ScanSegments(ctx, entry.spec)
	if err != nil {
		return nil, entry.spec, err
	}
	return segments, entry.spec, nil
}

// segmentsContainBuffer reports whether any segment is the in-memory buffer
// (no path, just unsealed pages). Metadata fast-paths use this to bail and
// let the regular scan path consume buffered rows.
func segmentsContainBuffer(segments []storage.ScanSegment) bool {
	for i := range segments {
		if segments[i].Path == "" {
			return true
		}
	}
	return false
}

// metadataPruneAllPages runs the segment/page prune loop for predicate-driven
// metadata fast-paths. Returns ok=true when every page was pruned (so the
// fast-path can answer the aggregate from metadata alone), ok=false when at
// least one page survives and the caller must fall back to a scan.
func metadataPruneAllPages(segments []storage.ScanSegment, pred storage.Predicate, stats *storage.ExecStats) bool {
	for _, segment := range segments {
		prune := storage.BindPrunePredicate(pred, segment.Meta)
		if !prune.SegmentCandidate() {
			stats.ObserveSegment(false)
			observeMetadataSkippedPages(stats, segment.PageInfos)
			continue
		}
		stats.ObserveSegment(true)
		for pageIndex, info := range segment.PageInfos {
			if prune.PageCandidate(pageIndex) {
				return false
			}
			stats.ObservePage(int(info.Rows), 0, 0, false)
		}
	}
	return true
}

func (db *DB) scalarMetadataBatch(ctx context.Context, source v3sql.Plan, specs []v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	if len(specs) == 0 {
		return types.Batch{}, false, nil
	}
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok || scan.Where != nil {
		return types.Batch{}, false, nil
	}
	for _, spec := range specs {
		if !scalarMetadataEligible(source, spec) {
			return types.Batch{}, false, nil
		}
	}
	segments, _, err := db.scanSegmentsForTable(ctx, scan.Table.Name)
	if err != nil {
		return types.Batch{}, false, err
	}
	if segmentsContainBuffer(segments) {
		return types.Batch{}, false, nil
	}

	states := make([]metadataAggState, len(specs))
	stats := storage.ExecStats{}
	for _, segment := range segments {
		stats.ObserveSegment(true)
		for _, info := range segment.PageInfos {
			rows := int(info.Rows)
			stats.ObservePage(rows, 0, rows, true)
		}
		for i, spec := range specs {
			ok, err := updateMetadataAggState(&states[i], spec, segment.Meta)
			if err != nil || !ok {
				return types.Batch{}, ok, err
			}
		}
	}
	sinks, err := metadataAggregateSinks(source, specs, states)
	if err != nil {
		return types.Batch{}, true, err
	}
	trace.recordScan(scan, stats)
	batch, err := scalarAggregateBatch(specs, sinks)
	return batch, true, err
}

func scalarMetadataEligible(source v3sql.Plan, spec v3sql.AggSpec) bool {
	switch spec.Func {
	case v3sql.AggregateCount:
		if spec.Star {
			return true
		}
		_, ok := sourceColumnType(source, spec.ArgColumn, spec.ArgName)
		return ok
	case v3sql.AggregateSum, v3sql.AggregateMin, v3sql.AggregateMax:
		typ, ok := sourceColumnType(source, spec.ArgColumn, spec.ArgName)
		return ok && (typ.Kind == types.KindInt32 || typ.Kind == types.KindInt64)
	default:
		return false
	}
}

func updateMetadataAggState(state *metadataAggState, spec v3sql.AggSpec, meta storage.SegmentMeta) (bool, error) {
	switch spec.Func {
	case v3sql.AggregateCount:
		if spec.Star {
			state.count += int64(meta.Rows)
			return true, nil
		}
		col, ok := segmentColumnMeta(meta, spec.ArgName)
		if !ok {
			return false, nil
		}
		state.count += int64(col.Rows - col.NullCount)
		return true, nil
	case v3sql.AggregateSum:
		col, ok := segmentColumnMeta(meta, spec.ArgName)
		if !ok {
			return false, nil
		}
		numeric, ok := numericColumnStatsFromMeta(col)
		if !ok || !numeric.sumValid {
			return false, nil
		}
		next, ok := v3exec.AddInt64(state.sum, numeric.sum)
		if !ok {
			return true, v3exec.ErrSumOverflow
		}
		state.sum = next
		state.count += int64(col.Rows - col.NullCount)
		return true, nil
	case v3sql.AggregateMin, v3sql.AggregateMax:
		col, ok := segmentColumnMeta(meta, spec.ArgName)
		if !ok {
			return false, nil
		}
		numeric, ok := numericColumnStatsFromMeta(col)
		if !ok {
			return false, nil
		}
		if !numeric.set {
			return true, nil
		}
		if !state.set || (spec.Func == v3sql.AggregateMin && numeric.min < state.value) || (spec.Func == v3sql.AggregateMax && numeric.max > state.value) {
			if spec.Func == v3sql.AggregateMin {
				state.value = numeric.min
			} else {
				state.value = numeric.max
			}
			state.set = true
		}
		return true, nil
	default:
		return false, nil
	}
}

func metadataAggregateSinks(source v3sql.Plan, specs []v3sql.AggSpec, states []metadataAggState) ([]v3exec.AggregateSink, error) {
	sinks := make([]v3exec.AggregateSink, len(specs))
	for i, spec := range specs {
		state := states[i]
		switch spec.Func {
		case v3sql.AggregateCount:
			sinks[i] = &v3exec.CountSink{N: state.count}
		case v3sql.AggregateSum:
			typ, ok := sourceColumnType(source, spec.ArgColumn, spec.ArgName)
			if !ok {
				return nil, fmt.Errorf("missing SUM column %q", spec.ArgName)
			}
			if typ.Kind == types.KindInt32 {
				sinks[i] = &v3exec.SumInt32Sink{Sum: state.sum, Count: state.count}
			} else {
				sinks[i] = &v3exec.SumInt64Sink{Sum: state.sum, Count: state.count}
			}
		case v3sql.AggregateMin:
			sinks[i] = &v3exec.MinInt64Sink{Value: state.value, Set: state.set}
		case v3sql.AggregateMax:
			sinks[i] = &v3exec.MaxInt64Sink{Value: state.value, Set: state.set}
		default:
			return nil, fmt.Errorf("unsupported aggregate %d", spec.Func)
		}
	}
	return sinks, nil
}

func (db *DB) countStarMetadataBatch(ctx context.Context, source v3sql.Plan, specs []v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	if len(specs) != 1 || specs[0].Func != v3sql.AggregateCount || !specs[0].Star {
		return types.Batch{}, false, nil
	}
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok {
		return types.Batch{}, false, nil
	}
	pred, pushed, err := predicateFromBoundExpr(scan.Where)
	if err != nil || !pushed {
		return types.Batch{}, false, err
	}
	segments, _, err := db.scanSegmentsForTable(ctx, scan.Table.Name)
	if err != nil {
		return types.Batch{}, false, err
	}
	if segmentsContainBuffer(segments) {
		return types.Batch{}, false, nil
	}

	var count int64
	stats := storage.ExecStats{}
	if scan.Where == nil {
		for _, segment := range segments {
			stats.ObserveSegment(true)
			for _, info := range segment.PageInfos {
				rows := int(info.Rows)
				count += int64(rows)
				stats.ObservePage(rows, 0, rows, true)
			}
		}
		trace.recordScan(scan, stats)
		batch, err := scalarAggregateBatch(specs, []v3exec.AggregateSink{&v3exec.CountSink{N: count}})
		return batch, true, err
	}
	if count, metadataStats, ok := countStarTextPredicateMetadata(segments, pred); ok {
		trace.recordScan(scan, metadataStats)
		batch, err := scalarAggregateBatch(specs, []v3exec.AggregateSink{&v3exec.CountSink{N: count}})
		return batch, true, err
	}

	if !metadataPruneAllPages(segments, pred, &stats) {
		return types.Batch{}, false, nil
	}
	trace.recordScan(scan, stats)
	batch, err := scalarAggregateBatch(specs, []v3exec.AggregateSink{&v3exec.CountSink{N: count}})
	return batch, true, err
}

func observeMetadataSkippedPages(stats *storage.ExecStats, infos []storage.SegmentPageInfo) {
	for _, info := range infos {
		stats.ObservePage(int(info.Rows), 0, 0, false)
	}
}

func (db *DB) sumMetadataBatch(ctx context.Context, source v3sql.Plan, specs []v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	if len(specs) != 1 || specs[0].Func != v3sql.AggregateSum {
		return types.Batch{}, false, nil
	}
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok {
		return types.Batch{}, false, nil
	}
	typ, ok := sourceColumnType(source, specs[0].ArgColumn, specs[0].ArgName)
	if !ok || (typ.Kind != types.KindInt32 && typ.Kind != types.KindInt64) {
		return types.Batch{}, false, nil
	}
	pred, pushed, err := predicateFromBoundExpr(scan.Where)
	if err != nil || !pushed {
		return types.Batch{}, false, err
	}
	segments, _, err := db.scanSegmentsForTable(ctx, scan.Table.Name)
	if err != nil {
		return types.Batch{}, false, err
	}
	if segmentsContainBuffer(segments) {
		return types.Batch{}, false, nil
	}

	var sum int64
	var count int64
	stats := storage.ExecStats{}
	if scan.Where == nil {
		for _, segment := range segments {
			colStats, ok := numericColumnStats(segment.Meta, specs[0].ArgName)
			if !ok || !colStats.sumValid {
				return types.Batch{}, false, nil
			}
			if next, ok := v3exec.AddInt64(sum, colStats.sum); ok {
				sum = next
			} else {
				return types.Batch{}, false, nil
			}
			stats.ObserveSegment(true)
			for _, info := range segment.PageInfos {
				rows := int(info.Rows)
				count += int64(rows)
				stats.ObservePage(rows, 0, rows, true)
			}
		}
		trace.recordScan(scan, stats)
		batch, err := scalarAggregateBatch(specs, []v3exec.AggregateSink{&v3exec.SumInt64Sink{Sum: sum, Count: count}})
		return batch, true, err
	}

	if !metadataPruneAllPages(segments, pred, &stats) {
		return types.Batch{}, false, nil
	}
	trace.recordScan(scan, stats)
	batch, err := scalarAggregateBatch(specs, []v3exec.AggregateSink{&v3exec.SumInt64Sink{}})
	return batch, true, err
}

func (db *DB) parallelScalarAggregateBatch(ctx context.Context, source v3sql.Plan, specs []v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok || len(specs) == 0 {
		return types.Batch{}, false, nil
	}
	pushExpr, postExpr, err := splitScanWhere(scan.Where)
	if err != nil {
		return types.Batch{}, false, err
	}
	if postExpr != nil {
		return types.Batch{}, false, nil
	}
	entry, ok := db.table(scan.Table.Name)
	if !ok {
		return types.Batch{}, false, fmt.Errorf("table %q does not exist", scan.Table.Name)
	}
	baseStats := &storage.ExecStats{}
	var base storage.SegmentScanIterator
	var pushPred storage.Predicate
	hasPushPred := false
	if pushExpr == nil {
		base, err = db.data.ScanIterator(ctx, entry.spec, nil, baseStats)
	} else {
		pred, pushed, err := predicateFromBoundExpr(pushExpr)
		if err != nil {
			return types.Batch{}, false, err
		}
		if !pushed {
			return types.Batch{}, false, nil
		}
		pushPred = pred
		hasPushPred = true
		base, err = db.data.ScanIteratorForPredicate(ctx, entry.spec, pred, baseStats)
	}
	if err != nil {
		return types.Batch{}, false, err
	}
	if len(base.Segments) < 2 {
		return types.Batch{}, false, nil
	}
	master, err := db.aggregateSinks(source, specs)
	if err != nil {
		return types.Batch{}, false, err
	}
	base.OutputColumns = columnNamesForIDs(scan.Table, scan.Columns)
	base.EncodedOutputColumns = columnSet((&v3exec.Aggregate{Sinks: master}).EncodedColumns())

	workers := min(len(base.Segments), runtime.GOMAXPROCS(0))
	if workers < 2 {
		return types.Batch{}, false, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerSinks := make([][]v3exec.AggregateSink, workers)
	workerStats := make([]storage.ExecStats, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		start := worker * len(base.Segments) / workers
		end := (worker + 1) * len(base.Segments) / workers
		if start == end {
			continue
		}
		sinks, err := db.aggregateSinks(source, specs)
		if err != nil {
			return types.Batch{}, false, err
		}
		workerSinks[worker] = sinks
		it := base
		it.Context = ctx
		it.Segments = base.Segments[start:end]
		it.Stats = &workerStats[worker]
		if hasPushPred {
			it.Predicate = storage.NewPredicateEvaluator(pushPred)
		}
		wg.Add(1)
		go func(it storage.SegmentScanIterator, sinks []v3exec.AggregateSink) {
			defer wg.Done()
			if err := consumeAggregateIterator(it, sinks); err != nil {
				cancel()
				errCh <- err
			}
		}(it, sinks)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return types.Batch{}, true, err
		}
	}
	mergedStats := storage.ExecStats{}
	for worker := range workerSinks {
		if workerSinks[worker] == nil {
			continue
		}
		if err := mergeAggregateSinks(master, workerSinks[worker], specs); err != nil {
			return types.Batch{}, true, err
		}
		mergeExecStats(&mergedStats, workerStats[worker])
	}
	trace.recordScan(scan, mergedStats)
	batch, err := scalarAggregateBatch(specs, master)
	return batch, true, err
}

func consumeAggregateIterator(it storage.SegmentScanIterator, sinks []v3exec.AggregateSink) error {
	return it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		for _, sink := range sinks {
			if err := sink.Consume(batch, sel); err != nil {
				return err
			}
		}
		return nil
	})
}

func mergeExecStats(dst *storage.ExecStats, src storage.ExecStats) {
	dst.SegmentsTotal += src.SegmentsTotal
	dst.SegmentsCandidate += src.SegmentsCandidate
	dst.PagesTotal += src.PagesTotal
	dst.PagesCandidate += src.PagesCandidate
	dst.RowsTotal += src.RowsTotal
	dst.RowsCandidate += src.RowsCandidate
	dst.RowsMatched += src.RowsMatched
	dst.PayloadBytesRead += src.PayloadBytesRead
}

type numericStats struct {
	min      int64
	max      int64
	sum      int64
	sumValid bool
	set      bool
}

func numericColumnStats(meta storage.SegmentMeta, column string) (numericStats, bool) {
	for _, col := range meta.Columns {
		if normalizeName(col.Name) != normalizeName(column) {
			continue
		}
		return numericColumnStatsFromMeta(col)
	}
	return numericStats{}, false
}

func segmentColumnMeta(meta storage.SegmentMeta, column string) (storage.ColumnMeta, bool) {
	for _, col := range meta.Columns {
		if normalizeName(col.Name) == normalizeName(column) {
			return col, true
		}
	}
	return storage.ColumnMeta{}, false
}

func numericColumnStatsFromMeta(col storage.ColumnMeta) (numericStats, bool) {
	if col.Int64 != nil {
		return numericStats{min: col.Int64.Min, max: col.Int64.Max, sum: col.Int64.Sum, sumValid: col.Int64.SumValid, set: true}, true
	}
	if col.Int32 != nil {
		return numericStats{min: int64(col.Int32.Min), max: int64(col.Int32.Max), sum: col.Int32.Sum, sumValid: col.Int32.SumValid, set: true}, true
	}
	if col.AllNull {
		return numericStats{sumValid: true}, true
	}
	return numericStats{}, false
}

func (db *DB) groupedAggregateBatch(ctx context.Context, plan *v3sql.AggregatePlan, trace *executionTrace) (types.Batch, error) {
	if len(plan.GroupBy) != 1 || len(plan.Aggregates) == 0 {
		return types.Batch{}, fmt.Errorf("GROUP BY execution requires one group key and at least one visible aggregate")
	}
	group := plan.GroupBy[0]
	if group.Kind != v3sql.BoundExprColumn {
		return types.Batch{}, fmt.Errorf("GROUP BY execution currently supports column GROUP BY")
	}
	if batch, ok, err := db.groupedTextCountSumBatch(ctx, plan, trace); err != nil || ok {
		return batch, err
	}
	if len(plan.Aggregates) != 1 {
		return db.groupedGenericAggregateBatch(ctx, plan, trace)
	}
	aggSpec := plan.Aggregates[0]
	if aggSpec.Func != v3sql.AggregateCount || !aggSpec.Star || len(plan.Hidden) != 0 {
		return db.groupedGenericAggregateBatch(ctx, plan, trace)
	}
	if batch, ok, err := db.groupedTextCountMetadataBatch(ctx, plan.Source, group, aggSpec, trace); err != nil || ok {
		return batch, err
	}
	if batch, ok, err := db.parallelGroupedCountBatch(ctx, plan.Source, group, aggSpec, trace); err != nil || ok {
		return batch, err
	}
	if group.Type.Kind != types.KindText {
		return db.groupAnyCountBatch(ctx, plan, group, aggSpec, trace)
	}
	sink := &v3exec.GroupStringCountSink{Column: group.Column}
	agg := &v3exec.Aggregate{Sinks: []v3exec.AggregateSink{sink}}
	if err := db.runSource(ctx, plan.Source, agg, trace); err != nil {
		return types.Batch{}, err
	}
	result, err := sink.Result()
	if err != nil {
		return types.Batch{}, err
	}
	return groupStringCountBatch(group, aggSpec, result.(map[string]int64))
}

type textGroupCountSumState struct {
	count int64
	sum   int64
}

type textGroupCountSumCollector struct {
	group  string
	sumCol string
	groups map[string]textGroupCountSumState
}

func (db *DB) groupedTextCountSumBatch(ctx context.Context, plan *v3sql.AggregatePlan, trace *executionTrace) (types.Batch, bool, error) {
	if len(plan.GroupBy) != 1 || len(plan.Aggregates) != 2 || len(plan.Hidden) != 0 {
		return types.Batch{}, false, nil
	}
	group := plan.GroupBy[0]
	if group.Kind != v3sql.BoundExprColumn || group.Type.Kind != types.KindText {
		return types.Batch{}, false, nil
	}
	countIndex, sumIndex := -1, -1
	for i, spec := range plan.Aggregates {
		switch spec.Func {
		case v3sql.AggregateCount:
			if !spec.Star {
				return types.Batch{}, false, nil
			}
			countIndex = i
		case v3sql.AggregateSum:
			typ, ok := sourceColumnType(plan.Source, spec.ArgColumn, spec.ArgName)
			if !ok || (typ.Kind != types.KindInt32 && typ.Kind != types.KindInt64) {
				return types.Batch{}, false, nil
			}
			sumIndex = i
		default:
			return types.Batch{}, false, nil
		}
	}
	if countIndex < 0 || sumIndex < 0 {
		return types.Batch{}, false, nil
	}
	collector := &textGroupCountSumCollector{group: group.Column, sumCol: plan.Aggregates[sumIndex].ArgName, groups: make(map[string]textGroupCountSumState)}
	if err := db.runSource(ctx, plan.Source, collector, trace); err != nil {
		return types.Batch{}, true, err
	}
	batch, err := groupTextCountSumBatch(group, plan.Aggregates, collector.groups)
	return batch, true, err
}

func (c *textGroupCountSumCollector) Open(context.Context) error { return nil }

func (c *textGroupCountSumCollector) Push(batch types.Batch, sel types.SelectionMask) error {
	if sel.Rows != batch.Len {
		return fmt.Errorf("selection rows %d do not match batch length %d", sel.Rows, batch.Len)
	}
	groupCol, ok := columnFromList(batch.Columns, c.group)
	if !ok {
		return fmt.Errorf("missing GROUP BY column %q", c.group)
	}
	sumCol, ok := columnFromList(batch.Columns, c.sumCol)
	if !ok {
		return fmt.Errorf("missing aggregate column %q", c.sumCol)
	}
	if groupCol.V.Kind != types.VecText {
		return fmt.Errorf("GROUP BY column %q has kind %s, want text", groupCol.Name, groupCol.V.Kind)
	}
	switch groupCol.V.Encoding {
	case types.EncodingDictionary:
		return c.pushDict(batch.Len, groupCol.V, sumCol.V, sel)
	case types.EncodingFlat:
		return c.pushFlat(batch.Len, groupCol.V, sumCol.V, sel)
	default:
		return fmt.Errorf("GROUP BY column %q has unsupported text encoding %s", groupCol.Name, groupCol.V.Encoding)
	}
}

func (c *textGroupCountSumCollector) pushDict(rows int, group types.Vec, sum types.Vec, sel types.SelectionMask) error {
	if len(group.DictIDs) < rows {
		return fmt.Errorf("dictionary ids length %d is shorter than rows %d", len(group.DictIDs), rows)
	}
	if sum.Kind != types.VecInt32 && sum.Kind != types.VecInt64 {
		return fmt.Errorf("aggregate column %q has kind %s, want int32 or int64", c.sumCol, sum.Kind)
	}
	dictRows := group.DictValues.Rows()
	if dictRows > 256 {
		return fmt.Errorf("dictionary group has %d values, max 256", dictRows)
	}
	var counts [256]int64
	var sums [256]int64
	addRow := func(row int) error {
		if !types.IsValid(group.Valid, row) {
			return nil
		}
		id := int(group.DictIDs[row])
		if id >= dictRows {
			return fmt.Errorf("dictionary id %d exceeds dictionary size %d", id, dictRows)
		}
		counts[id]++
		if !types.IsValid(sum.Valid, row) {
			return nil
		}
		var value int64
		if sum.Kind == types.VecInt32 {
			value = int64(sum.I32[row])
		} else {
			value = sum.I64[row]
		}
		next, ok := v3exec.AddInt64(sums[id], value)
		if !ok {
			return v3exec.ErrSumOverflow
		}
		sums[id] = next
		return nil
	}
	if selectionAllRows(sel) {
		for row := 0; row < rows; row++ {
			if err := addRow(row); err != nil {
				return err
			}
		}
	} else {
		var pushErr error
		sel.IterSet(func(row int) {
			if pushErr != nil {
				return
			}
			pushErr = addRow(row)
		})
		if pushErr != nil {
			return pushErr
		}
	}
	for id := 0; id < dictRows; id++ {
		if counts[id] == 0 {
			continue
		}
		key := group.DictValues.String(id)
		state, ok := c.groups[key]
		if !ok {
			key = group.DictValues.StringCopy(id)
		}
		if err := mergeTextGroupCountSum(c.groups, key, state, counts[id], sums[id]); err != nil {
			return err
		}
	}
	return nil
}

func (c *textGroupCountSumCollector) pushFlat(rows int, group types.Vec, sum types.Vec, sel types.SelectionMask) error {
	if sum.Kind != types.VecInt32 && sum.Kind != types.VecInt64 {
		return fmt.Errorf("aggregate column %q has kind %s, want int32 or int64", c.sumCol, sum.Kind)
	}
	addRow := func(row int) error {
		if !types.IsValid(group.Valid, row) {
			return nil
		}
		key := group.Var.String(row)
		state, ok := c.groups[key]
		if !ok {
			key = group.Var.StringCopy(row)
		}
		var rowSum int64
		if types.IsValid(sum.Valid, row) {
			if sum.Kind == types.VecInt32 {
				rowSum = int64(sum.I32[row])
			} else {
				rowSum = sum.I64[row]
			}
		}
		return mergeTextGroupCountSum(c.groups, key, state, 1, rowSum)
	}
	if selectionAllRows(sel) {
		for row := 0; row < rows; row++ {
			if err := addRow(row); err != nil {
				return err
			}
		}
		return nil
	}
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		pushErr = addRow(row)
	})
	return pushErr
}

func (c *textGroupCountSumCollector) Close() error { return nil }

func mergeTextGroupCountSum(groups map[string]textGroupCountSumState, key string, state textGroupCountSumState, count int64, sum int64) error {
	next, ok := v3exec.AddInt64(state.sum, sum)
	if !ok {
		return v3exec.ErrSumOverflow
	}
	state.count += count
	state.sum = next
	groups[key] = state
	return nil
}

func selectionAllRows(sel types.SelectionMask) bool {
	return sel.Rows == 0 || sel.PopCount() == sel.Rows
}

func groupTextCountSumBatch(group v3sql.BoundExpr, specs []v3sql.AggSpec, groups map[string]textGroupCountSumState) (types.Batch, error) {
	keys := make([]string, 0, len(groups))
	dataBytes := 0
	for key := range groups {
		keys = append(keys, key)
		dataBytes += len(key)
	}
	sort.Strings(keys)

	groupValues := types.NewVarBytes(len(keys), dataBytes)
	cols := make([]types.Column, 0, len(specs)+1)
	for i, key := range keys {
		groupValues.AppendString(i, key)
	}
	cols = append(cols, types.Column{Name: group.Column, Type: group.Type, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(keys), Var: groupValues}})
	for _, spec := range specs {
		values := make([]int64, len(keys))
		for i, key := range keys {
			state := groups[key]
			if spec.Func == v3sql.AggregateCount {
				values[i] = state.count
			} else {
				values[i] = state.sum
			}
		}
		cols = append(cols, int64Column(aggregateOutputName(spec), values, nil))
	}
	return types.NewBatch(cols)
}

func countStarTextPredicateMetadata(segments []storage.ScanSegment, pred storage.Predicate) (int64, storage.ExecStats, bool) {
	if pred.Op != storage.PredicateOpEq && pred.Op != storage.PredicateOpIn {
		return 0, storage.ExecStats{}, false
	}
	var count int64
	stats := storage.ExecStats{}
	for _, segment := range segments {
		colIndex := textStatsColumnIndex(segment.Meta, pred.Column)
		if colIndex < 0 {
			return 0, storage.ExecStats{}, false
		}
		segmentCount, ok := textStatsPredicateCount(segment.Meta.Columns[colIndex].Text, pred)
		if !ok {
			return 0, storage.ExecStats{}, false
		}
		stats.ObserveSegment(segmentCount != 0)
		for pageIndex, info := range segment.PageInfos {
			pageCount, ok := textStatsPredicateCount(segment.Meta.Columns[colIndex].Pages[pageIndex].Text, pred)
			if !ok {
				return 0, storage.ExecStats{}, false
			}
			stats.ObservePage(int(info.Rows), 0, int(pageCount), pageCount != 0)
		}
		count += segmentCount
	}
	return count, stats, true
}

func (db *DB) groupedTextCountMetadataBatch(ctx context.Context, source v3sql.Plan, group v3sql.BoundExpr, spec v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok || scan.Where != nil || group.Kind != v3sql.BoundExprColumn || group.Type.Kind != types.KindText || spec.Func != v3sql.AggregateCount || !spec.Star {
		return types.Batch{}, false, nil
	}
	segments, _, err := db.scanSegmentsForTable(ctx, scan.Table.Name)
	if err != nil {
		return types.Batch{}, false, err
	}
	if segmentsContainBuffer(segments) {
		return types.Batch{}, false, nil
	}
	counts := make(map[string]int64)
	stats := storage.ExecStats{}
	for _, segment := range segments {
		colIndex := textStatsColumnIndex(segment.Meta, group.Column)
		if colIndex < 0 {
			return types.Batch{}, false, nil
		}
		if !mergeTextStatsCounts(counts, segment.Meta.Columns[colIndex].Text) {
			return types.Batch{}, false, nil
		}
		stats.ObserveSegment(true)
		for _, info := range segment.PageInfos {
			stats.ObservePage(int(info.Rows), 0, int(info.Rows), true)
		}
	}
	trace.recordScan(scan, stats)
	batch, err := groupStringCountBatch(group, spec, counts)
	return batch, true, err
}

func textStatsColumnIndex(meta storage.SegmentMeta, column string) int {
	for i, col := range meta.Columns {
		if normalizeName(col.Name) == normalizeName(column) && col.Type.Kind == types.KindText {
			return i
		}
	}
	return -1
}

func textStatsPredicateCount(stats *storage.TextStats, pred storage.Predicate) (int64, bool) {
	if !textStatsExact(stats) {
		return 0, false
	}
	switch pred.Op {
	case storage.PredicateOpEq:
		for i, value := range stats.Values {
			if value == pred.Text {
				return int64(stats.Counts[i]), true
			}
		}
		return 0, true
	case storage.PredicateOpIn:
		want := make(map[string]struct{}, len(pred.Texts))
		for _, value := range pred.Texts {
			want[value] = struct{}{}
		}
		var count int64
		for i, value := range stats.Values {
			if _, ok := want[value]; ok {
				count += int64(stats.Counts[i])
			}
		}
		return count, true
	default:
		return 0, false
	}
}

func mergeTextStatsCounts(dst map[string]int64, stats *storage.TextStats) bool {
	if !textStatsExact(stats) {
		return false
	}
	for i, value := range stats.Values {
		dst[value] += int64(stats.Counts[i])
	}
	return true
}

func textStatsExact(stats *storage.TextStats) bool {
	return stats != nil && !stats.Truncated && len(stats.Values) == len(stats.Counts)
}

func (db *DB) parallelGroupedCountBatch(ctx context.Context, source v3sql.Plan, group v3sql.BoundExpr, spec v3sql.AggSpec, trace *executionTrace) (types.Batch, bool, error) {
	scan, ok := source.(*v3sql.ScanPlan)
	if !ok || group.Kind != v3sql.BoundExprColumn || spec.Func != v3sql.AggregateCount || !spec.Star {
		return types.Batch{}, false, nil
	}
	pushExpr, postExpr, err := splitScanWhere(scan.Where)
	if err != nil {
		return types.Batch{}, false, err
	}
	if postExpr != nil {
		return types.Batch{}, false, nil
	}
	entry, ok := db.table(scan.Table.Name)
	if !ok {
		return types.Batch{}, false, fmt.Errorf("table %q does not exist", scan.Table.Name)
	}
	baseStats := &storage.ExecStats{}
	var base storage.SegmentScanIterator
	var pushPred storage.Predicate
	hasPushPred := false
	if pushExpr == nil {
		base, err = db.data.ScanIterator(ctx, entry.spec, nil, baseStats)
	} else {
		pred, pushed, err := predicateFromBoundExpr(pushExpr)
		if err != nil {
			return types.Batch{}, false, err
		}
		if !pushed {
			return types.Batch{}, false, nil
		}
		pushPred = pred
		hasPushPred = true
		base, err = db.data.ScanIteratorForPredicate(ctx, entry.spec, pred, baseStats)
	}
	if err != nil {
		return types.Batch{}, false, err
	}
	if len(base.Segments) < 2 {
		return types.Batch{}, false, nil
	}
	workers := min(len(base.Segments), runtime.GOMAXPROCS(0))
	if workers < 2 {
		return types.Batch{}, false, nil
	}
	base.OutputColumns = []string{group.Column}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerStats := make([]storage.ExecStats, workers)
	errCh := make(chan error, workers)
	var wg sync.WaitGroup

	if group.Type.Kind == types.KindText {
		counts := make([]map[string]int64, workers)
		for worker := 0; worker < workers; worker++ {
			start := worker * len(base.Segments) / workers
			end := (worker + 1) * len(base.Segments) / workers
			if start == end {
				continue
			}
			counts[worker] = make(map[string]int64)
			sink := &v3exec.GroupStringCountSink{Column: group.Column, Counts: counts[worker]}
			it := base
			it.Context = ctx
			it.Segments = base.Segments[start:end]
			it.Stats = &workerStats[worker]
			if hasPushPred {
				it.Predicate = storage.NewPredicateEvaluator(pushPred)
			}
			wg.Add(1)
			go func(it storage.SegmentScanIterator, sink v3exec.AggregateSink) {
				defer wg.Done()
				if err := consumeAggregateIterator(it, []v3exec.AggregateSink{sink}); err != nil {
					cancel()
					errCh <- err
				}
			}(it, sink)
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return types.Batch{}, true, err
			}
		}
		merged := make(map[string]int64)
		mergedStats := storage.ExecStats{}
		for worker := range counts {
			for key, count := range counts[worker] {
				merged[key] += count
			}
			mergeExecStats(&mergedStats, workerStats[worker])
		}
		trace.recordScan(scan, mergedStats)
		batch, err := groupStringCountBatch(group, spec, merged)
		return batch, true, err
	}

	counts := make([]map[v3exec.GroupKey]int64, workers)
	for worker := 0; worker < workers; worker++ {
		start := worker * len(base.Segments) / workers
		end := (worker + 1) * len(base.Segments) / workers
		if start == end {
			continue
		}
		counts[worker] = make(map[v3exec.GroupKey]int64)
		sink := &v3exec.GroupAnyCountSink{Column: group.Column, Counts: counts[worker]}
		it := base
		it.Context = ctx
		it.Segments = base.Segments[start:end]
		it.Stats = &workerStats[worker]
		if hasPushPred {
			it.Predicate = storage.NewPredicateEvaluator(pushPred)
		}
		wg.Add(1)
		go func(it storage.SegmentScanIterator, sink v3exec.AggregateSink) {
			defer wg.Done()
			if err := consumeAggregateIterator(it, []v3exec.AggregateSink{sink}); err != nil {
				cancel()
				errCh <- err
			}
		}(it, sink)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return types.Batch{}, true, err
		}
	}
	merged := make(map[v3exec.GroupKey]int64)
	mergedStats := storage.ExecStats{}
	for worker := range counts {
		for key, count := range counts[worker] {
			merged[key] += count
		}
		mergeExecStats(&mergedStats, workerStats[worker])
	}
	trace.recordScan(scan, mergedStats)
	col, ok := sourceColumnDef(source, group.Column)
	if !ok {
		return types.Batch{}, true, fmt.Errorf("missing GROUP BY column %q", group.Column)
	}
	batch, err := groupAnyCountBatch(group, col.Labels, spec, merged)
	return batch, true, err
}

func (db *DB) groupedGenericAggregateBatch(ctx context.Context, plan *v3sql.AggregatePlan, trace *executionTrace) (types.Batch, error) {
	group := plan.GroupBy[0]
	col, ok := sourceColumnDef(plan.Source, group.Column)
	if !ok {
		return types.Batch{}, fmt.Errorf("missing GROUP BY column %q", group.Column)
	}
	specs := append([]v3sql.AggSpec{}, plan.Aggregates...)
	specs = append(specs, plan.Hidden...)
	collector := &groupedAggregateCollector{
		group:  group.Column,
		specs:  specs,
		groups: make(map[v3exec.GroupKey]*groupedAggregateState),
	}
	if err := db.runSource(ctx, plan.Source, collector, trace); err != nil {
		return types.Batch{}, err
	}
	return groupedAggregateResultBatch(group, col.Labels, specs, collector.groups)
}

type groupedAggregateCollector struct {
	group  string
	specs  []v3sql.AggSpec
	groups map[v3exec.GroupKey]*groupedAggregateState
}

type groupedAggregateState struct {
	acc []groupedAccumulator
}

type groupedAccumulator struct {
	count int64
	sum   int64
	value int64
	set   bool
}

func (c *groupedAggregateCollector) Open(context.Context) error { return nil }

func (c *groupedAggregateCollector) Push(batch types.Batch, sel types.SelectionMask) error {
	if sel.Rows != batch.Len {
		return fmt.Errorf("selection rows %d do not match batch length %d", sel.Rows, batch.Len)
	}
	groupCol, ok := columnFromList(batch.Columns, c.group)
	if !ok {
		return fmt.Errorf("missing GROUP BY column %q", c.group)
	}
	argCols := make([]types.Column, len(c.specs))
	for i, spec := range c.specs {
		if spec.Star {
			continue
		}
		col, ok := columnFromList(batch.Columns, spec.ArgName)
		if !ok {
			return fmt.Errorf("missing aggregate column %q", spec.ArgName)
		}
		argCols[i] = col
	}
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		key, ok, err := groupKeyFromColumn(groupCol, row)
		if err != nil {
			pushErr = err
			return
		}
		if !ok {
			return
		}
		state := c.groups[key]
		if state == nil {
			state = &groupedAggregateState{acc: make([]groupedAccumulator, len(c.specs))}
			c.groups[key] = state
		}
		for i, spec := range c.specs {
			if err := accumulateGroupedValue(&state.acc[i], spec, argCols[i], row); err != nil {
				pushErr = err
				return
			}
		}
	})
	return pushErr
}

func (c *groupedAggregateCollector) Close() error { return nil }

func groupKeyFromColumn(col types.Column, row int) (v3exec.GroupKey, bool, error) {
	if !types.IsValid(col.V.Valid, row) {
		return v3exec.GroupKey{}, false, nil
	}
	switch col.V.Kind {
	case types.VecBool:
		return v3exec.GroupKey{Kind: col.V.Kind, Bool: col.V.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0}, true, nil
	case types.VecInt16:
		return v3exec.GroupKey{Kind: col.V.Kind, I64: int64(col.V.I16[row])}, true, nil
	case types.VecInt32, types.VecDate:
		return v3exec.GroupKey{Kind: col.V.Kind, I64: int64(col.V.I32[row])}, true, nil
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		return v3exec.GroupKey{Kind: col.V.Kind, I64: col.V.I64[row]}, true, nil
	case types.VecText, types.VecBytes:
		value, ok := v3exec.TextValueCopy(col.V, row)
		if !ok {
			return v3exec.GroupKey{}, false, fmt.Errorf("GROUP BY column %q has unsupported text encoding %s", col.Name, col.V.Encoding)
		}
		return v3exec.GroupKey{Kind: col.V.Kind, Bytes: value}, true, nil
	case types.VecUUID:
		return v3exec.GroupKey{Kind: col.V.Kind, UUID: col.V.UUID[row]}, true, nil
	case types.VecEnum32:
		return v3exec.GroupKey{Kind: col.V.Kind, U32: col.V.U32[row]}, true, nil
	default:
		return v3exec.GroupKey{}, false, fmt.Errorf("GROUP BY column %q has unsupported kind %s", col.Name, col.V.Kind)
	}
}

func accumulateGroupedValue(acc *groupedAccumulator, spec v3sql.AggSpec, col types.Column, row int) error {
	switch spec.Func {
	case v3sql.AggregateCount:
		if spec.Star || types.IsValid(col.V.Valid, row) {
			acc.count++
		}
		return nil
	case v3sql.AggregateSum:
		value, ok, err := groupedInt64Value(col, row)
		if err != nil || !ok {
			return err
		}
		next, ok := v3exec.AddInt64(acc.sum, value)
		if !ok {
			return v3exec.ErrSumOverflow
		}
		acc.sum = next
		acc.count++
		return nil
	case v3sql.AggregateMin, v3sql.AggregateMax:
		value, ok, err := groupedInt64Value(col, row)
		if err != nil || !ok {
			return err
		}
		if !acc.set || (spec.Func == v3sql.AggregateMin && value < acc.value) || (spec.Func == v3sql.AggregateMax && value > acc.value) {
			acc.value = value
			acc.set = true
		}
		return nil
	default:
		return fmt.Errorf("unsupported grouped aggregate %d", spec.Func)
	}
}

func groupedInt64Value(col types.Column, row int) (int64, bool, error) {
	if !types.IsValid(col.V.Valid, row) {
		return 0, false, nil
	}
	switch col.V.Kind {
	case types.VecInt32:
		return int64(col.V.I32[row]), true, nil
	case types.VecInt64:
		return col.V.I64[row], true, nil
	default:
		return 0, false, fmt.Errorf("aggregate column %q has kind %s, want int32 or int64", col.Name, col.V.Kind)
	}
}

func (db *DB) groupAnyCountBatch(ctx context.Context, plan *v3sql.AggregatePlan, group v3sql.BoundExpr, aggSpec v3sql.AggSpec, trace *executionTrace) (types.Batch, error) {
	col, ok := sourceColumnDef(plan.Source, group.Column)
	if !ok {
		return types.Batch{}, fmt.Errorf("missing GROUP BY column %q", group.Column)
	}
	sink := &v3exec.GroupAnyCountSink{Column: group.Column}
	agg := &v3exec.Aggregate{Sinks: []v3exec.AggregateSink{sink}}
	if err := db.runSource(ctx, plan.Source, agg, trace); err != nil {
		return types.Batch{}, err
	}
	result, err := sink.Result()
	if err != nil {
		return types.Batch{}, err
	}
	return groupAnyCountBatch(group, col.Labels, aggSpec, result.(map[v3exec.GroupKey]int64))
}

func (db *DB) aggregateSinks(source v3sql.Plan, specs []v3sql.AggSpec) ([]v3exec.AggregateSink, error) {
	sinks := make([]v3exec.AggregateSink, 0, len(specs))
	for _, spec := range specs {
		sink, err := db.aggregateSink(source, spec)
		if err != nil {
			return nil, err
		}
		sinks = append(sinks, sink)
	}
	return sinks, nil
}

func mergeAggregateSinks(dst []v3exec.AggregateSink, src []v3exec.AggregateSink, specs []v3sql.AggSpec) error {
	if len(dst) != len(src) || len(dst) != len(specs) {
		return fmt.Errorf("aggregate sink merge count mismatch")
	}
	for i, spec := range specs {
		switch spec.Func {
		case v3sql.AggregateCount:
			if spec.Star {
				d, ok := dst[i].(*v3exec.CountSink)
				s, ok2 := src[i].(*v3exec.CountSink)
				if !ok || !ok2 {
					return fmt.Errorf("COUNT sink merge type mismatch")
				}
				d.N += s.N
				continue
			}
			d, ok := dst[i].(*v3exec.CountNonNullSink)
			s, ok2 := src[i].(*v3exec.CountNonNullSink)
			if !ok || !ok2 {
				return fmt.Errorf("COUNT column sink merge type mismatch")
			}
			d.N += s.N
		case v3sql.AggregateSum:
			switch d := dst[i].(type) {
			case *v3exec.SumInt64Sink:
				s, ok := src[i].(*v3exec.SumInt64Sink)
				if !ok {
					return fmt.Errorf("SUM int64 sink merge type mismatch")
				}
				next, ok := v3exec.AddInt64(d.Sum, s.Sum)
				if !ok {
					return v3exec.ErrSumOverflow
				}
				d.Sum = next
				d.Count += s.Count
			case *v3exec.SumInt32Sink:
				s, ok := src[i].(*v3exec.SumInt32Sink)
				if !ok {
					return fmt.Errorf("SUM int32 sink merge type mismatch")
				}
				d.Sum += s.Sum
				d.Count += s.Count
			default:
				return fmt.Errorf("SUM sink merge type mismatch")
			}
		case v3sql.AggregateMin:
			d, ok := dst[i].(*v3exec.MinInt64Sink)
			s, ok2 := src[i].(*v3exec.MinInt64Sink)
			if !ok || !ok2 {
				return fmt.Errorf("MIN sink merge type mismatch")
			}
			if s.Set && (!d.Set || s.Value < d.Value) {
				d.Value = s.Value
				d.Set = true
			}
		case v3sql.AggregateMax:
			d, ok := dst[i].(*v3exec.MaxInt64Sink)
			s, ok2 := src[i].(*v3exec.MaxInt64Sink)
			if !ok || !ok2 {
				return fmt.Errorf("MAX sink merge type mismatch")
			}
			if s.Set && (!d.Set || s.Value > d.Value) {
				d.Value = s.Value
				d.Set = true
			}
		default:
			return fmt.Errorf("unsupported aggregate %d", spec.Func)
		}
	}
	return nil
}

func (db *DB) aggregateSink(source v3sql.Plan, spec v3sql.AggSpec) (v3exec.AggregateSink, error) {
	switch spec.Func {
	case v3sql.AggregateCount:
		if spec.Star {
			return &v3exec.CountSink{}, nil
		}
		return &v3exec.CountNonNullSink{Column: spec.ArgName}, nil
	case v3sql.AggregateSum:
		typ, ok := sourceColumnType(source, spec.ArgColumn, spec.ArgName)
		if !ok {
			return nil, fmt.Errorf("missing SUM column %q", spec.ArgName)
		}
		if typ.Kind == types.KindInt32 {
			return &v3exec.SumInt32Sink{Column: spec.ArgName}, nil
		}
		if typ.Kind == types.KindInt64 {
			return &v3exec.SumInt64Sink{Column: spec.ArgName}, nil
		}
		return nil, fmt.Errorf("SUM column %q is %s, want int32 or int64", spec.ArgName, typ)
	case v3sql.AggregateMin:
		return &v3exec.MinInt64Sink{Column: spec.ArgName}, nil
	case v3sql.AggregateMax:
		return &v3exec.MaxInt64Sink{Column: spec.ArgName}, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate %d", spec.Func)
	}
}

func sourceColumnType(plan v3sql.Plan, id v3sql.ColumnID, name string) (types.Type, bool) {
	col, ok := sourceColumnDef(plan, name)
	if ok && (id == 0 || col.ID == id) {
		return col.Type, true
	}
	if id == 0 {
		return types.Type{}, false
	}
	switch plan := plan.(type) {
	case *v3sql.ScanPlan:
		for _, col := range plan.Table.Columns {
			if col.ID == id {
				return col.Type, true
			}
		}
	}
	return types.Type{}, false
}

func sourceColumnDef(plan v3sql.Plan, name string) (v3sql.BoundColumnDef, bool) {
	switch plan := plan.(type) {
	case *v3sql.ScanPlan:
		for _, col := range plan.Table.Columns {
			if normalizeName(col.Name) == normalizeName(name) {
				return col, true
			}
		}
	}
	return v3sql.BoundColumnDef{}, false
}

func scalarAggregateBatch(specs []v3sql.AggSpec, sinks []v3exec.AggregateSink) (types.Batch, error) {
	cols := make([]types.Column, 0, len(specs))
	for i, spec := range specs {
		result, err := sinks[i].Result()
		if err != nil {
			return types.Batch{}, err
		}
		col, err := scalarAggregateColumn(spec, result)
		if err != nil {
			return types.Batch{}, err
		}
		cols = append(cols, col)
	}
	return types.NewBatch(cols)
}

func scalarAggregateColumn(spec v3sql.AggSpec, result any) (types.Column, error) {
	name := aggregateOutputName(spec)
	switch spec.Func {
	case v3sql.AggregateCount:
		value, ok := result.(int64)
		if !ok {
			return types.Column{}, fmt.Errorf("COUNT produced %T", result)
		}
		return int64Column(name, []int64{value}, nil), nil
	case v3sql.AggregateSum:
		value, ok := result.(v3exec.SumInt64Result)
		if !ok {
			return types.Column{}, fmt.Errorf("SUM produced %T", result)
		}
		return int64Column(name, []int64{value.Sum}, nil), nil
	case v3sql.AggregateMin, v3sql.AggregateMax:
		value, ok := result.(v3exec.MinMaxInt64Result)
		if !ok {
			return types.Column{}, fmt.Errorf("MIN/MAX produced %T", result)
		}
		valid := types.NewValidity(1)
		if !value.Set {
			types.SetInvalid(valid, 0)
		}
		return int64Column(name, []int64{value.Value}, valid), nil
	default:
		return types.Column{}, fmt.Errorf("unsupported aggregate %d", spec.Func)
	}
}

func groupStringCountBatch(group v3sql.BoundExpr, spec v3sql.AggSpec, counts map[string]int64) (types.Batch, error) {
	keys := make([]string, 0, len(counts))
	dataBytes := 0
	for key := range counts {
		keys = append(keys, key)
		dataBytes += len(key)
	}
	sort.Strings(keys)

	groups := types.NewVarBytes(len(keys), dataBytes)
	values := make([]int64, len(keys))
	for i, key := range keys {
		groups.AppendString(i, key)
		values[i] = counts[key]
	}
	return types.NewBatch([]types.Column{
		{Name: group.Column, Type: group.Type, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(keys), Var: groups}},
		int64Column(aggregateOutputName(spec), values, nil),
	})
}

func groupAnyCountBatch(group v3sql.BoundExpr, labels []string, spec v3sql.AggSpec, counts map[v3exec.GroupKey]int64) (types.Batch, error) {
	keys := make([]v3exec.GroupKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return lessGroupKey(keys[i], keys[j]) })
	groupCol, err := groupAnyColumn(group, labels, keys)
	if err != nil {
		return types.Batch{}, err
	}
	values := make([]int64, len(keys))
	for i, key := range keys {
		values[i] = counts[key]
	}
	return types.NewBatch([]types.Column{groupCol, int64Column(aggregateOutputName(spec), values, nil)})
}

func groupedAggregateResultBatch(group v3sql.BoundExpr, labels []string, specs []v3sql.AggSpec, groups map[v3exec.GroupKey]*groupedAggregateState) (types.Batch, error) {
	keys := make([]v3exec.GroupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return lessGroupKey(keys[i], keys[j]) })
	groupCol, err := groupAnyColumn(group, labels, keys)
	if err != nil {
		return types.Batch{}, err
	}
	cols := make([]types.Column, 0, len(specs)+1)
	cols = append(cols, groupCol)
	for specIndex, spec := range specs {
		col, err := groupedAggregateColumn(spec, keys, groups, specIndex)
		if err != nil {
			return types.Batch{}, err
		}
		cols = append(cols, col)
	}
	return types.NewBatch(cols)
}

func groupedAggregateColumn(spec v3sql.AggSpec, keys []v3exec.GroupKey, groups map[v3exec.GroupKey]*groupedAggregateState, specIndex int) (types.Column, error) {
	values := make([]int64, len(keys))
	var valid types.Validity
	for row, key := range keys {
		acc := groups[key].acc[specIndex]
		switch spec.Func {
		case v3sql.AggregateCount:
			values[row] = acc.count
		case v3sql.AggregateSum:
			values[row] = acc.sum
		case v3sql.AggregateMin, v3sql.AggregateMax:
			values[row] = acc.value
			if !acc.set {
				if valid == nil {
					valid = types.NewValidity(len(keys))
				}
				types.SetInvalid(valid, row)
			}
		default:
			return types.Column{}, fmt.Errorf("unsupported grouped aggregate %d", spec.Func)
		}
	}
	return int64Column(aggregateOutputName(spec), values, valid), nil
}

func groupAnyColumn(group v3sql.BoundExpr, labels []string, keys []v3exec.GroupKey) (types.Column, error) {
	kind, err := types.VecKindOf(group.Type)
	if err != nil {
		return types.Column{}, err
	}
	v := types.Vec{Kind: kind, Encoding: types.EncodingFlat, Len: len(keys)}
	switch kind {
	case types.VecBool:
		v.BoolBits = make([]uint64, types.ValidityWords(len(keys)))
		for row, key := range keys {
			if key.Bool {
				v.BoolBits[row>>6] |= uint64(1) << uint(row&63)
			}
		}
	case types.VecInt16:
		v.I16 = make([]int16, len(keys))
		for row, key := range keys {
			v.I16[row] = int16(key.I64)
		}
	case types.VecInt32, types.VecDate:
		v.I32 = make([]int32, len(keys))
		for row, key := range keys {
			v.I32[row] = int32(key.I64)
		}
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		v.I64 = make([]int64, len(keys))
		for row, key := range keys {
			v.I64[row] = key.I64
		}
	case types.VecUUID:
		v.UUID = make([]types.UUID16, len(keys))
		for row, key := range keys {
			v.UUID[row] = key.UUID
		}
	case types.VecEnum32:
		v.U32 = make([]uint32, len(keys))
		for row, key := range keys {
			v.U32[row] = key.U32
		}
	case types.VecText, types.VecBytes:
		varBytes := types.NewVarBytes(len(keys), groupKeyBytes(keys))
		for row, key := range keys {
			varBytes.AppendString(row, key.Bytes)
		}
		v.Var = varBytes
	default:
		return types.Column{}, fmt.Errorf("GROUP BY column %q has unsupported kind %s", group.Column, kind)
	}
	return types.Column{Name: group.Column, Type: group.Type, EnumLabels: labels, V: v}, nil
}

func groupKeyBytes(keys []v3exec.GroupKey) int {
	total := 0
	for _, key := range keys {
		total += len(key.Bytes)
	}
	return total
}

func lessGroupKey(left v3exec.GroupKey, right v3exec.GroupKey) bool {
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	switch left.Kind {
	case types.VecBool:
		return !left.Bool && right.Bool
	case types.VecInt16, types.VecInt32, types.VecDate, types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		return left.I64 < right.I64
	case types.VecUUID:
		return string(left.UUID[:]) < string(right.UUID[:])
	case types.VecEnum32:
		return left.U32 < right.U32
	case types.VecText, types.VecBytes:
		return left.Bytes < right.Bytes
	default:
		return false
	}
}

func int64Column(name string, values []int64, valid types.Validity) types.Column {
	return types.Column{Name: name, Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, I64: values}}
}

func aggregateOutputName(spec v3sql.AggSpec) string {
	if spec.Alias != "" {
		return spec.Alias
	}
	switch spec.Func {
	case v3sql.AggregateSum:
		return "sum"
	case v3sql.AggregateMin:
		return "min"
	case v3sql.AggregateMax:
		return "max"
	default:
		return "count"
	}
}

type rowCollector struct {
	rows *Rows
}

func newRowCollector(columns []string) *rowCollector {
	return &rowCollector{rows: &Rows{Columns: append([]string(nil), columns...)}}
}

func (c *rowCollector) Open(context.Context) error { return nil }

func (c *rowCollector) Push(batch types.Batch, sel types.SelectionMask) error {
	if sel.Rows != batch.Len {
		return fmt.Errorf("selection rows %d do not match batch length %d", sel.Rows, batch.Len)
	}
	if len(sel.Words) < types.ValidityWords(sel.Rows) {
		return fmt.Errorf("selection has %d words for %d rows", len(sel.Words), sel.Rows)
	}
	if len(c.rows.Columns) == 0 && len(batch.Columns) != 0 {
		c.rows.Columns = batchColumnNames(batch)
	}
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		values := make([]any, len(batch.Columns))
		for i, col := range batch.Columns {
			value, err := rowValue(col, row)
			if err != nil {
				pushErr = err
				return
			}
			values[i] = value
		}
		c.rows.Values = append(c.rows.Values, values)
	})
	return pushErr
}

func (c *rowCollector) Close() error { return nil }

func batchColumnNames(batch types.Batch) []string {
	names := make([]string, len(batch.Columns))
	for i, col := range batch.Columns {
		names[i] = col.Name
	}
	return names
}

func rowValue(col types.Column, row int) (any, error) {
	if !types.IsValid(col.V.Valid, row) {
		return nil, nil
	}
	switch col.V.Kind {
	case types.VecBool:
		return col.V.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0, nil
	case types.VecInt16:
		return col.V.I16[row], nil
	case types.VecInt32:
		return col.V.I32[row], nil
	case types.VecDate:
		return formatDateDays(col.V.I32[row]), nil
	case types.VecInt64, types.VecDecimal64, types.VecTime:
		return col.V.I64[row], nil
	case types.VecTimestamp:
		return formatTimestampNanos(col.V.I64[row]), nil
	case types.VecFloat32:
		return col.V.F32[row], nil
	case types.VecFloat64:
		return col.V.F64[row], nil
	case types.VecText, types.VecBytes, types.VecJSON:
		value, ok := v3exec.TextValueCopy(col.V, row)
		if !ok {
			return nil, fmt.Errorf("column %q has unsupported text encoding %s", col.Name, col.V.Encoding)
		}
		return value, nil
	case types.VecUUID:
		return types.FormatUUID(col.V.UUID[row]), nil
	case types.VecEnum32:
		label, ok := enumLabelForCode(col.V.U32[row], col.EnumLabels)
		if !ok {
			return nil, fmt.Errorf("column %q has invalid enum code %d", col.Name, col.V.U32[row])
		}
		return label, nil
	default:
		return nil, fmt.Errorf("column %q has unsupported vector kind %s", col.Name, col.V.Kind)
	}
}

func enumLabelForCode(code uint32, labels []string) (string, bool) {
	if code == 0 || int(code) > len(labels) {
		return "", false
	}
	return labels[code-1], true
}

func formatDateDays(days int32) string {
	return time.Unix(int64(days)*secondsPerDay, 0).UTC().Format("2006-01-02")
}

func formatTimestampNanos(nanos int64) string {
	return time.Unix(0, nanos).UTC().Format(timestampLayout)
}

func planOutputColumns(plan v3sql.Plan) []string {
	switch plan := plan.(type) {
	case *v3sql.LimitPlan:
		return planOutputColumns(plan.Source)
	case *v3sql.SortPlan:
		return planOutputColumns(plan.Source)
	case *v3sql.ProjectPlan:
		columns := make([]string, 0, len(plan.Exprs))
		for _, output := range plan.Exprs {
			columns = append(columns, boundOutputName(output))
		}
		return columns
	case *v3sql.ScanPlan:
		return columnNamesForIDs(plan.Table, plan.Columns)
	case *v3sql.AggregatePlan:
		columns := make([]string, 0, len(plan.GroupBy)+len(plan.Aggregates)+len(plan.Hidden))
		for _, group := range plan.GroupBy {
			columns = append(columns, group.Column)
		}
		for _, agg := range plan.Aggregates {
			columns = append(columns, aggregateOutputName(agg))
		}
		for _, agg := range plan.Hidden {
			columns = append(columns, aggregateOutputName(agg))
		}
		return columns
	default:
		return nil
	}
}

func boundOutputName(output v3sql.BoundOutput) string {
	if output.Alias != "" {
		return output.Alias
	}
	if output.Expr.Kind == v3sql.BoundExprColumn {
		return output.Expr.Column
	}
	return ""
}

func columnNamesForIDs(table v3sql.BoundTableDef, ids []v3sql.ColumnID) []string {
	if ids == nil {
		return nil
	}
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		for _, col := range table.Columns {
			if col.ID == id {
				names = append(names, col.Name)
				break
			}
		}
	}
	return names
}
