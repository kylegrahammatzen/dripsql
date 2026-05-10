package sql

import (
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func BindSelect(stmt *SelectStmt, def BoundTableDef) (Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("SELECT statement is nil")
	}
	if def.Name != "" && normalizeName(stmt.Table) != normalizeName(def.Name) {
		return nil, fmt.Errorf("SELECT target %q does not match table %q", stmt.Table, def.Name)
	}
	columns := buildColumnIndex(def.Columns)
	groupExpr, err := bindGroupExpr(columns, stmt.GroupBy)
	if err != nil {
		return nil, err
	}
	hasGroup := groupExpr != nil
	aggregates, hasAgg, err := bindSelectAggregates(stmt.Select, columns, hasGroup)
	if err != nil {
		return nil, err
	}
	if !hasGroup && !hasAgg {
		return bindScanSelect(stmt, def, columns)
	}
	if !hasAgg {
		return nil, fmt.Errorf("only count(*), count(column), sum(column), min(column), and max(column) SELECT queries are supported")
	}
	scan, err := bindBaseScan(stmt, def, columns)
	if err != nil {
		return nil, err
	}
	aggPlan := &AggregatePlan{
		Source:     scan,
		Aggregates: aggregates,
	}
	outputs := make([]BoundOutput, 0, len(stmt.Select))
	if hasGroup {
		selectFirst, err := bindExpr(columns, stmt.Select[0].Expr)
		if err != nil {
			return nil, err
		}
		if !boundExprEqual(selectFirst, *groupExpr) {
			return nil, fmt.Errorf("selected group expression must match GROUP BY expression")
		}
		if groupExpr.Kind != BoundExprColumn && stmt.Select[0].Alias == "" {
			return nil, fmt.Errorf("GROUP BY computed expressions require a selected alias")
		}
		if !groupableKind(groupExpr.Type.Kind) {
			return nil, fmt.Errorf("GROUP BY expression is %s, want text, bytes, uuid, int16, int32, int64, bool, date, timestamp, or enum", groupExpr.Type)
		}
		aggPlan.GroupBy = []BoundExpr{*groupExpr}
		outputs = append(outputs, BoundOutput{Alias: stmt.Select[0].Alias, Expr: *groupExpr})
	}
	for _, spec := range aggregates {
		outputs = append(outputs, BoundOutput{Alias: spec.Alias, Expr: BoundExpr{Kind: BoundExprColumn, Type: types.Int64, Column: aggregateOutputName(spec)}})
	}
	if stmt.Having != nil {
		having, err := bindHaving(aggPlan, outputs, columns, stmt.Having)
		if err != nil {
			return nil, err
		}
		aggPlan.Having = &having
	}
	scan.Columns = aggregateScanColumnIDs(aggPlan)

	var plan Plan = &ProjectPlan{Source: aggPlan, Exprs: outputs}
	plan, err = bindOrderLimit(plan, stmt, outputs, columns)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func aggregateScanColumnIDs(plan *AggregatePlan) []ColumnID {
	if plan == nil {
		return nil
	}
	seen := make(map[ColumnID]struct{})
	var ids []ColumnID
	for _, group := range plan.GroupBy {
		ids = appendBoundExprColumnIDs(ids, seen, group)
	}
	for _, spec := range plan.Aggregates {
		ids = appendAggSpecColumnID(ids, seen, spec)
	}
	for _, spec := range plan.Hidden {
		ids = appendAggSpecColumnID(ids, seen, spec)
	}
	if len(ids) == 0 {
		return []ColumnID{}
	}
	return ids
}

func appendAggSpecColumnID(ids []ColumnID, seen map[ColumnID]struct{}, spec AggSpec) []ColumnID {
	if spec.Star {
		return ids
	}
	return appendColumnID(ids, seen, spec.ArgColumn)
}

func appendBoundExprColumnIDs(ids []ColumnID, seen map[ColumnID]struct{}, expr BoundExpr) []ColumnID {
	switch expr.Kind {
	case BoundExprColumn:
		return appendColumnID(ids, seen, expr.ColumnID)
	case BoundExprBinary, BoundExprUnary:
		if expr.Left != nil {
			ids = appendBoundExprColumnIDs(ids, seen, *expr.Left)
		}
		if expr.Right != nil {
			ids = appendBoundExprColumnIDs(ids, seen, *expr.Right)
		}
	}
	return ids
}

func appendColumnID(ids []ColumnID, seen map[ColumnID]struct{}, id ColumnID) []ColumnID {
	if id == 0 {
		return ids
	}
	if _, ok := seen[id]; ok {
		return ids
	}
	seen[id] = struct{}{}
	return append(ids, id)
}

func BindExplain(stmt *ExplainStmt, def BoundTableDef) (*ExplainPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("EXPLAIN statement is nil")
	}
	selectStmt, ok := stmt.Inner.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN supports SELECT only")
	}
	inner, err := BindSelect(selectStmt, def)
	if err != nil {
		return nil, err
	}
	return &ExplainPlan{Inner: inner, Analyze: stmt.Analyze}, nil
}

func bindScanSelect(stmt *SelectStmt, def BoundTableDef, columns map[string]BoundColumnDef) (Plan, error) {
	if len(stmt.GroupBy) != 0 {
		return nil, fmt.Errorf("GROUP BY requires an aggregate query")
	}
	if stmt.Having != nil {
		return nil, fmt.Errorf("HAVING requires an aggregate query")
	}
	scan, err := bindBaseScan(stmt, def, columns)
	if err != nil {
		return nil, err
	}
	outputs, err := bindScanOutputs(stmt, def, columns)
	if err != nil {
		return nil, err
	}
	var plan Plan = &ProjectPlan{Source: scan, Exprs: outputs}
	plan, err = bindOrderLimit(plan, stmt, outputs, columns)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func bindBaseScan(stmt *SelectStmt, def BoundTableDef, columns map[string]BoundColumnDef) (*ScanPlan, error) {
	plan := &ScanPlan{Table: def, Columns: allColumnIDs(def.Columns)}
	if stmt.Where != nil {
		where, err := bindWhereExpr(columns, stmt.Where)
		if err != nil {
			return nil, err
		}
		plan.Where = &where
	}
	return plan, nil
}

func bindScanOutputs(stmt *SelectStmt, def BoundTableDef, columns map[string]BoundColumnDef) ([]BoundOutput, error) {
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].Expr.(*StarRef); ok {
			outputs := make([]BoundOutput, 0, len(def.Columns))
			for _, col := range def.Columns {
				outputs = append(outputs, BoundOutput{Expr: BoundExpr{Kind: BoundExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}})
			}
			return outputs, nil
		}
	}
	outputs := make([]BoundOutput, 0, len(stmt.Select))
	for _, sel := range stmt.Select {
		out, err := bindScanOutputExpr(columns, sel)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

func bindScanOutputExpr(columns map[string]BoundColumnDef, sel SelectExpr) (BoundOutput, error) {
	bound, err := bindExpr(columns, sel.Expr)
	if err != nil {
		return BoundOutput{}, err
	}
	switch bound.Kind {
	case BoundExprColumn:
		return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
	case BoundExprLiteral:
		if sel.Alias == "" {
			return BoundOutput{}, fmt.Errorf("scan SELECT literal expressions require an alias")
		}
		return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
	case BoundExprBinary, BoundExprUnary:
		if sel.Alias == "" {
			return BoundOutput{}, fmt.Errorf("scan SELECT computed expressions require an alias")
		}
		if !isScanComputedOp(bound.Op) {
			return BoundOutput{}, fmt.Errorf("scan SELECT only supports column, literal, and computed expressions")
		}
		return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
	default:
		return BoundOutput{}, fmt.Errorf("scan SELECT only supports column, literal, and computed expressions")
	}
}

func isScanComputedOp(op BoundOp) bool {
	switch op {
	case BoundOpAdd, BoundOpSubtract, BoundOpMultiply, BoundOpDivide, BoundOpModulo, BoundOpIntDivide,
		BoundOpConcat, BoundOpLower, BoundOpUpper:
		return true
	default:
		return false
	}
}

func bindWhereExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	bound, err := bindWhereLogicalExpr(columns, expr)
	if err == nil {
		return bound, nil
	}
	if col := whereColumnRef(expr); col != "" {
		if _, ok := findColumn(columns, col); !ok {
			return BoundExpr{}, fmt.Errorf("missing WHERE column %q", col)
		}
	}
	return BoundExpr{}, err
}

func whereColumnRef(expr Expr) string {
	switch expr := expr.(type) {
	case *BinaryExpr:
		col, _ := expr.Left.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *BetweenExpr:
		col, _ := expr.Expr.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *InExpr:
		col, _ := expr.Expr.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	default:
		return ""
	}
}

func bindWhereLogicalExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch expr := expr.(type) {
	case *AndExpr:
		left, err := bindWhereLogicalExpr(columns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindWhereLogicalExpr(columns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: BoundOpAnd, Left: &left, Right: &right}, nil
	case *OrExpr:
		left, err := bindWhereLogicalExpr(columns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindWhereLogicalExpr(columns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: BoundOpOr, Left: &left, Right: &right}, nil
	case *NotExpr:
		child, err := bindWhereLogicalExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprUnary, Type: types.Bool, Op: BoundOpNot, Left: &child}, nil
	case *BinaryExpr:
		op, err := bindBinaryOp(expr.Op)
		if err != nil {
			return BoundExpr{}, fmt.Errorf("unsupported WHERE expression")
		}
		left, right, err := bindBinary(columns, expr.Left, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateWhereComparison(left, op, right); err != nil {
			return BoundExpr{}, err
		}
		normalizeComparison(&left, &right)
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: filterOpToExprOp(op), Left: &left, Right: &right}, nil
	case *BetweenExpr:
		target, err := bindExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		low, err := bindExpr(columns, expr.Low)
		if err != nil {
			return BoundExpr{}, err
		}
		high, err := bindExpr(columns, expr.High)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateBetween(target, low, high, whereBetweenRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return BoundExpr{Kind: BoundExprBetween, Type: types.Bool, Left: &target, Args: []BoundExpr{low, high}}, nil
	case *InExpr:
		target, err := bindExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		values := make([]BoundExpr, 0, len(expr.Values))
		for _, value := range expr.Values {
			bound, err := bindExpr(columns, value)
			if err != nil {
				return BoundExpr{}, err
			}
			if err := validateWhereInValue(target, bound); err != nil {
				return BoundExpr{}, err
			}
			normalizeBound(target, &bound)
			values = append(values, bound)
		}
		return BoundExpr{Kind: BoundExprIn, Type: types.Bool, Left: &target, Args: values, Not: expr.Not}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported WHERE expression")
	}
}

func bindHaving(plan *AggregatePlan, outputs []BoundOutput, baseColumns map[string]BoundColumnDef, having Expr) (BoundExpr, error) {
	columns := make([]BoundColumnDef, 0, len(outputs))
	for i, output := range outputs {
		name := outputExprName(output)
		if name == "" {
			continue
		}
		columns = append(columns, BoundColumnDef{ID: ColumnID(i + 1), Name: name, Type: output.Expr.Type})
	}
	expr, err := bindHavingLogicalExpr(plan, buildColumnIndex(columns), baseColumns, having)
	if err != nil {
		return BoundExpr{}, err
	}
	return expr, nil
}

func bindHavingLogicalExpr(plan *AggregatePlan, columns map[string]BoundColumnDef, baseColumns map[string]BoundColumnDef, having Expr) (BoundExpr, error) {
	switch expr := having.(type) {
	case *AndExpr:
		left, err := bindHavingLogicalExpr(plan, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingLogicalExpr(plan, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: BoundOpAnd, Left: &left, Right: &right}, nil
	case *OrExpr:
		left, err := bindHavingLogicalExpr(plan, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingLogicalExpr(plan, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: BoundOpOr, Left: &left, Right: &right}, nil
	case *NotExpr:
		child, err := bindHavingLogicalExpr(plan, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Kind: BoundExprUnary, Type: types.Bool, Op: BoundOpNot, Left: &child}, nil
	case *BinaryExpr:
		if isArithmeticOp(expr.Op) {
			return BoundExpr{}, fmt.Errorf("HAVING requires a predicate expression")
		}
		left, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		op, err := bindBinaryOp(expr.Op)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateComparison(left, op, right, havingCmpRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeComparison(&left, &right)
		return BoundExpr{Kind: BoundExprBinary, Type: types.Bool, Op: filterOpToExprOp(op), Left: &left, Right: &right}, nil
	case *BetweenExpr:
		target, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		low, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Low)
		if err != nil {
			return BoundExpr{}, err
		}
		high, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.High)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateBetween(target, low, high, havingBetweenRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return BoundExpr{Kind: BoundExprBetween, Type: types.Bool, Left: &target, Args: []BoundExpr{low, high}}, nil
	case *InExpr:
		target, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		values := make([]BoundExpr, 0, len(expr.Values))
		for _, value := range expr.Values {
			bound, err := bindHavingScalarExpr(plan, columns, baseColumns, value)
			if err != nil {
				return BoundExpr{}, err
			}
			if err := validateWhereInValue(target, bound); err != nil {
				return BoundExpr{}, err
			}
			normalizeBound(target, &bound)
			values = append(values, bound)
		}
		return BoundExpr{Kind: BoundExprIn, Type: types.Bool, Left: &target, Args: values, Not: expr.Not}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported HAVING expression")
	}
}

func bindHavingScalarExpr(plan *AggregatePlan, columns map[string]BoundColumnDef, baseColumns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch expr := expr.(type) {
	case *ColumnRef:
		col, ok := findColumn(columns, expr.Name)
		if !ok {
			return BoundExpr{}, fmt.Errorf("HAVING column %q must be a selected output column", expr.Name)
		}
		return BoundExpr{Kind: BoundExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}, nil
	case *FuncCall:
		if !isAggregateName(expr.Name) {
			return bindScalarCall(columns, expr)
		}
		return bindHavingAggregate(plan, baseColumns, expr)
	case *Literal:
		return literalExpr(expr.Value), nil
	case *BinaryExpr:
		if expr.Op == BinaryConcat {
			return bindBinaryExpr(columns, expr)
		}
		if !isArithmeticOp(expr.Op) {
			return BoundExpr{}, fmt.Errorf("HAVING scalar expression contains a predicate operator")
		}
		left, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingScalarExpr(plan, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		if !isIntegerExpr(left) || !isIntegerExpr(right) {
			return BoundExpr{}, fmt.Errorf("HAVING arithmetic expressions require integer operands")
		}
		op, ok := arithmeticOp(expr.Op)
		if !ok {
			return BoundExpr{}, fmt.Errorf("unsupported HAVING arithmetic operator")
		}
		return BoundExpr{Kind: BoundExprBinary, Type: types.Int64, Op: op, Left: &left, Right: &right}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported HAVING scalar expression")
	}
}

func bindHavingAggregate(plan *AggregatePlan, baseColumns map[string]BoundColumnDef, call *FuncCall) (BoundExpr, error) {
	if len(plan.Aggregates) == 0 {
		return BoundExpr{}, fmt.Errorf("HAVING aggregate requires an aggregate query")
	}
	want, argName, star, err := decomposeAggregateCall(call)
	if err != nil {
		return BoundExpr{}, err
	}
	for _, selected := range plan.Aggregates {
		if selected.Func == want && selected.Star == star && normalizeName(selected.ArgName) == normalizeName(argName) {
			return BoundExpr{Kind: BoundExprColumn, Type: types.Int64, Column: aggregateOutputName(selected)}, nil
		}
	}
	hidden, err := bindHiddenHavingAggregate(baseColumns, want, argName, star)
	if err != nil {
		return BoundExpr{}, err
	}
	for _, existing := range plan.Hidden {
		if existing.Func == hidden.Func && existing.ArgColumn == hidden.ArgColumn && existing.Star == hidden.Star {
			return BoundExpr{Kind: BoundExprColumn, Type: types.Int64, Column: existing.Alias}, nil
		}
	}
	plan.Hidden = append(plan.Hidden, hidden)
	return BoundExpr{Kind: BoundExprColumn, Type: types.Int64, Column: hidden.Alias}, nil
}

func bindHiddenHavingAggregate(columns map[string]BoundColumnDef, fn AggregateFunc, argName string, star bool) (AggSpec, error) {
	if fn == AggregateCount && star {
		return AggSpec{Func: fn, Star: true, Alias: hiddenHavingAggregateName(fn, "", true)}, nil
	}
	col, ok := findColumn(columns, argName)
	if !ok {
		return AggSpec{}, fmt.Errorf("missing HAVING aggregate column %q", argName)
	}
	if fn != AggregateCount {
		if err := validateAggregateColumn(fn, col.Name, columns); err != nil {
			return AggSpec{}, err
		}
	}
	return AggSpec{Func: fn, ArgColumn: col.ID, ArgName: col.Name, Alias: hiddenHavingAggregateName(fn, col.Name, false)}, nil
}

func hiddenHavingAggregateName(fn AggregateFunc, column string, star bool) string {
	if fn == AggregateCount && star {
		return "__having_count_star"
	}
	return "__having_" + defaultAggregateName(fn) + "_" + normalizeName(column)
}

func decomposeAggregateCall(call *FuncCall) (AggregateFunc, string, bool, error) {
	fn, ok := aggregateFuncByName(call.Name)
	if !ok {
		return AggregateInvalid, "", false, fmt.Errorf("unsupported HAVING aggregate %q", call.Name)
	}
	if call.Star {
		if fn != AggregateCount || len(call.Args) != 0 {
			return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate star is only supported for count(*)")
		}
		return fn, "", true, nil
	}
	if len(call.Args) != 1 {
		return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate must have one argument")
	}
	col, ok := call.Args[0].(*ColumnRef)
	if !ok {
		return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate argument must be a column")
	}
	return fn, col.Name, false, nil
}

var exprAsFilter = map[BoundOp]FilterOp{
	BoundOpEqual:        FilterEqual,
	BoundOpNotEqual:     FilterNotEqual,
	BoundOpLess:         FilterLess,
	BoundOpLessEqual:    FilterLessEqual,
	BoundOpGreater:      FilterGreater,
	BoundOpGreaterEqual: FilterGreaterEqual,
}

func bindGroupExpr(columns map[string]BoundColumnDef, groupBy []Expr) (*BoundExpr, error) {
	if len(groupBy) == 0 {
		return nil, nil
	}
	if len(groupBy) != 1 {
		return nil, fmt.Errorf("only one GROUP BY expression is supported")
	}
	bound, err := bindExpr(columns, groupBy[0])
	if err != nil {
		if col, ok := groupBy[0].(*ColumnRef); ok {
			return nil, fmt.Errorf("missing GROUP BY column %q", col.Name)
		}
		return nil, err
	}
	if bound.Type.Kind == types.KindInvalid {
		return nil, fmt.Errorf("unsupported GROUP BY expression")
	}
	return &bound, nil
}

func bindSelectAggregates(exprs []SelectExpr, columns map[string]BoundColumnDef, grouped bool) ([]AggSpec, bool, error) {
	start := 0
	if grouped {
		start = 1
	}
	if start >= len(exprs) {
		return nil, false, nil
	}
	specs := make([]AggSpec, 0, len(exprs)-start)
	for i := start; i < len(exprs); i++ {
		spec, ok, err := bindSelectAggregate(exprs[i], columns)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			if len(specs) == 0 {
				return nil, false, nil
			}
			return nil, true, fmt.Errorf("aggregate SELECT expressions must be aggregate calls")
		}
		if err := validateAggregateColumn(spec.Func, spec.ArgName, columns); err != nil {
			return nil, true, err
		}
		specs = append(specs, spec)
	}
	return specs, len(specs) != 0, nil
}

func bindSelectAggregate(sel SelectExpr, columns map[string]BoundColumnDef) (AggSpec, bool, error) {
	call, ok := sel.Expr.(*FuncCall)
	if !ok {
		return AggSpec{}, false, nil
	}
	fn, ok := aggregateFuncByName(call.Name)
	if !ok {
		return AggSpec{}, false, nil
	}
	if call.Star {
		if fn != AggregateCount || len(call.Args) != 0 {
			return AggSpec{}, true, fmt.Errorf("aggregate star is only supported for count(*)")
		}
		return AggSpec{Func: fn, Star: true, Alias: sel.Alias}, true, nil
	}
	if len(call.Args) != 1 {
		return AggSpec{}, true, fmt.Errorf("aggregate must have one argument")
	}
	colRef, ok := call.Args[0].(*ColumnRef)
	if !ok {
		return AggSpec{}, true, fmt.Errorf("aggregate argument must be a column")
	}
	def, ok := findColumn(columns, colRef.Name)
	if !ok {
		return AggSpec{}, true, fmt.Errorf("missing aggregate column %q", colRef.Name)
	}
	return AggSpec{Func: fn, ArgColumn: def.ID, ArgName: def.Name, Alias: sel.Alias}, true, nil
}

func validateAggregateColumn(agg AggregateFunc, column string, columns map[string]BoundColumnDef) error {
	if column == "" || agg == AggregateCount {
		return nil
	}
	col, ok := columns[normalizeName(column)]
	if !ok {
		return nil
	}
	switch col.Type.Kind {
	case types.KindInt32, types.KindInt64:
		return nil
	default:
		return fmt.Errorf("%s column %q is %s, want int32 or int64", strings.ToUpper(defaultAggregateName(agg)), col.Name, col.Type)
	}
}

func bindOrderLimit(source Plan, stmt *SelectStmt, outputs []BoundOutput, columns map[string]BoundColumnDef) (Plan, error) {
	plan := source
	if len(stmt.OrderBy) != 0 {
		keys, err := bindSortKeys(stmt.OrderBy, outputs, columns)
		if err != nil {
			return nil, err
		}
		plan = &SortPlan{Source: plan, Keys: keys}
	}
	if stmt.Limit != nil || stmt.Offset != nil {
		limit := int64(-1)
		offset := int64(0)
		if stmt.Limit != nil {
			if *stmt.Limit < 0 {
				return nil, fmt.Errorf("LIMIT must be non-negative")
			}
			limit = *stmt.Limit
		}
		if stmt.Offset != nil {
			if *stmt.Offset < 0 {
				return nil, fmt.Errorf("OFFSET must be non-negative")
			}
			offset = *stmt.Offset
		}
		plan = &LimitPlan{Source: plan, N: limit, Offset: offset}
	}
	return plan, nil
}

func bindSortKeys(orderBy []OrderExpr, outputs []BoundOutput, columns map[string]BoundColumnDef) ([]SortKey, error) {
	keys := make([]SortKey, 0, len(orderBy))
	for _, order := range orderBy {
		name := normalizeName(order.Name)
		matched := false
		for _, output := range outputs {
			if name == normalizeName(outputExprName(output)) {
				keys = append(keys, SortKey{Name: outputExprName(output), Expr: output.Expr, Desc: order.Desc})
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if order.Expr == nil {
			return nil, fmt.Errorf("ORDER BY column %q must be a selected output column", order.Name)
		}
		bound, err := bindExpr(columns, order.Expr)
		if err != nil {
			return nil, err
		}
		keys = append(keys, SortKey{Expr: bound, Desc: order.Desc})
	}
	return keys, nil
}

func outputExprName(output BoundOutput) string {
	if output.Alias != "" {
		return output.Alias
	}
	if output.Expr.Kind == BoundExprColumn {
		return output.Expr.Column
	}
	return ""
}

func selectedAlias(exprs []SelectExpr, index int) string {
	if index < 0 || index >= len(exprs) {
		return ""
	}
	return exprs[index].Alias
}

func defaultAggregateName(aggregate AggregateFunc) string {
	switch aggregate {
	case AggregateSum:
		return "sum"
	case AggregateMin:
		return "min"
	case AggregateMax:
		return "max"
	default:
		return "count"
	}
}

func aggregateOutputName(spec AggSpec) string {
	if spec.Alias != "" {
		return spec.Alias
	}
	return defaultAggregateName(spec.Func)
}

func groupableKind(kind types.Kind) bool {
	switch kind {
	case types.KindText, types.KindBytes, types.KindUUID, types.KindInt16, types.KindInt32, types.KindInt64, types.KindBool, types.KindDate, types.KindTimestamp, types.KindNamed:
		return true
	default:
		return false
	}
}

func boundExprEqual(left BoundExpr, right BoundExpr) bool {
	if left.Kind != right.Kind || left.Op != right.Op || left.ColumnID != right.ColumnID {
		return false
	}
	switch left.Kind {
	case BoundExprColumn:
		return normalizeName(left.Column) == normalizeName(right.Column)
	case BoundExprLiteral:
		return left.Literal == right.Literal
	case BoundExprBinary, BoundExprUnary:
		if !boundExprEqualPtr(left.Left, right.Left) {
			return false
		}
		return boundExprEqualPtr(left.Right, right.Right)
	}
	return false
}

func boundExprEqualPtr(left, right *BoundExpr) bool {
	if left == nil || right == nil {
		return left == right
	}
	return boundExprEqual(*left, *right)
}

func allColumnIDs(columns []BoundColumnDef) []ColumnID {
	ids := make([]ColumnID, 0, len(columns))
	for _, col := range columns {
		ids = append(ids, col.ID)
	}
	return ids
}
