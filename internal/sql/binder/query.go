package binder

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func BindSelect(stmt *ast.SelectStmt, def catalog.TableDef) (logical.Query, error) {
	if stmt == nil {
		return logical.Query{}, fmt.Errorf("SELECT statement is nil")
	}
	if def.Name != "" && normalizeName(stmt.Table) != normalizeName(def.Name) {
		return logical.Query{}, fmt.Errorf("SELECT target %q does not match table %q", stmt.Table, def.Name)
	}
	columns := buildColumnIndex(def.Columns)
	groupExpr, err := bindGroupExpr(columns, stmt.GroupBy)
	if err != nil {
		return logical.Query{}, err
	}
	hasGroup := groupExpr != nil
	agg, aggCol, _, hasAgg := selectedAggregate(stmt.Select, columns, hasGroup)
	if !hasGroup && !hasAgg {
		return bindScanSelect(stmt, def, columns)
	}
	if !hasGroup && len(stmt.OrderBy) != 0 {
		return logical.Query{}, fmt.Errorf("ORDER BY is only supported for GROUP BY aggregate queries")
	}
	if !hasAgg {
		return logical.Query{}, fmt.Errorf("only count(*), count(column), sum(column), min(column), and max(column) SELECT queries are supported")
	}
	if err := validateAggregateColumn(agg, aggCol, columns); err != nil {
		return logical.Query{}, err
	}

	plan := logical.Query{Kind: logical.QueryAggregate, Table: def, Aggregate: agg, AggregateColumn: aggCol}
	if err := bindLimit(&plan, stmt.Limit); err != nil {
		return logical.Query{}, err
	}
	if err := bindOffset(&plan, stmt.Offset); err != nil {
		return logical.Query{}, err
	}
	if n := len(stmt.Select); n > 0 {
		plan.AggregateAlias = stmt.Select[n-1].Alias
	}
	if hasGroup {
		selectFirst, err := bindExpr(columns, stmt.Select[0].Expr)
		if err != nil {
			return logical.Query{}, err
		}
		if !logicalExprEqual(selectFirst, *groupExpr) {
			return logical.Query{}, fmt.Errorf("selected group expression must match GROUP BY expression")
		}
		if groupExpr.Kind != logical.ExprColumn && stmt.Select[0].Alias == "" {
			return logical.Query{}, fmt.Errorf("GROUP BY computed expressions require a selected alias")
		}
		switch groupExpr.Type.Kind {
		case sqltype.KindText, sqltype.KindBytes, sqltype.KindUUID, sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64, sqltype.KindBool, sqltype.KindDate, sqltype.KindTimestamp, sqltype.KindNamed:
		default:
			return logical.Query{}, fmt.Errorf("GROUP BY expression is %s, want text, bytes, uuid, int16, int32, int64, bool, date, timestamp, or enum", groupExpr.Type)
		}
		plan.GroupExpr = groupExpr
		if groupExpr.Kind == logical.ExprColumn {
			plan.GroupColumn = groupExpr.Column
		}
		plan.GroupType = groupExpr.Type.Kind
		plan.GroupAlias = stmt.Select[0].Alias
	}
	if stmt.Where != nil {
		where, err := bindWhereExpr(columns, stmt.Where)
		if err != nil {
			return logical.Query{}, err
		}
		if err := bindWhereClause(&plan, where); err != nil {
			return logical.Query{}, err
		}
	}
	if err := bindHaving(&plan, columns, stmt.Having); err != nil {
		return logical.Query{}, err
	}
	if hasGroup {
		if err := bindOrderBy(&plan, stmt.OrderBy); err != nil {
			return logical.Query{}, err
		}
	}
	return plan, nil
}

// validateAggregateColumn enforces int32/int64 typing for SUM/MIN/MAX inputs.
func validateAggregateColumn(agg logical.AggregateFunc, column string, columns map[string]catalog.ColumnDef) error {
	if column == "" || agg == logical.AggregateCount {
		return nil
	}
	col, ok := columns[normalizeName(column)]
	if !ok {
		return nil
	}
	return validateIntAggregateColumn(strings.ToUpper(aggregateNames[agg]), col)
}

func bindScanSelect(stmt *ast.SelectStmt, def catalog.TableDef, columns map[string]catalog.ColumnDef) (logical.Query, error) {
	if len(stmt.GroupBy) != 0 {
		return logical.Query{}, fmt.Errorf("GROUP BY requires an aggregate query")
	}
	if stmt.Having != nil {
		return logical.Query{}, fmt.Errorf("HAVING requires an aggregate query")
	}
	plan := logical.Query{Kind: logical.QueryScan, Table: def}
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].Expr.(*ast.StarRef); ok {
			for _, col := range def.Columns {
				plan.SelectOutputs = append(plan.SelectOutputs, logical.OutputExpr{Expr: logical.Expr{Kind: logical.ExprColumn, Type: col.Type, Column: col.Name}})
			}
			if err := bindScanOrderBy(&plan, columns, stmt.OrderBy); err != nil {
				return logical.Query{}, err
			}
			return bindScanClauses(&plan, columns, stmt)
		}
	}
	for _, sel := range stmt.Select {
		out, err := bindScanOutputExpr(columns, sel)
		if err != nil {
			return logical.Query{}, err
		}
		plan.SelectOutputs = append(plan.SelectOutputs, out)
	}
	if err := bindScanOrderBy(&plan, columns, stmt.OrderBy); err != nil {
		return logical.Query{}, err
	}
	return bindScanClauses(&plan, columns, stmt)
}

func bindScanOutputExpr(columns map[string]catalog.ColumnDef, sel ast.SelectExpr) (logical.OutputExpr, error) {
	bound, err := bindExpr(columns, sel.Expr)
	if err != nil {
		return logical.OutputExpr{}, err
	}
	switch bound.Kind {
	case logical.ExprColumn:
		return logical.OutputExpr{Alias: sel.Alias, Expr: bound}, nil
	case logical.ExprLiteral:
		if sel.Alias == "" {
			return logical.OutputExpr{}, fmt.Errorf("scan SELECT literal expressions require an alias")
		}
		return logical.OutputExpr{Alias: sel.Alias, Expr: bound}, nil
	case logical.ExprBinary, logical.ExprUnary:
		if sel.Alias == "" {
			return logical.OutputExpr{}, fmt.Errorf("scan SELECT computed expressions require an alias")
		}
		if !isScanComputedOp(bound.Op) {
			return logical.OutputExpr{}, fmt.Errorf("scan SELECT only supports column, literal, and computed expressions")
		}
		return logical.OutputExpr{Alias: sel.Alias, Expr: bound}, nil
	default:
		return logical.OutputExpr{}, fmt.Errorf("scan SELECT only supports column, literal, and computed expressions")
	}
}

func isScanComputedOp(op logical.Op) bool {
	switch op {
	case logical.OpAdd, logical.OpSubtract, logical.OpMultiply, logical.OpDivide, logical.OpModulo, logical.OpIntDivide,
		logical.OpConcat, logical.OpLower, logical.OpUpper:
		return true
	}
	return false
}

var arithOps = map[ast.BinaryOp]logical.Op{
	ast.BinaryAdd:       logical.OpAdd,
	ast.BinarySubtract:  logical.OpSubtract,
	ast.BinaryMultiply:  logical.OpMultiply,
	ast.BinaryDivide:    logical.OpDivide,
	ast.BinaryModulo:    logical.OpModulo,
	ast.BinaryIntDivide: logical.OpIntDivide,
}

func logicalExprEqual(left logical.Expr, right logical.Expr) bool {
	if left.Kind != right.Kind || left.Op != right.Op {
		return false
	}
	switch left.Kind {
	case logical.ExprColumn:
		return normalizeName(left.Column) == normalizeName(right.Column)
	case logical.ExprLiteral:
		return left.Literal == right.Literal
	case logical.ExprBinary, logical.ExprUnary:
		if !logicalExprEqualPtr(left.Left, right.Left) {
			return false
		}
		if !logicalExprEqualPtr(left.Right, right.Right) {
			return false
		}
		return true
	}
	return false
}

func logicalExprEqualPtr(left, right *logical.Expr) bool {
	if left == nil || right == nil {
		return left == right
	}
	return logicalExprEqual(*left, *right)
}

func literalExpr(value ast.Value) logical.Expr {
	expr := logical.Expr{Kind: logical.ExprLiteral, Literal: literalValue(value)}
	switch value.Kind {
	case ast.ValueBool:
		expr.Type = sqltype.Bool
	case ast.ValueInt:
		expr.Type = sqltype.Int64
	case ast.ValueFloat:
		expr.Type = sqltype.Float64
	case ast.ValueString:
		expr.Type = sqltype.Text
	}
	return expr
}

func literalValue(value ast.Value) any {
	switch value.Kind {
	case ast.ValueBool:
		return value.Bool
	case ast.ValueInt:
		return value.Int
	case ast.ValueFloat:
		return value.Float
	case ast.ValueString:
		return value.String
	case ast.ValueNull:
		return nil
	default:
		return nil
	}
}

func bindScanClauses(plan *logical.Query, columns map[string]catalog.ColumnDef, stmt *ast.SelectStmt) (logical.Query, error) {
	if stmt.Where != nil {
		where, err := bindWhereExpr(columns, stmt.Where)
		if err != nil {
			return logical.Query{}, err
		}
		if err := bindWhereClause(plan, where); err != nil {
			return logical.Query{}, err
		}
	}
	if err := bindLimit(plan, stmt.Limit); err != nil {
		return logical.Query{}, err
	}
	if err := bindOffset(plan, stmt.Offset); err != nil {
		return logical.Query{}, err
	}
	return *plan, nil
}

func bindScanOrderBy(plan *logical.Query, columns map[string]catalog.ColumnDef, orderBy []ast.OrderExpr) error {
	lookup := func(name string) (string, logical.Expr, bool) {
		for _, output := range plan.SelectOutputs {
			if name == normalizeName(outputExprName(output)) {
				return outputExprName(output), output.Expr, true
			}
		}
		return "", logical.Expr{}, false
	}
	return bindOrderByCommon(plan, orderBy, lookup, func(e ast.Expr) (logical.Expr, error) {
		return bindExpr(columns, e)
	})
}

// bindOrderByCommon resolves the single ORDER BY clause: lookup matches the name against
// caller-defined targets; bindCustom binds an explicit ORDER BY expression when no name matches.
func bindOrderByCommon(plan *logical.Query, orderBy []ast.OrderExpr, lookup func(name string) (string, logical.Expr, bool), bindCustom func(ast.Expr) (logical.Expr, error)) error {
	if len(orderBy) == 0 {
		return nil
	}
	if len(orderBy) != 1 {
		return fmt.Errorf("only one ORDER BY expression is supported")
	}
	order := orderBy[0]
	name := normalizeName(order.Name)
	if column, expr, ok := lookup(name); ok {
		plan.OrderColumn = column
		exprCopy := expr
		plan.OrderExpr = &exprCopy
		plan.OrderDesc = order.Desc
		return nil
	}
	if order.Expr == nil {
		return fmt.Errorf("ORDER BY column %q must be a selected output column", order.Name)
	}
	bound, err := bindCustom(order.Expr)
	if err != nil {
		return err
	}
	plan.OrderExpr = &bound
	plan.OrderDesc = order.Desc
	return nil
}

func outputExprName(output logical.OutputExpr) string {
	if output.Alias != "" {
		return output.Alias
	}
	if output.Expr.Kind == logical.ExprColumn {
		return output.Expr.Column
	}
	return ""
}

func bindLimit(plan *logical.Query, limit *int64) error {
	if limit == nil {
		return nil
	}
	if *limit < 0 {
		return fmt.Errorf("LIMIT must be non-negative")
	}
	plan.HasLimit = true
	plan.Limit = uint64(*limit)
	return nil
}

func bindOffset(plan *logical.Query, offset *int64) error {
	if offset == nil {
		return nil
	}
	if *offset < 0 {
		return fmt.Errorf("OFFSET must be non-negative")
	}
	plan.HasOffset = true
	plan.Offset = uint64(*offset)
	return nil
}

func bindHaving(plan *logical.Query, columns map[string]catalog.ColumnDef, having ast.Expr) error {
	if having == nil {
		return nil
	}
	expr, err := bindHavingLogicalExpr(plan, columns, having)
	if err != nil {
		return err
	}
	if err := validateHavingEnumLabelsExpr(plan, columns, expr); err != nil {
		return err
	}
	plan.HasHaving = true
	plan.HavingFilterExpr = &expr
	return nil
}

// validateHavingEnumLabelsExpr rejects HAVING literals that don't match the column's enum/temporal/uuid format.
func validateHavingEnumLabelsExpr(plan *logical.Query, columns map[string]catalog.ColumnDef, expr logical.Expr) error {
	switch expr.Kind {
	case logical.ExprBinary:
		if expr.Left != nil {
			if err := validateHavingEnumLabelsExpr(plan, columns, *expr.Left); err != nil {
				return err
			}
		}
		if expr.Right != nil {
			if err := validateHavingEnumLabelsExpr(plan, columns, *expr.Right); err != nil {
				return err
			}
		}
		if expr.Left != nil && expr.Right != nil {
			if err := havingLiteralCheck(plan, columns, *expr.Left, *expr.Right); err != nil {
				return err
			}
			if err := havingLiteralCheck(plan, columns, *expr.Right, *expr.Left); err != nil {
				return err
			}
		}
	case logical.ExprUnary:
		if expr.Left != nil {
			return validateHavingEnumLabelsExpr(plan, columns, *expr.Left)
		}
	case logical.ExprIn, logical.ExprBetween:
		if expr.Left != nil && expr.Left.Kind == logical.ExprColumn {
			for _, arg := range expr.Args {
				if err := havingLiteralCheck(plan, columns, *expr.Left, arg); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func havingLiteralCheck(plan *logical.Query, columns map[string]catalog.ColumnDef, col logical.Expr, lit logical.Expr) error {
	if col.Kind != logical.ExprColumn || lit.Kind != logical.ExprLiteral {
		return nil
	}
	value, ok := lit.Literal.(string)
	if !ok {
		return nil
	}
	switch col.Type.Kind {
	case sqltype.KindNamed:
		groupCol := plan.GroupColumn
		if normalizeName(col.Column) != normalizeName(groupResultName(*plan)) {
			groupCol = col.Column
		}
		def, ok := columns[normalizeName(groupCol)]
		if !ok {
			return nil
		}
		if _, ok := enumCodeForLabel(value, def.Labels); !ok {
			return fmt.Errorf("HAVING column %q invalid enum label %q", col.Column, value)
		}
	case sqltype.KindTimestamp:
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return fmt.Errorf("HAVING column %q invalid timestamp literal %q", col.Column, value)
		}
	case sqltype.KindDate:
		if _, err := time.Parse("2006-01-02", value); err != nil {
			return fmt.Errorf("HAVING column %q invalid date literal %q", col.Column, value)
		}
	case sqltype.KindUUID:
		if _, err := vector.ParseUUID(value); err != nil {
			return fmt.Errorf("HAVING column %q invalid uuid literal %q", col.Column, value)
		}
	}
	return nil
}

func bindHavingLogicalExpr(plan *logical.Query, columns map[string]catalog.ColumnDef, having ast.Expr) (logical.Expr, error) {
	switch expr := having.(type) {
	case *ast.AndExpr:
		left, err := bindHavingLogicalExpr(plan, columns, expr.Left)
		if err != nil {
			return logical.Expr{}, err
		}
		right, err := bindHavingLogicalExpr(plan, columns, expr.Right)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: logical.OpAnd, Left: &left, Right: &right}, nil
	case *ast.OrExpr:
		left, err := bindHavingLogicalExpr(plan, columns, expr.Left)
		if err != nil {
			return logical.Expr{}, err
		}
		right, err := bindHavingLogicalExpr(plan, columns, expr.Right)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: logical.OpOr, Left: &left, Right: &right}, nil
	case *ast.NotExpr:
		child, err := bindHavingLogicalExpr(plan, columns, expr.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprUnary, Type: sqltype.Bool, Op: logical.OpNot, Left: &child}, nil
	case *ast.BinaryExpr:
		if isArithmeticOp(expr.Op) {
			return logical.Expr{}, fmt.Errorf("HAVING requires a predicate expression")
		}
		left, err := bindHavingScalarExpr(plan, columns, expr.Left)
		if err != nil {
			return logical.Expr{}, err
		}
		right, err := bindHavingScalarExpr(plan, columns, expr.Right)
		if err != nil {
			return logical.Expr{}, err
		}
		op, err := bindBinaryOp(expr.Op)
		if err != nil {
			return logical.Expr{}, err
		}
		if err := validateHavingComparison(left, op, right); err != nil {
			return logical.Expr{}, err
		}
		normalizeComparison(&left, &right)
		exprOp := filterOpToExprOp(op)
		return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: exprOp, Left: &left, Right: &right}, nil
	case *ast.BetweenExpr:
		target, err := bindHavingScalarExpr(plan, columns, expr.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		low, err := bindHavingScalarExpr(plan, columns, expr.Low)
		if err != nil {
			return logical.Expr{}, err
		}
		high, err := bindHavingScalarExpr(plan, columns, expr.High)
		if err != nil {
			return logical.Expr{}, err
		}
		if err := validateBetween(target, low, high, havingBetweenRules); err != nil {
			return logical.Expr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return logical.Expr{Kind: logical.ExprBetween, Type: sqltype.Bool, Left: &target, Args: []logical.Expr{low, high}}, nil
	case *ast.InExpr:
		target, err := bindHavingScalarExpr(plan, columns, expr.Expr)
		if err != nil {
			return logical.Expr{}, err
		}
		values := make([]logical.Expr, 0, len(expr.Values))
		for _, value := range expr.Values {
			bound, err := bindHavingScalarExpr(plan, columns, value)
			if err != nil {
				return logical.Expr{}, err
			}
			if err := validateWhereInValue(target, bound); err != nil {
				return logical.Expr{}, err
			}
			if target.Type.Kind == sqltype.KindUUID {
				if err := validateHavingUUIDBound(bound); err != nil {
					return logical.Expr{}, err
				}
			}
			normalizeBound(target, &bound)
			values = append(values, bound)
		}
		return logical.Expr{Kind: logical.ExprIn, Type: sqltype.Bool, Left: &target, Args: values, Not: expr.Not}, nil
	default:
		return logical.Expr{}, fmt.Errorf("unsupported HAVING expression")
	}
}

func bindHavingScalarExpr(plan *logical.Query, columns map[string]catalog.ColumnDef, expr ast.Expr) (logical.Expr, error) {
	switch expr := expr.(type) {
	case *ast.ColumnRef:
		name := normalizeName(expr.Name)
		if name == normalizeName(aggregateResultName(*plan)) {
			return logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Int64, Column: aggregateResultName(*plan)}, nil
		}
		if name == normalizeName(groupResultName(*plan)) {
			return logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Type{Kind: plan.GroupType}, Column: groupResultName(*plan)}, nil
		}
		for _, agg := range plan.HavingAggregates {
			if name == normalizeName(agg.Column) {
				return logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Int64, Column: agg.Column}, nil
			}
		}
		return logical.Expr{}, fmt.Errorf("HAVING column %q must be a selected output column", expr.Name)
	case *ast.FuncCall:
		if !isAggregateName(expr.Name) {
			return bindHavingOutputScalarExpr(plan, expr)
		}
		return bindHavingAggregateExpr(plan, columns, expr)
	case *ast.Literal:
		return literalExpr(expr.Value), nil
	case *ast.BinaryExpr:
		if expr.Op == ast.BinaryConcat {
			return bindHavingOutputScalarExpr(plan, expr)
		}
		if !isArithmeticOp(expr.Op) {
			return logical.Expr{}, fmt.Errorf("HAVING scalar expression contains a predicate operator")
		}
		left, err := bindHavingScalarExpr(plan, columns, expr.Left)
		if err != nil {
			return logical.Expr{}, err
		}
		right, err := bindHavingScalarExpr(plan, columns, expr.Right)
		if err != nil {
			return logical.Expr{}, err
		}
		if !isIntegerExpr(left) || !isIntegerExpr(right) {
			return logical.Expr{}, fmt.Errorf("HAVING arithmetic expressions require integer operands")
		}
		op, ok := arithOps[expr.Op]
		if !ok {
			return logical.Expr{}, fmt.Errorf("unsupported HAVING arithmetic operator")
		}
		return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Int64, Op: op, Left: &left, Right: &right}, nil
	default:
		return logical.Expr{}, fmt.Errorf("unsupported HAVING scalar expression")
	}
}

func bindHavingOutputScalarExpr(plan *logical.Query, expr ast.Expr) (logical.Expr, error) {
	columns := []catalog.ColumnDef{{Name: aggregateResultName(*plan), Type: sqltype.Int64}}
	if name := groupResultName(*plan); name != "" {
		columns = append(columns, catalog.ColumnDef{Name: name, Type: sqltype.Type{Kind: plan.GroupType}})
	}
	for _, agg := range plan.HavingAggregates {
		columns = append(columns, catalog.ColumnDef{Name: agg.Column, Type: sqltype.Int64})
	}
	return bindExpr(buildColumnIndex(columns), expr)
}

func bindHavingAggregateExpr(plan *logical.Query, columns map[string]catalog.ColumnDef, call *ast.FuncCall) (logical.Expr, error) {
	clause, err := decomposeHavingLeft(call)
	if err != nil {
		return logical.Expr{}, err
	}
	selectedAggregate := aggregateResultName(*plan)
	if havingAggregateMatchesPlan(*plan, clause) {
		return logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Int64, Column: selectedAggregate}, nil
	}
	hiddenColumn, aggregate, err := bindExtraHavingAggregate(plan, columns, clause)
	if err != nil {
		return logical.Expr{}, err
	}
	_ = aggregate
	return logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Int64, Column: hiddenColumn}, nil
}

func validateHavingComparison(left logical.Expr, op logical.FilterOp, right logical.Expr) error {
	return validateComparison(left, op, right, havingCmpRules)
}

// havingMismatchError returns a column-aware "expects/invalid X literal" error for HAVING type mismatches.
func havingMismatchError(col logical.Expr, lit logical.Expr) error {
	if col.Kind != logical.ExprColumn || lit.Kind != logical.ExprLiteral {
		return nil
	}
	switch col.Type.Kind {
	case sqltype.KindBool:
		return fmt.Errorf("HAVING column %q expects bool literal", col.Column)
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64:
		return fmt.Errorf("HAVING column %q expects int literal", col.Column)
	case sqltype.KindFloat32, sqltype.KindFloat64:
		return fmt.Errorf("HAVING column %q expects numeric literal", col.Column)
	case sqltype.KindTimestamp:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid timestamp literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects timestamp string literal", col.Column)
	case sqltype.KindDate:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid date literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects date string literal", col.Column)
	case sqltype.KindUUID:
		if value, ok := lit.Literal.(string); ok {
			return fmt.Errorf("HAVING column %q invalid uuid literal %q", col.Column, value)
		}
		return fmt.Errorf("HAVING column %q expects uuid string literal", col.Column)
	default:
		return fmt.Errorf("HAVING column %q expects string literal", col.Column)
	}
}

// checkCountLiteralRange rejects negative literals compared against count columns.
func checkCountLiteralRange(col logical.Expr, lit logical.Expr) error {
	if col.Kind != logical.ExprColumn || lit.Kind != logical.ExprLiteral {
		return nil
	}
	if normalizeName(col.Column) != "count" {
		return nil
	}
	if value, ok := lit.Literal.(int64); ok && value < 0 {
		return fmt.Errorf("HAVING count literal must be non-negative")
	}
	return nil
}


func bindExtraHavingAggregate(plan *logical.Query, columns map[string]catalog.ColumnDef, clause havingClause) (string, logical.AggregateFunc, error) {
	aggregate, err := havingAggregateFunc(clause.AggregateName)
	if err != nil {
		return "", 0, err
	}
	if aggregate == logical.AggregateCount {
		if !clause.AggregateStar {
			col, ok := columns[normalizeName(clause.AggregateColumn)]
			if !ok {
				return "", 0, fmt.Errorf("missing HAVING aggregate column %q", clause.AggregateColumn)
			}
			clause.AggregateColumn = col.Name
		}
	} else {
		if clause.AggregateStar {
			return "", 0, fmt.Errorf("HAVING aggregate %s requires a column", clause.AggregateName)
		}
		col, ok := columns[normalizeName(clause.AggregateColumn)]
		if !ok {
			return "", 0, fmt.Errorf("missing HAVING aggregate column %q", clause.AggregateColumn)
		}
		if err := validateIntAggregateColumn(clause.AggregateName, col); err != nil {
			return "", 0, err
		}
		clause.AggregateColumn = col.Name
	}
	hiddenColumn := hiddenHavingAggregateColumn(aggregate, clause.AggregateColumn, clause.AggregateStar)
	for _, existing := range plan.HavingAggregates {
		if existing.Column == hiddenColumn {
			return hiddenColumn, aggregate, nil
		}
	}
	plan.HavingAggregates = append(plan.HavingAggregates, logical.HavingAggregate{Column: hiddenColumn, Aggregate: aggregate, ArgColumn: clause.AggregateColumn})
	plan.HiddenColumns = append(plan.HiddenColumns, hiddenColumn)
	return hiddenColumn, aggregate, nil
}

var nameToAgg = map[string]logical.AggregateFunc{
	"count": logical.AggregateCount,
	"sum":   logical.AggregateSum,
	"min":   logical.AggregateMin,
	"max":   logical.AggregateMax,
}

var aggregateNames = map[logical.AggregateFunc]string{
	logical.AggregateCount: "count",
	logical.AggregateSum:   "sum",
	logical.AggregateMin:   "min",
	logical.AggregateMax:   "max",
}

func havingAggregateFunc(name string) (logical.AggregateFunc, error) {
	if fn, ok := nameToAgg[normalizeName(name)]; ok {
		return fn, nil
	}
	return 0, fmt.Errorf("unsupported HAVING aggregate %q", name)
}

func hiddenHavingAggregateColumn(aggregate logical.AggregateFunc, column string, star bool) string {
	if aggregate == logical.AggregateCount && star {
		return "__having_count_star"
	}
	return "__having_" + defaultAggregateName(aggregate) + "_" + normalizeName(column)
}

type havingClause struct {
	Name            string
	AggregateName   string
	AggregateColumn string
	AggregateStar   bool
}

func decomposeHavingLeft(expr ast.Expr) (havingClause, error) {
	switch expr := expr.(type) {
	case *ast.ColumnRef:
		return havingClause{Name: expr.Name}, nil
	case *ast.FuncCall:
		if !isAggregateName(expr.Name) {
			return havingClause{}, fmt.Errorf("HAVING left side must be an output column or aggregate call")
		}
		clause := havingClause{AggregateName: normalizeName(expr.Name), AggregateStar: expr.Star}
		if expr.Star {
			if len(expr.Args) != 0 {
				return havingClause{}, fmt.Errorf("HAVING aggregate star cannot have arguments")
			}
			return clause, nil
		}
		if len(expr.Args) != 1 {
			return havingClause{}, fmt.Errorf("HAVING aggregate must have one argument")
		}
		col, ok := expr.Args[0].(*ast.ColumnRef)
		if !ok {
			return havingClause{}, fmt.Errorf("HAVING aggregate argument must be a column")
		}
		clause.AggregateColumn = col.Name
		return clause, nil
	default:
		return havingClause{}, fmt.Errorf("HAVING left side must be an output column or aggregate call")
	}
}

func havingAggregateMatchesPlan(plan logical.Query, clause havingClause) bool {
	if normalizeName(clause.AggregateName) != defaultAggregateName(plan.Aggregate) {
		return false
	}
	if plan.Aggregate == logical.AggregateCount && plan.AggregateColumn == "" {
		return clause.AggregateStar
	}
	if clause.AggregateStar {
		return false
	}
	return normalizeName(clause.AggregateColumn) == normalizeName(plan.AggregateColumn)
}

func bindOrderBy(plan *logical.Query, orderBy []ast.OrderExpr) error {
	groupName := normalizeName(plan.GroupColumn)
	if plan.GroupAlias != "" {
		groupName = normalizeName(plan.GroupAlias)
	}
	aggName := normalizeName(defaultAggregateName(plan.Aggregate))
	if plan.AggregateAlias != "" {
		aggName = normalizeName(plan.AggregateAlias)
	}
	lookup := func(name string) (string, logical.Expr, bool) {
		switch name {
		case groupName:
			col := groupResultName(*plan)
			return col, logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Type{Kind: plan.GroupType}, Column: col}, true
		case aggName:
			col := aggregateResultName(*plan)
			return col, logical.Expr{Kind: logical.ExprColumn, Type: sqltype.Int64, Column: col}, true
		}
		return "", logical.Expr{}, false
	}
	return bindOrderByCommon(plan, orderBy, lookup, func(e ast.Expr) (logical.Expr, error) {
		columns := []catalog.ColumnDef{
			{Name: groupResultName(*plan), Type: sqltype.Type{Kind: plan.GroupType}},
			{Name: aggregateResultName(*plan), Type: sqltype.Int64},
		}
		for _, agg := range plan.HavingAggregates {
			columns = append(columns, catalog.ColumnDef{Name: agg.Column, Type: sqltype.Int64})
		}
		return bindExpr(buildColumnIndex(columns), e)
	})
}

func defaultAggregateName(aggregate logical.AggregateFunc) string {
	if name, ok := aggregateNames[aggregate]; ok {
		return name
	}
	return "count"
}

func aggregateResultName(plan logical.Query) string {
	if plan.AggregateAlias != "" {
		return plan.AggregateAlias
	}
	return defaultAggregateName(plan.Aggregate)
}

func groupResultName(plan logical.Query) string {
	if plan.GroupAlias != "" {
		return plan.GroupAlias
	}
	return plan.GroupColumn
}

func validateIntAggregateColumn(name string, col catalog.ColumnDef) error {
	switch col.Type.Kind {
	case sqltype.KindInt32, sqltype.KindInt64:
		return nil
	default:
		return fmt.Errorf("%s column %q is %s, want int32 or int64", name, col.Name, col.Type)
	}
}

func bindWhereExpr(columns map[string]catalog.ColumnDef, expr ast.Expr) (logical.Expr, error) {
	bound, err := bindExpr(columns, expr)
	if err == nil {
		return bound, nil
	}
	if col := whereColumnRef(expr); col != "" {
		if _, ok := findColumn(columns, col); !ok {
			return logical.Expr{}, fmt.Errorf("missing WHERE column %q", col)
		}
	}
	return logical.Expr{}, err
}

func whereColumnRef(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		col, _ := expr.Left.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *ast.BetweenExpr:
		col, _ := expr.Expr.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *ast.InExpr:
		col, _ := expr.Expr.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	default:
		return ""
	}
}

func bindGroupExpr(columns map[string]catalog.ColumnDef, groupBy []ast.Expr) (*logical.Expr, error) {
	if len(groupBy) == 0 {
		return nil, nil
	}
	if len(groupBy) != 1 {
		return nil, fmt.Errorf("only one GROUP BY expression is supported")
	}
	bound, err := bindExpr(columns, groupBy[0])
	if err != nil {
		if col, ok := groupBy[0].(*ast.ColumnRef); ok {
			return nil, fmt.Errorf("missing GROUP BY column %q", col.Name)
		}
		return nil, err
	}
	if bound.Type.Kind == sqltype.KindInvalid {
		return nil, fmt.Errorf("unsupported GROUP BY expression")
	}
	return &bound, nil
}

// selectedAggregate finds the aggregate slot in SELECT and returns its function/column/star, working from the AST.
func selectedAggregate(exprs []ast.SelectExpr, columns map[string]catalog.ColumnDef, grouped bool) (fn logical.AggregateFunc, column string, star bool, ok bool) {
	index := aggregateSelectIndex(exprs, grouped)
	if index < 0 {
		return 0, "", false, false
	}
	call, callOK := exprs[index].Expr.(*ast.FuncCall)
	if !callOK {
		return 0, "", false, false
	}
	fn, ok = nameToAgg[normalizeName(call.Name)]
	if !ok {
		return 0, "", false, false
	}
	if call.Star {
		if fn != logical.AggregateCount || len(call.Args) != 0 {
			return 0, "", false, false
		}
		return fn, "", true, true
	}
	if len(call.Args) != 1 {
		return 0, "", false, false
	}
	colRef, refOK := call.Args[0].(*ast.ColumnRef)
	if !refOK {
		return 0, "", false, false
	}
	def, defOK := findColumn(columns, colRef.Name)
	if !defOK {
		return 0, "", false, false
	}
	return fn, def.Name, false, true
}

func aggregateSelectIndex(exprs []ast.SelectExpr, grouped bool) int {
	if grouped {
		if len(exprs) != 2 {
			return -1
		}
		return 1
	}
	if len(exprs) != 1 {
		return -1
	}
	return 0
}

func bindWhereClause(plan *logical.Query, where logical.Expr) error {
	whereExpr, err := bindWhereLogicalExpr(where)
	if err != nil {
		return err
	}
	plan.HasFilter = true
	plan.WhereExpr = &whereExpr
	return nil
}

// bindWhereLogicalExpr walks an already-lowered WHERE expression to validate compatibility and normalize temporal/UUID literals.
func bindWhereLogicalExpr(expr logical.Expr) (logical.Expr, error) {
	switch expr.Kind {
	case logical.ExprBinary:
		switch expr.Op {
		case logical.OpAnd, logical.OpOr:
			left, err := bindWhereLogicalExpr(*expr.Left)
			if err != nil {
				return logical.Expr{}, err
			}
			right, err := bindWhereLogicalExpr(*expr.Right)
			if err != nil {
				return logical.Expr{}, err
			}
			return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: expr.Op, Left: &left, Right: &right}, nil
		case logical.OpEqual, logical.OpNotEqual, logical.OpLess, logical.OpLessEqual, logical.OpGreater, logical.OpGreaterEqual:
			left := *expr.Left
			right := *expr.Right
			filterOp := exprAsFilter[expr.Op]
			if err := validateWhereComparison(left, filterOp, right); err != nil {
				return logical.Expr{}, err
			}
			normalizeComparison(&left, &right)
			return logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: expr.Op, Left: &left, Right: &right}, nil
		default:
			return logical.Expr{}, fmt.Errorf("unsupported WHERE expression")
		}
	case logical.ExprUnary:
		if expr.Op != logical.OpNot {
			return logical.Expr{}, fmt.Errorf("unsupported WHERE expression")
		}
		child, err := bindWhereLogicalExpr(*expr.Left)
		if err != nil {
			return logical.Expr{}, err
		}
		return logical.Expr{Kind: logical.ExprUnary, Type: sqltype.Bool, Op: logical.OpNot, Left: &child}, nil
	case logical.ExprBetween:
		target := *expr.Left
		low := expr.Args[0]
		high := expr.Args[1]
		if err := validateBetween(target, low, high, whereBetweenRules); err != nil {
			return logical.Expr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return logical.Expr{Kind: logical.ExprBetween, Type: sqltype.Bool, Left: &target, Args: []logical.Expr{low, high}}, nil
	case logical.ExprIn:
		target := *expr.Left
		values := make([]logical.Expr, 0, len(expr.Args))
		for _, v := range expr.Args {
			if err := validateWhereInValue(target, v); err != nil {
				return logical.Expr{}, err
			}
			normalizeBound(target, &v)
			values = append(values, v)
		}
		return logical.Expr{Kind: logical.ExprIn, Type: sqltype.Bool, Left: &target, Args: values, Not: expr.Not}, nil
	default:
		return logical.Expr{}, fmt.Errorf("unsupported WHERE expression")
	}
}

var exprAsFilter = map[logical.Op]logical.FilterOp{
	logical.OpEqual:        logical.FilterEqual,
	logical.OpNotEqual:     logical.FilterNotEqual,
	logical.OpLess:         logical.FilterLess,
	logical.OpLessEqual:    logical.FilterLessEqual,
	logical.OpGreater:      logical.FilterGreater,
	logical.OpGreaterEqual: logical.FilterGreaterEqual,
}

func arithmeticResultType(op logical.Op, left logical.Expr, right logical.Expr) (sqltype.Type, error) {
	if op == logical.OpModulo || op == logical.OpIntDivide {
		if !isIntegerExpr(left) || !isIntegerExpr(right) {
			return sqltype.Type{}, fmt.Errorf("MOD and DIV require integer operands")
		}
		return sqltype.Int64, nil
	}
	if !isNumericExpr(left) || !isNumericExpr(right) {
		return sqltype.Type{}, fmt.Errorf("arithmetic expressions require numeric operands")
	}
	if classOf(left)&cFloat != 0 || classOf(right)&cFloat != 0 {
		return sqltype.Float64, nil
	}
	return sqltype.Int64, nil
}

func validateWhereComparison(left logical.Expr, op logical.FilterOp, right logical.Expr) error {
	return validateComparison(left, op, right, whereCmpRules)
}

type cmpRules struct {
	label               string
	intRangeCheck       func(col, lit logical.Expr) error
	allowFloat          bool
	textOrderingAllowed bool
	uuidExtraCheck      func(logical.Expr) error
	mismatchError       func(col, lit logical.Expr) error
}

var whereCmpRules = cmpRules{
	label:         "WHERE",
	intRangeCheck: checkInt32LiteralRange,
	allowFloat:    true,
	mismatchError: columnLiteralMismatchError,
}

var havingCmpRules = cmpRules{
	label:               "HAVING",
	intRangeCheck:       checkCountLiteralRange,
	textOrderingAllowed: true,
	uuidExtraCheck:      validateHavingUUIDBound,
	mismatchError:       havingMismatchError,
}

func validateComparison(left logical.Expr, op logical.FilterOp, right logical.Expr, r cmpRules) error {
	if isIntegerExpr(left) && isIntegerExpr(right) {
		if err := r.intRangeCheck(left, right); err != nil {
			return err
		}
		return r.intRangeCheck(right, left)
	}
	if r.allowFloat && isNumericExpr(left) && isNumericExpr(right) {
		return nil
	}
	if left.Type.Kind == sqltype.KindText && right.Type.Kind == sqltype.KindText {
		if !r.textOrderingAllowed && isOrderingFilter(op) {
			return fmt.Errorf("%s text comparisons only support = and !=", r.label)
		}
		return nil
	}
	if left.Type.Kind == sqltype.KindBool && right.Type.Kind == sqltype.KindBool {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s bool comparisons only support = and !=", r.label)
		}
		return nil
	}
	if areXComparable(sqltype.KindDate, left, right) {
		return nil
	}
	if areXComparable(sqltype.KindTimestamp, left, right) {
		return nil
	}
	if areXComparable(sqltype.KindUUID, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s uuid comparisons only support = and !=", r.label)
		}
		if r.uuidExtraCheck != nil {
			if err := r.uuidExtraCheck(left); err != nil {
				return err
			}
			if err := r.uuidExtraCheck(right); err != nil {
				return err
			}
		}
		return nil
	}
	if areXComparable(sqltype.KindBytes, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s bytes comparisons only support = and !=", r.label)
		}
		return nil
	}
	if areXComparable(sqltype.KindNamed, left, right) {
		if isOrderingFilter(op) {
			return fmt.Errorf("%s enum comparisons only support = and !=", r.label)
		}
		return nil
	}
	if err := r.mismatchError(left, right); err != nil {
		return err
	}
	if err := r.mismatchError(right, left); err != nil {
		return err
	}
	return fmt.Errorf("%s comparison operands have incompatible types", r.label)
}

// columnLiteralMismatchError returns "WHERE column X expects Y literal" when col is a column and lit is an incompatible literal.
func columnLiteralMismatchError(col logical.Expr, lit logical.Expr) error {
	if col.Kind != logical.ExprColumn || lit.Kind != logical.ExprLiteral {
		return nil
	}
	return fmt.Errorf("WHERE column %q expects %s literal", col.Column, literalKindName(col.Type.Kind))
}

// literalKindName returns the user-facing literal name for a column kind (matches insert.go style).
func literalKindName(kind sqltype.Kind) string {
	switch kind {
	case sqltype.KindBool:
		return "bool"
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64:
		return "int64"
	case sqltype.KindFloat32, sqltype.KindFloat64:
		return "numeric"
	case sqltype.KindText, sqltype.KindBytes:
		return "string"
	case sqltype.KindDate:
		return "date string"
	case sqltype.KindTimestamp:
		return "timestamp string"
	case sqltype.KindUUID:
		return "uuid string"
	case sqltype.KindNamed:
		return "enum string"
	default:
		return "compatible"
	}
}

// checkInt32LiteralRange rejects int literals outside int32's range when col is int16/int32.
func checkInt32LiteralRange(col logical.Expr, lit logical.Expr) error {
	if col.Kind != logical.ExprColumn || lit.Kind != logical.ExprLiteral {
		return nil
	}
	if col.Type.Kind != sqltype.KindInt16 && col.Type.Kind != sqltype.KindInt32 {
		return nil
	}
	value, ok := lit.Literal.(int64)
	if !ok || (value >= math.MinInt32 && value <= math.MaxInt32) {
		return nil
	}
	return fmt.Errorf("WHERE column %q int32 literal out of range", col.Column)
}

type betweenRules struct {
	label      string
	allowFloat bool
	allowText  bool
	mismatch   string
}

var (
	whereBetweenRules  = betweenRules{label: "WHERE", allowFloat: true, mismatch: "WHERE BETWEEN is only supported for numeric and temporal expressions"}
	havingBetweenRules = betweenRules{label: "HAVING", allowText: true, mismatch: "HAVING BETWEEN operands have incompatible types"}
)

func validateBetween(target, low, high logical.Expr, r betweenRules) error {
	if isIntegerExpr(target) && isIntegerExpr(low) && isIntegerExpr(high) {
		return nil
	}
	if r.allowFloat && isNumericExpr(target) && isNumericExpr(low) && isNumericExpr(high) {
		return nil
	}
	if r.allowText && target.Type.Kind == sqltype.KindText && low.Type.Kind == sqltype.KindText && high.Type.Kind == sqltype.KindText {
		return nil
	}
	if target.Type.Kind == sqltype.KindDate && textBound(sqltype.KindDate, low) && textBound(sqltype.KindDate, high) {
		return nil
	}
	if target.Type.Kind == sqltype.KindTimestamp && textBound(sqltype.KindTimestamp, low) && textBound(sqltype.KindTimestamp, high) {
		return nil
	}
	return fmt.Errorf("%s", r.mismatch)
}

func validateWhereInValue(target logical.Expr, value logical.Expr) error {
	if isIntegerExpr(target) && isIntegerExpr(value) {
		return nil
	}
	if isNumericExpr(target) && isNumericExpr(value) {
		return nil
	}
	if target.Type.Kind == sqltype.KindText && value.Type.Kind == sqltype.KindText {
		return nil
	}
	if target.Type.Kind == sqltype.KindBool && value.Type.Kind == sqltype.KindBool {
		return nil
	}
	if target.Type.Kind == sqltype.KindDate && textBound(sqltype.KindDate, value) {
		return nil
	}
	if target.Type.Kind == sqltype.KindTimestamp && textBound(sqltype.KindTimestamp, value) {
		return nil
	}
	if target.Type.Kind == sqltype.KindUUID && textBound(sqltype.KindUUID, value) {
		return nil
	}
	if target.Type.Kind == sqltype.KindBytes && textBound(sqltype.KindBytes, value) {
		return nil
	}
	if target.Type.Kind == sqltype.KindNamed && textBound(sqltype.KindNamed, value) {
		return nil
	}
	return fmt.Errorf("WHERE IN operands have incompatible types")
}

// literalNormalizer canonicalizes a text literal whose target column has the matching kind.
type literalNormalizer struct {
	targetKind sqltype.Kind
	normalize  func(string) (string, bool)
}

var literalNormalizers = []literalNormalizer{
	{sqltype.KindTimestamp, normalizeTimestampString},
	{sqltype.KindUUID, normalizeUUIDString},
}

// normalizeBound rewrites bound's literal in-place if target's kind has a normalizer.
func normalizeBound(target logical.Expr, bound *logical.Expr) {
	if bound.Kind != logical.ExprLiteral || bound.Type.Kind != sqltype.KindText {
		return
	}
	value, ok := bound.Literal.(string)
	if !ok {
		return
	}
	for _, n := range literalNormalizers {
		if target.Type.Kind != n.targetKind {
			continue
		}
		if v, ok := n.normalize(value); ok {
			bound.Literal = v
		}
		return
	}
}

func normalizeComparison(left, right *logical.Expr) {
	normalizeBound(*left, right)
	normalizeBound(*right, left)
}

func normalizeTimestampString(s string) (string, bool) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return "", false
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z"), true
}

func normalizeUUIDString(s string) (string, bool) {
	u, err := vector.ParseUUID(s)
	if err != nil {
		return "", false
	}
	return vector.FormatUUID(u), true
}

// tclass groups column kinds with shared comparison/bound rules.
type tclass uint16

const (
	cInt tclass = 1 << iota
	cFloat
	cText
	cBool
	cDate
	cTimestamp
	cUUID
	cBytes
	cNamed
)

const cNumeric = cInt | cFloat

var kindClass = map[sqltype.Kind]tclass{
	sqltype.KindInt16: cInt, sqltype.KindInt32: cInt, sqltype.KindInt64: cInt,
	sqltype.KindFloat32: cFloat, sqltype.KindFloat64: cFloat,
	sqltype.KindText:  cText,
	sqltype.KindBool:  cBool,
	sqltype.KindDate:  cDate, sqltype.KindTimestamp: cTimestamp,
	sqltype.KindUUID:  cUUID,
	sqltype.KindBytes: cBytes,
	sqltype.KindNamed: cNamed,
}

func classOf(e logical.Expr) tclass     { return kindClass[e.Type.Kind] }

func isIntegerExpr(e logical.Expr) bool { return classOf(e)&cInt != 0 }
func isNumericExpr(e logical.Expr) bool { return classOf(e)&cNumeric != 0 }

// textBound reports whether expr matches target either by kind or by parseable text literal.
func textBound(target sqltype.Kind, expr logical.Expr) bool {
	if expr.Type.Kind == target {
		return true
	}
	if expr.Kind != logical.ExprLiteral || expr.Type.Kind != sqltype.KindText {
		return target == sqltype.KindUUID || target == sqltype.KindBytes || target == sqltype.KindNamed
	}
	value, ok := expr.Literal.(string)
	if !ok {
		return false
	}
	switch target {
	case sqltype.KindDate:
		_, err := time.Parse("2006-01-02", value)
		return err == nil
	case sqltype.KindTimestamp:
		_, err := time.Parse(time.RFC3339Nano, value)
		return err == nil
	case sqltype.KindUUID, sqltype.KindBytes, sqltype.KindNamed:
		return true
	}
	return false
}

func areXComparable(target sqltype.Kind, left, right logical.Expr) bool {
	if left.Type.Kind == target {
		return textBound(target, right)
	}
	if right.Type.Kind == target {
		return textBound(target, left)
	}
	return false
}

func validateHavingUUIDBound(expr logical.Expr) error {
	if expr.Type.Kind == sqltype.KindUUID || expr.Kind != logical.ExprLiteral || expr.Type.Kind != sqltype.KindText {
		return nil
	}
	value, ok := expr.Literal.(string)
	if !ok {
		return nil
	}
	if _, err := vector.ParseUUID(value); err != nil {
		return fmt.Errorf("HAVING invalid uuid literal %q", value)
	}
	return nil
}

var filterAsExpr = map[logical.FilterOp]logical.Op{
	logical.FilterEqual:        logical.OpEqual,
	logical.FilterNotEqual:     logical.OpNotEqual,
	logical.FilterLess:         logical.OpLess,
	logical.FilterLessEqual:    logical.OpLessEqual,
	logical.FilterGreater:      logical.OpGreater,
	logical.FilterGreaterEqual: logical.OpGreaterEqual,
}

func filterOpToExprOp(op logical.FilterOp) logical.Op {
	if out, ok := filterAsExpr[op]; ok {
		return out
	}
	return logical.OpInvalid
}

func isOrderingFilter(op logical.FilterOp) bool {
	switch op {
	case logical.FilterLess, logical.FilterLessEqual, logical.FilterGreater, logical.FilterGreaterEqual:
		return true
	default:
		return false
	}
}

func buildColumnIndex(columns []catalog.ColumnDef) map[string]catalog.ColumnDef {
	index := make(map[string]catalog.ColumnDef, len(columns))
	for _, col := range columns {
		index[normalizeName(col.Name)] = col
	}
	return index
}

func findColumn(columns map[string]catalog.ColumnDef, name string) (catalog.ColumnDef, bool) {
	col, ok := columns[normalizeName(name)]
	return col, ok
}
