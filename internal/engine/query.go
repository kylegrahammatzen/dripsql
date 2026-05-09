package engine

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (db *DB) executeAggregateQuery(ctx context.Context, def catalog.TableDef, plan logical.Query, scratch *storage.QueryScratch) (*Rows, error) {
	pred, pushed, err := db.predicateFromQuery(def, plan)
	if err != nil {
		return nil, err
	}
	if !pushed {
		return db.executeAggregateQueryRowFilter(ctx, def, plan)
	}
	if hasGroup(plan) {
		if plan.GroupColumn == "" {
			return db.executeAggregateQueryRowFilter(ctx, def, plan)
		}
		return db.executeGroupedAggregateQuery(ctx, def, plan, pred, scratch)
	}

	switch plan.Aggregate {
	case logical.AggregateCount:
		if plan.AggregateColumn == "" {
			count, err := db.data.Count(ctx, def, pred, scratch)
			if err != nil {
				return nil, err
			}
			rows := &Rows{Columns: []string{aggregateResultColumn(plan, "count")}, Values: [][]any{{count}}}
			return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
		}
		col, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing count column %q", plan.AggregateColumn)
		}
		count, err := db.data.CountNonNull(ctx, def, col.ID, pred, scratch)
		if err != nil {
			return nil, err
		}
		rows := &Rows{Columns: []string{aggregateResultColumn(plan, "count")}, Values: [][]any{{count}}}
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	case logical.AggregateSum:
		col, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing SUM column %q", plan.AggregateColumn)
		}
		sum, err := db.data.SumInt(ctx, def, col.ID, pred, scratch)
		if err != nil {
			return nil, err
		}
		rows := &Rows{Columns: []string{aggregateResultColumn(plan, "sum")}, Values: [][]any{{sum}}}
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	case logical.AggregateMin, logical.AggregateMax:
		col, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", plan.AggregateColumn)
		}
		value, found, err := db.data.MinMaxInt(ctx, def, col.ID, pred, plan.Aggregate == logical.AggregateMax, scratch)
		if err != nil {
			return nil, err
		}
		name := "min"
		var result any
		if found {
			result = value
		}
		if plan.Aggregate == logical.AggregateMax {
			name = "max"
		}
		rows := &Rows{Columns: []string{aggregateResultColumn(plan, name)}, Values: [][]any{{result}}}
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	default:
		return nil, fmt.Errorf("unsupported aggregate %d", plan.Aggregate)
	}
}

func (db *DB) executeScanQuery(ctx context.Context, def catalog.TableDef, plan logical.Query) (*Rows, error) {
	pred, pushed, err := db.predicateFromQuery(def, plan)
	if err != nil {
		return nil, err
	}
	sources := scanSourceColumns(plan.SelectOutputs)
	if !pushed && plan.WhereExpr != nil {
		seen := sourceSet(sources)
		sources = appendExprSourceColumns(sources, seen, *plan.WhereExpr)
	}
	if plan.OrderExpr != nil {
		seen := sourceSet(sources)
		sources = appendExprSourceColumns(sources, seen, *plan.OrderExpr)
	}
	columnIDs := make([]catalog.ColumnID, 0, len(sources))
	for _, name := range sources {
		col, ok := findTableColumn(def, name)
		if !ok {
			return nil, fmt.Errorf("missing SELECT column %q", name)
		}
		columnIDs = append(columnIDs, col.ID)
	}
	if len(columnIDs) == 0 {
		if len(def.Columns) == 0 {
			return nil, fmt.Errorf("literal-only SELECT requires at least one table column")
		}
		columnIDs = append(columnIDs, def.Columns[0].ID)
	}
	values, err := db.data.ScanRows(ctx, def, columnIDs, pred)
	if err != nil {
		return nil, err
	}
	if !pushed && plan.WhereExpr != nil {
		values, err = filterRowsByExprValues(values, sources, *plan.WhereExpr)
		if err != nil {
			return nil, err
		}
	}
	if plan.OrderExpr != nil {
		values, err = orderValueRows(values, sources, *plan.OrderExpr, plan.OrderDesc)
		if err != nil {
			return nil, err
		}
	}
	columns := scanOutputColumns(plan.SelectOutputs)
	if !scanOutputsMatchSources(plan.SelectOutputs, sources) {
		values, err = evalScanOutputRows(values, plan.SelectOutputs, sources)
		if err != nil {
			return nil, err
		}
	}
	rows := &Rows{Columns: columns, Values: values}
	if plan.OrderExpr == nil {
		rows, err = orderRows(rows, plan)
		if err != nil {
			return nil, err
		}
	}
	rows = offsetRows(rows, plan)
	rows = limitRows(rows, plan)
	return rows, nil
}

func scanOutputsMatchSources(outputs []logical.OutputExpr, sources []string) bool {
	if len(outputs) != len(sources) {
		return false
	}
	for i, output := range outputs {
		if output.Expr.Kind != logical.ExprColumn || output.Expr.Column != sources[i] {
			return false
		}
	}
	return true
}

func scanOutputColumns(outputs []logical.OutputExpr) []string {
	columns := make([]string, 0, len(outputs))
	for _, output := range outputs {
		columns = append(columns, scanOutputColumnName(output))
	}
	return columns
}

func scanOutputColumnName(output logical.OutputExpr) string {
	if output.Alias != "" {
		return output.Alias
	}
	if output.Expr.Kind == logical.ExprColumn {
		return output.Expr.Column
	}
	return ""
}

func scanSourceColumns(outputs []logical.OutputExpr) []string {
	seen := make(map[string]struct{})
	columns := make([]string, 0, len(outputs))
	for _, output := range outputs {
		columns = appendExprSourceColumns(columns, seen, output.Expr)
	}
	return columns
}

func sourceSet(columns []string) map[string]struct{} {
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		seen[column] = struct{}{}
	}
	return seen
}

func appendExprSourceColumns(columns []string, seen map[string]struct{}, expr logical.Expr) []string {
	switch expr.Kind {
	case logical.ExprColumn:
		if _, ok := seen[expr.Column]; ok {
			return columns
		}
		seen[expr.Column] = struct{}{}
		return append(columns, expr.Column)
	case logical.ExprBinary:
		if expr.Left != nil {
			columns = appendExprSourceColumns(columns, seen, *expr.Left)
		}
		if expr.Right != nil {
			columns = appendExprSourceColumns(columns, seen, *expr.Right)
		}
	case logical.ExprUnary, logical.ExprBetween, logical.ExprIn:
		if expr.Left != nil {
			columns = appendExprSourceColumns(columns, seen, *expr.Left)
		}
		for _, arg := range expr.Args {
			columns = appendExprSourceColumns(columns, seen, arg)
		}
	}
	return columns
}

func evalScanOutputRows(values [][]any, outputs []logical.OutputExpr, sources []string) ([][]any, error) {
	if len(outputs) == 0 {
		return values, nil
	}
	sourceIndex := make(map[string]int, len(sources))
	for i, source := range sources {
		sourceIndex[source] = i
	}
	out := make([][]any, 0, len(values))
	for _, row := range values {
		expanded := make([]any, len(outputs))
		for i, output := range outputs {
			value, err := evalScanExpr(output.Expr, row, sourceIndex)
			if err != nil {
				return nil, err
			}
			expanded[i] = value
		}
		out = append(out, expanded)
	}
	return out, nil
}

func evalScanExpr(expr logical.Expr, row []any, sourceIndex map[string]int) (any, error) {
	return evalRowExpr(expr, row, sourceIndex)
}

func filterRowsByExprValues(values [][]any, sources []string, expr logical.Expr) ([][]any, error) {
	sourceIndex := make(map[string]int, len(sources))
	for i, source := range sources {
		sourceIndex[source] = i
	}
	out := values[:0]
	for _, row := range values {
		value, err := evalRowExpr(expr, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		matched, _ := value.(bool)
		if matched {
			out = append(out, row)
		}
	}
	return out, nil
}

func evalRowExpr(expr logical.Expr, row []any, sourceIndex map[string]int) (any, error) {
	switch expr.Kind {
	case logical.ExprColumn:
		index, ok := sourceIndex[expr.Column]
		if !ok || index >= len(row) {
			return nil, nil
		}
		return row[index], nil
	case logical.ExprLiteral:
		return expr.Literal, nil
	case logical.ExprBinary:
		if expr.Left == nil || expr.Right == nil {
			return nil, nil
		}
		left, err := evalRowExpr(*expr.Left, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		right, err := evalRowExpr(*expr.Right, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		return computeBinaryValue(left, expr.Op, right)
	case logical.ExprUnary:
		if expr.Left == nil {
			return nil, nil
		}
		value, err := evalRowExpr(*expr.Left, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		switch expr.Op {
		case logical.OpNot:
			matched, _ := value.(bool)
			return !matched, nil
		case logical.OpLower:
			text, ok := value.(string)
			if !ok {
				return nil, nil
			}
			return strings.ToLower(text), nil
		case logical.OpUpper:
			text, ok := value.(string)
			if !ok {
				return nil, nil
			}
			return strings.ToUpper(text), nil
		default:
			return nil, fmt.Errorf("unsupported unary expression operator")
		}
	case logical.ExprBetween:
		if expr.Left == nil || len(expr.Args) != 2 {
			return nil, nil
		}
		value, err := evalRowExpr(*expr.Left, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		low, err := evalRowExpr(expr.Args[0], row, sourceIndex)
		if err != nil {
			return nil, err
		}
		high, err := evalRowExpr(expr.Args[1], row, sourceIndex)
		if err != nil {
			return nil, err
		}
		return computeBetweenValue(value, low, high), nil
	case logical.ExprIn:
		if expr.Left == nil {
			return nil, nil
		}
		value, err := evalRowExpr(*expr.Left, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		matched := false
		if value != nil {
			for _, arg := range expr.Args {
				candidate, err := evalRowExpr(arg, row, sourceIndex)
				if err != nil {
					return nil, err
				}
				if valuesEqual(value, candidate) {
					matched = true
					break
				}
			}
		}
		if expr.Not && value != nil {
			return !matched, nil
		}
		return matched, nil
	default:
		return nil, nil
	}
}

func computeBinaryValue(leftValue any, op logical.Op, rightValue any) (any, error) {
	switch op {
	case logical.OpAdd, logical.OpSubtract, logical.OpMultiply, logical.OpDivide, logical.OpModulo, logical.OpIntDivide:
		return computeArithmeticValue(leftValue, op, rightValue)
	case logical.OpConcat:
		return computeConcatValue(leftValue, rightValue), nil
	case logical.OpEqual, logical.OpNotEqual, logical.OpLess, logical.OpLessEqual, logical.OpGreater, logical.OpGreaterEqual:
		return computeComparisonValue(leftValue, op, rightValue), nil
	case logical.OpAnd:
		left, _ := leftValue.(bool)
		right, _ := rightValue.(bool)
		return left && right, nil
	case logical.OpOr:
		left, _ := leftValue.(bool)
		right, _ := rightValue.(bool)
		return left || right, nil
	default:
		return nil, fmt.Errorf("unsupported binary expression operator")
	}
}

func computeConcatValue(leftValue any, rightValue any) any {
	if leftValue == nil || rightValue == nil {
		return nil
	}
	left, leftOK := leftValue.(string)
	right, rightOK := rightValue.(string)
	if !leftOK || !rightOK {
		return nil
	}
	return left + right
}

func computeArithmeticValue(leftValue any, op logical.Op, rightValue any) (any, error) {
	if leftValue == nil || rightValue == nil {
		return nil, nil
	}
	if _, ok := scanFloatValue(leftValue); ok {
		return computeFloatArithmeticValue(leftValue, op, rightValue)
	}
	if _, ok := scanFloatValue(rightValue); ok {
		return computeFloatArithmeticValue(leftValue, op, rightValue)
	}
	left, ok := scanIntValue(leftValue)
	if !ok {
		return nil, nil
	}
	right, ok := scanIntValue(rightValue)
	if !ok {
		return nil, nil
	}
	switch op {
	case logical.OpAdd:
		return left + right, nil
	case logical.OpSubtract:
		return left - right, nil
	case logical.OpMultiply:
		return left * right, nil
	case logical.OpDivide, logical.OpIntDivide:
		if right == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return left / right, nil
	case logical.OpModulo:
		if right == 0 {
			return nil, fmt.Errorf("modulo by zero")
		}
		return left % right, nil
	default:
		return nil, fmt.Errorf("unsupported scan computed expression operator")
	}
}

func computeFloatArithmeticValue(leftValue any, op logical.Op, rightValue any) (any, error) {
	left, ok := scanNumericValue(leftValue)
	if !ok {
		return nil, nil
	}
	right, ok := scanNumericValue(rightValue)
	if !ok {
		return nil, nil
	}
	switch op {
	case logical.OpAdd:
		return left + right, nil
	case logical.OpSubtract:
		return left - right, nil
	case logical.OpMultiply:
		return left * right, nil
	case logical.OpDivide:
		if right == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return left / right, nil
	case logical.OpModulo, logical.OpIntDivide:
		return nil, fmt.Errorf("MOD and DIV require integer operands")
	default:
		return nil, fmt.Errorf("unsupported scan computed expression operator")
	}
}

func computeComparisonValue(leftValue any, op logical.Op, rightValue any) bool {
	if leftValue == nil || rightValue == nil {
		return false
	}
	if left, ok := scanIntValue(leftValue); ok {
		if right, ok := scanIntValue(rightValue); ok {
			return compareIntValues(left, op, right)
		}
	}
	if _, ok := scanFloatValue(leftValue); ok {
		if left, right, ok := scanNumericPair(leftValue, rightValue); ok {
			return compareFloatValues(left, op, right)
		}
	}
	if _, ok := scanFloatValue(rightValue); ok {
		if left, right, ok := scanNumericPair(leftValue, rightValue); ok {
			return compareFloatValues(left, op, right)
		}
	}
	switch left := leftValue.(type) {
	case string:
		right, ok := rightValue.(string)
		return ok && compareStringValues(left, op, right)
	case bool:
		right, ok := rightValue.(bool)
		return ok && compareBoolValues(left, op, right)
	default:
		return false
	}
}

func computeBetweenValue(value any, low any, high any) bool {
	if value == nil || low == nil || high == nil {
		return false
	}
	if value, ok := scanIntValue(value); ok {
		lo, loOK := scanIntValue(low)
		hi, hiOK := scanIntValue(high)
		return loOK && hiOK && lo <= value && value <= hi
	}
	if _, ok := scanFloatValue(value); ok {
		value, valueOK := scanNumericValue(value)
		lo, loOK := scanNumericValue(low)
		hi, hiOK := scanNumericValue(high)
		return valueOK && loOK && hiOK && lo <= value && value <= hi
	}
	if _, ok := scanFloatValue(low); ok {
		value, valueOK := scanNumericValue(value)
		lo, loOK := scanNumericValue(low)
		hi, hiOK := scanNumericValue(high)
		return valueOK && loOK && hiOK && lo <= value && value <= hi
	}
	if _, ok := scanFloatValue(high); ok {
		value, valueOK := scanNumericValue(value)
		lo, loOK := scanNumericValue(low)
		hi, hiOK := scanNumericValue(high)
		return valueOK && loOK && hiOK && lo <= value && value <= hi
	}
	if value, ok := value.(string); ok {
		lo, loOK := low.(string)
		hi, hiOK := high.(string)
		return loOK && hiOK && lo <= value && value <= hi
	}
	return false
}

func valuesEqual(left any, right any) bool {
	if left == nil || right == nil {
		return false
	}
	if left, ok := scanIntValue(left); ok {
		right, ok := scanIntValue(right)
		return ok && left == right
	}
	if _, ok := scanFloatValue(left); ok {
		left, right, ok := scanNumericPair(left, right)
		return ok && left == right
	}
	if _, ok := scanFloatValue(right); ok {
		left, right, ok := scanNumericPair(left, right)
		return ok && left == right
	}
	switch left := left.(type) {
	case string:
		right, ok := right.(string)
		return ok && left == right
	case bool:
		right, ok := right.(bool)
		return ok && left == right
	default:
		return false
	}
}

func compareFloatValues(left float64, op logical.Op, right float64) bool {
	if math.IsNaN(left) || math.IsNaN(right) {
		return false
	}
	switch op {
	case logical.OpEqual:
		return left == right
	case logical.OpNotEqual:
		return left != right
	case logical.OpLess:
		return left < right
	case logical.OpLessEqual:
		return left <= right
	case logical.OpGreater:
		return left > right
	case logical.OpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func compareIntValues(left int64, op logical.Op, right int64) bool {
	switch op {
	case logical.OpEqual:
		return left == right
	case logical.OpNotEqual:
		return left != right
	case logical.OpLess:
		return left < right
	case logical.OpLessEqual:
		return left <= right
	case logical.OpGreater:
		return left > right
	case logical.OpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func compareStringValues(left string, op logical.Op, right string) bool {
	switch op {
	case logical.OpEqual:
		return left == right
	case logical.OpNotEqual:
		return left != right
	case logical.OpLess:
		return left < right
	case logical.OpLessEqual:
		return left <= right
	case logical.OpGreater:
		return left > right
	case logical.OpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func compareBoolValues(left bool, op logical.Op, right bool) bool {
	switch op {
	case logical.OpEqual:
		return left == right
	case logical.OpNotEqual:
		return left != right
	default:
		return false
	}
}

func scanIntValue(value any) (int64, bool) {
	switch value := value.(type) {
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		if value > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return int64(value), true
	default:
		return 0, false
	}
}

func scanFloatValue(value any) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	default:
		return 0, false
	}
}

func scanNumericValue(value any) (float64, bool) {
	if value, ok := scanFloatValue(value); ok {
		return value, true
	}
	if value, ok := scanIntValue(value); ok {
		return float64(value), true
	}
	return 0, false
}

func scanNumericPair(left any, right any) (float64, float64, bool) {
	leftValue, leftOK := scanNumericValue(left)
	rightValue, rightOK := scanNumericValue(right)
	return leftValue, rightValue, leftOK && rightOK
}

func (db *DB) executeAggregateQueryRowFilter(ctx context.Context, def catalog.TableDef, plan logical.Query) (*Rows, error) {
	sources := aggregateRowFilterSourceColumns(plan)
	columnIDs := make([]catalog.ColumnID, 0, len(sources))
	for _, name := range sources {
		col, ok := findTableColumn(def, name)
		if !ok {
			return nil, fmt.Errorf("missing query column %q", name)
		}
		columnIDs = append(columnIDs, col.ID)
	}
	if len(columnIDs) == 0 {
		if len(def.Columns) == 0 {
			return nil, fmt.Errorf("filtered aggregate requires at least one table column")
		}
		columnIDs = append(columnIDs, def.Columns[0].ID)
	}
	values, err := db.data.ScanRows(ctx, def, columnIDs, storage.Predicate{})
	if err != nil {
		return nil, err
	}
	if plan.WhereExpr != nil {
		values, err = filterRowsByExprValues(values, sources, *plan.WhereExpr)
		if err != nil {
			return nil, err
		}
	}
	sourceIndex := makeSourceIndex(sources)
	if hasGroup(plan) {
		return executeGroupedAggregateRows(values, sourceIndex, plan)
	}
	return executeScalarAggregateRows(values, sourceIndex, plan)
}

func aggregateRowFilterSourceColumns(plan logical.Query) []string {
	seen := make(map[string]struct{})
	columns := make([]string, 0, 4)
	if plan.WhereExpr != nil {
		columns = appendExprSourceColumns(columns, seen, *plan.WhereExpr)
	}
	if plan.GroupExpr != nil {
		columns = appendExprSourceColumns(columns, seen, *plan.GroupExpr)
	} else {
		columns = appendSourceColumn(columns, seen, plan.GroupColumn)
	}
	columns = appendSourceColumn(columns, seen, plan.AggregateColumn)
	for _, agg := range plan.HavingAggregates {
		columns = appendSourceColumn(columns, seen, agg.ArgColumn)
	}
	return columns
}

func appendSourceColumn(columns []string, seen map[string]struct{}, column string) []string {
	if column == "" {
		return columns
	}
	if _, ok := seen[column]; ok {
		return columns
	}
	seen[column] = struct{}{}
	return append(columns, column)
}

func makeSourceIndex(sources []string) map[string]int {
	sourceIndex := make(map[string]int, len(sources))
	for i, source := range sources {
		sourceIndex[source] = i
	}
	return sourceIndex
}

func executeScalarAggregateRows(values [][]any, sourceIndex map[string]int, plan logical.Query) (*Rows, error) {
	name := aggregateFallbackName(plan.Aggregate)
	value, err := aggregateRows(values, sourceIndex, plan.Aggregate, plan.AggregateColumn)
	if err != nil {
		return nil, err
	}
	rows := &Rows{Columns: []string{aggregateResultColumn(plan, name)}, Values: [][]any{{value}}}
	for _, agg := range plan.HavingAggregates {
		value, err := aggregateRows(values, sourceIndex, agg.Aggregate, agg.ArgColumn)
		if err != nil {
			return nil, err
		}
		rows.Columns = append(rows.Columns, agg.Column)
		rows.Values[0] = append(rows.Values[0], value)
	}
	return finalizeRows(rows, plan)
}

func executeGroupedAggregateRows(values [][]any, sourceIndex map[string]int, plan logical.Query) (*Rows, error) {
	name := aggregateFallbackName(plan.Aggregate)
	grouped, err := aggregateGroupedRows(values, sourceIndex, plan, plan.Aggregate, plan.AggregateColumn)
	if err != nil {
		return nil, err
	}
	rows := groupedAnyRows(groupResultColumn(plan), aggregateResultColumn(plan, name), grouped)
	for _, agg := range plan.HavingAggregates {
		valuesByGroup, err := aggregateGroupedRows(values, sourceIndex, plan, agg.Aggregate, agg.ArgColumn)
		if err != nil {
			return nil, err
		}
		rows.Columns = append(rows.Columns, agg.Column)
		for i := range rows.Values {
			key := rows.Values[i][0]
			value, ok := valuesByGroup[key]
			if !ok && agg.Aggregate == logical.AggregateCount {
				value = uint64(0)
			}
			rows.Values[i] = append(rows.Values[i], value)
		}
	}
	return finalizeRows(rows, plan)
}

func aggregateRows(values [][]any, sourceIndex map[string]int, aggregate logical.AggregateFunc, column string) (any, error) {
	switch aggregate {
	case logical.AggregateCount:
		if column == "" {
			return uint64(len(values)), nil
		}
		index, ok := sourceIndex[column]
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", column)
		}
		var count uint64
		for _, row := range values {
			if index < len(row) && row[index] != nil {
				count++
			}
		}
		return count, nil
	case logical.AggregateSum:
		index, ok := sourceIndex[column]
		if !ok {
			return nil, fmt.Errorf("missing SUM column %q", column)
		}
		var sum int64
		for _, row := range values {
			if index >= len(row) || row[index] == nil {
				continue
			}
			value, ok := scanIntValue(row[index])
			if !ok {
				return nil, fmt.Errorf("SUM column %q produced non-integer value", column)
			}
			sum += value
		}
		return sum, nil
	case logical.AggregateMin, logical.AggregateMax:
		index, ok := sourceIndex[column]
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", column)
		}
		var out int64
		found := false
		for _, row := range values {
			if index >= len(row) || row[index] == nil {
				continue
			}
			value, ok := scanIntValue(row[index])
			if !ok {
				return nil, fmt.Errorf("aggregate column %q produced non-integer value", column)
			}
			if !found || (aggregate == logical.AggregateMin && value < out) || (aggregate == logical.AggregateMax && value > out) {
				out = value
				found = true
			}
		}
		if !found {
			return nil, nil
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate %d", aggregate)
	}
}

func aggregateGroupedRows(values [][]any, sourceIndex map[string]int, plan logical.Query, aggregate logical.AggregateFunc, aggregateColumn string) (map[any]any, error) {
	if plan.GroupExpr == nil {
		return nil, fmt.Errorf("missing GROUP BY expression")
	}
	aggIndex := -1
	if aggregateColumn != "" {
		index, ok := sourceIndex[aggregateColumn]
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", aggregateColumn)
		}
		aggIndex = index
	}
	groups := make(map[any]any)
	for _, row := range values {
		key, err := evalRowExpr(*plan.GroupExpr, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		if key == nil {
			continue
		}
		switch aggregate {
		case logical.AggregateCount:
			if aggIndex < 0 {
				current, _ := groups[key].(uint64)
				groups[key] = current + 1
				continue
			}
			if aggIndex < len(row) && row[aggIndex] != nil {
				current, _ := groups[key].(uint64)
				groups[key] = current + 1
			}
		case logical.AggregateSum:
			if aggIndex >= len(row) || row[aggIndex] == nil {
				continue
			}
			value, ok := scanIntValue(row[aggIndex])
			if !ok {
				return nil, fmt.Errorf("SUM column %q produced non-integer value", aggregateColumn)
			}
			current, _ := groups[key].(int64)
			groups[key] = current + value
		case logical.AggregateMin, logical.AggregateMax:
			if aggIndex >= len(row) || row[aggIndex] == nil {
				continue
			}
			value, ok := scanIntValue(row[aggIndex])
			if !ok {
				return nil, fmt.Errorf("aggregate column %q produced non-integer value", aggregateColumn)
			}
			current, found := groups[key].(int64)
			if !found || (aggregate == logical.AggregateMin && value < current) || (aggregate == logical.AggregateMax && value > current) {
				groups[key] = value
			}
		default:
			return nil, fmt.Errorf("unsupported aggregate %d", aggregate)
		}
	}
	return groups, nil
}

func aggregateFallbackName(aggregate logical.AggregateFunc) string {
	switch aggregate {
	case logical.AggregateSum:
		return "sum"
	case logical.AggregateMin:
		return "min"
	case logical.AggregateMax:
		return "max"
	default:
		return "count"
	}
}

func hasGroup(plan logical.Query) bool {
	return plan.GroupExpr != nil || plan.GroupColumn != ""
}

func (db *DB) executeGroupedAggregateQuery(ctx context.Context, def catalog.TableDef, plan logical.Query, pred storage.Predicate, scratch *storage.QueryScratch) (*Rows, error) {
	groupCol, ok := findTableColumn(def, plan.GroupColumn)
	if !ok {
		return nil, fmt.Errorf("missing GROUP BY column %q", plan.GroupColumn)
	}
	if groupCol.Type.Kind != sqltype.KindText {
		return db.executeGroupedAnyAggregateQuery(ctx, def, plan, groupCol, pred, scratch)
	}
	if plan.Aggregate == logical.AggregateSum {
		sumCol, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing SUM column %q", plan.AggregateColumn)
		}
		sums, err := db.data.GroupStringSumsInt(ctx, def, groupCol.ID, sumCol.ID, pred)
		if err != nil {
			return nil, err
		}
		rows := groupedSumRows(groupResultColumn(plan), aggregateResultColumn(plan, "sum"), sums)
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	}
	if plan.Aggregate == logical.AggregateMin || plan.Aggregate == logical.AggregateMax {
		aggCol, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", plan.AggregateColumn)
		}
		values, err := db.data.GroupStringMinMaxInt(ctx, def, groupCol.ID, aggCol.ID, pred, plan.Aggregate == logical.AggregateMax)
		if err != nil {
			return nil, err
		}
		name := "min"
		if plan.Aggregate == logical.AggregateMax {
			name = "max"
		}
		rows := groupedIntRows(groupResultColumn(plan), aggregateResultColumn(plan, name), values)
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	}
	if plan.Aggregate != logical.AggregateCount {
		return nil, fmt.Errorf("GROUP BY only supports count(*), count(column), sum(column), min(column), and max(column) for now")
	}
	if plan.AggregateColumn != "" {
		countCol, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing count column %q", plan.AggregateColumn)
		}
		counts, err := db.data.GroupStringCountNonNull(ctx, def, groupCol.ID, countCol.ID, pred)
		if err != nil {
			return nil, err
		}
		rows := groupedCountRows(groupResultColumn(plan), aggregateResultColumn(plan, "count"), counts)
		return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
	}
	counts, err := db.data.GroupStringCounts(ctx, def, groupCol.ID, pred, scratch)
	if err != nil {
		return nil, err
	}
	rows := groupedCountRows(groupResultColumn(plan), aggregateResultColumn(plan, "count"), counts)
	return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
}

func (db *DB) executeGroupedAnyAggregateQuery(ctx context.Context, def catalog.TableDef, plan logical.Query, groupCol catalog.ColumnDef, pred storage.Predicate, scratch *storage.QueryScratch) (*Rows, error) {
	agg := storage.GroupAggregateCountStar
	var aggColID catalog.ColumnID
	name := "count"
	switch plan.Aggregate {
	case logical.AggregateCount:
		if plan.AggregateColumn != "" {
			countCol, ok := findTableColumn(def, plan.AggregateColumn)
			if !ok {
				return nil, fmt.Errorf("missing count column %q", plan.AggregateColumn)
			}
			agg = storage.GroupAggregateCountNonNull
			aggColID = countCol.ID
		}
	case logical.AggregateSum:
		sumCol, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing SUM column %q", plan.AggregateColumn)
		}
		agg = storage.GroupAggregateSum
		aggColID = sumCol.ID
		name = "sum"
	case logical.AggregateMin, logical.AggregateMax:
		aggCol, ok := findTableColumn(def, plan.AggregateColumn)
		if !ok {
			return nil, fmt.Errorf("missing aggregate column %q", plan.AggregateColumn)
		}
		agg = storage.GroupAggregateMin
		name = "min"
		if plan.Aggregate == logical.AggregateMax {
			agg = storage.GroupAggregateMax
			name = "max"
		}
		aggColID = aggCol.ID
	default:
		return nil, fmt.Errorf("GROUP BY only supports count(*), count(column), sum(column), min(column), and max(column) for now")
	}
	values, err := db.data.GroupAggregateAny(ctx, def, groupCol.ID, aggColID, pred, agg)
	if err != nil {
		return nil, err
	}
	rows := groupedAnyRows(groupResultColumn(plan), aggregateResultColumn(plan, name), values)
	return db.finalizeAggregateRows(ctx, def, rows, plan, pred, scratch)
}

func (db *DB) finalizeAggregateRows(ctx context.Context, def catalog.TableDef, rows *Rows, plan logical.Query, pred storage.Predicate, scratch *storage.QueryScratch) (*Rows, error) {
	var err error
	if plan.GroupColumn == "" {
		rows, err = db.appendScalarHavingAggregates(ctx, def, rows, plan, pred, scratch)
	} else {
		rows, err = db.appendGroupedHavingAggregates(ctx, def, rows, plan, pred)
	}
	if err != nil {
		return nil, err
	}
	return finalizeRows(rows, plan)
}

func (db *DB) appendScalarHavingAggregates(ctx context.Context, def catalog.TableDef, rows *Rows, plan logical.Query, pred storage.Predicate, scratch *storage.QueryScratch) (*Rows, error) {
	for _, agg := range plan.HavingAggregates {
		value, err := db.scalarHavingAggregate(ctx, def, agg, pred, scratch)
		if err != nil {
			return nil, err
		}
		rows.Columns = append(rows.Columns, agg.Column)
		rows.Values[0] = append(rows.Values[0], value)
	}
	return rows, nil
}

func (db *DB) scalarHavingAggregate(ctx context.Context, def catalog.TableDef, agg logical.HavingAggregate, pred storage.Predicate, scratch *storage.QueryScratch) (any, error) {
	switch agg.Aggregate {
	case logical.AggregateCount:
		if agg.ArgColumn == "" {
			return db.data.Count(ctx, def, pred, scratch)
		}
		col, ok := findTableColumn(def, agg.ArgColumn)
		if !ok {
			return nil, fmt.Errorf("missing HAVING count column %q", agg.ArgColumn)
		}
		return db.data.CountNonNull(ctx, def, col.ID, pred, scratch)
	case logical.AggregateSum:
		col, ok := findTableColumn(def, agg.ArgColumn)
		if !ok {
			return nil, fmt.Errorf("missing HAVING sum column %q", agg.ArgColumn)
		}
		return db.data.SumInt(ctx, def, col.ID, pred, scratch)
	case logical.AggregateMin, logical.AggregateMax:
		col, ok := findTableColumn(def, agg.ArgColumn)
		if !ok {
			return nil, fmt.Errorf("missing HAVING aggregate column %q", agg.ArgColumn)
		}
		value, found, err := db.data.MinMaxInt(ctx, def, col.ID, pred, agg.Aggregate == logical.AggregateMax, scratch)
		if err != nil || !found {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported HAVING aggregate %d", agg.Aggregate)
	}
}

func (db *DB) appendGroupedHavingAggregates(ctx context.Context, def catalog.TableDef, rows *Rows, plan logical.Query, pred storage.Predicate) (*Rows, error) {
	if len(plan.HavingAggregates) == 0 {
		return rows, nil
	}
	groupCol, ok := findTableColumn(def, plan.GroupColumn)
	if !ok {
		return nil, fmt.Errorf("missing GROUP BY column %q", plan.GroupColumn)
	}
	for _, agg := range plan.HavingAggregates {
		values, err := db.groupedHavingAggregate(ctx, def, groupCol.ID, agg, pred)
		if err != nil {
			return nil, err
		}
		rows.Columns = append(rows.Columns, agg.Column)
		for i := range rows.Values {
			key, _ := rows.Values[i][0].(string)
			rows.Values[i] = append(rows.Values[i], values[key])
		}
	}
	return rows, nil
}

func (db *DB) groupedHavingAggregate(ctx context.Context, def catalog.TableDef, groupColumnID catalog.ColumnID, agg logical.HavingAggregate, pred storage.Predicate) (map[string]any, error) {
	out := make(map[string]any)
	switch agg.Aggregate {
	case logical.AggregateCount:
		var counts map[string]uint64
		var err error
		if agg.ArgColumn == "" {
			counts, err = db.data.GroupStringCounts(ctx, def, groupColumnID, pred, nil)
		} else {
			col, ok := findTableColumn(def, agg.ArgColumn)
			if !ok {
				return nil, fmt.Errorf("missing HAVING count column %q", agg.ArgColumn)
			}
			counts, err = db.data.GroupStringCountNonNull(ctx, def, groupColumnID, col.ID, pred)
		}
		for key, value := range counts {
			out[key] = value
		}
		return out, err
	case logical.AggregateSum:
		col, ok := findTableColumn(def, agg.ArgColumn)
		if !ok {
			return nil, fmt.Errorf("missing HAVING sum column %q", agg.ArgColumn)
		}
		values, err := db.data.GroupStringSumsInt(ctx, def, groupColumnID, col.ID, pred)
		for key, value := range values {
			out[key] = value
		}
		return out, err
	case logical.AggregateMin, logical.AggregateMax:
		col, ok := findTableColumn(def, agg.ArgColumn)
		if !ok {
			return nil, fmt.Errorf("missing HAVING aggregate column %q", agg.ArgColumn)
		}
		values, err := db.data.GroupStringMinMaxInt(ctx, def, groupColumnID, col.ID, pred, agg.Aggregate == logical.AggregateMax)
		for key, value := range values {
			out[key] = value
		}
		return out, err
	default:
		return nil, fmt.Errorf("unsupported HAVING aggregate %d", agg.Aggregate)
	}
}

func finalizeRows(rows *Rows, plan logical.Query) (*Rows, error) {
	var err error
	rows, err = filterHavingRows(rows, plan)
	if err != nil {
		return nil, err
	}
	rows, err = orderRows(rows, plan)
	if err != nil {
		return nil, err
	}
	rows = removeHiddenRows(rows, plan.HiddenColumns)
	rows = offsetRows(rows, plan)
	return limitRows(rows, plan), nil
}

func removeHiddenRows(rows *Rows, hidden []string) *Rows {
	if rows == nil || len(hidden) == 0 {
		return rows
	}
	hiddenSet := make(map[string]struct{}, len(hidden))
	for _, column := range hidden {
		hiddenSet[column] = struct{}{}
	}
	keep := make([]int, 0, len(rows.Columns))
	columns := rows.Columns[:0]
	for i, column := range rows.Columns {
		if _, ok := hiddenSet[column]; ok {
			continue
		}
		keep = append(keep, i)
		columns = append(columns, column)
	}
	for rowIndex, row := range rows.Values {
		values := row[:0]
		for _, index := range keep {
			values = append(values, row[index])
		}
		rows.Values[rowIndex] = values
	}
	rows.Columns = columns
	return rows
}

func filterHavingRows(rows *Rows, plan logical.Query) (*Rows, error) {
	if rows == nil || !plan.HasHaving {
		return rows, nil
	}
	if plan.HavingFilterExpr == nil {
		return rows, nil
	}
	return filterRowsByExpr(rows.Values, rows.Columns, *plan.HavingFilterExpr, rows)
}

func filterRowsByExpr(values [][]any, sources []string, expr logical.Expr, rows *Rows) (*Rows, error) {
	filtered, err := filterRowsByExprValues(values, sources, expr)
	if err != nil {
		return nil, err
	}
	rows.Values = filtered
	return rows, nil
}

func offsetRows(rows *Rows, plan logical.Query) *Rows {
	if rows == nil || !plan.HasOffset || plan.Offset == 0 {
		return rows
	}
	if plan.Offset >= uint64(len(rows.Values)) {
		rows.Values = nil
		return rows
	}
	rows.Values = rows.Values[plan.Offset:]
	return rows
}

func limitRows(rows *Rows, plan logical.Query) *Rows {
	if rows == nil || !plan.HasLimit || uint64(len(rows.Values)) <= plan.Limit {
		return rows
	}
	rows.Values = rows.Values[:plan.Limit]
	return rows
}

func orderRows(rows *Rows, plan logical.Query) (*Rows, error) {
	if rows == nil || (plan.OrderColumn == "" && plan.OrderExpr == nil) {
		return rows, nil
	}
	if plan.OrderExpr != nil {
		values, err := orderValueRows(rows.Values, rows.Columns, *plan.OrderExpr, plan.OrderDesc)
		if err != nil {
			return nil, err
		}
		rows.Values = values
		return rows, nil
	}
	orderIndex := -1
	for i, column := range rows.Columns {
		if column == plan.OrderColumn {
			orderIndex = i
			break
		}
	}
	if orderIndex < 0 {
		return rows, nil
	}
	sort.SliceStable(rows.Values, func(i, j int) bool {
		if plan.OrderDesc {
			return lessOrderValue(rows.Values[j][orderIndex], rows.Values[i][orderIndex])
		}
		return lessOrderValue(rows.Values[i][orderIndex], rows.Values[j][orderIndex])
	})
	return rows, nil
}

func orderValueRows(values [][]any, sources []string, expr logical.Expr, desc bool) ([][]any, error) {
	sourceIndex := makeSourceIndex(sources)
	type keyedRow struct {
		row []any
		key any
	}
	keyed := make([]keyedRow, len(values))
	for i, row := range values {
		key, err := evalRowExpr(expr, row, sourceIndex)
		if err != nil {
			return nil, err
		}
		keyed[i] = keyedRow{row: row, key: key}
	}
	sort.SliceStable(keyed, func(i, j int) bool {
		if desc {
			return lessOrderValue(keyed[j].key, keyed[i].key)
		}
		return lessOrderValue(keyed[i].key, keyed[j].key)
	})
	for i := range keyed {
		values[i] = keyed[i].row
	}
	return values, nil
}

func lessOrderValue(left any, right any) bool {
	if leftValue, ok := scanIntValue(left); ok {
		if rightValue, ok := scanIntValue(right); ok {
			return leftValue < rightValue
		}
	}
	if leftValue, rightValue, ok := scanNumericPair(left, right); ok {
		return leftValue < rightValue
	}
	switch left := left.(type) {
	case string:
		right, _ := right.(string)
		return left < right
	case uint64:
		right, _ := right.(uint64)
		return left < right
	case int64:
		right, _ := right.(int64)
		return left < right
	case int16:
		right, _ := right.(int16)
		return left < right
	case int32:
		right, _ := right.(int32)
		return left < right
	case bool:
		right, _ := right.(bool)
		return !left && right
	default:
		return false
	}
}

func (db *DB) predicateFromQuery(def catalog.TableDef, plan logical.Query) (storage.Predicate, bool, error) {
	if !plan.HasFilter {
		return storage.Predicate{}, true, nil
	}
	if plan.WhereExpr == nil {
		return storage.Predicate{}, false, nil
	}
	return predicateFromWhereExpr(def, *plan.WhereExpr)
}

func predicateFromWhereExpr(def catalog.TableDef, expr logical.Expr) (storage.Predicate, bool, error) {
	switch expr.Kind {
	case logical.ExprBinary:
		if expr.Left == nil || expr.Right == nil {
			return storage.Predicate{}, false, nil
		}
		switch expr.Op {
		case logical.OpAnd, logical.OpOr:
			left, leftOK, err := predicateFromWhereExpr(def, *expr.Left)
			if err != nil || !leftOK {
				return storage.Predicate{}, leftOK, err
			}
			right, rightOK, err := predicateFromWhereExpr(def, *expr.Right)
			if err != nil || !rightOK {
				return storage.Predicate{}, rightOK, err
			}
			op := storage.PredicateAnd
			if expr.Op == logical.OpOr {
				op = storage.PredicateOr
			}
			return storage.Predicate{Op: op, Predicates: []storage.Predicate{left, right}}, true, nil
		case logical.OpEqual, logical.OpNotEqual, logical.OpLess, logical.OpLessEqual, logical.OpGreater, logical.OpGreaterEqual:
			pred, ok, err := predicateFromWhereComparison(def, *expr.Left, expr.Op, *expr.Right)
			if err != nil || ok {
				return pred, ok, err
			}
			return predicateFromWhereComparison(def, *expr.Right, flipComparisonOp(expr.Op), *expr.Left)
		default:
			return storage.Predicate{}, false, nil
		}
	case logical.ExprUnary:
		if expr.Op != logical.OpNot || expr.Left == nil {
			return storage.Predicate{}, false, nil
		}
		child, ok, err := predicateFromWhereExpr(def, *expr.Left)
		if err != nil || !ok {
			return storage.Predicate{}, ok, err
		}
		return storage.Predicate{Op: storage.PredicateNot, Predicates: []storage.Predicate{child}}, true, nil
	case logical.ExprBetween:
		if expr.Left == nil || len(expr.Args) != 2 {
			return storage.Predicate{}, false, nil
		}
		return predicateFromWhereBetween(def, *expr.Left, expr.Args[0], expr.Args[1])
	case logical.ExprIn:
		if expr.Left == nil {
			return storage.Predicate{}, false, nil
		}
		return predicateFromWhereIn(def, *expr.Left, expr.Args, expr.Not)
	default:
		return storage.Predicate{}, false, nil
	}
}

func predicateFromWhereComparison(def catalog.TableDef, left logical.Expr, op logical.Op, right logical.Expr) (storage.Predicate, bool, error) {
	if left.Kind != logical.ExprColumn || right.Kind != logical.ExprLiteral {
		return storage.Predicate{}, false, nil
	}
	col, ok := findTableColumn(def, left.Column)
	if !ok {
		return storage.Predicate{}, true, fmt.Errorf("missing WHERE column %q", left.Column)
	}
	filterOp, ok := exprOpToFilterOp(op)
	if !ok {
		return storage.Predicate{}, false, nil
	}
	vals := predicateValues{Op: filterOp}
	switch col.Type.Kind {
	case sqltype.KindBool:
		value, ok := right.Literal.(bool)
		if !ok {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects bool literal", col.Name)
		}
		vals.Bool = value
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64:
		value, ok := scanIntValue(right.Literal)
		if !ok {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects int literal", col.Name)
		}
		vals.Int64 = value
	case sqltype.KindFloat32, sqltype.KindFloat64:
		value, ok := scanNumericValue(right.Literal)
		if !ok {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects numeric literal", col.Name)
		}
		op, ok := floatComparisonPredicate(filterOp)
		if !ok {
			return storage.Predicate{}, false, nil
		}
		return storage.Predicate{ColumnID: col.ID, Op: op, Float64: value}, true, nil
	case sqltype.KindText:
		value, ok := right.Literal.(string)
		if !ok {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects string literal", col.Name)
		}
		vals.String = value
	case sqltype.KindDate:
		value, err := datePredicateLiteral(col.Name, right)
		if err != nil {
			return storage.Predicate{}, true, err
		}
		vals.Int64 = int64(value)
	case sqltype.KindTimestamp:
		value, err := timestampPredicateLiteral(col.Name, right)
		if err != nil {
			return storage.Predicate{}, true, err
		}
		vals.Int64 = value
	case sqltype.KindUUID:
		value, err := uuidPredicateLiteral(col.Name, right)
		if err != nil {
			return storage.Predicate{}, true, err
		}
		switch filterOp {
		case logical.FilterEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, UUID: value}, true, nil
		case logical.FilterNotEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, UUID: value}, true, nil
		default:
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q only supports = and != for uuid", col.Name)
		}
	case sqltype.KindBytes:
		value, ok := right.Literal.(string)
		if !ok {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects string literal", col.Name)
		}
		switch filterOp {
		case logical.FilterEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Text: value}, true, nil
		case logical.FilterNotEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Text: value}, true, nil
		default:
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q only supports = and != for bytes", col.Name)
		}
	case sqltype.KindNamed:
		value, err := enumPredicateLiteral(col, right)
		if err != nil {
			return storage.Predicate{}, true, err
		}
		switch filterOp {
		case logical.FilterEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Enum: value}, true, nil
		case logical.FilterNotEqual:
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Enum: value}, true, nil
		default:
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q only supports = and != for enum", col.Name)
		}
	default:
		return storage.Predicate{}, true, fmt.Errorf("column %q: unsupported WHERE column type %s for current table bridge", col.Name, col.Type)
	}
	pred, err := predicateFromLogical(col, col.Type.Kind, vals)
	return pred, true, err
}

func predicateFromWhereBetween(def catalog.TableDef, target logical.Expr, low logical.Expr, high logical.Expr) (storage.Predicate, bool, error) {
	if target.Kind != logical.ExprColumn || low.Kind != logical.ExprLiteral || high.Kind != logical.ExprLiteral {
		return storage.Predicate{}, false, nil
	}
	col, ok := findTableColumn(def, target.Column)
	if !ok {
		return storage.Predicate{}, true, fmt.Errorf("missing WHERE column %q", target.Column)
	}
	if col.Type.Kind == sqltype.KindFloat32 || col.Type.Kind == sqltype.KindFloat64 {
		lo, loOK := scanNumericValue(low.Literal)
		hi, hiOK := scanNumericValue(high.Literal)
		if !loOK || !hiOK {
			return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects numeric literal", col.Name)
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpBetween, LoFloat64: lo, HiFloat64: hi}, true, nil
	}
	lo, hi, err := predicateRangeLiterals(col, low, high)
	if err != nil {
		return storage.Predicate{}, true, err
	}
	pred, err := predicateFromLogical(col, col.Type.Kind, predicateValues{Op: logical.FilterBetween, LoInt64: lo, HiInt64: hi})
	return pred, true, err
}

func predicateFromWhereIn(def catalog.TableDef, target logical.Expr, values []logical.Expr, not bool) (storage.Predicate, bool, error) {
	if target.Kind != logical.ExprColumn {
		return storage.Predicate{}, false, nil
	}
	col, ok := findTableColumn(def, target.Column)
	if !ok {
		return storage.Predicate{}, true, fmt.Errorf("missing WHERE column %q", target.Column)
	}
	filterOp := logical.FilterIn
	if not {
		filterOp = logical.FilterNotIn
	}
	vals := predicateValues{Op: filterOp}
	switch col.Type.Kind {
	case sqltype.KindBool:
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			boolValue, ok := value.Literal.(bool)
			if !ok {
				return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects bool literal", col.Name)
			}
			vals.Bools = append(vals.Bools, boolValue)
		}
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64:
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			intValue, ok := scanIntValue(value.Literal)
			if !ok {
				return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects int literal", col.Name)
			}
			vals.Ints = append(vals.Ints, intValue)
		}
	case sqltype.KindFloat32, sqltype.KindFloat64:
		floatValues := make([]float64, 0, len(values))
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			floatValue, ok := scanNumericValue(value.Literal)
			if !ok {
				return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects numeric literal", col.Name)
			}
			floatValues = append(floatValues, floatValue)
		}
		op := storage.PredicateOpIn
		if not {
			op = storage.PredicateOpNotIn
		}
		return storage.Predicate{ColumnID: col.ID, Op: op, Float64s: floatValues}, true, nil
	case sqltype.KindDate:
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			dateValue, err := datePredicateLiteral(col.Name, value)
			if err != nil {
				return storage.Predicate{}, true, err
			}
			vals.Ints = append(vals.Ints, int64(dateValue))
		}
	case sqltype.KindTimestamp:
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			timestampValue, err := timestampPredicateLiteral(col.Name, value)
			if err != nil {
				return storage.Predicate{}, true, err
			}
			vals.Ints = append(vals.Ints, timestampValue)
		}
	case sqltype.KindText:
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			textValue, ok := value.Literal.(string)
			if !ok {
				return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects string literal", col.Name)
			}
			vals.Strings = append(vals.Strings, textValue)
		}
	case sqltype.KindUUID:
		uuidValues := make([]vector.UUID16, 0, len(values))
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			uuidValue, err := uuidPredicateLiteral(col.Name, value)
			if err != nil {
				return storage.Predicate{}, true, err
			}
			uuidValues = append(uuidValues, uuidValue)
		}
		op := storage.PredicateOpIn
		if not {
			op = storage.PredicateOpNotIn
		}
		return storage.Predicate{ColumnID: col.ID, Op: op, UUIDs: uuidValues}, true, nil
	case sqltype.KindBytes:
		byteValues := make([]string, 0, len(values))
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			byteValue, ok := value.Literal.(string)
			if !ok {
				return storage.Predicate{}, true, fmt.Errorf("WHERE column %q expects string literal", col.Name)
			}
			byteValues = append(byteValues, byteValue)
		}
		op := storage.PredicateOpIn
		if not {
			op = storage.PredicateOpNotIn
		}
		return storage.Predicate{ColumnID: col.ID, Op: op, Texts: byteValues}, true, nil
	case sqltype.KindNamed:
		enumValues := make([]uint32, 0, len(values))
		for _, value := range values {
			if value.Kind != logical.ExprLiteral {
				return storage.Predicate{}, false, nil
			}
			enumValue, err := enumPredicateLiteral(col, value)
			if err != nil {
				return storage.Predicate{}, true, err
			}
			enumValues = append(enumValues, enumValue)
		}
		op := storage.PredicateOpIn
		if not {
			op = storage.PredicateOpNotIn
		}
		return storage.Predicate{ColumnID: col.ID, Op: op, Enums: enumValues}, true, nil
	default:
		return storage.Predicate{}, true, fmt.Errorf("column %q: unsupported WHERE column type %s for current table bridge", col.Name, col.Type)
	}
	pred, err := predicateFromLogical(col, col.Type.Kind, vals)
	return pred, true, err
}

func predicateRangeLiterals(col catalog.ColumnDef, low logical.Expr, high logical.Expr) (int64, int64, error) {
	switch col.Type.Kind {
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64:
		lo, loOK := scanIntValue(low.Literal)
		hi, hiOK := scanIntValue(high.Literal)
		if !loOK || !hiOK {
			return 0, 0, fmt.Errorf("WHERE column %q expects int literal", col.Name)
		}
		return lo, hi, nil
	case sqltype.KindDate:
		lo, err := datePredicateLiteral(col.Name, low)
		if err != nil {
			return 0, 0, err
		}
		hi, err := datePredicateLiteral(col.Name, high)
		if err != nil {
			return 0, 0, err
		}
		return int64(lo), int64(hi), nil
	case sqltype.KindTimestamp:
		lo, err := timestampPredicateLiteral(col.Name, low)
		if err != nil {
			return 0, 0, err
		}
		hi, err := timestampPredicateLiteral(col.Name, high)
		if err != nil {
			return 0, 0, err
		}
		return lo, hi, nil
	default:
		return 0, 0, fmt.Errorf("column %q: unsupported WHERE column type %s for current table bridge", col.Name, col.Type)
	}
}

func datePredicateLiteral(column string, expr logical.Expr) (int32, error) {
	value, ok := expr.Literal.(string)
	if !ok {
		return 0, fmt.Errorf("WHERE column %q expects date string literal", column)
	}
	parsed, err := parseDateLiteral(value)
	if err != nil {
		return 0, fmt.Errorf("WHERE column %q: %w", column, err)
	}
	return parsed, nil
}

func timestampPredicateLiteral(column string, expr logical.Expr) (int64, error) {
	value, ok := expr.Literal.(string)
	if !ok {
		return 0, fmt.Errorf("WHERE column %q expects timestamp string literal", column)
	}
	parsed, err := parseTimestampLiteral(value)
	if err != nil {
		return 0, fmt.Errorf("WHERE column %q: %w", column, err)
	}
	return parsed, nil
}

func uuidPredicateLiteral(column string, expr logical.Expr) (vector.UUID16, error) {
	value, ok := expr.Literal.(string)
	if !ok {
		return vector.UUID16{}, fmt.Errorf("WHERE column %q expects uuid string literal", column)
	}
	parsed, err := vector.ParseUUID(value)
	if err != nil {
		return vector.UUID16{}, fmt.Errorf("WHERE column %q: %w", column, err)
	}
	return parsed, nil
}

func enumPredicateLiteral(col catalog.ColumnDef, expr logical.Expr) (uint32, error) {
	value, ok := expr.Literal.(string)
	if !ok {
		return 0, fmt.Errorf("WHERE column %q expects enum string literal", col.Name)
	}
	code, ok := enumCodeForLabel(value, col.Labels)
	if !ok {
		return 0, fmt.Errorf("WHERE column %q invalid enum label %q", col.Name, value)
	}
	return code, nil
}

func floatComparisonPredicate(op logical.FilterOp) (storage.PredicateOp, bool) {
	switch op {
	case logical.FilterEqual:
		return storage.PredicateOpEq, true
	case logical.FilterNotEqual:
		return storage.PredicateOpNotEq, true
	case logical.FilterLess:
		return storage.PredicateOpLess, true
	case logical.FilterLessEqual:
		return storage.PredicateOpLessEqual, true
	case logical.FilterGreater:
		return storage.PredicateOpGreater, true
	case logical.FilterGreaterEqual:
		return storage.PredicateOpGreaterEqual, true
	default:
		return storage.PredicateNone, false
	}
}

func exprOpToFilterOp(op logical.Op) (logical.FilterOp, bool) {
	switch op {
	case logical.OpEqual:
		return logical.FilterEqual, true
	case logical.OpNotEqual:
		return logical.FilterNotEqual, true
	case logical.OpLess:
		return logical.FilterLess, true
	case logical.OpLessEqual:
		return logical.FilterLessEqual, true
	case logical.OpGreater:
		return logical.FilterGreater, true
	case logical.OpGreaterEqual:
		return logical.FilterGreaterEqual, true
	default:
		return 0, false
	}
}

func flipComparisonOp(op logical.Op) logical.Op {
	switch op {
	case logical.OpLess:
		return logical.OpGreater
	case logical.OpLessEqual:
		return logical.OpGreaterEqual
	case logical.OpGreater:
		return logical.OpLess
	case logical.OpGreaterEqual:
		return logical.OpLessEqual
	default:
		return op
	}
}

func aggregateResultColumn(plan logical.Query, fallback string) string {
	if plan.AggregateAlias != "" {
		return plan.AggregateAlias
	}
	return fallback
}

func groupResultColumn(plan logical.Query) string {
	if plan.GroupAlias != "" {
		return plan.GroupAlias
	}
	return plan.GroupColumn
}

func groupedCountRows(groupColumn string, countColumn string, counts map[string]uint64) *Rows {
	keys := sortedMapKeys(counts)
	values := make([][]any, 0, len(keys))
	for _, key := range keys {
		values = append(values, []any{key, counts[key]})
	}
	return &Rows{Columns: []string{groupColumn, countColumn}, Values: values}
}

func groupedSumRows(groupColumn string, sumColumn string, sums map[string]int64) *Rows {
	return groupedIntRows(groupColumn, sumColumn, sums)
}

func groupedIntRows(groupColumn string, valueColumn string, valuesByGroup map[string]int64) *Rows {
	keys := sortedMapKeys(valuesByGroup)
	values := make([][]any, 0, len(keys))
	for _, key := range keys {
		values = append(values, []any{key, valuesByGroup[key]})
	}
	return &Rows{Columns: []string{groupColumn, valueColumn}, Values: values}
}

func groupedAnyRows(groupColumn string, valueColumn string, valuesByGroup map[any]any) *Rows {
	keys := make([]any, 0, len(valuesByGroup))
	for key := range valuesByGroup {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return lessOrderValue(keys[i], keys[j]) })
	values := make([][]any, 0, len(keys))
	for _, key := range keys {
		values = append(values, []any{key, valuesByGroup[key]})
	}
	return &Rows{Columns: []string{groupColumn, valueColumn}, Values: values}
}

func sortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// predicateValues holds the literal values referenced by a single-column
// predicate, decoupled from logical.Query so the engine can build storage
// predicates without populating a transient logical.Query.
type predicateValues struct {
	Op      logical.FilterOp
	Int64   int64
	LoInt64 int64
	HiInt64 int64
	Bool    bool
	String  string
	Ints    []int64
	Bools   []bool
	Strings []string
}

func predicateFromLogical(col catalog.ColumnDef, kind sqltype.Kind, vals predicateValues) (storage.Predicate, error) {
	switch kind {
	case sqltype.KindBool:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Bools: vals.Bools}, nil
		}
		if vals.Op == logical.FilterNotEqual {
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Bool: vals.Bool}, nil
		}
		if vals.Op != logical.FilterEqual {
			return storage.Predicate{}, fmt.Errorf("SELECT predicate execution for bool BETWEEN is not implemented while %s", storageRebuildMessage)
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Bool: vals.Bool}, nil
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindDate:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			values, err := int32PredicateValues(col, vals.Ints)
			if err != nil {
				return storage.Predicate{}, err
			}
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Int32s: values}, nil
		}
		if vals.Op == logical.FilterBetween {
			lo, err := int32PredicateValue(col, vals.LoInt64)
			if err != nil {
				return storage.Predicate{}, err
			}
			hi, err := int32PredicateValue(col, vals.HiInt64)
			if err != nil {
				return storage.Predicate{}, err
			}
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpBetween, Lo32: lo, Hi32: hi}, nil
		}
		value, err := int32PredicateValue(col, vals.Int64)
		if err != nil {
			return storage.Predicate{}, err
		}
		if op, ok := int32ComparisonPredicate(vals.Op); ok {
			return storage.Predicate{ColumnID: col.ID, Op: op, Int32: value}, nil
		}
		if vals.Op == logical.FilterNotEqual {
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Int32: value}, nil
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Int32: value}, nil
	case sqltype.KindInt64, sqltype.KindTimestamp:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Int64s: vals.Ints}, nil
		}
		if vals.Op == logical.FilterBetween {
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpBetween, Lo: vals.LoInt64, Hi: vals.HiInt64}, nil
		}
		if op, ok := int64ComparisonPredicate(vals.Op); ok {
			return storage.Predicate{ColumnID: col.ID, Op: op, Int64: vals.Int64}, nil
		}
		if vals.Op == logical.FilterNotEqual {
			return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Int64: vals.Int64}, nil
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Int64: vals.Int64}, nil
	case sqltype.KindText:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Texts: vals.Strings}, nil
		}
		if vals.Op != logical.FilterEqual {
			if vals.Op == logical.FilterNotEqual {
				return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Text: vals.String}, nil
			}
			return storage.Predicate{}, fmt.Errorf("SELECT predicate execution for text BETWEEN is not implemented while %s", storageRebuildMessage)
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Text: vals.String}, nil
	case sqltype.KindBytes:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Texts: vals.Strings}, nil
		}
		if vals.Op != logical.FilterEqual {
			if vals.Op == logical.FilterNotEqual {
				return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Text: vals.String}, nil
			}
			return storage.Predicate{}, fmt.Errorf("SELECT predicate execution for bytes BETWEEN is not implemented while %s", storageRebuildMessage)
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Text: vals.String}, nil
	case sqltype.KindNamed:
		if vals.Op == logical.FilterIn || vals.Op == logical.FilterNotIn {
			values := make([]uint32, 0, len(vals.Strings))
			for _, value := range vals.Strings {
				code, ok := enumCodeForLabel(value, col.Labels)
				if !ok {
					return storage.Predicate{}, fmt.Errorf("WHERE column %q invalid enum label %q", col.Name, value)
				}
				values = append(values, code)
			}
			op := storage.PredicateOpIn
			if vals.Op == logical.FilterNotIn {
				op = storage.PredicateOpNotIn
			}
			return storage.Predicate{ColumnID: col.ID, Op: op, Enums: values}, nil
		}
		code, ok := enumCodeForLabel(vals.String, col.Labels)
		if !ok {
			return storage.Predicate{}, fmt.Errorf("WHERE column %q invalid enum label %q", col.Name, vals.String)
		}
		if vals.Op != logical.FilterEqual {
			if vals.Op == logical.FilterNotEqual {
				return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpNotEq, Enum: code}, nil
			}
			return storage.Predicate{}, fmt.Errorf("SELECT predicate execution for enum BETWEEN is not implemented while %s", storageRebuildMessage)
		}
		return storage.Predicate{ColumnID: col.ID, Op: storage.PredicateOpEq, Enum: code}, nil
	default:
		return storage.Predicate{}, fmt.Errorf("SELECT predicate execution for %s is not implemented while %s", kind, storageRebuildMessage)
	}
}

func int32PredicateValues(col catalog.ColumnDef, values []int64) ([]int32, error) {
	out := make([]int32, 0, len(values))
	for _, value := range values {
		converted, err := int32PredicateValue(col, value)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func int32PredicateValue(col catalog.ColumnDef, value int64) (int32, error) {
	lo, hi := int64(-1<<31), int64(1<<31-1)
	if col.Type.Kind == sqltype.KindInt16 {
		lo, hi = -1<<15, 1<<15-1
	}
	if value < lo || value > hi {
		return 0, fmt.Errorf("WHERE column %q literal out of range for %s", col.Name, col.Type)
	}
	return int32(value), nil
}

func int32ComparisonPredicate(op logical.FilterOp) (storage.PredicateOp, bool) {
	switch op {
	case logical.FilterLess:
		return storage.PredicateOpLess, true
	case logical.FilterLessEqual:
		return storage.PredicateOpLessEqual, true
	case logical.FilterGreater:
		return storage.PredicateOpGreater, true
	case logical.FilterGreaterEqual:
		return storage.PredicateOpGreaterEqual, true
	default:
		return storage.PredicateNone, false
	}
}

func int64ComparisonPredicate(op logical.FilterOp) (storage.PredicateOp, bool) {
	switch op {
	case logical.FilterLess:
		return storage.PredicateOpLess, true
	case logical.FilterLessEqual:
		return storage.PredicateOpLessEqual, true
	case logical.FilterGreater:
		return storage.PredicateOpGreater, true
	case logical.FilterGreaterEqual:
		return storage.PredicateOpGreaterEqual, true
	default:
		return storage.PredicateNone, false
	}
}
