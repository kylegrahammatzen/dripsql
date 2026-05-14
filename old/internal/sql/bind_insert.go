package sql

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type InsertValues struct {
	Table    string
	Columns  []InsertColumn
	RowCount int
}

type InsertColumn struct {
	Name      string
	ID        ColumnID
	Type      types.Type
	Labels    []string
	Nullable  bool
	Values    []Value
	NullCount int
}

func BindInsertPlan(stmt *InsertStmt, def BoundTableDef) (*InsertPlan, error) {
	bound, err := BindInsertValues(stmt, def)
	if err != nil {
		return nil, err
	}
	columns := make([]ColumnID, 0, len(bound.Columns))
	for _, col := range bound.Columns {
		columns = append(columns, col.ID)
	}
	return &InsertPlan{Table: def, Columns: columns, Values: stmt.Values}, nil
}

func BindInsertValues(stmt *InsertStmt, def BoundTableDef) (InsertValues, error) {
	if stmt == nil {
		return InsertValues{}, fmt.Errorf("INSERT statement is nil")
	}
	if def.Name != "" && normalizeName(stmt.Table) != normalizeName(def.Name) {
		return InsertValues{}, fmt.Errorf("INSERT target %q does not match table %q", stmt.Table, def.Name)
	}
	if len(def.Columns) == 0 {
		return InsertValues{}, fmt.Errorf("table %q has no columns", def.Name)
	}
	if len(stmt.Values) == 0 {
		return InsertValues{}, fmt.Errorf("INSERT requires at least one row")
	}

	sourceIndexes, sourceCount, err := insertSourceIndexes(stmt.Columns, def.Columns)
	if err != nil {
		return InsertValues{}, err
	}

	for rowIndex, row := range stmt.Values {
		if len(row) != sourceCount {
			return InsertValues{}, fmt.Errorf("INSERT row %d has %d values, want %d", rowIndex+1, len(row), sourceCount)
		}
	}

	bound := InsertValues{Table: def.Name, RowCount: len(stmt.Values), Columns: make([]InsertColumn, 0, len(def.Columns))}
	for tableIndex, col := range def.Columns {
		sourceIndex := sourceIndexes[tableIndex]
		boundCol := InsertColumn{Name: col.Name, ID: col.ID, Type: col.Type, Labels: col.Labels, Nullable: col.Nullable, Values: make([]Value, len(stmt.Values))}
		for rowIndex, row := range stmt.Values {
			lit := row[sourceIndex]
			if lit.Kind == ValueNull {
				if !col.Nullable {
					return InsertValues{}, fmt.Errorf("row %d: column %q is NOT NULL", rowIndex+1, col.Name)
				}
				boundCol.NullCount++
				boundCol.Values[rowIndex] = lit
				continue
			}
			if err := validateLiteralForColumn(col, lit); err != nil {
				return InsertValues{}, fmt.Errorf("row %d: %w", rowIndex+1, err)
			}
			boundCol.Values[rowIndex] = lit
		}
		bound.Columns = append(bound.Columns, boundCol)
	}
	return bound, nil
}

func insertSourceIndexes(names []string, columns []BoundColumnDef) ([]int, int, error) {
	if len(names) == 0 {
		indexes := make([]int, len(columns))
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, len(columns), nil
	}
	if len(names) != len(columns) {
		return nil, 0, fmt.Errorf("INSERT column count %d does not match table column count %d", len(names), len(columns))
	}

	byName := make(map[string]int, len(columns))
	for i, col := range columns {
		byName[normalizeName(col.Name)] = i
	}
	sourceByTableIndex := make([]int, len(columns))
	usedAt := make(map[string]int, len(names))
	for sourceIndex, name := range names {
		norm := normalizeName(name)
		if prev, ok := usedAt[norm]; ok {
			return nil, 0, fmt.Errorf("duplicate INSERT column %q (positions %d and %d)", name, prev+1, sourceIndex+1)
		}
		usedAt[norm] = sourceIndex

		tableIndex, ok := byName[norm]
		if !ok {
			return nil, 0, fmt.Errorf("unknown INSERT column %q", name)
		}
		sourceByTableIndex[tableIndex] = sourceIndex
	}
	return sourceByTableIndex, len(names), nil
}

func validateLiteralForColumn(col BoundColumnDef, lit Value) error {
	if col.Type.Kind != types.KindNamed {
		return validateLiteralForType(col.Name, lit, col.Type)
	}
	value, err := bindStringLiteral(col.Name, lit)
	if err != nil {
		return err
	}
	if _, ok := enumCodeForLabel(value, col.Labels); !ok {
		return fmt.Errorf("column %q invalid enum label %q", col.Name, value)
	}
	return nil
}

func validateLiteralForType(column string, lit Value, typ types.Type) error {
	switch typ.Kind {
	case types.KindBool:
		_, err := bindBoolLiteral(column, lit)
		return err
	case types.KindInt16:
		value, err := bindInt64Literal(column, lit)
		if err != nil {
			return err
		}
		if value < math.MinInt16 || value > math.MaxInt16 {
			return fmt.Errorf("column %q int16 literal out of range", column)
		}
		return nil
	case types.KindInt32:
		_, err := bindInt32Literal(column, lit)
		return err
	case types.KindInt64:
		_, err := bindInt64Literal(column, lit)
		return err
	case types.KindFloat32, types.KindFloat64:
		value, err := bindFloat64Literal(column, lit)
		if err != nil {
			return err
		}
		if typ.Kind == types.KindFloat32 && (value < -math.MaxFloat32 || value > math.MaxFloat32) {
			return fmt.Errorf("column %q float32 literal out of range", column)
		}
		return nil
	case types.KindDecimal:
		if lit.Kind != ValueInt && lit.Kind != ValueString {
			return fmt.Errorf("column %q expects decimal literal", column)
		}
		return nil
	case types.KindText, types.KindJSON, types.KindNamed:
		_, err := bindStringLiteral(column, lit)
		return err
	case types.KindBytes, types.KindUUID, types.KindTimestamp, types.KindTime, types.KindDate:
		if lit.Kind != ValueString {
			return fmt.Errorf("column %q expects string literal", column)
		}
		return nil
	default:
		return fmt.Errorf("column %q has unsupported INSERT type %s", column, typ)
	}
}

func enumCodeForLabel(value string, labels []string) (uint32, bool) {
	for i, label := range labels {
		if value == label {
			return uint32(i + 1), true
		}
	}
	return 0, false
}

func bindInt64Literal(column string, lit Value) (int64, error) {
	if lit.Kind != ValueInt {
		return 0, fmt.Errorf("column %q expects int64 literal", column)
	}
	return lit.Int, nil
}

func bindInt32Literal(column string, lit Value) (int32, error) {
	value, err := bindInt64Literal(column, lit)
	if err != nil {
		return 0, err
	}
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("column %q int32 literal out of range", column)
	}
	return int32(value), nil
}

func bindFloat64Literal(column string, lit Value) (float64, error) {
	var value float64
	switch lit.Kind {
	case ValueInt:
		value = float64(lit.Int)
	case ValueFloat:
		value = lit.Float
	default:
		return 0, fmt.Errorf("column %q expects float literal", column)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("column %q float literal out of range", column)
	}
	return value, nil
}

func bindBoolLiteral(column string, lit Value) (bool, error) {
	if lit.Kind != ValueBool {
		return false, fmt.Errorf("column %q expects bool literal", column)
	}
	return lit.Bool, nil
}

func bindStringLiteral(column string, lit Value) (string, error) {
	if lit.Kind != ValueString {
		return "", fmt.Errorf("column %q expects string literal", column)
	}
	return lit.String, nil
}
