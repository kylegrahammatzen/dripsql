package storage

import (
	"cmp"
	"fmt"
	"math"
	"os"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// PredicateOp identifies the leaf operator of a predicate; the column type
// determines which value field of Predicate is read at evaluation time.
type PredicateOp uint8

const (
	PredicateNone PredicateOp = iota
	PredicateOpEq
	PredicateOpNotEq
	PredicateOpLess
	PredicateOpLessEqual
	PredicateOpGreater
	PredicateOpGreaterEqual
	PredicateOpBetween
	PredicateOpIn
	PredicateOpNotIn
	PredicateAnd
	PredicateOr
	PredicateNot
)

type Predicate struct {
	ColumnID   catalog.ColumnID
	Op         PredicateOp
	Int32      int32
	Lo32       int32
	Hi32       int32
	Int64      int64
	Lo         int64
	Hi         int64
	Float64    float64
	LoFloat64  float64
	HiFloat64  float64
	Text       string
	Bool       bool
	Int32s     []int32
	Int64s     []int64
	Float64s   []float64
	UUID       vector.UUID16
	UUIDs      []vector.UUID16
	Enum       uint32
	Enums      []uint32
	Texts      []string
	Bools      []bool
	Predicates []Predicate
}

// orderedOps lists the operators that need < <= > >= semantics on top of
// = != IN NOT IN BETWEEN. Used by validatePredicateColumn.
var orderedOps = []PredicateOp{
	PredicateOpEq, PredicateOpNotEq,
	PredicateOpLess, PredicateOpLessEqual, PredicateOpGreater, PredicateOpGreaterEqual,
	PredicateOpBetween, PredicateOpIn, PredicateOpNotIn,
}

// equatableOps lists the operators allowed on column kinds that support
// equality but not ordering (bool, uuid, bytes, enum, text).
var equatableOps = []PredicateOp{
	PredicateOpEq, PredicateOpNotEq,
	PredicateOpIn, PredicateOpNotIn,
}

// opsForKind maps each supported column kind to the set of leaf ops it accepts.
var opsForKind = map[sqltype.Kind][]PredicateOp{
	sqltype.KindBool:      equatableOps,
	sqltype.KindInt16:     orderedOps,
	sqltype.KindInt32:     orderedOps,
	sqltype.KindDate:      orderedOps,
	sqltype.KindInt64:     orderedOps,
	sqltype.KindTimestamp: orderedOps,
	sqltype.KindFloat32:   orderedOps,
	sqltype.KindFloat64:   orderedOps,
	sqltype.KindUUID:      equatableOps,
	sqltype.KindBytes:     equatableOps,
	sqltype.KindNamed:     equatableOps,
	sqltype.KindText:      equatableOps,
}

// validatePredicate verifies that the predicate tree references valid columns
// in table and that each leaf op is compatible with its column kind.
func validatePredicate(table catalog.TableDef, pred Predicate) error {
	if pred.Op == PredicateAnd || pred.Op == PredicateOr {
		if len(pred.Predicates) == 0 {
			return fmt.Errorf("compound predicate requires at least one child predicate")
		}
		for _, child := range pred.Predicates {
			if err := validatePredicate(table, child); err != nil {
				return err
			}
		}
		return nil
	}
	if pred.Op == PredicateNot {
		if len(pred.Predicates) != 1 {
			return fmt.Errorf("NOT predicate requires exactly one child predicate")
		}
		return validatePredicate(table, pred.Predicates[0])
	}
	col, _, ok := tableColumnByID(table, pred.ColumnID)
	if !ok {
		return fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
	}
	return validatePredicateColumn(col, pred)
}

// validatePredicateColumn checks that pred.Op is allowed on col's kind.
func validatePredicateColumn(col catalog.ColumnDef, pred Predicate) error {
	allowed, ok := opsForKind[col.Type.Kind]
	if !ok {
		return fmt.Errorf("predicate column %q is %s, unsupported", col.Name, col.Type)
	}
	if !slices.Contains(allowed, pred.Op) {
		return fmt.Errorf("unsupported predicate op %d on column %q (%s)", pred.Op, col.Name, col.Type)
	}
	return nil
}

// predicateColumnIndexes returns table indexes of every column referenced by
// the predicate tree, in the order matchPredicateColumnsAt expects.
func predicateColumnIndexes(table catalog.TableDef, pred Predicate) ([]int, error) {
	if pred.Op == PredicateNone {
		return nil, nil
	}
	if pred.Op != PredicateAnd && pred.Op != PredicateOr {
		if pred.Op == PredicateNot {
			return predicateColumnIndexes(table, pred.Predicates[0])
		}
		_, index, ok := tableColumnByID(table, pred.ColumnID)
		if !ok {
			return nil, fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
		}
		return []int{index}, nil
	}
	indexes := make([]int, 0, len(pred.Predicates))
	for _, child := range pred.Predicates {
		childIndexes, err := predicateColumnIndexes(table, child)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, childIndexes...)
	}
	return indexes, nil
}

// predicateColumnMetas returns segment column metas for every column referenced
// by the predicate tree, in the same order as predicateColumnIndexes.
func predicateColumnMetas(segment SegmentMeta, pred Predicate) ([]ColumnMeta, error) {
	if pred.Op == PredicateNone {
		return nil, nil
	}
	if pred.Op != PredicateAnd && pred.Op != PredicateOr {
		if pred.Op == PredicateNot {
			return predicateColumnMetas(segment, pred.Predicates[0])
		}
		meta, _, ok := columnMetaByID(segment, pred.ColumnID)
		if !ok {
			return nil, fmt.Errorf("segment %d missing predicate column ID %d", segment.ID, pred.ColumnID)
		}
		return []ColumnMeta{meta}, nil
	}
	metas := make([]ColumnMeta, 0, len(pred.Predicates))
	for _, child := range pred.Predicates {
		childMetas, err := predicateColumnMetas(segment, child)
		if err != nil {
			return nil, err
		}
		metas = append(metas, childMetas...)
	}
	return metas, nil
}

// readPredicateColumns decodes one page worth of every predicate column.
func readPredicateColumns(file *os.File, predMetas []ColumnMeta, pageIndex int) ([]vector.Column, error) {
	cols := make([]vector.Column, 0, len(predMetas))
	for _, meta := range predMetas {
		col, err := readColumnPageFromFile(file, meta, meta.Pages[pageIndex])
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// predicateColumnsFromBatch picks predicate columns out of a buffered batch.
func predicateColumnsFromBatch(batch vector.Batch, indexes []int, pred Predicate) ([]vector.Column, error) {
	if pred.Op == PredicateNone {
		return nil, nil
	}
	cols := make([]vector.Column, 0, len(indexes))
	for _, index := range indexes {
		if index >= len(batch.Columns) {
			return nil, fmt.Errorf("missing buffered predicate column index %d", index)
		}
		cols = append(cols, batch.Columns[index])
	}
	return cols, nil
}

// matchPredicateColumns evaluates the full predicate tree (compound or leaf)
// for one row across cols, which contains every predicate column in order.
func matchPredicateColumns(cols []vector.Column, row int, pred Predicate) (bool, error) {
	if pred.Op == PredicateNone {
		return true, nil
	}
	if pred.Op != PredicateAnd && pred.Op != PredicateOr {
		if pred.Op == PredicateNot {
			matched, used, err := matchPredicateColumnsAt(cols, row, pred.Predicates[0], 0)
			if err != nil || used != len(cols) {
				return false, err
			}
			return !matched, nil
		}
		if len(cols) == 0 {
			return false, fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
		}
		return matchPredicateRow(cols[0], row, pred)
	}
	matched, used, err := matchPredicateColumnsAt(cols, row, pred, 0)
	if err != nil {
		return false, err
	}
	if used != len(cols) {
		return false, fmt.Errorf("compound predicate column count mismatch")
	}
	return matched, nil
}

// matchPredicateColumnsAt walks the tree, consuming columns left-to-right;
// returns the number of columns consumed so siblings continue from the right
// position.
func matchPredicateColumnsAt(cols []vector.Column, row int, pred Predicate, offset int) (bool, int, error) {
	switch pred.Op {
	case PredicateAnd:
		used := offset
		matchedAll := true
		for _, child := range pred.Predicates {
			matched, next, err := matchPredicateColumnsAt(cols, row, child, used)
			if err != nil {
				return false, next, err
			}
			matchedAll = matchedAll && matched
			used = next
		}
		return matchedAll, used, nil
	case PredicateOr:
		used := offset
		matchedAny := false
		for _, child := range pred.Predicates {
			matched, next, err := matchPredicateColumnsAt(cols, row, child, used)
			if err != nil {
				return false, next, err
			}
			matchedAny = matchedAny || matched
			used = next
		}
		return matchedAny, used, nil
	case PredicateNot:
		matched, used, err := matchPredicateColumnsAt(cols, row, pred.Predicates[0], offset)
		return !matched, used, err
	default:
		if offset >= len(cols) {
			return false, offset, fmt.Errorf("missing predicate column ID %d", pred.ColumnID)
		}
		matched, err := matchPredicateRow(cols[offset], row, pred)
		return matched, offset + 1, err
	}
}

// matchPredicateRow evaluates a single-column leaf predicate for one row,
// dispatching on the column's kind and returning false (not error) for null
// rows.
func matchPredicateRow(col vector.Column, row int, pred Predicate) (bool, error) {
	if !vector.IsValid(col.V.Valid, row) {
		return false, nil
	}
	switch col.Type.Kind {
	case sqltype.KindBool:
		return matchEquatable(boolAt(col.V.BoolBits, row), pred.Bool, pred.Bools, pred.Op)
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindDate:
		v, err := predicateInt32Value(col, row)
		if err != nil {
			return false, err
		}
		return matchOrdered(v, pred.Int32, pred.Lo32, pred.Hi32, pred.Int32s, pred.Op)
	case sqltype.KindInt64, sqltype.KindTimestamp:
		v, err := predicateInt64Value(col, row)
		if err != nil {
			return false, err
		}
		return matchOrdered(v, pred.Int64, pred.Lo, pred.Hi, pred.Int64s, pred.Op)
	case sqltype.KindFloat32, sqltype.KindFloat64:
		v, err := predicateFloatValue(col, row)
		if err != nil {
			return false, err
		}
		return matchFloat(v, pred)
	case sqltype.KindUUID:
		return matchEquatable(col.V.UUID[row], pred.UUID, pred.UUIDs, pred.Op)
	case sqltype.KindNamed:
		return matchEquatable(col.V.U32[row], pred.Enum, pred.Enums, pred.Op)
	case sqltype.KindBytes:
		return matchBytes(col, row, pred)
	case sqltype.KindText:
		return matchText(col, row, pred)
	default:
		return false, fmt.Errorf("predicate column %q is %s, unsupported", col.Name, col.Type)
	}
}

// matchOrdered evaluates pred.Op against an ordered scalar (int/float-shaped).
func matchOrdered[T cmp.Ordered](value, target, lo, hi T, in []T, op PredicateOp) (bool, error) {
	switch op {
	case PredicateOpEq:
		return value == target, nil
	case PredicateOpNotEq:
		return value != target, nil
	case PredicateOpLess:
		return value < target, nil
	case PredicateOpLessEqual:
		return value <= target, nil
	case PredicateOpGreater:
		return value > target, nil
	case PredicateOpGreaterEqual:
		return value >= target, nil
	case PredicateOpBetween:
		return lo <= value && value <= hi, nil
	case PredicateOpIn:
		return slices.Contains(in, value), nil
	case PredicateOpNotIn:
		return !slices.Contains(in, value), nil
	}
	return false, fmt.Errorf("unsupported ordered op %d", op)
}

// matchEquatable evaluates pred.Op against a comparable scalar (bool, uuid,
// enum code).
func matchEquatable[T comparable](value, target T, in []T, op PredicateOp) (bool, error) {
	switch op {
	case PredicateOpEq:
		return value == target, nil
	case PredicateOpNotEq:
		return value != target, nil
	case PredicateOpIn:
		return slices.Contains(in, value), nil
	case PredicateOpNotIn:
		return !slices.Contains(in, value), nil
	}
	return false, fmt.Errorf("unsupported equatable op %d", op)
}

// matchFloat handles float comparisons with NaN propagating to false on every
// op (SQL semantics: NaN never satisfies =, !=, <, <=, >, >=).
func matchFloat(value float64, pred Predicate) (bool, error) {
	switch pred.Op {
	case PredicateOpEq:
		return floatCompare(value, pred.Float64, pred.Op), nil
	case PredicateOpNotEq:
		return floatCompare(value, pred.Float64, pred.Op), nil
	case PredicateOpLess, PredicateOpLessEqual, PredicateOpGreater, PredicateOpGreaterEqual:
		return floatCompare(value, pred.Float64, pred.Op), nil
	case PredicateOpBetween:
		return floatCompare(value, pred.LoFloat64, PredicateOpGreaterEqual) && floatCompare(value, pred.HiFloat64, PredicateOpLessEqual), nil
	case PredicateOpIn:
		return predicateFloatIn(value, pred.Float64s), nil
	case PredicateOpNotIn:
		return !predicateFloatIn(value, pred.Float64s), nil
	}
	return false, fmt.Errorf("unsupported float op %d", pred.Op)
}

// matchBytes handles bytes columns; the predicate carries the literal in
// pred.Text / pred.Texts (UTF-8 byte literals from the SQL parser).
func matchBytes(col vector.Column, row int, pred Predicate) (bool, error) {
	value, err := predicateBytesValue(col, row)
	if err != nil {
		return false, err
	}
	switch pred.Op {
	case PredicateOpEq:
		return bytesEqualString(value, pred.Text), nil
	case PredicateOpNotEq:
		return !bytesEqualString(value, pred.Text), nil
	case PredicateOpIn:
		return predicateBytesIn(value, pred.Texts), nil
	case PredicateOpNotIn:
		return !predicateBytesIn(value, pred.Texts), nil
	}
	return false, fmt.Errorf("unsupported bytes op %d", pred.Op)
}

// matchText handles text columns; like matchBytes but reads the column's text
// payload directly from offsets/data.
func matchText(col vector.Column, row int, pred Predicate) (bool, error) {
	if col.Type.Kind != sqltype.KindText {
		return false, fmt.Errorf("predicate column %q is %s, want text", col.Name, col.Type)
	}
	start := int(col.V.Var.Offsets[row])
	end := int(col.V.Var.Offsets[row+1])
	if start > end || end > len(col.V.Var.Data) {
		return false, fmt.Errorf("text offsets are out of bounds")
	}
	value := col.V.Var.Data[start:end]
	switch pred.Op {
	case PredicateOpEq:
		return bytesEqualString(value, pred.Text), nil
	case PredicateOpNotEq:
		return !bytesEqualString(value, pred.Text), nil
	case PredicateOpIn:
		return predicateBytesIn(value, pred.Texts), nil
	case PredicateOpNotIn:
		return !predicateBytesIn(value, pred.Texts), nil
	}
	return false, fmt.Errorf("unsupported text op %d", pred.Op)
}

// floatCompare returns false when either operand is NaN; otherwise applies op.
func floatCompare(left, right float64, op PredicateOp) bool {
	if math.IsNaN(left) || math.IsNaN(right) {
		return false
	}
	switch op {
	case PredicateOpEq:
		return left == right
	case PredicateOpNotEq:
		return left != right
	case PredicateOpLess:
		return left < right
	case PredicateOpLessEqual:
		return left <= right
	case PredicateOpGreater:
		return left > right
	case PredicateOpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func predicateFloatIn(value float64, values []float64) bool {
	for _, candidate := range values {
		if floatCompare(value, candidate, PredicateOpEq) {
			return true
		}
	}
	return false
}

// predicateBytesIn can't use slices.Contains since []byte and string differ
// and we want to avoid the allocating string(value) cast.
func predicateBytesIn(value []byte, values []string) bool {
	for _, candidate := range values {
		if bytesEqualString(value, candidate) {
			return true
		}
	}
	return false
}

// countPredicateSegmentMeta returns the segment-level count for text-eq when
// the text stats fully decide the predicate without page reads.
func countPredicateSegmentMeta(col ColumnMeta, pred Predicate) (uint32, bool) {
	if pred.Op != PredicateOpEq || col.Type.Kind != sqltype.KindText || col.Text == nil {
		return 0, false
	}
	return col.Text.Count(pred.Text)
}

// countPredicatePageMeta is the per-page analogue of countPredicateSegmentMeta.
func countPredicatePageMeta(col ColumnMeta, page PageMeta, pred Predicate) (uint32, bool) {
	if pred.Op != PredicateOpEq || col.Type.Kind != sqltype.KindText || page.Text == nil {
		return 0, false
	}
	return page.Text.Count(pred.Text)
}

func predicateInt32Value(col vector.Column, row int) (int32, error) {
	switch col.Type.Kind {
	case sqltype.KindInt16:
		return int32(col.V.I16[row]), nil
	case sqltype.KindInt32, sqltype.KindDate:
		return col.V.I32[row], nil
	default:
		return 0, fmt.Errorf("predicate column %q is %s, want int16, int32, or date", col.Name, col.Type)
	}
}

func predicateInt64Value(col vector.Column, row int) (int64, error) {
	switch col.Type.Kind {
	case sqltype.KindInt64, sqltype.KindTimestamp:
		return col.V.I64[row], nil
	default:
		return 0, fmt.Errorf("predicate column %q is %s, want int64 or timestamp", col.Name, col.Type)
	}
}

func predicateFloatValue(col vector.Column, row int) (float64, error) {
	switch col.Type.Kind {
	case sqltype.KindFloat32:
		return float64(col.V.F32[row]), nil
	case sqltype.KindFloat64:
		return col.V.F64[row], nil
	default:
		return 0, fmt.Errorf("predicate column %q is %s, want float32 or float64", col.Name, col.Type)
	}
}

func predicateBytesValue(col vector.Column, row int) ([]byte, error) {
	if col.Type.Kind != sqltype.KindBytes {
		return nil, fmt.Errorf("predicate column %q is %s, want bytes", col.Name, col.Type)
	}
	start := int(col.V.Var.Offsets[row])
	end := int(col.V.Var.Offsets[row+1])
	if start > end || end > len(col.V.Var.Data) {
		return nil, fmt.Errorf("bytes offsets are out of bounds")
	}
	return col.V.Var.Data[start:end], nil
}

// intValueAt extracts an int64-shaped value, widening int32 if needed; used
// by the sum/min/max/group_any aggregate paths.
func intValueAt(col vector.Column, row int) (int64, error) {
	switch col.Type.Kind {
	case sqltype.KindInt32:
		return int64(col.V.I32[row]), nil
	case sqltype.KindInt64:
		return col.V.I64[row], nil
	default:
		return 0, fmt.Errorf("sum column %q is %s, want int32 or int64", col.Name, col.Type)
	}
}
