package exec

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type SortKey struct {
	Column string
	Desc   bool
}

type Sort struct {
	Keys       []SortKey
	Downstream Consumer

	batches []types.Batch
	items   []sortItem
	state   operatorState
}

type sortItem struct {
	batch int
	row   int
	seq   int
}

func (s *Sort) Open(ctx context.Context) error {
	if s.Downstream == nil {
		return fmt.Errorf("sort downstream is nil")
	}
	if len(s.Keys) == 0 {
		return fmt.Errorf("sort requires at least one key")
	}
	for _, key := range s.Keys {
		if key.Column == "" {
			return fmt.Errorf("sort key column is required")
		}
	}
	if err := s.state.open(); err != nil {
		return err
	}
	return s.Downstream.Open(ctx)
}

func (s *Sort) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := s.state.requireOpen(); err != nil {
		return err
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	if sel.PopCount() == 0 {
		return nil
	}
	if err := s.validateSortKeys(batch); err != nil {
		return err
	}
	batchIndex := len(s.batches)
	s.batches = append(s.batches, batch)
	sel.IterSet(func(row int) {
		s.items = append(s.items, sortItem{batch: batchIndex, row: row, seq: len(s.items)})
	})
	return nil
}

func (s *Sort) Close() error {
	if err := s.state.requireOpen(); err != nil {
		return err
	}
	if len(s.items) != 0 {
		s.sortItems()
		batch, sel, err := s.sortedBatch()
		if err != nil {
			_ = s.Downstream.Close()
			_ = s.state.close()
			return err
		}
		if err := s.Downstream.Push(batch, sel); err != nil {
			_ = s.Downstream.Close()
			_ = s.state.close()
			return err
		}
	}
	if err := s.Downstream.Close(); err != nil {
		_ = s.state.close()
		return err
	}
	return s.state.close()
}

func (s *Sort) validateSortKeys(batch types.Batch) error {
	for _, key := range s.Keys {
		col, ok := columnByName(batch, key.Column)
		if !ok {
			return fmt.Errorf("missing sort column %q", key.Column)
		}
		if !sortableKind(col.V.Kind) {
			return fmt.Errorf("sort column %q has unsupported kind %s", key.Column, col.V.Kind)
		}
	}
	return nil
}

func sortableKind(kind types.VecKind) bool {
	switch kind {
	case types.VecBool,
		types.VecInt16,
		types.VecInt32,
		types.VecDate,
		types.VecInt64,
		types.VecTimestamp,
		types.VecTime,
		types.VecFloat32,
		types.VecFloat64,
		types.VecText:
		return true
	default:
		return false
	}
}

func (s *Sort) sortItems() {
	sort.SliceStable(s.items, func(i int, j int) bool {
		left := s.items[i]
		right := s.items[j]
		for _, key := range s.Keys {
			cmp := s.compareKey(left, right, key.Column)
			if cmp == 0 {
				continue
			}
			if key.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return left.seq < right.seq
	})
}

func (s *Sort) compareKey(left sortItem, right sortItem, column string) int {
	leftCol, _ := columnByName(s.batches[left.batch], column)
	rightCol, _ := columnByName(s.batches[right.batch], column)
	leftValid := types.IsValid(leftCol.V.Valid, left.row)
	rightValid := types.IsValid(rightCol.V.Valid, right.row)
	if !leftValid || !rightValid {
		if leftValid == rightValid {
			return 0
		}
		if leftValid {
			return -1
		}
		return 1
	}
	switch leftCol.V.Kind {
	case types.VecBool:
		leftValue := leftCol.V.BoolBits[left.row>>6]&(uint64(1)<<uint(left.row&63)) != 0
		rightValue := rightCol.V.BoolBits[right.row>>6]&(uint64(1)<<uint(right.row&63)) != 0
		if leftValue == rightValue {
			return 0
		}
		if !leftValue {
			return -1
		}
		return 1
	case types.VecInt16:
		return compareOrdered(leftCol.V.I16[left.row], rightCol.V.I16[right.row])
	case types.VecInt32, types.VecDate:
		return compareOrdered(leftCol.V.I32[left.row], rightCol.V.I32[right.row])
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return compareOrdered(leftCol.V.I64[left.row], rightCol.V.I64[right.row])
	case types.VecFloat32:
		return compareOrdered(leftCol.V.F32[left.row], rightCol.V.F32[right.row])
	case types.VecFloat64:
		return compareOrdered(leftCol.V.F64[left.row], rightCol.V.F64[right.row])
	case types.VecText:
		leftValue, _ := leftCol.V.TextCopy(left.row)
		rightValue, _ := rightCol.V.TextCopy(right.row)
		return strings.Compare(leftValue, rightValue)
	default:
		return 0
	}
}

func compareOrdered[T ~int16 | ~int32 | ~int64 | ~uint32 | ~float32 | ~float64](left T, right T) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func (s *Sort) sortedBatch() (types.Batch, types.SelectionMask, error) {
	rows := len(s.items)
	first := s.batches[s.items[0].batch]
	cols := make([]types.Column, len(first.Columns))
	for colIndex, col := range first.Columns {
		outCol, err := s.gatherColumn(colIndex, col, rows)
		if err != nil {
			return types.Batch{}, types.SelectionMask{}, err
		}
		cols[colIndex] = outCol
	}
	batch, err := types.NewBatch(cols)
	if err != nil {
		return types.Batch{}, types.SelectionMask{}, err
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	return batch, sel, nil
}

func (s *Sort) gatherColumn(colIndex int, template types.Column, rows int) (types.Column, error) {
	valid := s.gatherValidity(colIndex, rows)
	v := types.Vec{Kind: template.V.Kind, Encoding: types.EncodingFlat, Len: rows, Valid: valid}
	switch template.V.Kind {
	case types.VecBool:
		v.BoolBits = make([]uint64, types.ValidityWords(rows))
		for outRow, item := range s.items {
			col := s.batches[item.batch].Columns[colIndex]
			if col.V.BoolBits[item.row>>6]&(uint64(1)<<uint(item.row&63)) != 0 {
				v.BoolBits[outRow>>6] |= uint64(1) << uint(outRow&63)
			}
		}
	case types.VecInt16:
		v.I16 = make([]int16, rows)
		for outRow, item := range s.items {
			v.I16[outRow] = s.batches[item.batch].Columns[colIndex].V.I16[item.row]
		}
	case types.VecInt32, types.VecDate:
		v.I32 = make([]int32, rows)
		for outRow, item := range s.items {
			v.I32[outRow] = s.batches[item.batch].Columns[colIndex].V.I32[item.row]
		}
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		v.I64 = make([]int64, rows)
		for outRow, item := range s.items {
			v.I64[outRow] = s.batches[item.batch].Columns[colIndex].V.I64[item.row]
		}
	case types.VecFloat32:
		v.F32 = make([]float32, rows)
		for outRow, item := range s.items {
			v.F32[outRow] = s.batches[item.batch].Columns[colIndex].V.F32[item.row]
		}
	case types.VecFloat64:
		v.F64 = make([]float64, rows)
		for outRow, item := range s.items {
			v.F64[outRow] = s.batches[item.batch].Columns[colIndex].V.F64[item.row]
		}
	case types.VecText:
		varText := types.NewVarBytes(rows, 0)
		for outRow, item := range s.items {
			value, ok := s.batches[item.batch].Columns[colIndex].V.TextCopy(item.row)
			if !ok {
				return types.Column{}, fmt.Errorf("cannot gather text column %q", template.Name)
			}
			varText.AppendString(outRow, value)
		}
		v.Var = varText
	default:
		return types.Column{}, fmt.Errorf("cannot gather column %q with kind %s", template.Name, template.V.Kind)
	}
	return types.Column{Name: template.Name, Type: template.Type, EnumLabels: template.EnumLabels, V: v}, nil
}

func (s *Sort) gatherValidity(colIndex int, rows int) types.Validity {
	var valid types.Validity
	for outRow, item := range s.items {
		col := s.batches[item.batch].Columns[colIndex]
		if types.IsValid(col.V.Valid, item.row) {
			continue
		}
		if valid == nil {
			valid = types.NewValidity(rows)
		}
		types.SetInvalid(valid, outRow)
	}
	return valid
}
