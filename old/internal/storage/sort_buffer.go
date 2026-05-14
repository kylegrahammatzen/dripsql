package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// sortBatchesBy concatenates the input batches, sorts the rows ascending by
// the named columns, and emits batches that preserve the original per-batch
// row counts. Only EncodingFlat inputs are supported, which is what the
// ingest buffer always produces.
//
// SQL nulls-last semantics are applied.
func sortBatchesBy(batches []types.Batch, sortByNames []string) ([]types.Batch, error) {
	if len(batches) == 0 || len(sortByNames) == 0 {
		return batches, nil
	}
	template := batches[0]
	sortCols := make([]int, 0, len(sortByNames))
	for _, name := range sortByNames {
		idx, ok := indexOfColumn(template, name)
		if !ok {
			return nil, fmt.Errorf("sort_by column %q not found in batch", name)
		}
		if !sortableSortByKind(template.Columns[idx].V.Kind) {
			return nil, fmt.Errorf("sort_by column %q has unsupported kind %s", name, template.Columns[idx].V.Kind)
		}
		sortCols = append(sortCols, idx)
	}

	total := 0
	for _, b := range batches {
		total += b.Len
		if !batchIsFlat(b) {
			return nil, fmt.Errorf("sort_by requires flat-encoded buffer batches")
		}
	}
	if total == 0 {
		return batches, nil
	}

	perm := make([]sortRow, total)
	cursor := 0
	for bi, b := range batches {
		for r := 0; r < b.Len; r++ {
			perm[cursor] = sortRow{batch: int32(bi), row: int32(r)}
			cursor++
		}
	}

	sort.SliceStable(perm, func(i, j int) bool {
		a, b := perm[i], perm[j]
		for _, ci := range sortCols {
			cmp := compareSortRow(batches, ci, int(a.batch), int(a.row), int(b.batch), int(b.row))
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	out := make([]types.Batch, len(batches))
	cursor = 0
	for bi, src := range batches {
		n := src.Len
		cols := make([]types.Column, len(src.Columns))
		for ci, srcCol := range src.Columns {
			gathered, err := gatherSortColumn(batches, ci, srcCol, perm[cursor:cursor+n])
			if err != nil {
				return nil, err
			}
			cols[ci] = gathered
		}
		nb, err := types.NewBatch(cols)
		if err != nil {
			return nil, err
		}
		out[bi] = nb
		cursor += n
	}
	return out, nil
}

type sortRow struct {
	batch int32
	row   int32
}

func indexOfColumn(batch types.Batch, name string) (int, bool) {
	target := strings.ToLower(name)
	for i, col := range batch.Columns {
		if strings.ToLower(col.Name) == target {
			return i, true
		}
	}
	return -1, false
}

func sortableSortByKind(kind types.VecKind) bool {
	switch kind {
	case types.VecBool,
		types.VecInt16,
		types.VecInt32,
		types.VecInt64,
		types.VecDate,
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

func batchIsFlat(batch types.Batch) bool {
	for _, col := range batch.Columns {
		if col.V.Encoding != types.EncodingFlat {
			return false
		}
	}
	return true
}

func compareSortRow(batches []types.Batch, colIdx int, ab, ar, bb, br int) int {
	left := batches[ab].Columns[colIdx].V
	right := batches[bb].Columns[colIdx].V
	leftValid := types.IsValid(left.Valid, ar)
	rightValid := types.IsValid(right.Valid, br)
	if !leftValid || !rightValid {
		switch {
		case leftValid == rightValid:
			return 0
		case leftValid:
			return -1
		default:
			return 1
		}
	}
	switch left.Kind {
	case types.VecBool:
		la := left.BoolBits[ar>>6]&(uint64(1)<<uint(ar&63)) != 0
		rb := right.BoolBits[br>>6]&(uint64(1)<<uint(br&63)) != 0
		switch {
		case la == rb:
			return 0
		case !la:
			return -1
		default:
			return 1
		}
	case types.VecInt16:
		return cmpOrdered(int64(left.I16[ar]), int64(right.I16[br]))
	case types.VecInt32, types.VecDate:
		return cmpOrdered(int64(left.I32[ar]), int64(right.I32[br]))
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return cmpOrdered(left.I64[ar], right.I64[br])
	case types.VecFloat32:
		return cmpFloat(float64(left.F32[ar]), float64(right.F32[br]))
	case types.VecFloat64:
		return cmpFloat(left.F64[ar], right.F64[br])
	case types.VecText:
		return strings.Compare(left.Var.String(ar), right.Var.String(br))
	}
	return 0
}

func cmpOrdered(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func gatherSortColumn(batches []types.Batch, colIdx int, template types.Column, perm []sortRow) (types.Column, error) {
	rows := len(perm)
	v := types.Vec{Kind: template.V.Kind, Encoding: types.EncodingFlat, Len: rows}
	v.Valid = gatherSortValidity(batches, colIdx, perm)
	switch template.V.Kind {
	case types.VecBool:
		v.BoolBits = make([]uint64, types.ValidityWords(rows))
		for outRow, item := range perm {
			src := batches[item.batch].Columns[colIdx].V
			if src.BoolBits[item.row>>6]&(uint64(1)<<uint(item.row&63)) != 0 {
				v.BoolBits[outRow>>6] |= uint64(1) << uint(outRow&63)
			}
		}
	case types.VecInt16:
		v.I16 = make([]int16, rows)
		for outRow, item := range perm {
			v.I16[outRow] = batches[item.batch].Columns[colIdx].V.I16[item.row]
		}
	case types.VecInt32, types.VecDate:
		v.I32 = make([]int32, rows)
		for outRow, item := range perm {
			v.I32[outRow] = batches[item.batch].Columns[colIdx].V.I32[item.row]
		}
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		v.I64 = make([]int64, rows)
		for outRow, item := range perm {
			v.I64[outRow] = batches[item.batch].Columns[colIdx].V.I64[item.row]
		}
	case types.VecFloat32:
		v.F32 = make([]float32, rows)
		for outRow, item := range perm {
			v.F32[outRow] = batches[item.batch].Columns[colIdx].V.F32[item.row]
		}
	case types.VecFloat64:
		v.F64 = make([]float64, rows)
		for outRow, item := range perm {
			v.F64[outRow] = batches[item.batch].Columns[colIdx].V.F64[item.row]
		}
	case types.VecEnum32:
		v.U32 = make([]uint32, rows)
		for outRow, item := range perm {
			v.U32[outRow] = batches[item.batch].Columns[colIdx].V.U32[item.row]
		}
	case types.VecUUID:
		v.UUID = make([]types.UUID16, rows)
		for outRow, item := range perm {
			v.UUID[outRow] = batches[item.batch].Columns[colIdx].V.UUID[item.row]
		}
	case types.VecText, types.VecBytes, types.VecJSON:
		varbytes := types.NewVarBytes(rows, rows*8)
		for outRow, item := range perm {
			src := batches[item.batch].Columns[colIdx].V
			varbytes.AppendBytes(outRow, src.Var.Bytes(int(item.row)))
		}
		v.Var = varbytes
	default:
		return types.Column{}, fmt.Errorf("sort_by gather unsupported kind %s for column %q", template.V.Kind, template.Name)
	}
	return types.Column{Name: template.Name, Type: template.Type, EnumLabels: template.EnumLabels, V: v}, nil
}

func gatherSortValidity(batches []types.Batch, colIdx int, perm []sortRow) types.Validity {
	var valid types.Validity
	for outRow, item := range perm {
		src := batches[item.batch].Columns[colIdx].V
		if types.IsValid(src.Valid, int(item.row)) {
			continue
		}
		if valid == nil {
			valid = types.NewValidity(len(perm))
		}
		types.SetInvalid(valid, outRow)
	}
	return valid
}
