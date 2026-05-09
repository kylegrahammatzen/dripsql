package storage

import "github.com/kylegrahammatzen/dripsql/internal/vector"

func cloneBatchesForBuffer(source []vector.Batch) []vector.Batch {
	out := make([]vector.Batch, len(source))
	for i, batch := range source {
		cols := make([]vector.Column, len(batch.Columns))
		for colIndex, col := range batch.Columns {
			cloned := vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: col.V.Kind, Len: col.V.Len}}
			if col.V.Valid != nil {
				cloned.V.Valid = append(vector.Validity(nil), col.V.Valid...)
			}
			switch col.V.Kind {
			case vector.Bool:
				cloned.V.BoolBits = append([]uint64(nil), col.V.BoolBits...)
			case vector.Int16:
				cloned.V.I16 = append([]int16(nil), col.V.I16...)
			case vector.Int32, vector.Date:
				cloned.V.I32 = append([]int32(nil), col.V.I32...)
			case vector.Int64, vector.Timestamp, vector.Time:
				cloned.V.I64 = append([]int64(nil), col.V.I64...)
			case vector.Float32:
				cloned.V.F32 = append([]float32(nil), col.V.F32...)
			case vector.Float64:
				cloned.V.F64 = append([]float64(nil), col.V.F64...)
			case vector.UUID:
				cloned.V.UUID = append([]vector.UUID16(nil), col.V.UUID...)
			case vector.Text:
				cloned.V.Var.Offsets = append([]uint32(nil), col.V.Var.Offsets...)
				cloned.V.Var.Data = append([]byte(nil), col.V.Var.Data...)
			}
			cols[colIndex] = cloned
		}
		out[i] = vector.Batch{Columns: cols, Len: batch.Len}
	}
	return out
}
