package storage

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// cloneBatchForIngest deep-copies a batch into freshly allocated buffers so
// the IngestBuffer can keep mutating its own pages without aliasing into the
// caller's data.
func cloneBatchForIngest(batch vector.Batch) (vector.Batch, error) {
	cols := make([]vector.Column, len(batch.Columns))
	for i, col := range batch.Columns {
		v := vector.Vec{Kind: col.V.Kind, Len: col.V.Len}
		if col.V.Valid != nil {
			v.Valid = append(vector.Validity(nil), col.V.Valid...)
		}
		switch col.V.Kind {
		case vector.Bool:
			v.BoolBits = append([]uint64(nil), col.V.BoolBits...)
		case vector.Int16:
			v.I16 = append([]int16(nil), col.V.I16...)
		case vector.Int32, vector.Date:
			v.I32 = append([]int32(nil), col.V.I32...)
		case vector.Int64, vector.Timestamp:
			v.I64 = append([]int64(nil), col.V.I64...)
		case vector.Float32:
			v.F32 = append([]float32(nil), col.V.F32...)
		case vector.Float64:
			v.F64 = append([]float64(nil), col.V.F64...)
		case vector.UUID:
			v.UUID = append([]vector.UUID16(nil), col.V.UUID...)
		case vector.Enum32:
			v.U32 = append([]uint32(nil), col.V.U32...)
		case vector.Text, vector.Bytes:
			v.Var.Offsets = append([]uint32(nil), col.V.Var.Offsets...)
			v.Var.Data = append([]byte(nil), col.V.Var.Data...)
		default:
			return vector.Batch{}, fmt.Errorf("unsupported ingest buffer vector kind %s", col.V.Kind)
		}
		cols[i] = vector.Column{Name: col.Name, Type: col.Type, EnumLabels: append([]string(nil), col.EnumLabels...), V: v}
	}
	return vector.Batch{Columns: cols, Len: batch.Len}, nil
}

// copyBatchForIngest reuses dst's existing column buffers to hold src's data,
// growing them only when capacity is insufficient. Used to recycle pages
// returned by RecycleRun without re-allocating per-batch buffers.
func copyBatchForIngest(dst vector.Batch, src vector.Batch) (vector.Batch, error) {
	if len(dst.Columns) != len(src.Columns) {
		return vector.Batch{}, fmt.Errorf("recycled ingest page has %d columns, source has %d", len(dst.Columns), len(src.Columns))
	}
	cols := dst.Columns[:len(src.Columns)]
	for i, srcCol := range src.Columns {
		dstCol := &cols[i]
		dstCol.Name = srcCol.Name
		dstCol.Type = srcCol.Type
		dstCol.EnumLabels = append(dstCol.EnumLabels[:0], srcCol.EnumLabels...)
		dstCol.V.Kind = srcCol.V.Kind
		dstCol.V.Len = srcCol.V.Len
		dstCol.V.Valid = cloneValidityInto(dstCol.V.Valid, srcCol.V.Valid)
		switch srcCol.V.Kind {
		case vector.Bool:
			words := vector.ValidityWords(srcCol.V.Len)
			dstCol.V.BoolBits = ensureLen(dstCol.V.BoolBits, words)
			copy(dstCol.V.BoolBits, srcCol.V.BoolBits[:words])
		case vector.Int16:
			dstCol.V.I16 = ensureLen(dstCol.V.I16, srcCol.V.Len)
			copy(dstCol.V.I16, srcCol.V.I16[:srcCol.V.Len])
		case vector.Int32, vector.Date:
			dstCol.V.I32 = ensureLen(dstCol.V.I32, srcCol.V.Len)
			copy(dstCol.V.I32, srcCol.V.I32[:srcCol.V.Len])
		case vector.Int64, vector.Timestamp:
			dstCol.V.I64 = ensureLen(dstCol.V.I64, srcCol.V.Len)
			copy(dstCol.V.I64, srcCol.V.I64[:srcCol.V.Len])
		case vector.Float32:
			dstCol.V.F32 = ensureLen(dstCol.V.F32, srcCol.V.Len)
			copy(dstCol.V.F32, srcCol.V.F32[:srcCol.V.Len])
		case vector.Float64:
			dstCol.V.F64 = ensureLen(dstCol.V.F64, srcCol.V.Len)
			copy(dstCol.V.F64, srcCol.V.F64[:srcCol.V.Len])
		case vector.UUID:
			dstCol.V.UUID = ensureLen(dstCol.V.UUID, srcCol.V.Len)
			copy(dstCol.V.UUID, srcCol.V.UUID[:srcCol.V.Len])
		case vector.Enum32:
			dstCol.V.U32 = ensureLen(dstCol.V.U32, srcCol.V.Len)
			copy(dstCol.V.U32, srcCol.V.U32[:srcCol.V.Len])
		case vector.Text, vector.Bytes:
			dstCol.V.Var.Offsets = ensureLen(dstCol.V.Var.Offsets, srcCol.V.Len+1)
			copy(dstCol.V.Var.Offsets, srcCol.V.Var.Offsets[:srcCol.V.Len+1])
			dstCol.V.Var.Data = append(dstCol.V.Var.Data[:0], srcCol.V.Var.Data...)
		default:
			return vector.Batch{}, fmt.Errorf("unsupported ingest buffer vector kind %s", srcCol.V.Kind)
		}
	}
	return vector.Batch{Columns: cols, Len: src.Len}, nil
}

func cloneValidityInto(dst vector.Validity, src vector.Validity) vector.Validity {
	if src == nil {
		return nil
	}
	if cap(dst) < len(src) {
		dst = make(vector.Validity, len(src))
	} else {
		dst = dst[:len(src)]
	}
	copy(dst, src)
	return dst
}

// copyValidity copies src's validity bits into dst at dstStart, allocating dst
// lazily when src marks any nulls. dst always uses DefaultPageRows-sized
// validity since mutable pages always preallocate to that capacity.
func copyValidity(dst *vector.Vec, src vector.Validity, dstStart int, srcStart int, rows int) {
	if src == nil {
		return
	}
	if dst.Valid == nil {
		dst.Valid = vector.NewValidity(DefaultPageRows)
	}
	for row := 0; row < rows; row++ {
		if !vector.IsValid(src, srcStart+row) {
			vector.SetInvalid(dst.Valid, dstStart+row)
		}
	}
}

// appendVarBytes appends rows from src into dst's variable-byte payload,
// keeping offsets contiguous. Null rows still need an offset entry; validity
// lives in the separate bitmap and is propagated by copyValidity.
func appendVarBytes(dst *vector.VarBytes, src vector.VarBytes, valid vector.Validity, dstStart int, srcStart int, rows int) {
	for row := 0; row < rows; row++ {
		if vector.IsValid(valid, srcStart+row) {
			dst.Data = append(dst.Data, src.Bytes(srcStart+row)...)
		}
		dst.Offsets[dstStart+row+1] = uint32(len(dst.Data))
	}
}

// ensureLen returns values resliced to length n, allocating a new backing
// array only when the existing capacity is insufficient. Used by the recycle
// path to reuse buffers across batches.
func ensureLen[T any](values []T, n int) []T {
	if cap(values) < n {
		return make([]T, n)
	}
	return values[:n]
}
