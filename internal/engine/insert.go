// Insert path: bind the InsertStmt, materialize one Batch per call, write a fresh segment, append manifest.
// One segment per INSERT call. Buffering and amortized flushes are a later optimization.
package engine

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func (db *DB) insert(ctx context.Context, plan *sql.Plan) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	def := plan.Table
	values := plan.Values
	if values.RowCount == 0 {
		return 0, nil
	}
	batch, err := batchFromInsert(values, def)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	path := db.nextSegmentPath(def.Name)
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(path)
		}
	}()
	if err := storage.WriteSegment(path, []types.Batch{batch}); err != nil {
		return 0, err
	}
	m, err := db.manifestFor(def.Name)
	if err != nil {
		return 0, err
	}
	if err := m.Append(storage.ManifestEntry{
		Version: uint64(time.Now().UnixNano()),
		Path:    path,
		Rows:    uint32(values.RowCount),
	}); err != nil {
		return 0, err
	}
	cleanup = false
	return int64(values.RowCount), nil
}

func batchFromInsert(values sql.InsertValues, def sql.BoundTableDef) (types.Batch, error) {
	byID := make(map[sql.ColumnID]sql.InsertColumn, len(values.Columns))
	for _, c := range values.Columns {
		byID[c.ID] = c
	}
	cols := make([]types.Column, len(def.Columns))
	for i, c := range def.Columns {
		ic, ok := byID[c.ID]
		if !ok {
			return types.Batch{}, fmt.Errorf("insert: column %q not provided", c.Name)
		}
		v, err := buildInsertVec(ic, c, values.RowCount)
		if err != nil {
			return types.Batch{}, fmt.Errorf("insert: column %q: %w", c.Name, err)
		}
		cols[i] = types.Column{Name: c.Name, Type: c.Type, EnumLabels: c.Labels, V: v}
	}
	return types.NewBatch(cols)
}

type insertValueWriter func(row int, val sql.Value) error

func buildInsertVec(ic sql.InsertColumn, def sql.BoundColumnDef, rows int) (types.Vec, error) {
	if len(ic.Values) != rows {
		return types.Vec{}, fmt.Errorf("got %d values for %d rows", len(ic.Values), rows)
	}
	if ic.NullCount > 0 && !def.Nullable {
		return types.Vec{}, fmt.Errorf("null value but column is NOT NULL")
	}
	vk, err := types.VecKindOf(def.Type)
	if err != nil {
		return types.Vec{}, err
	}
	v, err := types.NewVecForKind(vk, rows)
	if err != nil {
		return types.Vec{}, err
	}
	var valid types.Validity
	if ic.NullCount > 0 {
		valid = make(types.Validity, types.ValidityWords(rows))
	}
	writer, err := newInsertValueWriter(&v, vk, def)
	if err != nil {
		return types.Vec{}, err
	}
	for row, val := range ic.Values {
		if val.Kind == sql.ValueNull {
			if valid == nil {
				return types.Vec{}, fmt.Errorf("row %d null literal but column rejects nulls", row)
			}
			continue
		}
		if valid != nil {
			valid.SetValid(row)
		}
		if err := writer(row, val); err != nil {
			return types.Vec{}, fmt.Errorf("row %d: %w", row, err)
		}
	}
	v.Valid = valid
	return v, nil
}

// newInsertValueWriter returns a per-column closure. The kind switch runs once per column,
// the typed destination slice is hoisted once, and the row loop in buildInsertVec only does
// the per-row work (null check, validity bit, typed store). Enum lookup is built once.
func newInsertValueWriter(v *types.Vec, vk types.VecKind, def sql.BoundColumnDef) (insertValueWriter, error) {
	switch vk {
	case types.VecBool:
		dst := v.BoolBits()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueBool {
				return fmt.Errorf("bool column got %v", val.Kind)
			}
			if val.Bool {
				dst[row>>3] |= 1 << (row & 7)
			}
			return nil
		}, nil
	case types.VecInt16:
		dst := v.I16()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("int column got %v", val.Kind)
			}
			dst[row] = int16(val.Int)
			return nil
		}, nil
	case types.VecInt32, types.VecDate:
		dst := v.I32()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("int column got %v", val.Kind)
			}
			dst[row] = int32(val.Int)
			return nil
		}, nil
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		dst := v.I64()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("int column got %v", val.Kind)
			}
			dst[row] = val.Int
			return nil
		}, nil
	case types.VecFloat32:
		dst := v.F32()
		return func(row int, val sql.Value) error {
			switch val.Kind {
			case sql.ValueInt:
				dst[row] = float32(val.Int)
			case sql.ValueFloat:
				dst[row] = float32(val.Float)
			default:
				return fmt.Errorf("float column got %v", val.Kind)
			}
			return nil
		}, nil
	case types.VecFloat64:
		dst := v.F64()
		return func(row int, val sql.Value) error {
			switch val.Kind {
			case sql.ValueInt:
				dst[row] = float64(val.Int)
			case sql.ValueFloat:
				dst[row] = val.Float
			default:
				return fmt.Errorf("float column got %v", val.Kind)
			}
			return nil
		}, nil
	case types.VecText, types.VecBytes, types.VecJSON:
		dst := v.Var()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueString {
				return fmt.Errorf("text column got %v", val.Kind)
			}
			dst.AppendString(row, val.String)
			return nil
		}, nil
	case types.VecUUID:
		dst := v.FixedBytes()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueString {
				return fmt.Errorf("uuid column got %v", val.Kind)
			}
			u, err := types.ParseUUID(val.String)
			if err != nil {
				return err
			}
			copy(dst[row*16:row*16+16], u[:])
			return nil
		}, nil
	case types.VecEnum32:
		dst := v.U32()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueEnum {
				return fmt.Errorf("enum column got %v (binder did not resolve to ValueEnum)", val.Kind)
			}
			dst[row] = val.Enum
			return nil
		}, nil
	}
	return nil, fmt.Errorf("insert: unsupported VecKind %v", vk)
}
