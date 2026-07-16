// Insert path. Bind the statement, materialize page-capped batches, write a fresh segment, append manifest.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// writeSegmentAdd writes batches to a fresh segment file and returns the matching manifest entry plus a cleanup func that removes the file when called.
func (db *DB) writeSegmentAdd(def sql.BoundTableDef, batches []vector.Batch, rows uint32) (storage.ManifestSegmentAdd, func(), error) {
	path := db.nextSegmentPath(def.Name)
	if _, err := storage.WriteSegmentWithIdentity(path, batches, columnCodecs(def), db.segmentIdentity(def)); err != nil {
		os.Remove(path)
		return storage.ManifestSegmentAdd{}, nil, err
	}
	return storage.ManifestSegmentAdd{Path: filepath.Base(path), Rows: rows, SchemaGeneration: uint64(db.catalog.Generation)}, func() { os.Remove(path) }, nil
}

func (db *DB) insert(ctx context.Context, plan *sql.Plan, commit commitFn) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	def := plan.Table
	values := plan.Values
	if values.RowCount == 0 {
		return 0, nil
	}
	batches, err := batchesFromInsert(values, def)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	add, cleanup, err := db.writeSegmentAdd(def, batches, uint32(values.RowCount))
	if err != nil {
		return 0, err
	}
	defer func() { cleanup() }()
	if err := commit(def.Name, []storage.ManifestSegmentAdd{add}, nil); err != nil {
		return 0, err
	}
	cleanup = func() {}
	return int64(values.RowCount), nil
}

// batchesFromInsert materializes the VALUES rows as page-capped batches so no
// single page ever exceeds StandardBatchRows.
func batchesFromInsert(values sql.InsertValues, def sql.BoundTableDef) ([]vector.Batch, error) {
	byID := make(map[sql.ColumnID]sql.InsertColumn, len(values.Columns))
	for _, c := range values.Columns {
		byID[c.ID] = c
	}
	for _, c := range def.Columns {
		ic, ok := byID[c.ID]
		if !ok {
			return nil, fmt.Errorf("insert: column %q not provided", c.Name)
		}
		if len(ic.Values) != values.RowCount {
			return nil, fmt.Errorf("insert: column %q: got %d values for %d rows", c.Name, len(ic.Values), values.RowCount)
		}
		if ic.NullCount > 0 && !c.Nullable {
			firstNull := 0
			for i, v := range ic.Values {
				if v.Kind == sql.ValueNull {
					firstNull = i
					break
				}
			}
			return nil, fmt.Errorf("insert: column %q: row %d is null but column is NOT NULL", c.Name, firstNull)
		}
	}
	batches := make([]vector.Batch, 0, (values.RowCount+vector.StandardBatchRows-1)/vector.StandardBatchRows)
	for start := 0; start < values.RowCount; start += vector.StandardBatchRows {
		count := min(start+vector.StandardBatchRows, values.RowCount) - start
		cols := make([]vector.Column, len(def.Columns))
		for i, c := range def.Columns {
			v, err := buildInsertVec(byID[c.ID], c, start, count)
			if err != nil {
				return nil, fmt.Errorf("insert: column %q: %w", c.Name, err)
			}
			cols[i] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.Labels, V: v}
		}
		b, err := vector.NewBatch(cols)
		if err != nil {
			return nil, err
		}
		batches = append(batches, b)
	}
	return batches, nil
}

type insertValueWriter func(row int, val sql.Value) error

func buildInsertVec(ic sql.InsertColumn, def sql.BoundColumnDef, start, count int) (vector.Vec, error) {
	vk, err := vector.VecKindOf(def.Type)
	if err != nil {
		return vector.Vec{}, err
	}
	v, err := vector.NewVecForKind(vk, count)
	if err != nil {
		return vector.Vec{}, err
	}
	var valid vector.Validity
	if ic.NullCount > 0 {
		valid = make(vector.Validity, vector.ValidityWords(count))
	}
	writer, err := newInsertValueWriter(&v, vk)
	if err != nil {
		return vector.Vec{}, err
	}
	for row, val := range ic.Values[start : start+count] {
		if val.Kind == sql.ValueNull {
			if valid == nil {
				return vector.Vec{}, fmt.Errorf("row %d null literal but column rejects nulls", start+row)
			}
			continue
		}
		if valid != nil {
			valid.SetValid(row)
		}
		if err := writer(row, val); err != nil {
			return vector.Vec{}, fmt.Errorf("row %d: %w", start+row, err)
		}
	}
	v.Valid = valid
	return v, nil
}

// normalizeTemporal converts a validated temporal string literal into the column encoding, days since the Unix epoch for dates, UnixNano for timestamps, and nanoseconds since midnight for times.
func normalizeTemporal(vk vector.VecKind, val sql.Value) (sql.Value, error) {
	if val.Kind != sql.ValueString {
		return val, nil
	}
	switch vk {
	case vector.VecDate:
		t, err := time.Parse("2006-01-02", val.String)
		if err != nil {
			return val, fmt.Errorf("invalid date literal %q", val.String)
		}
		return sql.Value{Kind: sql.ValueInt, Int: t.Unix() / 86400}, nil
	case vector.VecTimestamp:
		t, err := time.Parse(time.RFC3339Nano, val.String)
		if err != nil {
			return val, fmt.Errorf("invalid timestamp literal %q", val.String)
		}
		return sql.Value{Kind: sql.ValueInt, Int: t.UnixNano()}, nil
	case vector.VecTime:
		t, err := time.Parse("15:04:05.999999999", val.String)
		if err != nil {
			return val, fmt.Errorf("invalid time literal %q", val.String)
		}
		ns := int64(t.Hour())*3_600_000_000_000 + int64(t.Minute())*60_000_000_000 + int64(t.Second())*1_000_000_000 + int64(t.Nanosecond())
		return sql.Value{Kind: sql.ValueInt, Int: ns}, nil
	}
	return val, nil
}

func newInsertValueWriter(v *vector.Vec, vk vector.VecKind) (insertValueWriter, error) {
	switch vk {
	case vector.VecBool:
		dst := v.BoolBits()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueBool {
				return fmt.Errorf("expected bool, got %v", val.Kind)
			}
			if val.Bool {
				dst[row>>3] |= 1 << (row & 7)
			}
			return nil
		}, nil
	case vector.VecInt16:
		dst := v.I16()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("expected int, got %v", val.Kind)
			}
			dst[row] = int16(val.Int)
			return nil
		}, nil
	case vector.VecInt32, vector.VecDate:
		dst := v.I32()
		return func(row int, val sql.Value) error {
			val, err := normalizeTemporal(vk, val)
			if err != nil {
				return err
			}
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("expected int, got %v", val.Kind)
			}
			dst[row] = int32(val.Int)
			return nil
		}, nil
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		dst := v.I64()
		return func(row int, val sql.Value) error {
			val, err := normalizeTemporal(vk, val)
			if err != nil {
				return err
			}
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("expected int, got %v", val.Kind)
			}
			dst[row] = val.Int
			return nil
		}, nil
	case vector.VecFloat32:
		dst := v.F32()
		return func(row int, val sql.Value) error {
			switch val.Kind {
			case sql.ValueInt:
				dst[row] = float32(val.Int)
			case sql.ValueFloat:
				dst[row] = float32(val.Float)
			default:
				return fmt.Errorf("expected float, got %v", val.Kind)
			}
			return nil
		}, nil
	case vector.VecFloat64:
		dst := v.F64()
		return func(row int, val sql.Value) error {
			switch val.Kind {
			case sql.ValueInt:
				dst[row] = float64(val.Int)
			case sql.ValueFloat:
				dst[row] = val.Float
			default:
				return fmt.Errorf("expected float, got %v", val.Kind)
			}
			return nil
		}, nil
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		dst := v.Var()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueString {
				return fmt.Errorf("expected text, got %v", val.Kind)
			}
			dst.AppendString(row, val.String)
			return nil
		}, nil
	case vector.VecUUID:
		dst := v.FixedBytes()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueString {
				return fmt.Errorf("expected uuid string, got %v", val.Kind)
			}
			u, err := vector.ParseUUID(val.String)
			if err != nil {
				return err
			}
			copy(dst[row*16:row*16+16], u[:])
			return nil
		}, nil
	case vector.VecEnum32:
		dst := v.U32()
		return func(row int, val sql.Value) error {
			if val.Kind != sql.ValueEnum {
				return fmt.Errorf("expected enum, got %v", val.Kind)
			}
			dst[row] = val.Enum
			return nil
		}, nil
	}
	return nil, fmt.Errorf("insert: unsupported VecKind %v", vk)
}
