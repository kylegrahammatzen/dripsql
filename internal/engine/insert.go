// Insert path. Bind the statement, materialize one Batch per call, write a fresh segment, append manifest.
// BulkInsert seals N batches into one multi-page segment to amortize file IO and manifest entries.
package engine

import (
	"context"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// BulkInsert parses and plans each statement, accumulates the resulting batches,
// and writes them as one multi-page segment with a single manifest append. All
// statements must target the same table. Returns the total rows inserted.
func (db *DB) BulkInsert(ctx context.Context, statements []string) (int64, error) {
	if len(statements) == 0 {
		return 0, nil
	}
	ctx = ctxOrBackground(ctx)

	if err := db.lockOpen(); err != nil {
		return 0, err
	}
	defer db.mu.Unlock()

	var (
		def     sql.BoundTableDef
		defSet  bool
		batches []vector.Batch
		total   int64
	)
	for i, text := range statements {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		stmts, err := sql.Parse(text)
		if err != nil {
			return total, fmt.Errorf("BulkInsert: parse stmt %d: %w", i, err)
		}
		for _, stmt := range stmts {
			plan, err := db.planner().Plan(stmt)
			if err != nil {
				return total, fmt.Errorf("BulkInsert: plan stmt %d: %w", i, err)
			}
			if plan.Kind != sql.PlanInsert {
				return total, fmt.Errorf("BulkInsert: stmt %d is not INSERT (got %v)", i, plan.Kind)
			}
			if plan.Values.RowCount == 0 {
				continue
			}
			if !defSet {
				def = plan.Table
				defSet = true
			} else if schema.NormalizeName(def.Name) != schema.NormalizeName(plan.Table.Name) {
				return total, fmt.Errorf("BulkInsert: stmt %d targets %q but bulk set is for %q", i, plan.Table.Name, def.Name)
			}
			batch, err := batchFromInsert(plan.Values, plan.Table)
			if err != nil {
				return total, fmt.Errorf("BulkInsert: build batch %d: %w", i, err)
			}
			batches = append(batches, batch)
			total += int64(plan.Values.RowCount)
		}
	}
	if len(batches) == 0 {
		return 0, nil
	}

	root := storage.NewSpan("BulkInsert " + def.Name)
	add, cleanup, err := db.writeSegmentAdd(def, batches, uint32(total), root)
	root.End()
	db.publishWriteSpan(root)
	if err != nil {
		return total, err
	}
	defer func() { cleanup() }()
	if err := db.commitManifestTxn(def.Name, []storage.ManifestSegmentAdd{add}, nil); err != nil {
		return total, err
	}
	cleanup = func() {}
	return total, nil
}

// writeSegmentAdd writes batches to a fresh segment file and returns the matching manifest entry plus a cleanup func that removes the file when called.
func (db *DB) writeSegmentAdd(def sql.BoundTableDef, batches []vector.Batch, rows uint32, parent *storage.Span) (storage.ManifestSegmentAdd, func(), error) {
	path := db.nextSegmentPath(def.Name)
	span, err := storage.WriteSegmentWithIdentity(path, batches, columnCodecs(def), db.segmentIdentity(def))
	if span != nil && parent != nil {
		parent.AppendChild(span)
	}
	if err != nil {
		os.Remove(path)
		return storage.ManifestSegmentAdd{}, nil, err
	}
	return storage.ManifestSegmentAdd{Path: path, Rows: rows, SchemaGeneration: uint64(db.catalog.Generation)}, func() { os.Remove(path) }, nil
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
	batch, err := batchFromInsert(values, def)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	stmt := storage.NewSpan("INSERT " + def.Name)
	add, cleanup, err := db.writeSegmentAdd(def, []vector.Batch{batch}, uint32(values.RowCount), stmt)
	stmt.End()
	db.publishWriteSpan(stmt)
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

func batchFromInsert(values sql.InsertValues, def sql.BoundTableDef) (vector.Batch, error) {
	byID := make(map[sql.ColumnID]sql.InsertColumn, len(values.Columns))
	for _, c := range values.Columns {
		byID[c.ID] = c
	}
	cols := make([]vector.Column, len(def.Columns))
	for i, c := range def.Columns {
		ic, ok := byID[c.ID]
		if !ok {
			return vector.Batch{}, fmt.Errorf("insert: column %q not provided", c.Name)
		}
		v, err := buildInsertVec(ic, c, values.RowCount)
		if err != nil {
			return vector.Batch{}, fmt.Errorf("insert: column %q: %w", c.Name, err)
		}
		cols[i] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.Labels, V: v}
	}
	return vector.NewBatch(cols)
}

type insertValueWriter func(row int, val sql.Value) error

func buildInsertVec(ic sql.InsertColumn, def sql.BoundColumnDef, rows int) (vector.Vec, error) {
	if len(ic.Values) != rows {
		return vector.Vec{}, fmt.Errorf("got %d values for %d rows", len(ic.Values), rows)
	}
	if ic.NullCount > 0 && !def.Nullable {
		firstNull := 0
		for i, v := range ic.Values {
			if v.Kind == sql.ValueNull {
				firstNull = i
				break
			}
		}
		return vector.Vec{}, fmt.Errorf("row %d is null but column is NOT NULL", firstNull)
	}
	vk, err := vector.VecKindOf(def.Type)
	if err != nil {
		return vector.Vec{}, err
	}
	v, err := vector.NewVecForKind(vk, rows)
	if err != nil {
		return vector.Vec{}, err
	}
	var valid vector.Validity
	if ic.NullCount > 0 {
		valid = make(vector.Validity, vector.ValidityWords(rows))
	}
	writer, err := newInsertValueWriter(&v, vk)
	if err != nil {
		return vector.Vec{}, err
	}
	for row, val := range ic.Values {
		if val.Kind == sql.ValueNull {
			if valid == nil {
				return vector.Vec{}, fmt.Errorf("row %d null literal but column rejects nulls", row)
			}
			continue
		}
		if valid != nil {
			valid.SetValid(row)
		}
		if err := writer(row, val); err != nil {
			return vector.Vec{}, fmt.Errorf("row %d: %w", row, err)
		}
	}
	v.Valid = valid
	return v, nil
}

// newInsertValueWriter returns a per-column closure. The kind switch runs once per column,
// the typed destination slice is hoisted once, and the row loop in buildInsertVec only does
// the per-row work (null check, validity bit, typed store). Enum lookup is built once.
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
			if val.Kind != sql.ValueInt {
				return fmt.Errorf("expected int, got %v", val.Kind)
			}
			dst[row] = int32(val.Int)
			return nil
		}, nil
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		dst := v.I64()
		return func(row int, val sql.Value) error {
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
