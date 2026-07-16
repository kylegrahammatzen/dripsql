// Columnar query results. QueryBatches answers from metadata when eligible or drains the
// operator tree into dense engine-owned batches of typed column slices instead of boxed rows.
package engine

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type BatchRows struct {
	Columns []string
	Batches []vector.Batch
	Rows    int
}

func (db *DB) QueryBatches(ctx context.Context, sqlText string, args ...any) (*BatchRows, error) {
	ctx = ctxOrBackground(ctx)
	if err := db.lockOpen(); err != nil {
		return nil, err
	}
	defer db.mu.Unlock()
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	bound, err := sql.BindParameters(plan, args)
	if err != nil {
		return nil, err
	}
	if bound != nil && bound.Kind == sql.PlanExplain {
		return nil, fmt.Errorf("engine: QueryBatches does not support EXPLAIN")
	}
	if mrows, ok, err := db.answerFromMetadata(bound); err != nil {
		return nil, err
	} else if ok {
		out := &BatchRows{Columns: mrows.Columns}
		outputs := bound.Rel.Outputs
		for start := 0; start < len(mrows.Values); start += vector.StandardBatchRows {
			chunk := mrows.Values[start:min(start+vector.StandardBatchRows, len(mrows.Values))]
			n := len(chunk)
			cols := make([]vector.Column, len(outputs))
			for ci := range outputs {
				// Runtime values pick the vec kind because narrowed min and max plus float avg override the planner's int64 label, an all-null column keeps it.
				t := outputs[ci].Expr.Type
				for _, r := range chunk {
					switch r[ci].(type) {
					case nil:
						continue
					case int64:
						t = schema.Int64
					case int32:
						t = schema.Int32
					case int16:
						t = schema.Int16
					case float64:
						t = schema.Float64
					}
					break
				}
				vk, err := vector.VecKindOf(t)
				if err != nil {
					return nil, fmt.Errorf("engine: metadata batch column %q: %w", mrows.Columns[ci], err)
				}
				v, err := vector.NewVecForKind(vk, n)
				if err != nil {
					return nil, fmt.Errorf("engine: metadata batch column %q: %w", mrows.Columns[ci], err)
				}
				var valid vector.Validity
				for r := range n {
					switch x := chunk[r][ci].(type) {
					case int64:
						v.I64()[r] = x
					case int32:
						v.I32()[r] = x
					case int16:
						v.I16()[r] = x
					case float64:
						v.F64()[r] = x
					case string:
						v.Var().AppendString(r, x)
					case nil:
						if valid == nil {
							valid = vector.NewAllValid(n)
						}
						valid.SetInvalid(r)
						if vk.IsVarBytes() {
							v.Var().AppendBytes(r, nil)
						}
					default:
						return nil, fmt.Errorf("engine: metadata batch column %q value %T unsupported", mrows.Columns[ci], x)
					}
				}
				v.Valid = valid
				cols[ci] = vector.Column{Name: mrows.Columns[ci], Type: t, V: v}
			}
			batch, err := vector.NewBatch(cols)
			if err != nil {
				return nil, fmt.Errorf("engine: metadata batch: %w", err)
			}
			out.Batches = append(out.Batches, batch)
			out.Rows += n
		}
		return out, nil
	}
	readTs := db.nextCommitTs.Load()
	resolve := cachedSegmentResolver(func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		ts := readTs
		if d.AsOf != 0 {
			ts = d.AsOf
		}
		return db.openSegmentsAt(d.Name, ts)
	})
	op, err := exec.BuildOperator(bound, resolve)
	if err != nil {
		return nil, err
	}
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()

	out := &BatchRows{Columns: planOutputNames(bound)}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, ok, err := op.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if out.Columns == nil {
			out.Columns = columnNames(batch)
		}
		dense, err := compactBatch(batch)
		if err != nil {
			return nil, err
		}
		if dense.Len == 0 {
			continue
		}
		out.Batches = append(out.Batches, dense)
		out.Rows += dense.Len
	}
	return out, nil
}

// compactBatch copies one operator batch into a dense engine-owned batch. Operator
// batches alias pooled scan buffers and carry selection masks, neither may escape.
func compactBatch(src vector.Batch) (vector.Batch, error) {
	n := src.Len
	var rows []int32
	if src.Sel != nil && !src.Sel.IsAllSet() {
		n = src.Sel.PopCount()
		rows = make([]int32, 0, n)
		src.Sel.IterSet(func(row int) { rows = append(rows, int32(row)) })
	}
	if n == 0 {
		return vector.Batch{}, nil
	}
	cols := make([]vector.Column, len(src.Columns))
	for ci := range src.Columns {
		c := &src.Columns[ci]
		v, err := compactVec(c.V, rows, n)
		if err != nil {
			return vector.Batch{}, fmt.Errorf("column %q: %w", c.Name, err)
		}
		cols[ci] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
	}
	return vector.NewBatch(cols)
}

// compactVec gathers rows into a fresh Vec. rows == nil means the first n rows verbatim.
func compactVec(src vector.Vec, rows []int32, n int) (vector.Vec, error) {
	out, err := vector.NewVecForKind(src.Kind, n)
	if err != nil {
		return vector.Vec{}, err
	}
	switch src.Kind {
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		gatherSlice(out.I64(), src.I64(), rows)
	case vector.VecInt32, vector.VecDate:
		gatherSlice(out.I32(), src.I32(), rows)
	case vector.VecInt16:
		gatherSlice(out.I16(), src.I16(), rows)
	case vector.VecFloat64:
		gatherSlice(out.F64(), src.F64(), rows)
	case vector.VecFloat32:
		gatherSlice(out.F32(), src.F32(), rows)
	case vector.VecEnum32:
		gatherSlice(out.U32(), src.U32(), rows)
	case vector.VecUUID:
		gatherSlice(out.UUID(), src.UUID(), rows)
	case vector.VecBool:
		bits := src.BoolBits()
		dst := out.BoolBits()
		for i := range n {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if bits[row>>3]&(1<<(row&7)) != 0 {
				dst[i>>3] |= 1 << (i & 7)
			}
		}
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		sv, dv := src.Var(), out.Var()
		for i := range n {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			dv.AppendBytes(i, sv.Bytes(row))
		}
	default:
		return vector.Vec{}, fmt.Errorf("unsupported VecKind %v", src.Kind)
	}
	if src.Valid != nil {
		valid := vector.NewAllValid(n)
		invalid := false
		for i := range n {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if !src.Valid.IsValid(row) {
				valid.SetInvalid(i)
				invalid = true
			}
		}
		if invalid {
			out.Valid = valid
		}
	}
	return out, nil
}

func gatherSlice[T any](dst, src []T, rows []int32) {
	if rows == nil {
		copy(dst, src)
		return
	}
	for i, row := range rows {
		dst[i] = src[row]
	}
}
