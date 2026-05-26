// UPDATE writes replacement segments and a new versioned DV per source segment, then
// publishes the whole transaction via one atomic Commit. Until that record lands the
// snapshot is unchanged, so readers never see either duplicate or hidden rows.
package engine

import (
	"context"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (db *DB) update(ctx context.Context, plan *sql.Plan, commit commitFn) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	def := plan.Table
	m, err := db.manifestFor(def.Name)
	if err != nil {
		return 0, err
	}
	entries := m.Snapshot().Entries

	uplan, err := buildUpdatePlan(plan)
	if err != nil {
		return 0, err
	}
	pending, err := newUpdateBuffer(uplan)
	if err != nil {
		return 0, err
	}

	stmt := storage.NewSpan("UPDATE " + def.Name)
	defer func() {
		stmt.End()
		db.publishWriteSpan(stmt)
	}()

	stage := stagedUpdate{}
	flush := func() error {
		if pending.rows == 0 {
			return nil
		}
		batch, err := pending.materialize()
		if err != nil {
			return err
		}
		path := db.nextSegmentPath(def.Name)
		span, err := storage.WriteSegment(path, []vector.Batch{batch}, columnCodecs(def))
		if span != nil {
			stmt.AppendChild(span)
		}
		if err != nil {
			return err
		}
		stage.adds = append(stage.adds, storage.ManifestSegmentAdd{Path: path, Rows: uint32(pending.rows)})
		stage.paths = append(stage.paths, path)
		return pending.reset()
	}

	var updated int64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			stage.cleanup()
			return updated, err
		}
		n, dvUpdate, err := db.applyUpdateToSegment(ctx, entry, plan, uplan, pending, flush)
		if err != nil {
			stage.cleanup()
			return updated, fmt.Errorf("UPDATE %s: %w", entry.Path, err)
		}
		updated += n
		if dvUpdate != nil {
			stage.dvUpdates = append(stage.dvUpdates, *dvUpdate)
			stage.paths = append(stage.paths, dvUpdate.DVPath)
		}
	}
	if err := flush(); err != nil {
		stage.cleanup()
		return updated, err
	}
	if err := commit(def.Name, stage.adds, stage.dvUpdates); err != nil {
		stage.cleanup()
		return updated, err
	}
	return updated, nil
}

type stagedUpdate struct {
	adds      []storage.ManifestSegmentAdd
	dvUpdates []storage.ManifestDVUpdate
	paths     []string
}

func (s *stagedUpdate) cleanup() {
	for _, p := range s.paths {
		os.Remove(p)
	}
}

type updatePlan struct {
	cols       []updateCol
	decodeDefs []sql.BoundColumnDef
}

type updateCol struct {
	def       sql.BoundColumnDef
	decodeIdx int
	value     sql.Value
	assigned  bool
}

func buildUpdatePlan(plan *sql.Plan) (*updatePlan, error) {
	tableDefs := plan.Table.Columns
	tableIdx := make(map[string]int, len(tableDefs))
	for i, d := range tableDefs {
		tableIdx[schema.NormalizeName(d.Name)] = i
	}

	assigned := make([]bool, len(tableDefs))
	values := make([]sql.Value, len(tableDefs))
	for i := range plan.Assignments {
		a := &plan.Assignments[i]
		idx, ok := tableIdx[schema.NormalizeName(a.Column.Name)]
		if !ok {
			return nil, fmt.Errorf("update: assigned column %q not in table", a.Column.Name)
		}
		assigned[idx] = true
		values[idx] = a.Value
	}

	need := make(map[string]struct{})
	if plan.Where != nil {
		addExprColumns(need, *plan.Where)
	}
	for ti, d := range tableDefs {
		if !assigned[ti] {
			need[schema.NormalizeName(d.Name)] = struct{}{}
		}
	}

	up := &updatePlan{cols: make([]updateCol, len(tableDefs))}
	decodeIdxByName := make(map[string]int, len(need))
	for _, d := range tableDefs {
		name := schema.NormalizeName(d.Name)
		if _, ok := need[name]; !ok {
			continue
		}
		decodeIdxByName[name] = len(up.decodeDefs)
		up.decodeDefs = append(up.decodeDefs, d)
	}

	for ti, d := range tableDefs {
		col := updateCol{def: d, decodeIdx: -1, assigned: assigned[ti], value: values[ti]}
		if !col.assigned {
			col.decodeIdx = decodeIdxByName[schema.NormalizeName(d.Name)]
		}
		up.cols[ti] = col
	}
	return up, nil
}

func (db *DB) applyUpdateToSegment(ctx context.Context, entry storage.ManifestEntry, plan *sql.Plan, up *updatePlan, pending *updateBuffer, flush func() error) (int64, *storage.ManifestDVUpdate, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	seg, err := storage.OpenSegmentWithDV(entry.Path, entry.DeletionVectorPath)
	if err != nil {
		return 0, nil, err
	}
	defer seg.Close()
	rows := int(seg.Rows())
	if rows == 0 {
		return 0, nil, nil
	}
	if len(seg.Cols) == 0 {
		return 0, nil, fmt.Errorf("update: segment has no columns")
	}

	hadDV := seg.DV != nil
	dv := cloneOrAllValidDV(seg, rows)

	idxByName := segmentColumnIndex(seg)
	segIdxs := make([]int, len(up.decodeDefs))
	for i, d := range up.decodeDefs {
		idx, ok := idxByName[schema.NormalizeName(d.Name)]
		if !ok {
			return 0, nil, fmt.Errorf("column %q not in segment", d.Name)
		}
		segIdxs[i] = idx
	}

	pageCount := len(seg.Cols[0].Pages)
	var updated int64
	// Single scratch is safe: every codec.Decode either copies into a fresh fixed-width
	// buffer or appends into a fresh VarVec, so the returned Vec never aliases scratch.
	var scratch []byte
	decoded := make([]vector.Column, len(up.decodeDefs))
	anyMatch := false

	for pi := range pageCount {
		if err := ctx.Err(); err != nil {
			return updated, nil, err
		}
		page := seg.Cols[0].Pages[pi]
		pageRows := int(page.Rows)
		rowStart := int(page.RowStart)
		live := vector.NewSelectionMask(pageRows)
		liveCount := 0
		if !hadDV {
			live.FillAll()
			liveCount = pageRows
		} else {
			for row := range pageRows {
				if dv.IsValid(rowStart + row) {
					live.Set(row)
					liveCount++
				}
			}
		}
		if liveCount == 0 {
			continue
		}
		for ci, d := range up.decodeDefs {
			v, s, err := seg.ReadPageInto(segIdxs[ci], pi, scratch)
			if err != nil {
				return updated, nil, fmt.Errorf("page %d col %q: %w", pi, d.Name, err)
			}
			scratch = s
			decoded[ci] = vector.Column{Name: d.Name, Type: d.Type, EnumLabels: d.Labels, V: v}
		}
		batch := vector.Batch{Len: pageRows, Columns: decoded, Sel: &live}

		var sel vector.SelectionMask
		if plan.Where == nil {
			sel = live
		} else {
			sel, err = exec.EvalPredicate(batch, *plan.Where)
			if err != nil {
				return updated, nil, fmt.Errorf("page %d: %w", pi, err)
			}
		}

		var loopErr error
		sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			abs := rowStart + row
			if err := pending.appendRow(batch, row); err != nil {
				loopErr = err
				return
			}
			dv.SetInvalid(abs)
			updated++
			anyMatch = true
			if pending.rows == vector.StandardBatchRows {
				if err := flush(); err != nil {
					loopErr = err
					return
				}
			}
		})
		if loopErr != nil {
			return updated, nil, loopErr
		}
	}
	if !anyMatch {
		return updated, nil, nil
	}
	dvPath := versionedDVPath(entry.Path)
	if err := storage.WriteDVAtPath(dvPath, rows, dv); err != nil {
		return updated, nil, err
	}
	return updated, &storage.ManifestDVUpdate{SegmentPath: entry.Path, DVPath: dvPath, Rows: uint32(rows)}, nil
}

type updateBuffer struct {
	plan *updatePlan
	cols []vector.Column
	rows int
}

func newUpdateBuffer(up *updatePlan) (*updateBuffer, error) {
	cols, err := allocUpdateCols(up, vector.StandardBatchRows)
	if err != nil {
		return nil, err
	}
	return &updateBuffer{plan: up, cols: cols}, nil
}

func allocUpdateCols(up *updatePlan, rows int) ([]vector.Column, error) {
	cols := make([]vector.Column, len(up.cols))
	for i, c := range up.cols {
		vk, err := vector.VecKindOf(c.def.Type)
		if err != nil {
			return nil, err
		}
		v, err := vector.NewVecForKind(vk, rows)
		if err != nil {
			return nil, err
		}
		// Assigned columns get the same value in every row, so their validity is fully
		// determined at allocation time: null assignment leaves the all-invalid zero
		// bitmap, non-null nullable gets all-valid up front. Unassigned columns let
		// CopyVecRow lazily allocate.
		if c.assigned {
			if c.value.Kind == sql.ValueNull {
				v.Valid = make(vector.Validity, vector.ValidityWords(rows))
			} else if c.def.Nullable {
				v.Valid = vector.NewAllValid(rows)
			}
		}
		cols[i] = vector.Column{Name: c.def.Name, Type: c.def.Type, EnumLabels: c.def.Labels, V: v}
	}
	return cols, nil
}

func (b *updateBuffer) reset() error {
	cols, err := allocUpdateCols(b.plan, vector.StandardBatchRows)
	if err != nil {
		return err
	}
	b.cols = cols
	b.rows = 0
	return nil
}

func (b *updateBuffer) appendRow(src vector.Batch, srcRow int) error {
	dst := b.rows
	for i, c := range b.plan.cols {
		if c.assigned {
			if c.value.Kind == sql.ValueNull {
				continue
			}
			if err := writeAssignedRow(&b.cols[i].V, b.cols[i].V.Kind, c.value, dst); err != nil {
				return err
			}
			continue
		}
		if err := vector.CopyVecRow(src.Columns[c.decodeIdx].V, srcRow, &b.cols[i].V, dst); err != nil {
			return err
		}
	}
	b.rows++
	return nil
}

// materialize returns the buffered batch without a second copy. allocUpdateCols sized each
// Vec at StandardBatchRows; setting Len to the actual row count makes the existing buffer
// the output. reset allocates a fresh buffer for the next chunk so callers never see
// overwrites after WriteSegment retains references.
func (b *updateBuffer) materialize() (vector.Batch, error) {
	out := make([]vector.Column, len(b.cols))
	for i, c := range b.cols {
		c.V.Truncate(b.rows)
		out[i] = c
	}
	return vector.NewBatch(out)
}

func writeAssignedRow(v *vector.Vec, vk vector.VecKind, val sql.Value, row int) error {
	switch vk {
	case vector.VecBool:
		if val.Bool {
			v.BoolBits()[row>>3] |= 1 << (row & 7)
		}
	case vector.VecInt16:
		v.I16()[row] = int16(val.Int)
	case vector.VecInt32, vector.VecDate:
		v.I32()[row] = int32(val.Int)
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		v.I64()[row] = val.Int
	case vector.VecFloat32:
		if val.Kind == sql.ValueInt {
			v.F32()[row] = float32(val.Int)
		} else {
			v.F32()[row] = float32(val.Float)
		}
	case vector.VecFloat64:
		if val.Kind == sql.ValueInt {
			v.F64()[row] = float64(val.Int)
		} else {
			v.F64()[row] = val.Float
		}
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		v.Var().AppendString(row, val.String)
	case vector.VecUUID:
		u, err := vector.ParseUUID(val.String)
		if err != nil {
			return err
		}
		copy(v.FixedBytes()[row*16:row*16+16], u[:])
	case vector.VecEnum32:
		if val.Kind != sql.ValueEnum {
			return fmt.Errorf("enum column got %v (binder did not resolve to ValueEnum)", val.Kind)
		}
		v.U32()[row] = val.Enum
	default:
		return fmt.Errorf("update: unsupported VecKind %v", vk)
	}
	return nil
}
