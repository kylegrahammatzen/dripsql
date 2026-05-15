// DELETE walks each segment, decodes only WHERE-referenced columns, marks matched rows in
// a new versioned DV, and publishes all DV path changes atomically via a single Commit.
package engine

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func (db *DB) delete(ctx context.Context, plan *sql.Plan) (int64, error) {
	def := plan.Table
	m, err := db.manifestFor(def.Name)
	if err != nil {
		return 0, err
	}
	entries := m.Snapshot().Entries
	var deleted int64
	var dvUpdates []storage.ManifestDVUpdate
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		n, dvUpdate, err := db.applyDeleteToSegment(ctx, entry, plan)
		if err != nil {
			return deleted, fmt.Errorf("DELETE %s: %w", entry.Path, err)
		}
		deleted += n
		if dvUpdate != nil {
			dvUpdates = append(dvUpdates, *dvUpdate)
		}
	}
	if err := m.Commit(nil, dvUpdates); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func (db *DB) applyDeleteToSegment(ctx context.Context, entry storage.ManifestEntry, plan *sql.Plan) (int64, *storage.ManifestDVUpdate, error) {
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

	if plan.Where == nil {
		var n int64
		if seg.DV == nil {
			n = int64(rows)
		} else {
			n = int64(rows - seg.DV.NullCount(rows))
		}
		if n == 0 {
			return 0, nil, nil
		}
		newDV := make(types.Validity, types.ValidityWords(rows))
		dvPath := versionedDVPath(entry.Path)
		if err := storage.WriteDVAtPath(dvPath, rows, newDV); err != nil {
			return 0, nil, err
		}
		return n, &storage.ManifestDVUpdate{SegmentPath: entry.Path, DVPath: dvPath, Rows: uint32(rows)}, nil
	}

	if len(seg.Cols) == 0 {
		return 0, nil, fmt.Errorf("delete: WHERE requires pages but segment has no columns")
	}

	type predCol struct {
		def    sql.BoundColumnDef
		segIdx int
	}
	predNames := exprColumnNames(*plan.Where)
	preds := make([]predCol, 0, len(predNames))
	for _, name := range predNames {
		var def sql.BoundColumnDef
		found := false
		for _, c := range plan.Table.Columns {
			if types.NormalizeName(c.Name) == name {
				def = c
				found = true
				break
			}
		}
		if !found {
			return 0, nil, fmt.Errorf("delete: column %q referenced in WHERE not in table", name)
		}
		idx, err := findSegmentColumn(seg, def.Name)
		if err != nil {
			return 0, nil, err
		}
		preds = append(preds, predCol{def: def, segIdx: idx})
	}

	hadDV := seg.DV != nil
	dv := cloneOrAllValidDV(seg, rows)
	pageCount := len(seg.Cols[0].Pages)
	var deleted int64
	// Single scratch is safe: every codec.Decode either copies into a fresh fixed-width
	// buffer or appends into a fresh VarVec, so the returned Vec never aliases scratch.
	var scratch []byte
	decoded := make([]types.Column, len(preds))

	for pi := range pageCount {
		if err := ctx.Err(); err != nil {
			return deleted, nil, err
		}
		page := seg.Cols[0].Pages[pi]
		pageRows := int(page.Rows)
		rowStart := int(page.RowStart)
		live := types.NewSelectionMask(pageRows)
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
		for ci, p := range preds {
			v, s, err := seg.ReadPageInto(p.segIdx, pi, scratch)
			if err != nil {
				return 0, nil, fmt.Errorf("page %d col %q: %w", pi, p.def.Name, err)
			}
			scratch = s
			decoded[ci] = types.Column{Name: p.def.Name, Type: p.def.Type, EnumLabels: p.def.Labels, V: v}
		}
		batch := types.Batch{Len: pageRows, Columns: decoded, Sel: &live}
		sel, err := exec.EvalPredicate(batch, *plan.Where)
		if err != nil {
			return 0, nil, fmt.Errorf("page %d: %w", pi, err)
		}
		sel.IterSet(func(row int) {
			deleted++
			dv.SetInvalid(rowStart + row)
		})
	}
	if deleted == 0 {
		return 0, nil, nil
	}
	dvPath := versionedDVPath(entry.Path)
	if err := storage.WriteDVAtPath(dvPath, rows, dv); err != nil {
		return 0, nil, err
	}
	return deleted, &storage.ManifestDVUpdate{SegmentPath: entry.Path, DVPath: dvPath, Rows: uint32(rows)}, nil
}

func cloneOrAllValidDV(seg *storage.Segment, rows int) types.Validity {
	if seg.DV != nil {
		return seg.DV.Clone()
	}
	return types.NewAllValid(rows)
}

func findSegmentColumn(seg *storage.Segment, name string) (int, error) {
	want := types.NormalizeName(name)
	for i, c := range seg.Cols {
		if types.NormalizeName(c.Name) == want {
			return i, nil
		}
	}
	return 0, fmt.Errorf("column %q not in segment", name)
}

func segmentColumnIndex(seg *storage.Segment) map[string]int {
	out := make(map[string]int, len(seg.Cols))
	for i, c := range seg.Cols {
		out[types.NormalizeName(c.Name)] = i
	}
	return out
}

// versionedDVPath returns a fresh per-update DV path. A unix-nano suffix gives uniqueness
// without a manifest-side counter, so the file can be written before the Commit knows its
// final version number.
func versionedDVPath(segPath string) string {
	return segPath + ".dv." + strconv.FormatInt(time.Now().UnixNano(), 36)
}
