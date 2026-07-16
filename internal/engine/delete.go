// DELETE walks each segment, decodes only WHERE-referenced columns, marks matched rows in
// a new versioned DV, and publishes all DV path changes atomically via a single Commit.
package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// addExprColumns walks a bound expression and seeds every ExprColumn name into the caller's set.
// exprColumnNames is the sorted-slice variant for callers that need deterministic order.
func addExprColumns(seen map[string]struct{}, expr sql.BoundExpr) {
	if expr.Op == sql.ExprColumn {
		seen[schema.NormalizeName(expr.Column)] = struct{}{}
		return
	}
	for _, a := range expr.Args {
		addExprColumns(seen, a)
	}
}

func exprColumnNames(expr sql.BoundExpr) []string {
	seen := make(map[string]struct{})
	addExprColumns(seen, expr)
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (db *DB) delete(ctx context.Context, plan *sql.Plan, commit commitFn) (int64, error) {
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
	if err := commit(def.Name, nil, dvUpdates); err != nil {
		return deleted, err
	}
	return deleted, nil
}

func (db *DB) applyDeleteToSegment(ctx context.Context, entry storage.ManifestEntry, plan *sql.Plan) (int64, *storage.ManifestDVUpdate, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	table := plan.Table.Name
	segPath := db.resolveTablePath(table, entry.Path)
	seg, err := storage.OpenSegmentWithDV(segPath, db.resolveTablePath(table, entry.DeletionVectorPath))
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
		newDV := make(vector.Validity, vector.ValidityWords(rows))
		dvPath := versionedDVPath(segPath)
		if err := storage.WriteDVAtPath(dvPath, rows, newDV); err != nil {
			return 0, nil, err
		}
		return n, &storage.ManifestDVUpdate{SegmentPath: entry.Path, DVPath: filepath.Base(dvPath), Rows: uint32(rows)}, nil
	}

	predNames := exprColumnNames(*plan.Where)
	defs := make([]sql.BoundColumnDef, 0, len(predNames))
	for _, name := range predNames {
		found := false
		for _, c := range plan.Table.Columns {
			if schema.NormalizeName(c.Name) == name {
				defs = append(defs, c)
				found = true
				break
			}
		}
		if !found {
			return 0, nil, fmt.Errorf("delete: column %q referenced in WHERE not in table", name)
		}
	}

	dv := cloneOrAllValidDV(seg, rows)
	var deleted int64
	err = walkLivePages(ctx, seg, dv, defs, plan.Where, func(_ vector.Batch, sel vector.SelectionMask, rowStart int) error {
		sel.IterSet(func(row int) {
			deleted++
			dv.SetInvalid(rowStart + row)
		})
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	if deleted == 0 {
		return 0, nil, nil
	}
	dvPath := versionedDVPath(segPath)
	if err := storage.WriteDVAtPath(dvPath, rows, dv); err != nil {
		return 0, nil, err
	}
	return deleted, &storage.ManifestDVUpdate{SegmentPath: entry.Path, DVPath: filepath.Base(dvPath), Rows: uint32(rows)}, nil
}

// walkLivePages iterates each page of seg, decoding only defs columns into a reused scratch, and invokes fn with the page batch plus a sel mask of rows matching where (or all live rows when where is nil).
func walkLivePages(ctx context.Context, seg *storage.Segment, dv vector.Validity, defs []sql.BoundColumnDef, where *sql.BoundExpr, fn func(batch vector.Batch, sel vector.SelectionMask, rowStart int) error) error {
	if len(seg.Cols) == 0 {
		return fmt.Errorf("walk: segment has no columns")
	}
	hadDV := seg.DV != nil
	idxByName := segmentColumnIndex(seg)
	segIdxs := make([]int, len(defs))
	for i, d := range defs {
		idx, ok := idxByName[schema.NormalizeName(d.Name)]
		if !ok {
			return fmt.Errorf("column %q not in segment", d.Name)
		}
		segIdxs[i] = idx
	}
	var scratch []byte
	decoded := make([]vector.Column, len(defs))
	pageCount := len(seg.Cols[0].Pages)
	for pi := range pageCount {
		if err := ctx.Err(); err != nil {
			return err
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
		for ci, d := range defs {
			v, s, err := seg.ReadPageInto(segIdxs[ci], pi, scratch)
			if err != nil {
				return fmt.Errorf("page %d col %q: %w", pi, d.Name, err)
			}
			scratch = s
			decoded[ci] = vector.Column{Name: d.Name, Type: d.Type, EnumLabels: d.Labels, V: v}
		}
		batch := vector.Batch{Len: pageRows, Columns: decoded, Sel: &live}
		sel := live
		if where != nil {
			s, err := exec.EvalPredicate(batch, *where)
			if err != nil {
				return fmt.Errorf("page %d: %w", pi, err)
			}
			sel = s
		}
		if err := fn(batch, sel, rowStart); err != nil {
			return err
		}
	}
	return nil
}

func cloneOrAllValidDV(seg *storage.Segment, rows int) vector.Validity {
	if seg.DV != nil {
		return seg.DV.Clone()
	}
	return vector.NewAllValid(rows)
}

func segmentColumnIndex(seg *storage.Segment) map[string]int {
	out := make(map[string]int, len(seg.Cols))
	for i, c := range seg.Cols {
		out[schema.NormalizeName(c.Name)] = i
	}
	return out
}

// versionedDVPath returns a fresh per-update DV path. A unix-nano suffix gives uniqueness
// without a manifest-side counter, so the file can be written before the Commit knows its
// final version number.
func versionedDVPath(segPath string) string {
	return segPath + ".dv." + strconv.FormatInt(time.Now().UnixNano(), 36)
}
