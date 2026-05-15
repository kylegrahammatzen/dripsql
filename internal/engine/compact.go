// Compact rewrites segments whose deletion-vector marks more than half their rows as
// invalid: read the live rows out, write a fresh single-page segment, and commit one
// atomic manifest entry that adds the replacement and tombstones the source. Vacuum
// removes versioned .dv files no longer referenced by the latest manifest snapshot.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// Compact rewrites any segment in `table` whose DV invalidates strictly more than half
// its rows. Returns the number of segments rewritten. Caller must not hold db.mu.
func (db *DB) Compact(ctx context.Context, table string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, fmt.Errorf("engine: database is closed")
	}
	m, err := db.manifestFor(table)
	if err != nil {
		return 0, err
	}
	view := m.Snapshot()
	rewritten := 0
	for _, entry := range view.Entries {
		if entry.Path == "" || entry.DeletionVectorPath == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return rewritten, err
		}
		segs, err := db.openSegmentsForQuery(table)
		if err != nil {
			return rewritten, err
		}
		var seg *storage.Segment
		for _, s := range segs {
			if s.Path() == entry.Path {
				seg = s
				break
			}
		}
		if seg == nil || seg.DV == nil {
			continue
		}
		rows := int(seg.Rows())
		liveCount := rows - seg.DV.NullCount(rows)
		if liveCount*2 > rows {
			continue
		}
		liveBatch, err := readSegmentLiveRows(seg)
		if err != nil {
			return rewritten, fmt.Errorf("compact %s: %w", entry.Path, err)
		}
		newPath := db.nextSegmentPath(table)
		if liveCount > 0 {
			if err := storage.WriteSegmentWithCodecs(newPath, []types.Batch{liveBatch}, columnCodecs(db.boundTable(db.tables[types.NormalizeName(table)]))); err != nil {
				return rewritten, err
			}
		}
		fullDV := types.NewValidity(rows)
		for i := range rows {
			fullDV.SetInvalid(i)
		}
		dvPath := versionedDVPath(entry.Path)
		if err := storage.WriteDVAtPath(dvPath, rows, fullDV); err != nil {
			if liveCount > 0 {
				os.Remove(newPath)
			}
			return rewritten, err
		}
		var adds []storage.ManifestSegmentAdd
		if liveCount > 0 {
			adds = append(adds, storage.ManifestSegmentAdd{Path: newPath, Rows: uint32(liveCount)})
		}
		if err := m.Commit(adds, []storage.ManifestDVUpdate{{SegmentPath: entry.Path, DVPath: dvPath, Rows: uint32(rows)}}); err != nil {
			if liveCount > 0 {
				os.Remove(newPath)
			}
			os.Remove(dvPath)
			return rewritten, err
		}
		rewritten++
	}
	return rewritten, nil
}

// Vacuum walks each table's segments directory and removes any versioned .dv file (the
// `<segment>.dv.<token>` form written by UPDATE / DELETE / Compact) that the current
// manifest snapshot no longer references. Returns the count of files removed.
func (db *DB) Vacuum() (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, fmt.Errorf("engine: database is closed")
	}
	total := 0
	for name := range db.tables {
		n, err := db.vacuumTableLocked(name)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (db *DB) vacuumTableLocked(table string) (int, error) {
	m, err := db.manifestFor(table)
	if err != nil {
		return 0, err
	}
	view := m.Snapshot()
	referenced := make(map[string]struct{}, 2*len(view.Entries))
	for _, e := range view.Entries {
		if e.DeletionVectorPath != "" {
			referenced[e.DeletionVectorPath] = struct{}{}
		}
	}
	dir := db.tableDir(table)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if !strings.Contains(name, ".dv.") {
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			continue
		}
		full := filepath.Join(dir, name)
		if _, ok := referenced[full]; ok {
			continue
		}
		if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// readSegmentLiveRows decodes every live row from seg into one Batch. Used by compaction
// to materialize a small replacement. Caller's responsibility to ensure the live row count
// fits in a single batch (StandardBatchRows).
func readSegmentLiveRows(seg *storage.Segment) (types.Batch, error) {
	opts := storage.ScanOpts{Segments: []*storage.Segment{seg}}
	var collected []types.Column
	var totalRows int
	err := storage.Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error {
		live := sel.PopCount()
		if live == 0 {
			return nil
		}
		if collected == nil {
			collected = make([]types.Column, len(batch.Columns))
			for i, c := range batch.Columns {
				v, err := types.NewVecForKind(c.V.Kind, types.StandardBatchRows)
				if err != nil {
					return err
				}
				collected[i] = types.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
			}
		}
		var loopErr error
		sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			for i, c := range batch.Columns {
				if err := types.CopyVecRow(c.V, row, &collected[i].V, totalRows); err != nil {
					loopErr = err
					return
				}
			}
			totalRows++
		})
		return loopErr
	})
	if err != nil {
		return types.Batch{}, err
	}
	if collected == nil {
		return types.Batch{}, fmt.Errorf("readSegmentLiveRows: no rows decoded")
	}
	for i := range collected {
		collected[i].V.Truncate(totalRows)
	}
	out, err := types.NewBatch(collected)
	if err != nil {
		return types.Batch{}, err
	}
	return out, nil
}
