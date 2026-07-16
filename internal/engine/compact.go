// Compact rewrites half-or-more-deleted segments via one atomic manifest swap. Vacuum
// removes versioned .dv files no longer referenced by the manifest snapshot.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func (db *DB) Compact(ctx context.Context, table string) (int, error) {
	if err := db.lockOpen(); err != nil {
		return 0, err
	}
	defer db.mu.Unlock()
	if db.readOnly.Load() {
		return 0, ErrReadOnly
	}
	m, err := db.manifestFor(table)
	if err != nil {
		return 0, err
	}
	view := m.Snapshot()
	segs, err := db.openSegmentsForQuery(table)
	if err != nil {
		return 0, err
	}
	segByPath := make(map[string]*storage.Segment, len(segs))
	for _, s := range segs {
		segByPath[s.Path()] = s
	}
	def := db.boundTable(db.tables[schema.NormalizeName(table)])
	codecs := columnCodecs(def)
	activeIDs := make(map[uint64]struct{}, len(def.Columns))
	for _, c := range def.Columns {
		activeIDs[uint64(c.ID)] = struct{}{}
	}
	rewritten := 0
	for _, entry := range view.Entries {
		if entry.Path == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return rewritten, err
		}
		segPath := db.resolveTablePath(table, entry.Path)
		seg := segByPath[segPath]
		if seg == nil {
			continue
		}
		rows := int(seg.Rows())
		liveCount := rows
		if seg.DV != nil {
			liveCount = rows - seg.DV.NullCount(rows)
		}
		halfDeleted := seg.DV != nil && liveCount*2 <= rows
		// A tombstoned column id still in the footer forces a rewrite on this pass.
		hasDroppedColumn := false
		if seg.TableID != 0 {
			for _, c := range seg.Cols {
				if c.ColumnID == 0 {
					continue
				}
				if _, ok := activeIDs[c.ColumnID]; !ok {
					hasDroppedColumn = true
					break
				}
			}
		}
		isLegacy := seg.TableID == 0
		if !halfDeleted && !hasDroppedColumn && !isLegacy {
			continue
		}
		var liveBatches []vector.Batch
		if liveCount > 0 {
			liveBatches, err = readSegmentLiveRows(seg, def)
			if err != nil {
				return rewritten, fmt.Errorf("compact %s: %w", entry.Path, err)
			}
		}
		newPath := db.nextSegmentPath(table)
		if liveCount > 0 {
			if err := storage.WriteSegmentWithIdentity(newPath, liveBatches, codecs, db.segmentIdentity(def)); err != nil {
				return rewritten, err
			}
		}
		fullDV := vector.NewValidity(rows)
		for i := range rows {
			fullDV.SetInvalid(i)
		}
		dvPath := versionedDVPath(segPath)
		if err := storage.WriteDVAtPath(dvPath, rows, fullDV); err != nil {
			if liveCount > 0 {
				os.Remove(newPath)
			}
			return rewritten, err
		}
		var adds []storage.ManifestSegmentAdd
		if liveCount > 0 {
			adds = append(adds, storage.ManifestSegmentAdd{Path: filepath.Base(newPath), Rows: uint32(liveCount), SchemaGeneration: uint64(db.catalog.Generation)})
		}
		if err := db.commitManifestTxn(table, adds, []storage.ManifestDVUpdate{{SegmentPath: entry.Path, DVPath: filepath.Base(dvPath), Rows: uint32(rows)}}); err != nil {
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

func (db *DB) Vacuum() (int, error) {
	if err := db.lockOpen(); err != nil {
		return 0, err
	}
	defer db.mu.Unlock()
	if db.readOnly.Load() {
		return 0, ErrReadOnly
	}
	total := 0
	for name := range db.tables {
		n, err := db.vacuumTableLocked(name)
		total += n
		if err != nil {
			return total, err
		}
	}
	if db.autoRetention.Load() {
		if lag := db.retentionLag.Load(); lag > 0 {
			cur := db.nextCommitTs.Load()
			if cur > lag {
				retired, err := db.vacuumRetentionLocked(cur - lag)
				total += retired
				if err != nil {
					return total, err
				}
			}
		}
	}
	return total, nil
}

func (db *DB) VacuumRetention(retainBefore uint64) (int, error) {
	if err := db.lockOpen(); err != nil {
		return 0, err
	}
	defer db.mu.Unlock()
	if db.readOnly.Load() {
		return 0, ErrReadOnly
	}
	return db.vacuumRetentionLocked(retainBefore)
}

func (db *DB) vacuumRetentionLocked(retainBefore uint64) (int, error) {
	cutoff := retainBefore
	for ts := range db.pinnedReadTs {
		if ts < cutoff {
			cutoff = ts
		}
	}
	if cutoff == 0 {
		return 0, nil
	}
	retired := 0
	for name := range db.tables {
		n, err := db.retireFullyDeletedSegments(name, cutoff)
		retired += n
		if err != nil {
			return retired, err
		}
	}
	return retired, nil
}

func (db *DB) retireFullyDeletedSegments(table string, cutoff uint64) (int, error) {
	m, err := db.manifestFor(table)
	if err != nil {
		return 0, err
	}
	view := m.Snapshot()
	// stored keeps the exact manifest strings for Retire while the resolved lists drive file removal.
	var stored []string
	var segPaths []string
	var dvPaths []string
	for _, entry := range view.Entries {
		// Retention is safe only when both seg.CommitTs and DV-out CommitTs are strictly
		// below cutoff, otherwise a reader pinned between them would expect to see live rows.
		effectiveTs := max(entry.CommitTs, entry.DVCommitTs)
		if effectiveTs == 0 || effectiveTs >= cutoff {
			continue
		}
		if entry.DeletionVectorPath == "" {
			continue
		}
		segPath := db.resolveTablePath(table, entry.Path)
		dvPath := db.resolveTablePath(table, entry.DeletionVectorPath)
		seg, err := storage.OpenSegmentWithDV(segPath, dvPath)
		if err != nil {
			return 0, fmt.Errorf("retention: open %q: %w", segPath, err)
		}
		rows := int(seg.Rows())
		fullyDead := seg.DV != nil && seg.DV.NullCount(rows) == rows
		seg.Close()
		if !fullyDead {
			continue
		}
		stored = append(stored, entry.Path)
		segPaths = append(segPaths, segPath)
		dvPaths = append(dvPaths, dvPath)
	}
	if len(stored) == 0 {
		return 0, nil
	}
	for _, p := range segPaths {
		db.segments.removeByPath(p)
	}
	commitTs := db.nextCommitTs.Add(1)
	if err := m.Retire(commitTs, stored); err != nil {
		return 0, err
	}
	for _, p := range segPaths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("retention: remove %q: %w", p, err)
		}
	}
	for _, p := range dvPaths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("retention: remove dv %q: %w", p, err)
		}
	}
	return len(stored), nil
}

func (db *DB) vacuumTableLocked(table string) (int, error) {
	m, err := db.manifestFor(table)
	if err != nil {
		return 0, err
	}
	view := m.Snapshot()
	// Keyed by basename so stored-format differences never orphan or double-free a DV file.
	referenced := make(map[string]struct{}, 2*len(view.Entries))
	for _, e := range view.Entries {
		if e.DeletionVectorPath != "" {
			referenced[filepath.Base(e.DeletionVectorPath)] = struct{}{}
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
		if _, ok := referenced[name]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// readSegmentLiveRows re-materializes every live row of seg into page-capped batches
// so the rewrite never exceeds StandardBatchRows per page regardless of live count.
func readSegmentLiveRows(seg *storage.Segment, def sql.BoundTableDef) ([]vector.Batch, error) {
	names := make([]string, len(def.Columns))
	ids := make([]uint64, len(def.Columns))
	kinds := make([]vector.VecKind, len(def.Columns))
	defaults := make([]storage.ScanDefault, len(def.Columns))
	for i, c := range def.Columns {
		names[i] = c.Name
		ids[i] = uint64(c.ID)
		k, err := vector.VecKindOf(c.Type)
		if err != nil {
			return nil, fmt.Errorf("compact: column %q vec kind: %w", c.Name, err)
		}
		kinds[i] = k
		defaults[i] = storage.ScanDefault{
			Set:   c.Default.Set,
			Null:  c.Default.Null,
			I64:   c.Default.I64,
			F64:   c.Default.F64,
			Bytes: c.Default.Bytes,
			Bool:  c.Default.Bool,
		}
	}
	opts := storage.ScanOpts{
		Segments:       []*storage.Segment{seg},
		Columns:        names,
		ColumnIDs:      ids,
		ColumnKinds:    kinds,
		ColumnDefaults: defaults,
	}
	var out []vector.Batch
	var open []vector.Column
	openRows := 0
	flush := func() error {
		if openRows == 0 {
			return nil
		}
		for i := range open {
			open[i].V.Truncate(openRows)
		}
		b, err := vector.NewBatch(open)
		if err != nil {
			return err
		}
		out = append(out, b)
		open = nil
		openRows = 0
		return nil
	}
	err := storage.Scan(opts, func(batch vector.Batch, sel *vector.SelectionMask) error {
		if sel.PopCount() == 0 {
			return nil
		}
		var loopErr error
		sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			if openRows == vector.StandardBatchRows {
				if err := flush(); err != nil {
					loopErr = err
					return
				}
			}
			if open == nil {
				open = make([]vector.Column, len(batch.Columns))
				for i, c := range batch.Columns {
					v, err := vector.NewVecForKind(c.V.Kind, vector.StandardBatchRows)
					if err != nil {
						loopErr = err
						return
					}
					open[i] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
				}
			}
			for i, c := range batch.Columns {
				if err := vector.CopyVecRow(c.V, row, &open[i].V, openRows); err != nil {
					loopErr = err
					return
				}
			}
			openRows++
		})
		return loopErr
	})
	if err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("readSegmentLiveRows: no rows decoded")
	}
	return out, nil
}
