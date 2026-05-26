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
	stmt := storage.NewSpan("COMPACT " + table)
	defer func() {
		stmt.End()
		db.publishWriteSpan(stmt)
	}()
	for _, entry := range view.Entries {
		if entry.Path == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return rewritten, err
		}
		seg := segByPath[entry.Path]
		if seg == nil {
			continue
		}
		rows := int(seg.Rows())
		liveCount := rows
		if seg.DV != nil {
			liveCount = rows - seg.DV.NullCount(rows)
		}
		halfDeleted := seg.DV != nil && liveCount*2 <= rows
		hasDroppedColumn := segmentHasInactiveColumn(seg, activeIDs)
		isLegacy := seg.TableID == 0
		if !halfDeleted && !hasDroppedColumn && !isLegacy {
			continue
		}
		liveBatch, err := readSegmentLiveRows(seg, def)
		if err != nil {
			return rewritten, fmt.Errorf("compact %s: %w", entry.Path, err)
		}
		newPath := db.nextSegmentPath(table)
		if liveCount > 0 {
			span, err := storage.WriteSegmentWithIdentity(newPath, []vector.Batch{liveBatch}, codecs, db.segmentIdentity(def))
			if span != nil {
				stmt.AppendChild(span)
			}
			if err != nil {
				return rewritten, err
			}
		}
		fullDV := vector.NewValidity(rows)
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
			adds = append(adds, storage.ManifestSegmentAdd{Path: newPath, Rows: uint32(liveCount), SchemaGeneration: uint64(db.catalog.Generation)})
		}
		if err := db.commitManifestTxn(table, adds, []storage.ManifestDVUpdate{{SegmentPath: entry.Path, DVPath: dvPath, Rows: uint32(rows)}}); err != nil {
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
	var paths []string
	var dvPaths []string
	for _, entry := range view.Entries {
		// Retention is safe only when both seg.CommitTs and DV-out CommitTs are strictly
		// below cutoff, otherwise a reader pinned between them would expect to see live rows.
		effectiveTs := entry.CommitTs
		if entry.DVCommitTs > effectiveTs {
			effectiveTs = entry.DVCommitTs
		}
		if effectiveTs == 0 || effectiveTs >= cutoff {
			continue
		}
		if entry.DeletionVectorPath == "" {
			continue
		}
		seg, err := storage.OpenSegmentWithDV(entry.Path, entry.DeletionVectorPath)
		if err != nil {
			return 0, fmt.Errorf("retention: open %q: %w", entry.Path, err)
		}
		rows := int(seg.Rows())
		fullyDead := seg.DV != nil && seg.DV.NullCount(rows) == rows
		seg.Close()
		if !fullyDead {
			continue
		}
		paths = append(paths, entry.Path)
		dvPaths = append(dvPaths, entry.DeletionVectorPath)
	}
	if len(paths) == 0 {
		return 0, nil
	}
	for _, p := range paths {
		db.segments.removeByPath(p)
	}
	commitTs := db.nextCommitTs.Add(1)
	if err := m.Retire(commitTs, paths); err != nil {
		return 0, err
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("retention: remove %q: %w", p, err)
		}
	}
	for _, p := range dvPaths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("retention: remove dv %q: %w", p, err)
		}
	}
	return len(paths), nil
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

// Caller must ensure the segment's live row count fits in a single batch
// (StandardBatchRows). Compact only invokes this when liveCount <= rows/2, and rows
// is bounded by the seal-time page size.
// segmentHasInactiveColumn flags segments whose footer still carries a column id that
// the current catalog has tombstoned, so they get rewritten next compaction pass.
func segmentHasInactiveColumn(seg *storage.Segment, activeIDs map[uint64]struct{}) bool {
	if seg.TableID == 0 {
		return false
	}
	for _, c := range seg.Cols {
		if c.ColumnID == 0 {
			continue
		}
		if _, ok := activeIDs[c.ColumnID]; !ok {
			return true
		}
	}
	return false
}

func readSegmentLiveRows(seg *storage.Segment, def sql.BoundTableDef) (vector.Batch, error) {
	names := make([]string, len(def.Columns))
	ids := make([]uint64, len(def.Columns))
	kinds := make([]vector.VecKind, len(def.Columns))
	defaults := make([]storage.ScanDefault, len(def.Columns))
	for i, c := range def.Columns {
		names[i] = c.Name
		ids[i] = uint64(c.ID)
		k, err := vector.VecKindOf(c.Type)
		if err != nil {
			return vector.Batch{}, fmt.Errorf("compact: column %q vec kind: %w", c.Name, err)
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
	var collected []vector.Column
	var totalRows int
	err := storage.Scan(opts, func(batch vector.Batch, sel *vector.SelectionMask) error {
		live := sel.PopCount()
		if live == 0 {
			return nil
		}
		if collected == nil {
			collected = make([]vector.Column, len(batch.Columns))
			for i, c := range batch.Columns {
				v, err := vector.NewVecForKind(c.V.Kind, vector.StandardBatchRows)
				if err != nil {
					return err
				}
				collected[i] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
			}
		}
		var loopErr error
		sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			for i, c := range batch.Columns {
				if err := vector.CopyVecRow(c.V, row, &collected[i].V, totalRows); err != nil {
					loopErr = err
					return
				}
			}
			totalRows++
		})
		return loopErr
	})
	if err != nil {
		return vector.Batch{}, err
	}
	if collected == nil {
		return vector.Batch{}, fmt.Errorf("readSegmentLiveRows: no rows decoded")
	}
	for i := range collected {
		collected[i].V.Truncate(totalRows)
	}
	out, err := vector.NewBatch(collected)
	if err != nil {
		return vector.Batch{}, err
	}
	return out, nil
}
