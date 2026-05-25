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
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

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
	stmt := storage.NewSpan("COMPACT " + table)
	defer func() {
		stmt.End()
		db.publishWriteSpan(stmt)
	}()
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
			span, err := storage.WriteSegment(newPath, []vector.Batch{liveBatch}, columnCodecs(db.boundTable(db.tables[schema.NormalizeName(table)])))
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
			adds = append(adds, storage.ManifestSegmentAdd{Path: newPath, Rows: uint32(liveCount)})
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
	if db.opts.AutoRetention && db.opts.RetentionLag > 0 {
		cur := db.nextCommitTs.Load()
		if cur > db.opts.RetentionLag {
			cutoff := cur - db.opts.RetentionLag
			retired, err := db.vacuumRetentionLocked(cutoff)
			total += retired
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

func (db *DB) VacuumRetention(retainBefore uint64) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, fmt.Errorf("engine: database is closed")
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
		for k, elem := range db.segCache {
			if k.path != p {
				continue
			}
			_ = elem.Value.(*segCacheEntry).seg.Close()
			db.segLRU.Remove(elem)
			delete(db.segCache, k)
		}
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
func readSegmentLiveRows(seg *storage.Segment) (vector.Batch, error) {
	opts := storage.ScanOpts{Segments: []*storage.Segment{seg}}
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
