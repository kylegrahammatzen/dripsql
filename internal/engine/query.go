// Query runner. Resolves segments via the DB-level handle cache, builds the operator chain, drains to Rows.
// Cache misses open lazily and stay open until DB.Close.
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

func (db *DB) runQuery(ctx context.Context, plan *sql.Plan) (*Rows, error) {
	if plan != nil && plan.Kind == sql.PlanExplain {
		return db.runExplain(ctx, plan)
	}
	if rows, ok, err := db.answerFromMetadata(plan); err != nil {
		return nil, err
	} else if ok {
		return rows, nil
	}
	// PR-V2c: pin a snapshot timestamp at the start of the statement so every scan,
	// including joins across multiple tables, sees the same committed view. ^uint64(0)
	// would also work (latest), but using nextCommitTs.Load lets us layer time-travel
	// (AS OF) on top without changing this code path.
	readTs := db.nextCommitTs.Load()
	return db.runQueryWith(ctx, plan, func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		ts := readTs
		if d.AsOf != 0 {
			ts = d.AsOf
		}
		return db.openSegmentsAt(d.Name, ts)
	})
}

func (db *DB) runQueryWith(ctx context.Context, plan *sql.Plan, resolveBase func(sql.BoundTableDef) ([]*storage.Segment, error)) (*Rows, error) {
	if plan != nil && plan.Kind == sql.PlanExplain {
		return db.runExplain(ctx, plan)
	}
	openSegs := make(map[string][]*storage.Segment)
	resolve := func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		key := schema.NormalizeName(d.Name)
		if segs, ok := openSegs[key]; ok {
			return segs, nil
		}
		segs, err := resolveBase(d)
		if err != nil {
			return nil, err
		}
		openSegs[key] = segs
		return segs, nil
	}
	op, err := exec.BuildOperator(plan, resolve)
	if err != nil {
		return nil, err
	}
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()

	rows := &Rows{Columns: planOutputNames(plan)}
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
		if rows.Columns == nil {
			rows.Columns = columnNames(batch)
		}
		if err := appendBatchRows(rows, batch); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func planOutputNames(plan *sql.Plan) []string {
	if plan == nil || plan.Rel == nil {
		return nil
	}
	out := make([]string, len(plan.Rel.Outputs))
	for i, o := range plan.Rel.Outputs {
		name := o.Alias
		if name == "" {
			name = o.Expr.Column
		}
		out[i] = name
	}
	return out
}

// openSegmentsForQuery resolves the table's segments using the DB-level cache. Callers must
// already hold db.mu so the cache + manifest mutation stays serialised with Close.
func (db *DB) openSegmentsForQuery(table string) ([]*storage.Segment, error) {
	return db.openSegmentsAt(table, ^uint64(0))
}

// openSegmentsAt resolves the table's segment view as of readTs. readTs of MaxUint64 (or any
// value >= nextCommitTs) gives the latest snapshot, preserving the pre-MVCC behavior.
// SnapshotAt picks the right DV path for each segment at the chosen timestamp, so deletes
// committed after readTs don't apply to this reader.
func (db *DB) openSegmentsAt(table string, readTs uint64) ([]*storage.Segment, error) {
	m, err := db.manifestFor(table)
	if err != nil {
		return nil, err
	}
	view := m.SnapshotAt(readTs)
	segs := make([]*storage.Segment, 0, len(view.Entries))
	working := make(map[segCacheKey]struct{}, len(view.Entries))
	for _, entry := range view.Entries {
		key := segCacheKey{path: entry.Path, dvPath: entry.DeletionVectorPath}
		working[key] = struct{}{}
		if elem, ok := db.segCache[key]; ok {
			db.segLRU.MoveToBack(elem)
			seg := elem.Value.(*segCacheEntry).seg
			// Cached *os.File is still valid after a manifest rewrite. CommitTs may differ
			// so refresh from the current entry.
			seg.CommitTs = entry.CommitTs
			segs = append(segs, seg)
			continue
		}
		seg, err := storage.OpenSegmentWithDV(entry.Path, entry.DeletionVectorPath)
		if err != nil {
			return nil, fmt.Errorf("engine: open segment %q: %w", entry.Path, err)
		}
		seg.CommitTs = entry.CommitTs
		ce := &segCacheEntry{key: key, seg: seg}
		db.segCache[key] = db.segLRU.PushBack(ce)
		segs = append(segs, seg)
	}
	db.evictColdSegments(working)
	return segs, nil
}

// evictColdSegments closes the oldest cached segments that are not in the current
// query's working set, until the cache fits under db.segCacheLimit. Segments needed
// by the in-progress query stay open. Called while db.mu is held.
func (db *DB) evictColdSegments(working map[segCacheKey]struct{}) {
	limit := db.segCacheLimit()
	for db.segLRU.Len() > limit {
		evicted := false
		for e := db.segLRU.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*segCacheEntry)
			if _, used := working[entry.key]; used {
				continue
			}
			_ = entry.seg.Close()
			db.segLRU.Remove(e)
			delete(db.segCache, entry.key)
			evicted = true
			break
		}
		if !evicted {
			return
		}
	}
}

func columnNames(batch vector.Batch) []string {
	names := make([]string, len(batch.Columns))
	for i, c := range batch.Columns {
		names[i] = c.Name
	}
	return names
}

func appendBatchRows(rows *Rows, batch vector.Batch) error {
	return vector.ForVisible(batch, func(row int) error {
		vals := make([]any, len(batch.Columns))
		for i, c := range batch.Columns {
			v, err := c.ValueAt(row)
			if err != nil {
				return err
			}
			vals[i] = v
		}
		rows.Values = append(rows.Values, vals)
		return nil
	})
}
