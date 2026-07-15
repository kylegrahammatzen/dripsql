// Query runner plus EXPLAIN driver. Resolves segments via the DB-level handle cache, builds the operator chain, drains to Rows.
// EXPLAIN emits the bound Rel tree as text and EXPLAIN ANALYZE wraps the inner plan with timing wrappers.
package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"

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
	op, err := exec.BuildOperator(plan, cachedSegmentResolver(resolveBase))
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
		if seg, ok := db.segments.touch(key); ok {
			// CommitTs may differ after a manifest rewrite even when the file handle is still valid.
			seg.CommitTs = entry.CommitTs
			segs = append(segs, seg)
			continue
		}
		seg, err := storage.OpenSegmentWithDV(entry.Path, entry.DeletionVectorPath)
		if err != nil {
			return nil, fmt.Errorf("engine: open segment %q: %w", entry.Path, err)
		}
		seg.CommitTs = entry.CommitTs
		db.segments.add(key, seg)
		segs = append(segs, seg)
	}
	db.segments.evictExcept(working)
	return segs, nil
}

func columnNames(batch vector.Batch) []string {
	names := make([]string, len(batch.Columns))
	for i, c := range batch.Columns {
		names[i] = c.Name
	}
	return names
}

// cachedSegmentResolver wraps base in a per-call cache so a multi-Rel plan does not reopen the same table twice.
func cachedSegmentResolver(base func(sql.BoundTableDef) ([]*storage.Segment, error)) exec.SegmentsFn {
	opened := make(map[string][]*storage.Segment)
	return func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		key := schema.NormalizeName(d.Name)
		if segs, ok := opened[key]; ok {
			return segs, nil
		}
		segs, err := base(d)
		if err != nil {
			return nil, err
		}
		opened[key] = segs
		return segs, nil
	}
}

// appendBatchRows fills one arena per batch through per-column typed loops so wide
// results skip the per-cell kind switch and the per-row slice allocation.
func appendBatchRows(rows *Rows, batch vector.Batch) error {
	n := batch.Len
	if batch.Sel != nil {
		n = batch.Sel.PopCount()
	}
	if n == 0 {
		return nil
	}
	stride := len(batch.Columns)
	arena := make([]any, n*stride)
	for ci := range batch.Columns {
		if err := fillColumnValues(&batch.Columns[ci], batch, arena, ci, stride); err != nil {
			return err
		}
	}
	rows.Values = slices.Grow(rows.Values, n)
	for r := range n {
		rows.Values = append(rows.Values, arena[r*stride:(r+1)*stride:(r+1)*stride])
	}
	return nil
}

func fillColumnValues(c *vector.Column, batch vector.Batch, arena []any, offset, stride int) error {
	valid := c.V.Valid
	idx := offset
	switch c.V.Kind {
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		src := c.V.I64()
		return vector.ForVisible(batch, func(row int) error {
			if valid == nil || valid.IsValid(row) {
				arena[idx] = src[row]
			}
			idx += stride
			return nil
		})
	case vector.VecFloat64:
		src := c.V.F64()
		return vector.ForVisible(batch, func(row int) error {
			if valid == nil || valid.IsValid(row) {
				arena[idx] = src[row]
			}
			idx += stride
			return nil
		})
	case vector.VecText, vector.VecBytes, vector.VecJSON:
		src := c.V.Var()
		return vector.ForVisible(batch, func(row int) error {
			if valid == nil || valid.IsValid(row) {
				arena[idx] = src.String(row)
			}
			idx += stride
			return nil
		})
	}
	return vector.ForVisible(batch, func(row int) error {
		v, err := c.ValueAt(row)
		if err != nil {
			return err
		}
		arena[idx] = v
		idx += stride
		return nil
	})
}

func (db *DB) runExplain(ctx context.Context, plan *sql.Plan) (*Rows, error) {
	if plan == nil || plan.Inner == nil || plan.Inner.Rel == nil {
		return nil, fmt.Errorf("engine: EXPLAIN missing inner SELECT")
	}
	body := plan.Inner.Rel.String()
	rows := &Rows{Columns: []string{"plan"}}
	for line := range strings.SplitSeq(body, "\n") {
		rows.Values = append(rows.Values, []any{line})
	}
	if !plan.Analyze {
		return rows, nil
	}
	root, err := db.runAnalyze(ctx, plan.Inner)
	if err != nil {
		return nil, err
	}
	rows.Values = append(rows.Values, []any{""})
	rows.Values = append(rows.Values, []any{"ANALYZE timings:"})
	for line := range strings.SplitSeq(strings.TrimRight(root.Tree(), "\n"), "\n") {
		rows.Values = append(rows.Values, []any{line})
	}
	return rows, nil
}

func (db *DB) runAnalyze(ctx context.Context, plan *sql.Plan) (*exec.TimingStats, error) {
	resolve := cachedSegmentResolver(func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		return db.openSegmentsForQuery(d.Name)
	})
	op, root, err := exec.BuildOperatorAnalyzed(plan, resolve)
	if err != nil {
		return nil, err
	}
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, ok, err := op.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
	}
	return root, nil
}
