// Query runner. Resolves segments via the DB-level handle cache, builds the operator chain, drains to Rows.
// Cached segments live until DB.Close; cache misses open lazily and are retained across queries.
package engine

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func (db *DB) runQuery(ctx context.Context, plan *sql.Plan, def sql.BoundTableDef) (*Rows, error) {
	openSegs := make(map[string][]*storage.Segment)
	resolve := func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		key := types.NormalizeName(d.Name)
		if segs, ok := openSegs[key]; ok {
			return segs, nil
		}
		segs, err := db.openSegmentsForQuery(d.Name)
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

func (db *DB) openSegmentsForQuery(table string) ([]*storage.Segment, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	m, err := db.manifestFor(table)
	if err != nil {
		return nil, err
	}
	view := m.Snapshot()
	segs := make([]*storage.Segment, 0, len(view.Entries))
	for _, entry := range view.Entries {
		key := segCacheKey{path: entry.Path, dvPath: entry.DeletionVectorPath}
		if seg, ok := db.segCache[key]; ok {
			segs = append(segs, seg)
			continue
		}
		seg, err := storage.OpenSegmentWithDV(entry.Path, entry.DeletionVectorPath)
		if err != nil {
			return nil, fmt.Errorf("engine: open segment %q: %w", entry.Path, err)
		}
		db.segCache[key] = seg
		segs = append(segs, seg)
	}
	return segs, nil
}

func columnNames(batch types.Batch) []string {
	names := make([]string, len(batch.Columns))
	for i, c := range batch.Columns {
		names[i] = c.Name
	}
	return names
}

func appendBatchRows(rows *Rows, batch types.Batch) error {
	rowValues := func(row int) ([]any, error) {
		out := make([]any, len(batch.Columns))
		for i, c := range batch.Columns {
			v, err := c.ValueAt(row)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	if batch.Sel == nil {
		for row := range batch.Len {
			vals, err := rowValues(row)
			if err != nil {
				return err
			}
			rows.Values = append(rows.Values, vals)
		}
		return nil
	}
	var loopErr error
	batch.Sel.IterSet(func(row int) {
		if loopErr != nil {
			return
		}
		vals, err := rowValues(row)
		if err != nil {
			loopErr = err
			return
		}
		rows.Values = append(rows.Values, vals)
	})
	return loopErr
}
