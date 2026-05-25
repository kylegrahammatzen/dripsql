// EXPLAIN emits the bound Rel tree as text rows via Rel.String. ANALYZE additionally
// runs the inner plan with timing wrappers and appends wall + rows + calls per operator.
package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/exec"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

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
	openSegs := make(map[string][]*storage.Segment)
	resolve := func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		key := schema.NormalizeName(d.Name)
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
