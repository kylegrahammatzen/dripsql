// EXPLAIN emits the bound Rel tree as text rows via Rel.String. ANALYZE prefixes a
// placeholder header until per-operator timing wrappers land.
package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
)

func (db *DB) runExplain(ctx context.Context, plan *sql.Plan) (*Rows, error) {
	if plan == nil || plan.Inner == nil || plan.Inner.Rel == nil {
		return nil, fmt.Errorf("engine: EXPLAIN missing inner SELECT")
	}
	_ = ctx
	body := plan.Inner.Rel.String()
	if plan.Analyze {
		body = "ANALYZE: per-operator timings not yet implemented\n" + body
	}
	rows := &Rows{Columns: []string{"plan"}}
	for _, line := range strings.Split(body, "\n") {
		rows.Values = append(rows.Values, []any{line})
	}
	return rows, nil
}
