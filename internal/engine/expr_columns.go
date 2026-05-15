// Shared expression helpers used by DML and (later) scan-projection pruning to discover
// which columns a BoundExpr references. addExprColumns mutates the caller's set so update
// and delete avoid the sort+slice round-trip; exprColumnNames keeps a sorted wrapper for
// callers that genuinely need deterministic order.
package engine

import (
	"sort"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func addExprColumns(seen map[string]struct{}, expr sql.BoundExpr) {
	if expr.Op == sql.ExprColumn {
		seen[types.NormalizeName(expr.Column)] = struct{}{}
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
