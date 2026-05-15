// Shared expression helpers used by DML and (later) scan-projection pruning to discover
// which columns a BoundExpr references. Output is sorted so callers get deterministic order.
package engine

import (
	"sort"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func exprColumnNames(expr sql.BoundExpr) []string {
	seen := make(map[string]struct{})
	var walk func(e sql.BoundExpr)
	walk = func(e sql.BoundExpr) {
		if e.Op == sql.ExprColumn {
			seen[types.NormalizeName(e.Column)] = struct{}{}
			return
		}
		for _, a := range e.Args {
			walk(a)
		}
	}
	walk(expr)
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
