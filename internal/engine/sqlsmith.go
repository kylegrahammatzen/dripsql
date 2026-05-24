// SQLsmith-style random SELECT generator used by the fuzz harness.
// Only emits grammar dripsql supports today, so errors must be panics or false hits.
package engine

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

type smithSchema struct {
	table    string
	intCols  []string
	textCols []string
	enum     map[string][]string
}

func defaultSmithSchema() smithSchema {
	return smithSchema{
		table:    "fuzz_t",
		intCols:  []string{"id", "x", "y"},
		textCols: []string{"name", "cat"},
		enum:     map[string][]string{"cat": {"a", "b", "c", "d"}},
	}
}

func smithDDL(s smithSchema) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	b.WriteString(s.table)
	b.WriteString(" (")
	first := true
	for _, c := range s.intCols {
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, "%s int64 NOT NULL", c)
	}
	for _, c := range s.textCols {
		if !first {
			b.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&b, "%s text NOT NULL", c)
	}
	b.WriteString(")")
	return b.String()
}

func smithSeed(s smithSchema, rows int, r *rand.Rand) string {
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (", s.table)
	for i, c := range s.intCols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c)
	}
	for _, c := range s.textCols {
		b.WriteString(", ")
		b.WriteString(c)
	}
	b.WriteString(") VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('(')
		for j := range s.intCols {
			if j > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%d", r.IntN(1000)-500)
		}
		for _, c := range s.textCols {
			b.WriteString(", ")
			if vals, ok := s.enum[c]; ok {
				fmt.Fprintf(&b, "'%s'", vals[r.IntN(len(vals))])
			} else {
				fmt.Fprintf(&b, "'n%d'", r.IntN(100))
			}
		}
		b.WriteByte(')')
	}
	return b.String()
}

// SmithQuery emits a single random SELECT statement against the default schema.
// Caller decides whether failure of the query is a bug or expected grammar drift.
func SmithQuery(r *rand.Rand) string {
	s := defaultSmithSchema()
	return smithSelect(s, r, 0)
}

func smithSelect(s smithSchema, r *rand.Rand, depth int) string {
	var b strings.Builder
	b.WriteString("SELECT ")
	hasAgg := r.IntN(3) == 0 && depth == 0
	var projCols []string
	var groupKey string
	switch {
	case hasAgg:
		if r.IntN(2) == 0 {
			groupKey = pickAny(s, r)
			projCols = append(projCols, groupKey)
		}
		projCols = append(projCols, smithAgg(s, r, "s1"))
		if r.IntN(2) == 0 {
			projCols = append(projCols, smithAgg(s, r, "s2"))
		}
	default:
		n := 1 + r.IntN(3)
		for i := 0; i < n; i++ {
			projCols = append(projCols, pickAny(s, r))
		}
	}
	b.WriteString(strings.Join(projCols, ", "))
	fmt.Fprintf(&b, " FROM %s", s.table)
	if r.IntN(2) == 0 {
		b.WriteString(" WHERE ")
		b.WriteString(smithPredicate(s, r))
	}
	if hasAgg && groupKey != "" {
		fmt.Fprintf(&b, " GROUP BY %s", groupKey)
	}
	if !hasAgg && r.IntN(2) == 0 {
		fmt.Fprintf(&b, " ORDER BY %s %s", pickAny(s, r), pickDir(r))
		if r.IntN(2) == 0 {
			fmt.Fprintf(&b, " LIMIT %d", 1+r.IntN(20))
		}
	}
	return b.String()
}

func smithAgg(s smithSchema, r *rand.Rand, alias string) string {
	switch r.IntN(4) {
	case 0:
		return "count(*) AS " + alias
	case 1:
		return fmt.Sprintf("count(%s) AS %s", pickAny(s, r), alias)
	case 2:
		return fmt.Sprintf("sum(%s) AS %s", pickInt(s, r), alias)
	default:
		return fmt.Sprintf("avg(%s) AS %s", pickInt(s, r), alias)
	}
}

func smithPredicate(s smithSchema, r *rand.Rand) string {
	build := func() string {
		switch r.IntN(5) {
		case 0:
			return fmt.Sprintf("%s = %d", pickInt(s, r), r.IntN(100)-50)
		case 1:
			return fmt.Sprintf("%s < %d", pickInt(s, r), r.IntN(100))
		case 2:
			return fmt.Sprintf("%s >= %d", pickInt(s, r), r.IntN(100))
		case 3:
			return fmt.Sprintf("%s BETWEEN %d AND %d", pickInt(s, r), r.IntN(50), 50+r.IntN(50))
		default:
			c := pickText(s, r)
			if vals, ok := s.enum[c]; ok {
				return fmt.Sprintf("%s = '%s'", c, vals[r.IntN(len(vals))])
			}
			return fmt.Sprintf("%s = 'n%d'", c, r.IntN(100))
		}
	}
	p := build()
	if r.IntN(3) == 0 {
		p = p + " AND " + build()
	}
	return p
}

func pickAny(s smithSchema, r *rand.Rand) string {
	cols := append(append([]string{}, s.intCols...), s.textCols...)
	return cols[r.IntN(len(cols))]
}

func pickInt(s smithSchema, r *rand.Rand) string  { return s.intCols[r.IntN(len(s.intCols))] }
func pickText(s smithSchema, r *rand.Rand) string { return s.textCols[r.IntN(len(s.textCols))] }
func pickDir(r *rand.Rand) string {
	if r.IntN(2) == 0 {
		return "ASC"
	}
	return "DESC"
}
