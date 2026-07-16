// Planner microbenches where the cold path covers parse plus bind plus plan and the warm path covers only the cache hit.
// Targets a sub-millisecond cold plan and a near-noise warm path.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func planBenchDef() BoundTableDef {
	return BoundTableDef{
		Name: "users",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: schema.Int64},
			{ID: 2, Name: "name", Type: schema.Text},
			{ID: 3, Name: "age", Type: schema.Int64},
			{ID: 4, Name: "category", Type: schema.Text},
		},
	}
}

func planBenchResolver(def BoundTableDef) TableResolver {
	return func(name string) (BoundTableDef, error) {
		if schema.NormalizeName(name) != schema.NormalizeName(def.Name) {
			return BoundTableDef{}, nil
		}
		return def, nil
	}
}

func runPlanCold(b *testing.B, sqlText string) {
	def := planBenchDef()
	planner := NewPlanner(planBenchResolver(def))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		stmt, err := ParseOne(sqlText)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := planner.Plan(stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanCold_TopAge(b *testing.B) {
	runPlanCold(b, "SELECT id, age FROM users ORDER BY age DESC LIMIT 10")
}

func BenchmarkPlanCold_CatEq(b *testing.B) {
	runPlanCold(b, "SELECT id FROM users WHERE category = 'alpha'")
}

func BenchmarkPlanCold_CategoryGroupby(b *testing.B) {
	runPlanCold(b, "SELECT category, count(id), sum(age) FROM users GROUP BY category")
}

func BenchmarkPlanWarm_TopAge(b *testing.B) {
	def := planBenchDef()
	planner := NewPlanner(planBenchResolver(def))
	cache := NewPlanCache(256)
	const sqlText = "SELECT id, age FROM users ORDER BY age DESC LIMIT 10"
	stmt, err := ParseOne(sqlText)
	if err != nil {
		b.Fatal(err)
	}
	plan, err := planner.Plan(stmt)
	if err != nil {
		b.Fatal(err)
	}
	const version SchemaVersion = 1
	cache.Put(sqlText, version, plan)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, ok := cache.Get(sqlText, version); !ok {
			b.Fatal("cache miss")
		}
	}
}
