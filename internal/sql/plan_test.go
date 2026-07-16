// PlanCache tests covering hit, version miss, eviction at max, and no-op repeat-put.
package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func dummyPlan(name string) *Plan {
	return &Plan{Kind: PlanCreateTable, TableSpec: schema.TableSpec{Name: name}}
}

func TestPlanCache_HitAndMiss(t *testing.T) {
	c := NewPlanCache(8)
	if _, ok := c.Get("select 1", 1); ok {
		t.Fatal("empty cache returned hit")
	}
	plan := dummyPlan("t")
	c.Put("select 1", 1, plan)
	got, ok := c.Get("select 1", 1)
	if !ok || got != plan {
		t.Fatalf("Get after Put: ok=%v got=%v want=%v", ok, got, plan)
	}
}

func TestPlanCache_VersionMissDoesNotInvalidate(t *testing.T) {
	c := NewPlanCache(8)
	plan := dummyPlan("t")
	c.Put("select 1", 1, plan)
	if _, ok := c.Get("select 1", 2); ok {
		t.Fatal("stale-version Get should miss")
	}
	got, ok := c.Get("select 1", 1)
	if !ok || got != plan {
		t.Fatalf("original version still present: ok=%v got=%v", ok, got)
	}
}

func TestPlanCache_VersionBumpOverwrites(t *testing.T) {
	c := NewPlanCache(8)
	c.Put("select 1", 1, dummyPlan("v1"))
	newPlan := dummyPlan("v2")
	c.Put("select 1", 2, newPlan)
	got, ok := c.Get("select 1", 2)
	if !ok || got != newPlan {
		t.Fatalf("version-2 Put did not overwrite: ok=%v got=%v", ok, got)
	}
	if _, ok := c.Get("select 1", 1); ok {
		t.Fatal("version-1 entry still reachable")
	}
}

func TestPlanCache_EvictsAtMax(t *testing.T) {
	c := NewPlanCache(2)
	c.Put("a", 1, dummyPlan("a"))
	c.Put("b", 1, dummyPlan("b"))
	c.Put("c", 1, dummyPlan("c"))
	if c.Len() != 2 {
		t.Fatalf("Len after 3 puts with max=2: got %d, want 2", c.Len())
	}
}

func TestPlanCache_RepeatPutSameVersionNoop(t *testing.T) {
	c := NewPlanCache(8)
	plan := dummyPlan("t")
	c.Put("select 1", 1, plan)
	c.Put("select 1", 1, dummyPlan("other"))
	got, _ := c.Get("select 1", 1)
	if got != plan {
		t.Fatalf("repeat Put at same version replaced entry: got %v want %v", got, plan)
	}
}

func TestPlanCache_DefaultMax(t *testing.T) {
	c := NewPlanCache(0)
	if c.max != 256 {
		t.Fatalf("default max = %d, want 256", c.max)
	}
}
