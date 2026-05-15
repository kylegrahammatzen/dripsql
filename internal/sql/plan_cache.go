// PlanCache memoizes bound Plans keyed by SQL text plus the catalog SchemaVersion
// they were bound against. Stale-version hits miss so DDL invalidates wholesale.
package sql

import "sync"

type PlanCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]planCacheEntry
}

type planCacheEntry struct {
	version SchemaVersion
	plan    *Plan
}

func NewPlanCache(max int) *PlanCache {
	if max <= 0 {
		max = 256
	}
	return &PlanCache{max: max, entries: make(map[string]planCacheEntry)}
}

func (c *PlanCache) Get(sql string, version SchemaVersion) (*Plan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sql]
	if !ok || e.version != version {
		return nil, false
	}
	return e.plan, true
}

func (c *PlanCache) Put(sql string, version SchemaVersion, plan *Plan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[sql]; ok && existing.version == version {
		return
	}
	if len(c.entries) >= c.max {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[sql] = planCacheEntry{version: version, plan: plan}
}

func (c *PlanCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
