package engine

import (
	"sync"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
)

// planCache memoizes parsed+bound query plans keyed by SQL text. Plans are
// immutable trees; the cache invalidates wholesale when db.version changes
// (CreateType/CreateTable bump it), so a stale plan can never reach Execute.
type planCache struct {
	mu      sync.Mutex
	entries map[string]planCacheEntry
}

type planCacheEntry struct {
	version v3sql.SchemaVersion
	plan    v3sql.Plan
}

const planCacheMax = 256

func newPlanCache() *planCache {
	return &planCache{entries: make(map[string]planCacheEntry)}
}

func (c *planCache) get(sqlText string, version v3sql.SchemaVersion) (v3sql.Plan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sqlText]
	if !ok || e.version != version {
		return nil, false
	}
	return e.plan, true
}

func (c *planCache) put(sqlText string, version v3sql.SchemaVersion, plan v3sql.Plan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= planCacheMax {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[sqlText] = planCacheEntry{version: version, plan: plan}
}
