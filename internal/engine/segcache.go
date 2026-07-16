// LRU-bounded cache of open segment file handles keyed by manifest path plus DV path.
package engine

import (
	"container/list"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

type segCacheKey struct {
	path   string
	dvPath string
}

type segCacheEntry struct {
	key segCacheKey
	seg *storage.Segment
}

type segmentCache struct {
	byKey   map[segCacheKey]*list.Element
	lru     *list.List
	limitFn func() int
}

// touch returns the cached segment and moves the entry to the back of the LRU.
func (c *segmentCache) touch(key segCacheKey) (*storage.Segment, bool) {
	elem, ok := c.byKey[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToBack(elem)
	return elem.Value.(*segCacheEntry).seg, true
}

func (c *segmentCache) add(key segCacheKey, seg *storage.Segment) {
	c.byKey[key] = c.lru.PushBack(&segCacheEntry{key: key, seg: seg})
}

// evictExcept closes cold entries not present in working until the cache fits the configured limit.
func (c *segmentCache) evictExcept(working map[segCacheKey]struct{}) {
	limit := c.limitFn()
	for c.lru.Len() > limit {
		evicted := false
		for e := c.lru.Front(); e != nil; e = e.Next() {
			entry := e.Value.(*segCacheEntry)
			if _, used := working[entry.key]; used {
				continue
			}
			_ = entry.seg.Close()
			c.lru.Remove(e)
			delete(c.byKey, entry.key)
			evicted = true
			break
		}
		if !evicted {
			return
		}
	}
}

// removeByPath drops every cached entry sharing the given segment path so compacted segments cannot be reopened by stale path keys.
func (c *segmentCache) removeByPath(path string) {
	for k, elem := range c.byKey {
		if k.path != path {
			continue
		}
		_ = elem.Value.(*segCacheEntry).seg.Close()
		c.lru.Remove(elem)
		delete(c.byKey, k)
	}
}

// close releases every cached segment and reports the first error encountered.
func (c *segmentCache) close() error {
	var firstErr error
	for e := c.lru.Front(); e != nil; e = e.Next() {
		if err := e.Value.(*segCacheEntry).seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.byKey = nil
	c.lru = nil
	return firstErr
}
