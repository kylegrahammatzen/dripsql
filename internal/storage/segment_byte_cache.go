package storage

import (
	"container/list"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const defaultSegmentByteCacheBytes = 256 << 20

type segmentReadKey struct {
	FileID uint64
	Offset int64
	Length int
}

// segmentFileIdentity hashes (path, size) so a path reused with a different
// payload (e.g. delete + rewrite) never aliases a stale cache entry.
func segmentFileIdentity(path string, size int64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	var sizeBuf [8]byte
	for i := 0; i < 8; i++ {
		sizeBuf[i] = byte(size >> (i * 8))
	}
	_, _ = h.Write(sizeBuf[:])
	return h.Sum64()
}

type SegmentByteCacheStats struct {
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	HitBytes  int64 `json:"hit_bytes"`
	MissBytes int64 `json:"miss_bytes"`
	Bytes     int64 `json:"bytes_resident"`
	MaxBytes  int64 `json:"max_bytes"`
	Entries   int   `json:"entries"`
}

type segmentByteCache struct {
	mu       sync.Mutex
	entries  map[segmentReadKey]*list.Element
	lru      *list.List
	bytes    int64
	maxBytes int64

	hits      atomic.Int64
	misses    atomic.Int64
	evictions atomic.Int64
	hitBytes  atomic.Int64
	missBytes atomic.Int64
}

type segmentByteCacheEntry struct {
	key     segmentReadKey
	payload []byte
}

func newSegmentByteCache(maxBytes int64) *segmentByteCache {
	return &segmentByteCache{
		entries:  make(map[segmentReadKey]*list.Element),
		lru:      list.New(),
		maxBytes: maxBytes,
	}
}

// Get returns the cached payload for key. The returned slice is owned by the
// cache; callers MUST treat it as read-only. A miss returns (nil, false).
func (c *segmentByteCache) Get(key segmentReadKey) ([]byte, bool) {
	if c == nil || c.maxBytes <= 0 {
		if c != nil {
			c.misses.Add(1)
		}
		return nil, false
	}
	c.mu.Lock()
	elem, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		c.misses.Add(1)
		return nil, false
	}
	c.lru.MoveToFront(elem)
	payload := elem.Value.(*segmentByteCacheEntry).payload
	c.mu.Unlock()
	c.hits.Add(1)
	c.hitBytes.Add(int64(len(payload)))
	return payload, true
}

// Put copies src into the cache so callers can safely reuse their buffer.
// Pages larger than maxBytes/4 are skipped to keep one giant page from
// evicting the entire working set.
func (c *segmentByteCache) Put(key segmentReadKey, src []byte) {
	if c == nil || c.maxBytes <= 0 {
		if c != nil {
			c.missBytes.Add(int64(len(src)))
		}
		return
	}
	c.missBytes.Add(int64(len(src)))
	if int64(len(src)) > c.maxBytes/2 {
		return
	}
	payload := make([]byte, len(src))
	copy(payload, src)

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[key]; ok {
		c.bytes -= int64(len(existing.Value.(*segmentByteCacheEntry).payload))
		c.lru.Remove(existing)
		delete(c.entries, key)
	}
	entry := &segmentByteCacheEntry{key: key, payload: payload}
	elem := c.lru.PushFront(entry)
	c.entries[key] = elem
	c.bytes += int64(len(payload))
	for c.bytes > c.maxBytes {
		victim := c.lru.Back()
		if victim == nil {
			break
		}
		c.lru.Remove(victim)
		ve := victim.Value.(*segmentByteCacheEntry)
		delete(c.entries, ve.key)
		c.bytes -= int64(len(ve.payload))
		c.evictions.Add(1)
	}
}

// Clear drops every cached entry and resets the resident-byte counter so the
// next reads behave as if the cache had just been instantiated. Hit/miss
// counters are intentionally preserved so bench harnesses can still compare
// cumulative cache effectiveness across samples.
func (c *segmentByteCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[segmentReadKey]*list.Element)
	c.lru.Init()
	c.bytes = 0
}

func (c *segmentByteCache) Stats() SegmentByteCacheStats {
	if c == nil {
		return SegmentByteCacheStats{}
	}
	c.mu.Lock()
	bytes := c.bytes
	entries := len(c.entries)
	maxBytes := c.maxBytes
	c.mu.Unlock()
	return SegmentByteCacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
		HitBytes:  c.hitBytes.Load(),
		MissBytes: c.missBytes.Load(),
		Bytes:     bytes,
		MaxBytes:  maxBytes,
		Entries:   entries,
	}
}
