package storage

import (
	"os"
	"sync"
)

// defaultSegmentFileCacheMax bounds how many segment file handles the store
// keeps open. The previous value of 256 thrashed on workloads where a single
// query touched more segments than that (e.g. tenant_id=42 on a sort-by-
// tenant_id table at 100M rows lands in every segment); each subsequent
// query re-opened the evicted handles via os.Open, which showed up as ~64%
// of warm-path CPU on Windows. 4096 covers the workloads we benchmark and
// is well inside OS open-file limits.
const defaultSegmentFileCacheMax = 4096

type segmentFileCache struct {
	mu     sync.Mutex
	files  map[string]*segmentFileCacheEntry
	order  []string
	max    int
	hits   int
	misses int
	closed bool
}

type segmentFileCacheEntry struct {
	file    *os.File
	refs    int
	evicted bool
	closed  bool
}

func newSegmentFileCache() *segmentFileCache {
	return &segmentFileCache{files: make(map[string]*segmentFileCacheEntry), max: defaultSegmentFileCacheMax}
}

func (c *segmentFileCache) Open(path string) (*os.File, func(), error) {
	if c == nil {
		file, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		return file, func() { _ = file.Close() }, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, os.ErrClosed
	}
	if entry := c.files[path]; entry != nil {
		entry.refs++
		c.touchLocked(path)
		c.hits++
		return entry.file, func() { c.release(path, entry) }, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	entry := &segmentFileCacheEntry{file: file, refs: 1}
	c.files[path] = entry
	c.order = append(c.order, path)
	c.misses++
	c.evictLocked()
	return file, func() { c.release(path, entry) }, nil
}

// Prewarm opens path and parks it in the cache with refs=0 so the next Open
// is a hit. Errors are swallowed: an open failure here just means the next
// real Open will pay the cost. Safe to call from a hot path.
func (c *segmentFileCache) Prewarm(path string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.files[path] != nil {
		return
	}
	file, err := os.Open(path)
	if err != nil {
		return
	}
	c.files[path] = &segmentFileCacheEntry{file: file, refs: 0}
	c.order = append(c.order, path)
	c.evictLocked()
}

func (c *segmentFileCache) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var firstErr error
	for path, entry := range c.files {
		entry.evicted = true
		if !entry.closed {
			entry.closed = true
			if err := entry.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(c.files, path)
	}
	c.order = nil
	return firstErr
}

func (c *segmentFileCache) release(_ string, entry *segmentFileCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.refs > 0 {
		entry.refs--
	}
	if entry.refs == 0 && entry.evicted && !entry.closed {
		entry.closed = true
		_ = entry.file.Close()
		return
	}
	if entry.refs == 0 && !c.closed {
		c.evictLocked()
	}
}

func (c *segmentFileCache) evictLocked() {
	if c.max <= 0 {
		return
	}
	scanned := 0
	for len(c.files) > c.max && scanned < len(c.order) {
		path := c.order[0]
		c.order = c.order[1:]
		entry := c.files[path]
		if entry == nil {
			continue
		}
		if entry.refs != 0 {
			c.order = append(c.order, path)
			scanned++
			continue
		}
		entry.evicted = true
		delete(c.files, path)
		if !entry.closed {
			entry.closed = true
			_ = entry.file.Close()
		}
		scanned = 0
	}
}

func (c *segmentFileCache) touchLocked(path string) {
	for i, cachedPath := range c.order {
		if cachedPath != path {
			continue
		}
		copy(c.order[i:], c.order[i+1:])
		c.order[len(c.order)-1] = path
		return
	}
	c.order = append(c.order, path)
}
