package storage

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"sync"
)

const defaultMaxOpenSegmentFiles = 256

type segmentFileCache struct {
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	max    int
	files  map[string]*cachedSegmentFile
	lru    *list.List
}

type cachedSegmentFile struct {
	path string
	file *os.File
	refs int
	elem *list.Element
}

func newSegmentFileCache() *segmentFileCache {
	return newSegmentFileCacheWithMax(defaultMaxOpenSegmentFiles)
}

func newSegmentFileCacheWithMax(max int) *segmentFileCache {
	c := &segmentFileCache{max: max, files: make(map[string]*cachedSegmentFile), lru: list.New()}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// Acquire returns a cached read-only segment file.
// Callers must not close the returned file; the cache owns file lifetime.
// Every successful Acquire must be paired with Release for the same path.
func (c *segmentFileCache) Acquire(path string) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, fmt.Errorf("segment file cache is closed")
	}
	if entry := c.files[path]; entry != nil {
		entry.refs++
		c.lru.MoveToFront(entry.elem)
		return entry.file, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	entry := &cachedSegmentFile{path: path, file: file, refs: 1}
	entry.elem = c.lru.PushFront(entry)
	c.files[path] = entry
	// The new entry is acquired (refs=1), so eviction cannot close it immediately.
	if err := c.evictLocked(); err != nil {
		return nil, err
	}
	return file, nil
}

func (c *segmentFileCache) Release(path string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.files[path]
	if entry == nil {
		return nil
	}
	if entry.refs > 0 {
		entry.refs--
		if c.closed {
			// Close may be waiting for the last outstanding ref; wake it on every drop.
			c.cond.Broadcast()
		}
	}
	if c.closed {
		return nil
	}
	if len(c.files) <= c.max {
		return nil
	}
	return c.evictLocked()
}

func (c *segmentFileCache) evictLocked() error {
	if c.max <= 0 {
		return nil
	}
	var closeErr error
	for len(c.files) > c.max {
		var victim *cachedSegmentFile
		for elem := c.lru.Back(); elem != nil; elem = elem.Prev() {
			entry := elem.Value.(*cachedSegmentFile)
			if entry.refs == 0 {
				victim = entry
				break
			}
		}
		if victim == nil {
			return closeErr
		}
		if err := c.closeEntryLocked(victim); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

func (c *segmentFileCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	// Wait for in-flight readers to release. New Acquires already error after closed=true.
	for c.hasOutstandingRefsLocked() {
		c.cond.Wait()
	}
	var closeErr error
	for _, entry := range c.files {
		if err := c.closeEntryLocked(entry); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
	}
	return closeErr
}

func (c *segmentFileCache) hasOutstandingRefsLocked() bool {
	for _, entry := range c.files {
		if entry.refs > 0 {
			return true
		}
	}
	return false
}

func (c *segmentFileCache) closeEntryLocked(entry *cachedSegmentFile) error {
	if entry.elem != nil {
		c.lru.Remove(entry.elem)
		entry.elem = nil
	}
	delete(c.files, entry.path)
	file := entry.file
	entry.file = nil
	if file == nil {
		return nil
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close segment file %q: %w", entry.path, err)
	}
	return nil
}

func (c *segmentFileCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.files)
}
