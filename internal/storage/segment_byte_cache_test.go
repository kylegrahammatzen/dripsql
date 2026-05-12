package storage

import (
	"bytes"
	"sync"
	"testing"
)

func key(off int64, n int) segmentReadKey {
	return segmentReadKey{FileID: 1, Offset: off, Length: n}
}

func TestSegmentByteCacheGetAfterPut(t *testing.T) {
	c := newSegmentByteCache(1 << 20)
	src := []byte("hello world")
	c.Put(key(0, len(src)), src)
	got, ok := c.Get(key(0, len(src)))
	if !ok {
		t.Fatal("Get returned !ok after Put")
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("Get = %q, want %q", got, src)
	}
}

func TestSegmentByteCacheMissUnknown(t *testing.T) {
	c := newSegmentByteCache(1 << 20)
	if got, ok := c.Get(key(99, 4)); ok {
		t.Fatalf("Get on unknown key returned %q, ok=true", got)
	}
}

func TestSegmentByteCacheCopyOnPut(t *testing.T) {
	c := newSegmentByteCache(1 << 20)
	src := []byte("abcdef")
	c.Put(key(0, len(src)), src)
	src[0] = 'Z'
	got, ok := c.Get(key(0, len(src)))
	if !ok {
		t.Fatal("Get returned !ok")
	}
	if got[0] != 'a' {
		t.Fatalf("cache poisoned by post-Put mutation: got %q", got)
	}
}

func TestSegmentByteCacheLRURefresh(t *testing.T) {
	c := newSegmentByteCache(30)
	c.Put(key(0, 10), bytes.Repeat([]byte("a"), 10))
	c.Put(key(1, 10), bytes.Repeat([]byte("b"), 10))
	c.Put(key(2, 10), bytes.Repeat([]byte("c"), 10))
	if _, ok := c.Get(key(0, 10)); !ok {
		t.Fatal("expected key 0 still resident")
	}
	c.Put(key(3, 10), bytes.Repeat([]byte("d"), 10))
	if _, ok := c.Get(key(1, 10)); ok {
		t.Fatal("expected key 1 evicted (was the LRU after touching 0)")
	}
	if _, ok := c.Get(key(0, 10)); !ok {
		t.Fatal("key 0 should survive due to recent Get")
	}
}

func TestSegmentByteCacheEvictsUnderBudget(t *testing.T) {
	c := newSegmentByteCache(25)
	c.Put(key(0, 10), bytes.Repeat([]byte("a"), 10))
	c.Put(key(1, 10), bytes.Repeat([]byte("b"), 10))
	c.Put(key(2, 10), bytes.Repeat([]byte("c"), 10))
	stats := c.Stats()
	if stats.Bytes > 25 {
		t.Fatalf("bytes resident %d exceeds budget %d", stats.Bytes, c.maxBytes)
	}
	if stats.Evictions == 0 {
		t.Fatal("expected at least one eviction")
	}
	if _, ok := c.Get(key(0, 10)); ok {
		t.Fatal("expected key 0 evicted as LRU")
	}
}

func TestSegmentByteCacheOversizeSkipped(t *testing.T) {
	c := newSegmentByteCache(40)
	huge := bytes.Repeat([]byte("x"), 25)
	c.Put(key(0, len(huge)), huge)
	if _, ok := c.Get(key(0, len(huge))); ok {
		t.Fatal("oversize entry should not be cached")
	}
	if c.Stats().Bytes != 0 {
		t.Fatalf("bytes resident = %d, expected 0 after oversize skip", c.Stats().Bytes)
	}
}

func TestSegmentByteCacheDisabled(t *testing.T) {
	c := newSegmentByteCache(0)
	c.Put(key(0, 4), []byte("data"))
	if _, ok := c.Get(key(0, 4)); ok {
		t.Fatal("disabled cache returned a hit")
	}
	if c.Stats().Bytes != 0 {
		t.Fatalf("disabled cache holds %d bytes", c.Stats().Bytes)
	}
}

func TestSegmentByteCacheStatsCounters(t *testing.T) {
	c := newSegmentByteCache(1 << 20)
	c.Get(key(0, 4))
	c.Put(key(0, 4), []byte("abcd"))
	c.Get(key(0, 4))
	c.Get(key(0, 4))
	stats := c.Stats()
	if stats.Hits != 2 {
		t.Fatalf("hits = %d, want 2", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Fatalf("misses = %d, want 1", stats.Misses)
	}
	if stats.HitBytes != 8 {
		t.Fatalf("hit bytes = %d, want 8", stats.HitBytes)
	}
	if stats.MissBytes != 4 {
		t.Fatalf("miss bytes = %d, want 4", stats.MissBytes)
	}
}

func TestSegmentByteCacheConcurrent(t *testing.T) {
	c := newSegmentByteCache(64 << 10)
	const writers = 8
	const ops = 500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := segmentReadKey{FileID: uint64(w), Offset: int64(i % 16), Length: 64} //nolint:gosec
				payload := bytes.Repeat([]byte{byte(w)}, 64)
				c.Put(k, payload)
				c.Get(k)
			}
		}(w)
	}
	wg.Wait()
	stats := c.Stats()
	if stats.Bytes > 64<<10 {
		t.Fatalf("bytes resident %d exceeds budget", stats.Bytes)
	}
}
