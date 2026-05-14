package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentFileCacheHits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	cache := newSegmentFileCache()
	first, releaseFirst, err := cache.Open(path)
	if err != nil {
		t.Fatalf("Open first: %v", err)
	}
	defer releaseFirst()
	second, releaseSecond, err := cache.Open(path)
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	defer releaseSecond()
	if first != second {
		t.Fatal("cache returned different file handles")
	}
	if cache.misses != 1 || cache.hits != 1 {
		t.Fatalf("cache hits/misses = %d/%d, want 1/1", cache.hits, cache.misses)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(cache.files) != 0 {
		t.Fatalf("cache files after close = %d, want 0", len(cache.files))
	}
}

func TestSegmentFileCacheEvictsLRU(t *testing.T) {
	dir := t.TempDir()
	paths := make([]string, 3)
	for i := range paths {
		paths[i] = filepath.Join(dir, string(rune('a'+i))+".dsv3")
		if err := os.WriteFile(paths[i], []byte("segment"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	cache := newSegmentFileCache()
	defer func() {
		if err := cache.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}()
	cache.max = 2
	for _, path := range paths {
		_, release, err := cache.Open(path)
		if err != nil {
			t.Fatalf("Open %q: %v", path, err)
		}
		release()
	}
	if len(cache.files) != 2 {
		t.Fatalf("cache files = %d, want 2", len(cache.files))
	}
	if _, ok := cache.files[paths[0]]; ok {
		t.Fatalf("least recently used path was not evicted")
	}
	_, release, err := cache.Open(paths[0])
	if err != nil {
		t.Fatalf("Reopen evicted path: %v", err)
	}
	release()
	if cache.misses != 4 {
		t.Fatalf("misses = %d, want 4", cache.misses)
	}
}
