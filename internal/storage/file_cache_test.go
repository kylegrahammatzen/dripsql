package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSegmentFileCacheEvictsLeastRecentlyUsedFile(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		writeCacheTestFile(t, dir, "a.dseg"),
		writeCacheTestFile(t, dir, "b.dseg"),
		writeCacheTestFile(t, dir, "c.dseg"),
	}
	cache := newSegmentFileCacheWithMax(2)
	defer cache.Close()

	fileA, err := cache.Acquire(paths[0])
	if err != nil {
		t.Fatalf("Acquire a: %v", err)
	}
	if err := cache.Release(paths[0]); err != nil {
		t.Fatalf("Release a: %v", err)
	}
	fileB, err := cache.Acquire(paths[1])
	if err != nil {
		t.Fatalf("Acquire b: %v", err)
	}
	if err := cache.Release(paths[1]); err != nil {
		t.Fatalf("Release b: %v", err)
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("cache len after initial releases = %d, want 2", got)
	}

	fileA2, err := cache.Acquire(paths[0])
	if err != nil {
		t.Fatalf("Acquire a again: %v", err)
	}
	if fileA2 != fileA {
		t.Fatalf("expected cached a file handle")
	}
	if err := cache.Release(paths[0]); err != nil {
		t.Fatalf("Release a again: %v", err)
	}

	fileC, err := cache.Acquire(paths[2])
	if err != nil {
		t.Fatalf("Acquire c: %v", err)
	}
	if err := cache.Release(paths[2]); err != nil {
		t.Fatalf("Release c: %v", err)
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("cache len = %d, want 2", got)
	}

	fileA3, err := cache.Acquire(paths[0])
	if err != nil {
		t.Fatalf("Acquire a after c: %v", err)
	}
	if fileA3 != fileA {
		t.Fatalf("expected a file handle to survive LRU eviction")
	}
	if err := cache.Release(paths[0]); err != nil {
		t.Fatalf("Release a after c: %v", err)
	}

	fileB2, err := cache.Acquire(paths[1])
	if err != nil {
		t.Fatalf("Acquire b after eviction: %v", err)
	}
	if fileB2 == fileB {
		t.Fatalf("expected b file handle to be reopened after LRU eviction")
	}
	if fileB2 == fileC {
		t.Fatalf("expected c file handle to remain distinct from reopened b")
	}
	if err := cache.Release(paths[1]); err != nil {
		t.Fatalf("Release b reopened: %v", err)
	}
}

func TestSegmentFileCacheDoesNotEvictAcquiredFiles(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		writeCacheTestFile(t, dir, "a.dseg"),
		writeCacheTestFile(t, dir, "b.dseg"),
		writeCacheTestFile(t, dir, "c.dseg"),
	}
	cache := newSegmentFileCacheWithMax(1)
	defer cache.Close()

	_, err := cache.Acquire(paths[0])
	if err != nil {
		t.Fatalf("Acquire a: %v", err)
	}
	_, err = cache.Acquire(paths[1])
	if err != nil {
		t.Fatalf("Acquire b: %v", err)
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("cache len with acquired files = %d, want 2", got)
	}
	// Releasing a lets eviction remove it; b stays because it is still acquired.
	if err := cache.Release(paths[0]); err != nil {
		t.Fatalf("Release a: %v", err)
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("cache len after releasing a = %d, want 1", got)
	}
	if err := cache.Release(paths[1]); err != nil {
		t.Fatalf("Release b: %v", err)
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("cache len after releasing b = %d, want 1", got)
	}

	_, err = cache.Acquire(paths[2])
	if err != nil {
		t.Fatalf("Acquire c: %v", err)
	}
	if err := cache.Release(paths[2]); err != nil {
		t.Fatalf("Release c: %v", err)
	}
	if got := cache.Len(); got != 1 {
		t.Fatalf("cache len after acquiring c = %d, want 1", got)
	}
}

func writeCacheTestFile(t *testing.T, dir string, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}
