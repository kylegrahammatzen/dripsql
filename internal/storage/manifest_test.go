package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestManifestIgnoresIncompleteFinalLine(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	manifestPath := filepath.Join(tableDir(store.root, table), manifestFileName)
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteString(`{"id":`); err != nil {
		_ = file.Close()
		t.Fatalf("WriteString: %v", err)
	}
	_ = file.Close()

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestManifestRejectsBadCompleteLine(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	manifestPath := filepath.Join(tableDir(store.root, table), manifestFileName)
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := file.WriteString("{\"id\":" + "\n"); err != nil {
		_ = file.Close()
		t.Fatalf("WriteString: %v", err)
	}
	_ = file.Close()

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	_, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err == nil {
		t.Fatal("expected manifest error")
	}
}
