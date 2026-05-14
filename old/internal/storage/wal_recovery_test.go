package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoverNoopOnEmptyStore(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	report, err := store.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.TempSegmentsRemoved != 0 {
		t.Fatalf("report = %#v", report)
	}
}

func TestRecoverRemovesTempSegments(t *testing.T) {
	root := t.TempDir()
	store, err := Open(root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.AppendBatch(context.Background(), storeTableSpec(), segmentBatch(t, []int64{1}, []string{"signup"})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	tmpPath := filepath.Join(root, "tables", "events", "segments", "0000000000000099.dsv3.tmp")
	if err := os.WriteFile(tmpPath, []byte("partial"), 0o644); err != nil {
		t.Fatalf("WriteFile tmp: %v", err)
	}

	report, err := store.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if report.TempSegmentsRemoved != 1 {
		t.Fatalf("report = %#v", report)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("tmp file still exists or stat failed differently: %v", err)
	}
}
