// Contract tests for OpenReadOnly and Refresh, no disk writes, replica visibility, corrupt tail tolerance.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// listTree captures every file under root as relative path mapped to size and mtime.
func listTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out[rel] = fmt.Sprintf("%d|%d", info.Size(), info.ModTime().UnixNano())
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func TestOpenReadOnly_ReadsWorkMutationsFailDiskUntouched(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, w, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, w, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	before := listTree(t, dir)

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	ctx := context.Background()
	wantRows(t, mustValues(t, r, "SELECT id, name FROM t ORDER BY id"), [][]any{
		{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"},
	})

	if _, err := r.Exec(ctx, "INSERT INTO t (id, name) VALUES (9, 'x')"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Exec INSERT err = %v, want ErrReadOnly", err)
	}
	if _, err := r.Exec(ctx, "CREATE TABLE u (id int64 NOT NULL)"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Exec DDL err = %v, want ErrReadOnly", err)
	}
	if _, err := r.BeginTx(ctx); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("BeginTx err = %v, want ErrReadOnly", err)
	}
	v := vector.NewVec(vector.VecInt64, 1)
	v.I64()[0] = 9
	b, err := vector.NewBatch([]vector.Column{{Name: "id", Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Ingest(ctx, IngestConfig{Table: "t", Batches: []vector.Batch{b}}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Ingest err = %v, want ErrReadOnly", err)
	}
	if _, err := r.Compact(ctx, "t"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Compact err = %v, want ErrReadOnly", err)
	}
	if _, err := r.Vacuum(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Vacuum err = %v, want ErrReadOnly", err)
	}
	if _, err := r.VacuumRetention(1); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("VacuumRetention err = %v, want ErrReadOnly", err)
	}
	if err := r.SetReadOnly(false); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("SetReadOnly(false) err = %v, want ErrReadOnly", err)
	}

	wantRows(t, mustValues(t, r, "SELECT count(*) FROM t"), [][]any{{int64(3)}})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	after := listTree(t, dir)
	if len(before) != len(after) {
		t.Fatalf("file count changed, before %d after %d", len(before), len(after))
	}
	for rel, sig := range before {
		if after[rel] != sig {
			t.Fatalf("file %s changed, before %s after %s", rel, sig, after[rel])
		}
	}
}

func TestOpenReadOnly_RefreshSeesWriterCommits(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	mustExec(t, w, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, w, "INSERT INTO t (id) VALUES (1), (2)")

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	wantRows(t, mustValues(t, r, "SELECT id FROM t ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})

	mustExec(t, w, "INSERT INTO t (id) VALUES (3), (4), (5)")
	if err := w.Refresh(); err == nil {
		t.Fatal("Refresh on a writer DB must be refused")
	}
	// The reader keeps its snapshot until it refreshes.
	wantRows(t, mustValues(t, r, "SELECT id FROM t ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})
	if err := r.Refresh(); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	wantRows(t, mustValues(t, r, "SELECT id FROM t ORDER BY id"), [][]any{
		{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)}, {int64(5)},
	})
}

func TestOpenReadOnly_CorruptManifestTailServedAndUntouched(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, w, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, w, "INSERT INTO t (id) VALUES (1), (2)")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(dir, "tables", "0000000000000001", "manifest")
	f, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"version":9,"path":"junk"}` + "\t" + "12"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	infoBefore, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatal(err)
	}

	r, err := OpenReadOnly(dir)
	if err != nil {
		t.Fatalf("OpenReadOnly with corrupt tail: %v", err)
	}
	wantRows(t, mustValues(t, r, "SELECT id FROM t ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	infoAfter, err := os.Stat(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if infoAfter.Size() != infoBefore.Size() {
		t.Fatalf("read-only open changed manifest size from %d to %d", infoBefore.Size(), infoAfter.Size())
	}
}
