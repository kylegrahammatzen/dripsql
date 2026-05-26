// Compact migrates legacy dsv4 segments (written before the identity sidecar landed)
// to identity-stamped segments so future schema evolution survives a snapshot reader.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestEngine_Compact_MigratesLegacySegments(t *testing.T) {
	dir := t.TempDir()
	tableDir := filepath.Join(dir, "segments", "users")
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog.json"), []byte(v1UsersCatalog), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write a true legacy segment without an identity sidecar, then publish it via
	// the manifest. This is the on-disk shape a database upgraded from v1 would
	// carry until Compact migrates it.
	legacyPath := filepath.Join(tableDir, "999000.dsv4")
	idVec := vector.NewVec(vector.VecInt64, 3)
	for i, v := range []int64{1, 2, 3} {
		idVec.I64()[i] = v
	}
	nameVec := vector.NewVarVec(vector.VecText, 3, 0)
	nb := nameVec.Var()
	for i, s := range []string{"a", "b", "c"} {
		nb.AppendString(i, s)
	}
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: idVec},
		{Name: "name", Type: schema.Text, V: nameVec},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.WriteSegment(legacyPath, []vector.Batch{batch}, nil); err != nil {
		t.Fatal(err)
	}

	manifest, err := storage.OpenManifest(filepath.Join(tableDir, "manifest"))
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Commit(1, []storage.ManifestSegmentAdd{{Path: legacyPath, Rows: 3}}, nil); err != nil {
		manifest.Close()
		t.Fatal(err)
	}
	manifest.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	seg, err := storage.OpenSegment(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if seg.TableID != 0 {
		seg.Close()
		t.Fatal("test setup expected a legacy segment with TableID == 0")
	}
	seg.Close()

	if _, err := db.Compact(ctx, "users"); err != nil {
		t.Fatalf("compact: %v", err)
	}

	m, err := db.manifestFor("users")
	if err != nil {
		t.Fatal(err)
	}
	migrated := 0
	for _, e := range m.Snapshot().Entries {
		if e.Path == "" || e.DeletionVectorPath != "" {
			continue
		}
		s, err := storage.OpenSegment(e.Path)
		if err != nil {
			t.Fatal(err)
		}
		if s.TableID == 0 {
			s.Close()
			t.Fatalf("active segment %s still legacy after compact", e.Path)
		}
		s.Close()
		migrated++
	}
	if migrated == 0 {
		t.Fatal("expected at least one active segment after compact")
	}

	rows := mustValues(t, db, "SELECT id, name FROM users ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	})
}
