// User-declared codec end-to-end: CREATE TABLE with WITH (codec=...) per column should
// force that codec on write. Reading the segment back through the engine round-trips
// values regardless of the override.
package engine

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestEngine_UserCodec_PlainForcedOnDictionaryFriendlyColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64, label text WITH (codec = 'plain'))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1, 'alpha'),(2, 'alpha'),(3, 'beta')"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT id, label FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 3 {
		t.Fatalf("got %d rows want 3", len(rows.Values))
	}

	segs, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("got %d segments want 1", len(segs))
	}
	seg := segs[0]
	labelCol := -1
	for i := range seg.Cols {
		if seg.Cols[i].Name == "label" {
			labelCol = i
			break
		}
	}
	if labelCol < 0 {
		t.Fatal("label column not in segment")
	}
	page := seg.Cols[labelCol].Pages[0]
	got, ok := schema.EncodingFromWire(page.Encoding)
	if !ok {
		t.Fatalf("page encoding %d unknown", page.Encoding)
	}
	if got != schema.EncodingFlat {
		t.Fatalf("label codec = %v, want EncodingFlat (plain)", got)
	}
	_ = storage.MagicLen
}

func TestEngine_UserCodec_UnknownCodecRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE t (id int64 WITH (codec = 'nonsense'))")
	if err == nil {
		t.Fatal("expected error on unknown codec, got nil")
	}
}
