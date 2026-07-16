// INSERT path tests, page-capped chunking, temporal literals, and identity stamping on segments and manifests.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestEngine_Insert_ManyRowsChunkIntoPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, tag text)"); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, tag) VALUES ")
	for i := range 3000 {
		if i > 0 {
			b.WriteString(",")
		}
		if i%7 == 0 {
			fmt.Fprintf(&b, "(%d, NULL)", i)
		} else {
			fmt.Fprintf(&b, "(%d, 'tag-%d')", i, i%13)
		}
	}
	res, err := db.Exec(ctx, b.String())
	if err != nil {
		t.Fatalf("insert 3000 rows: %v", err)
	}
	if res.RowsAffected != 3000 {
		t.Fatalf("rows affected = %d, want 3000", res.RowsAffected)
	}
	wantRows(t, mustValues(t, db, "SELECT count(*), sum(id) FROM t"), [][]any{{int64(3000), int64(4498500)}})
	// 429 of the 3000 tags are NULL so a null-skipping count(tag) proves validity survives chunking.
	wantRows(t, mustValues(t, db, "SELECT count(tag) FROM t"), [][]any{{int64(2571)}})
	wantRows(t, mustValues(t, db, "SELECT tag FROM t WHERE id = 2996"), [][]any{{nil}})
	wantRows(t, mustValues(t, db, "SELECT tag FROM t WHERE id = 2999"), [][]any{{"tag-9"}})
	shapes := activeSegmentShapes(t, db, "t")
	if len(shapes) != 1 {
		t.Fatalf("active segments = %d, want 1", len(shapes))
	}
	if shapes[0].cols != 2 {
		t.Fatalf("segment columns = %d, want 2", shapes[0].cols)
	}
	if got := shapes[0].pageRows; len(got) != 2 || got[0] != 2048 || got[1] != 952 {
		t.Fatalf("page rows = %v, want [2048 952]", got)
	}
}

func TestEngine_TemporalLiteralsWriteThroughSQL(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE ev (id int64 NOT NULL, d date, ts timestamp, tm time)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO ev (id, d, ts, tm) VALUES (1, '2024-03-05', '2024-03-05T12:30:45Z', '01:02:03'), (2, NULL, NULL, NULL)"); err != nil {
		t.Fatalf("temporal insert: %v", err)
	}
	day := func(s string) int64 {
		tv, err := time.Parse("2006-01-02", s)
		if err != nil {
			t.Fatal(err)
		}
		return tv.Unix() / 86400
	}
	tsWant, err := time.Parse(time.RFC3339Nano, "2024-03-05T12:30:45Z")
	if err != nil {
		t.Fatal(err)
	}
	tmWant := int64(1)*3_600_000_000_000 + int64(2)*60_000_000_000 + int64(3)*1_000_000_000
	wantRows(t, mustValues(t, db, "SELECT d, ts, tm FROM ev WHERE id = 1"), [][]any{{day("2024-03-05"), tsWant.UnixNano(), tmWant}})
	wantRows(t, mustValues(t, db, "SELECT d FROM ev WHERE id = 2"), [][]any{{nil}})
	if _, err := db.Exec(ctx, "UPDATE ev SET d = '2025-01-31' WHERE id = 1"); err != nil {
		t.Fatalf("temporal update: %v", err)
	}
	wantRows(t, mustValues(t, db, "SELECT d FROM ev WHERE id = 1"), [][]any{{day("2025-01-31")}})
}

func TestEngine_InsertStampsSegmentIdentity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')"); err != nil {
		t.Fatal(err)
	}

	tab := db.tables["t"]
	if tab == nil {
		t.Fatal("table t missing from db.tables")
	}
	tableDir := filepath.Join(dir, "tables", "0000000000000001")
	segPath := ""
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".dsv4") {
			segPath = filepath.Join(tableDir, e.Name())
			break
		}
	}
	if segPath == "" {
		t.Fatal("no .dsv4 segment produced by INSERT")
	}

	seg, err := storage.OpenSegment(segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.TableID != uint64(tab.TableID) {
		t.Fatalf("segment.TableID = %d, want %d", seg.TableID, tab.TableID)
	}
	if seg.SchemaGeneration != uint64(db.catalog.Generation) {
		t.Fatalf("segment.SchemaGeneration = %d, want %d", seg.SchemaGeneration, db.catalog.Generation)
	}
	if len(seg.Cols) != 2 || seg.Cols[0].ColumnID != 1 || seg.Cols[1].ColumnID != 2 {
		t.Fatalf("column ids: got %+v, want [1 2]", []uint64{seg.Cols[0].ColumnID, seg.Cols[1].ColumnID})
	}
}

func TestEngine_InsertStampsManifestSchemaGeneration(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatal(err)
	}
	view := m.Snapshot()
	if len(view.Entries) == 0 {
		t.Fatal("no manifest entries after INSERT")
	}
	wantGen := uint64(db.catalog.Generation)
	for _, e := range view.Entries {
		if e.SchemaGeneration != wantGen {
			t.Fatalf("entry %s SchemaGeneration = %d, want %d", e.Path, e.SchemaGeneration, wantGen)
		}
	}
}
