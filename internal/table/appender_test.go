package table

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestAppendRejectsSchemaMismatch(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())

	err := tbl.Append(mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7})},
	))
	if err == nil {
		t.Fatal("expected column order mismatch error")
	}
	if !strings.Contains(err.Error(), `batch column 0 is "event_type", want "tenant_id"`) {
		t.Fatalf("error = %q, want column order mismatch", err)
	}

	err = tbl.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewString([]string{"7"})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
	))
	if err == nil {
		t.Fatal("expected column kind mismatch error")
	}
	if !strings.Contains(err.Error(), `batch column "tenant_id" is string, want int64`) {
		t.Fatalf("error = %q, want kind mismatch", err)
	}
}

func TestAppenderPersistsMultipleSegmentsOnClose(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	appender, err := tbl.NewAppender()
	if err != nil {
		t.Fatal(err)
	}

	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "checkout"})},
	)); err != nil {
		t.Fatal(err)
	}
	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"cancel", "checkout"})},
	)); err != nil {
		t.Fatal(err)
	}

	uncommitted, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if uncommitted.Rows() != 0 || uncommitted.Segments() != 0 {
		t.Fatalf("uncommitted rows/segments = %d/%d, want 0/0", uncommitted.Rows(), uncommitted.Segments())
	}

	if err := appender.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Rows() != 5 || reopened.Segments() != 2 {
		t.Fatalf("reopened rows/segments = %d/%d, want 5/2", reopened.Rows(), reopened.Segments())
	}
	if reopened.manifest.Segments[0].Offset != 0 {
		t.Fatalf("first offset = %d, want 0", reopened.manifest.Segments[0].Offset)
	}
	if reopened.manifest.Segments[1].Offset != reopened.manifest.Segments[0].Bytes {
		t.Fatalf("second offset = %d, want %d", reopened.manifest.Segments[1].Offset, reopened.manifest.Segments[0].Bytes)
	}
}

func TestAppenderCloseRollsBackManifestFailure(t *testing.T) {
	dir := t.TempDir()
	tbl := createEventsTable(t, dir)
	appender, err := tbl.NewAppender()
	if err != nil {
		t.Fatal(err)
	}
	if err := appender.Append(mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup"})},
	)); err != nil {
		t.Fatal(err)
	}

	badDir := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(badDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	tbl.dir = badDir
	err = appender.Close()
	tbl.dir = dir
	if err == nil {
		t.Fatal("expected manifest write error")
	}
	if tbl.Rows() != 0 || tbl.Segments() != 0 {
		t.Fatalf("table rows/segments = %d/%d, want 0/0", tbl.Rows(), tbl.Segments())
	}
	info, err := os.Stat(filepath.Join(dir, dataFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("data file size = %d, want 0", info.Size())
	}
}
