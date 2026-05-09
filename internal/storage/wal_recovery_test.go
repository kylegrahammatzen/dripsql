package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
	"github.com/kylegrahammatzen/dripsql/internal/wal"
)

func TestRecoverReplaysCommittedWALWhenManifestMissing(t *testing.T) {
	store, table := newTestStore(t)
	meta := appendRows(t, store, table, []int64{1}, []string{"a"})
	manifestPath := filepath.Join(tableDir(store.root, table), manifestFileName)
	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Remove manifest: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != uint64(meta.Rows) {
		t.Fatalf("count = %d, want %d", count, meta.Rows)
	}

	segments, err := reopened.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 1 || segments[0].ID != meta.ID {
		t.Fatalf("segments = %#v", segments)
	}

	next, err := reopened.AppendBatch(context.Background(), table, makeIntTextBatch(t, []int64{2}, []string{"b"}, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if next.ID != 2 {
		t.Fatalf("next segment ID = %d, want 2", next.ID)
	}
	records, err := wal.ReadAll(filepath.Join(tableDir(reopened.root, table), walLogFileName))
	if err != nil {
		t.Fatalf("ReadAll WAL: %v", err)
	}
	// Recovery backfilled the manifest and truncated the WAL, so only the post-recovery
	// append (insert+commit for LSN 2) should remain.
	if len(records) != 2 {
		t.Fatalf("WAL records = %d, want 2", len(records))
	}
	if records[0].LSN != 2 || records[1].LSN != 2 {
		t.Fatalf("WAL LSNs = %d,%d; want 2,2", records[0].LSN, records[1].LSN)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	reopenedAgain, err := Open(store.root)
	if err != nil {
		t.Fatalf("Reopen again: %v", err)
	}
	defer reopenedAgain.Close()
	count, err = reopenedAgain.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count after second reopen: %v", err)
	}
	if count != uint64(meta.Rows+next.Rows) {
		t.Fatalf("count after second reopen = %d, want %d", count, meta.Rows+next.Rows)
	}
}

// runRecoverErrorCase corrupts WAL/manifest state via inject, then verifies
// that reopening + Count returns an error containing wantSubstr.
func runRecoverErrorCase(t *testing.T, inject func(t *testing.T, store *Store, table catalog.TableDef), wantSubstr string) {
	t.Helper()
	store, table := newTestStore(t)
	if err := ensureTableDirs(store.root, table); err != nil {
		t.Fatalf("ensureTableDirs: %v", err)
	}
	inject(t, store, table)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()
	_, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err == nil {
		t.Fatalf("expected error containing %q", wantSubstr)
	}
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("error = %v, want substr %q", err, wantSubstr)
	}
}

// writeSegmentForWAL writes one segment file under store.root for table; tests
// use it to set up WAL records that point at a real on-disk segment.
func writeSegmentForWAL(t *testing.T, store *Store, table catalog.TableDef) SegmentMeta {
	t.Helper()
	batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
	meta, err := writeSegmentBatches(context.Background(), store.root, table, []vector.Batch{batch}, 1)
	if err != nil {
		t.Fatalf("writeSegmentBatches: %v", err)
	}
	return meta
}

// appendWALInsertOrFail wraps appendWALInsert with a t.Fatalf on error.
func appendWALInsertOrFail(t *testing.T, store *Store, table catalog.TableDef, lsn wal.LSN, meta SegmentMeta) {
	t.Helper()
	if err := appendWALInsert(tableDir(store.root, table), table, lsn, meta); err != nil {
		t.Fatalf("appendWALInsert: %v", err)
	}
}

// appendWALOrFail wraps wal.Append with a t.Fatalf on error.
func appendWALOrFail(t *testing.T, store *Store, table catalog.TableDef, rec wal.Record) {
	t.Helper()
	if err := wal.Append(filepath.Join(tableDir(store.root, table), walLogFileName), rec); err != nil {
		t.Fatalf("wal.Append: %v", err)
	}
}

func TestRecoverRejections(t *testing.T) {
	t.Run("uncommitted insert", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			appendWALInsertOrFail(t, store, table, 1, meta)
		}, "no matching commit")
	})
	t.Run("commit without insert", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
		}, "no matching insert")
	})
	t.Run("malformed insert payload", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordInsert, Payload: []byte(`not-json`)})
		}, "wal insert 1:")
	})
	t.Run("out of order LSN", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			appendWALOrFail(t, store, table, wal.Record{LSN: 2, Type: wal.RecordInsert, Payload: []byte(`{"id":2}`)})
			appendWALOrFail(t, store, table, wal.Record{LSN: 2, Type: wal.RecordCommit})
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordInsert, Payload: []byte(`{"id":1}`)})
		}, "less than previous")
	})
	t.Run("empty segment path", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			meta.Path = ""
			appendWALInsertOrFail(t, store, table, 1, meta)
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
		}, "empty segment path")
	})
	t.Run("absolute segment path", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			meta.Path = filepath.Join(os.TempDir(), "0000000000000001.dseg")
			appendWALInsertOrFail(t, store, table, 1, meta)
		}, "absolute segment path")
	})
	t.Run("duplicate insert", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			appendWALInsertOrFail(t, store, table, 1, meta)
			appendWALInsertOrFail(t, store, table, 1, meta)
		}, "is duplicated")
	})
	t.Run("insert after commit", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			appendWALInsertOrFail(t, store, table, 1, meta)
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
			appendWALInsertOrFail(t, store, table, 1, meta)
		}, "appears after commit")
	})
	t.Run("unsupported record type", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordType(255)})
		}, "unsupported type")
	})
	t.Run("duplicate commit", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			appendWALInsertOrFail(t, store, table, 1, meta)
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
		}, "is duplicated")
	})
	t.Run("segment table mismatch", func(t *testing.T) {
		runRecoverErrorCase(t, func(t *testing.T, store *Store, table catalog.TableDef) {
			meta := writeSegmentForWAL(t, store, table)
			meta.TableID = table.ID + 1
			appendWALInsertOrFail(t, store, table, 1, meta)
			appendWALOrFail(t, store, table, wal.Record{LSN: 1, Type: wal.RecordCommit})
		}, "has table ID")
	})
}

func TestRecoverIgnoresOrphanSegmentAndReservesSegmentID(t *testing.T) {
	store, table := newTestStore(t)
	if err := ensureTableDirs(store.root, table); err != nil {
		t.Fatalf("ensureTableDirs: %v", err)
	}
	batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
	orphan, err := writeSegmentBatches(context.Background(), store.root, table, []vector.Batch{batch}, 1)
	if err != nil {
		t.Fatalf("writeSegmentBatches: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}
	next, err := reopened.AppendBatch(context.Background(), table, makeIntTextBatch(t, []int64{2}, []string{"b"}, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if next.ID != orphan.ID+1 {
		t.Fatalf("next segment ID = %d, want %d", next.ID, orphan.ID+1)
	}
	count, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count after append: %v", err)
	}
	if count != uint64(next.Rows) {
		t.Fatalf("count after append = %d, want %d", count, next.Rows)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	reopenedAgain, err := Open(store.root)
	if err != nil {
		t.Fatalf("Reopen again: %v", err)
	}
	defer reopenedAgain.Close()
	count, err = reopenedAgain.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count after second reopen: %v", err)
	}
	if count != uint64(next.Rows) {
		t.Fatalf("count after second reopen = %d, want %d", count, next.Rows)
	}
}

func TestRecoverRejectsWALReplayMissingSegmentFooter(t *testing.T) {
	store, table := newTestStore(t)
	if err := ensureTableDirs(store.root, table); err != nil {
		t.Fatalf("ensureTableDirs: %v", err)
	}
	meta := SegmentMeta{
		ID:            99,
		TableID:       table.ID,
		SchemaVersion: table.Version,
		Path:          filepath.Join("segments", "0000000000000099.dseg"),
		Rows:          1,
		PageRows:      uint32(DefaultPageRows),
		Columns: []ColumnMeta{
			{
				ColumnID: table.Columns[0].ID,
				Name:     table.Columns[0].Name,
				Type:     table.Columns[0].Type,
				Codec:    CodecPlain,
				Rows:     1,
			},
			{
				ColumnID: table.Columns[1].ID,
				Name:     table.Columns[1].Name,
				Type:     table.Columns[1].Type,
				Codec:    CodecPlain,
				Rows:     1,
			},
		},
	}
	if err := appendWALInsert(tableDir(store.root, table), table, 1, meta); err != nil {
		t.Fatalf("appendWALInsert: %v", err)
	}
	if err := wal.Append(filepath.Join(tableDir(store.root, table), walLogFileName), wal.Record{LSN: 1, Type: wal.RecordCommit, Payload: nil}); err != nil {
		t.Fatalf("wal.Append: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	_, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err == nil {
		t.Fatal("expected WAL recovery error for missing segment footer")
	}
	if !strings.Contains(err.Error(), "footer") {
		t.Fatalf("Count error = %v, want footer error", err)
	}
}

func TestRecoverRejectsSegmentMetadataMismatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*SegmentMeta)
		want   string
	}{
		{
			name: "rows",
			mutate: func(meta *SegmentMeta) {
				meta.Rows++
			},
			want: "segment rows",
		},
		{
			name: "schema_version",
			mutate: func(meta *SegmentMeta) {
				meta.SchemaVersion++
			},
			want: "segment schema version",
		},
		{
			name: "page_rows",
			mutate: func(meta *SegmentMeta) {
				meta.PageRows++
			},
			want: "segment page rows",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, table := newTestStore(t)
			if err := ensureTableDirs(store.root, table); err != nil {
				t.Fatalf("ensureTableDirs: %v", err)
			}
			batch := makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)
			meta, err := writeSegmentBatches(context.Background(), store.root, table, []vector.Batch{batch}, 1)
			if err != nil {
				t.Fatalf("writeSegmentBatches: %v", err)
			}
			bad := meta
			tc.mutate(&bad)
			if err := appendWALInsert(tableDir(store.root, table), table, 1, bad); err != nil {
				t.Fatalf("appendWALInsert: %v", err)
			}
			if err := wal.Append(filepath.Join(tableDir(store.root, table), walLogFileName), wal.Record{LSN: 1, Type: wal.RecordCommit, Payload: nil}); err != nil {
				t.Fatalf("wal.Append: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			reopened, err := Open(store.root)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer reopened.Close()

			_, err = reopened.Count(context.Background(), table, Predicate{}, nil)
			if err == nil {
				t.Fatal("expected WAL recovery error for footer mismatch")
			}
			if !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "does not match footer") {
				t.Fatalf("Count error = %v, want %q footer mismatch", err, tc.want)
			}
		})
	}
}

func TestRecoverManifestAheadOfWAL(t *testing.T) {
	store, table := newTestStore(t)
	meta := appendRows(t, store, table, []int64{1, 2}, []string{"a", "b"})
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := os.Remove(filepath.Join(tableDir(store.root, table), walLogFileName)); err != nil {
		t.Fatalf("remove WAL: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != uint64(meta.Rows) {
		t.Fatalf("count = %d, want %d", count, meta.Rows)
	}
}

func TestRecoverManifestAheadWithCorruptWALTail(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	appendRows(t, store, table, []int64{2}, []string{"b"})

	walPath := filepath.Join(tableDir(store.root, table), walLogFileName)
	walData, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("ReadFile wal: %v", err)
	}
	if err := os.WriteFile(walPath, append(walData, 0xff), 0o644); err != nil {
		t.Fatalf("WriteFile wal: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	segments, err := reopened.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
}

func TestRecoverManifestBehindWAL(t *testing.T) {
	store, table := newTestStore(t)
	first := appendRows(t, store, table, []int64{1}, []string{"a"})

	second := makeIntTextBatch(t, []int64{2}, []string{"b"}, nil)
	secondMeta, err := writeSegmentBatches(context.Background(), store.root, table, []vector.Batch{second}, first.ID+1)
	if err != nil {
		t.Fatalf("writeSegmentBatches: %v", err)
	}
	walDir := filepath.Join(tableDir(store.root, table), walLogFileName)
	if err := appendWALInsert(tableDir(store.root, table), table, wal.LSN(secondMeta.ID), secondMeta); err != nil {
		t.Fatalf("appendWALInsert: %v", err)
	}
	if err := wal.Append(walDir, wal.Record{LSN: wal.LSN(secondMeta.ID), Type: wal.RecordCommit, Payload: nil}); err != nil {
		t.Fatalf("wal.Append: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	segments, err := reopened.Segments(context.Background(), table)
	if err != nil {
		t.Fatalf("Segments: %v", err)
	}
	if len(segments) != 2 {
		t.Fatalf("segments = %d, want 2", len(segments))
	}
}

func TestRecoverReplaysFromWALWithPartialManifestWrite(t *testing.T) {
	store, table := newTestStore(t)
	first := appendRows(t, store, table, []int64{1}, []string{"a"})

	second := makeIntTextBatch(t, []int64{2}, []string{"b"}, nil)
	secondMeta, err := writeSegmentBatches(context.Background(), store.root, table, []vector.Batch{second}, first.ID+1)
	if err != nil {
		t.Fatalf("writeSegmentBatches: %v", err)
	}
	walPath := filepath.Join(tableDir(store.root, table), walLogFileName)
	if err := appendWALInsert(tableDir(store.root, table), table, wal.LSN(secondMeta.ID), secondMeta); err != nil {
		t.Fatalf("appendWALInsert: %v", err)
	}
	if err := wal.Append(walPath, wal.Record{LSN: wal.LSN(secondMeta.ID), Type: wal.RecordCommit, Payload: nil}); err != nil {
		t.Fatalf("wal.Append: %v", err)
	}

	manifestPath := filepath.Join(tableDir(store.root, table), manifestFileName)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(manifestData, []byte(`{"id":0`)...), 0o644); err != nil {
		t.Fatalf("WriteFile manifest: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	count, err := reopened.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	next, err := reopened.AppendBatch(context.Background(), table, makeIntTextBatch(t, []int64{3}, []string{"c"}, nil))
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if next.ID != secondMeta.ID+1 {
		t.Fatalf("next segment ID = %d, want %d", next.ID, secondMeta.ID+1)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}

	reopenedAgain, err := Open(store.root)
	if err != nil {
		t.Fatalf("Reopen again: %v", err)
	}
	defer reopenedAgain.Close()
	count, err = reopenedAgain.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count after second reopen: %v", err)
	}
	if count != 3 {
		t.Fatalf("count after second reopen = %d, want 3", count)
	}
}

func TestRecoverRejectsTruncatedWAL(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	path := filepath.Join(tableDir(store.root, table), walLogFileName)
	walData, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := os.WriteFile(path, walData[:len(walData)-1], 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	_, err = reopened.Count(context.Background(), table, Predicate{}, nil)
	if err == nil {
		t.Fatal("expected WAL recovery error for truncated WAL")
	}
	if !strings.Contains(err.Error(), "truncated") && !strings.Contains(err.Error(), "wal insert 1 has no matching commit") {
		t.Fatalf("Count error = %v, want truncated WAL or no-matching-commit error", err)
	}
}

func TestRecoveryBackfillsManifestAndTruncatesWAL(t *testing.T) {
	store, table := newTestStore(t)
	appendRows(t, store, table, []int64{1}, []string{"a"})
	appendRows(t, store, table, []int64{2}, []string{"b"})

	dir := tableDir(store.root, table)
	manifestPath := filepath.Join(dir, manifestFileName)
	walPath := filepath.Join(dir, walLogFileName)

	walBefore, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(walBefore) == 0 {
		t.Fatalf("wal empty before recovery scenario")
	}

	if err := os.Remove(manifestPath); err != nil {
		t.Fatalf("Remove manifest: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(store.root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := reopened.Count(context.Background(), table, Predicate{}, nil); err != nil {
		t.Fatalf("Count after reopen (forces recovery): %v", err)
	}

	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest after recovery: %v", err)
	}
	manifestSegments, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(manifestSegments) != 2 {
		t.Fatalf("manifest segments after backfill = %d, want 2 (manifestData=%q)", len(manifestSegments), string(manifestData))
	}

	walAfter, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal after recovery: %v", err)
	}
	if len(walAfter) != 0 {
		t.Fatalf("wal not truncated after recovery: len=%d", len(walAfter))
	}

	// A second reopen must be a clean no-op: nothing to replay, manifest already complete.
	if err := reopened.Close(); err != nil {
		t.Fatalf("Close reopened: %v", err)
	}
	reopenedAgain, err := Open(store.root)
	if err != nil {
		t.Fatalf("Reopen again: %v", err)
	}
	defer reopenedAgain.Close()
	count, err := reopenedAgain.Count(context.Background(), table, Predicate{}, nil)
	if err != nil {
		t.Fatalf("Count after second reopen: %v", err)
	}
	if count != 2 {
		t.Fatalf("count after second reopen = %d, want 2", count)
	}
}
