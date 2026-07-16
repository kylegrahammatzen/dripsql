// Time-travel and retention tests covering commit timestamps, snapshot reads via QueryAt and AS OF, ReadTs-pinned segment views, and retention vacuum interaction with pinned readers and DV-out commit timestamps.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func snapshotOf(t *testing.T, db *DB, table string) storage.ManifestView {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	m, err := db.manifestFor(table)
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	return m.Snapshot()
}

func segCommitTimestamps(segs []*storage.Segment) []uint64 {
	out := make([]uint64, len(segs))
	for i, s := range segs {
		out[i] = s.CommitTs
	}
	return out
}

func TestSQLAsOf_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	q := fmt.Sprintf("SELECT id FROM t AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("Query AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("AS OF tsAfterFirst: got %d rows, want 1", len(rows.Values))
	}
}

func TestSQLAsOf_ParserDoesNotBreakBareAlias(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if len(mustValues(t, db, "SELECT id FROM t alias")) != 1 {
		t.Fatal("bare alias regressed")
	}
}

func TestSQLAsOf_ParserDoesNotBreakASAlias(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if len(mustValues(t, db, "SELECT id FROM t AS alias")) != 1 {
		t.Fatal("AS alias regressed")
	}
}

func TestSQLAsOf_AliasAndAsOf(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	q := fmt.Sprintf("SELECT id FROM t alias AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("alias + AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("alias + AS OF: got %d rows, want 1", len(rows.Values))
	}
}

func TestSQLAsOf_CountStarRespectsSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (3), (4)")

	q := fmt.Sprintf("SELECT count(*) FROM t AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("count rows = %d, want 1", len(rows.Values))
	}
	if got := rows.Values[0][0]; got != int64(2) {
		t.Fatalf("count(*) AS OF earlyTs = %v, want 2", got)
	}
}

func TestSQLAsOf_PerTableInJoin(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL, w int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id, v) VALUES (1, 100)")
	mustExec(t, db, "INSERT INTO b (id, w) VALUES (1, 999)")
	tsAfterFirstB := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO b (id, w) VALUES (1, 222)")

	q := fmt.Sprintf("SELECT a.v, b.w FROM a JOIN b AS OF %d ON a.id = b.id", tsAfterFirstB)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("join with per-table AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows.Values))
	}
	got := rows.Values[0]
	if got[1] != int64(999) {
		t.Fatalf("b.w = %v, want 999 (snapshot before second insert)", got[1])
	}
}

func TestCommitTs_MonotonicAcrossOperations(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (2, 20)")
	if _, err := db.Exec(ctx, "UPDATE t SET v = 99 WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM t WHERE id = 2"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	view := m.Snapshot()
	if len(view.Entries) == 0 {
		t.Fatal("expected manifest entries")
	}
	var prev uint64
	seen := 0
	for _, e := range view.Entries {
		if e.CommitTs == 0 {
			t.Errorf("entry %q has zero CommitTs; every PR-V1 record must be stamped", e.Path)
		}
		if e.CommitTs < prev {
			t.Errorf("CommitTs went backwards: %d < %d", e.CommitTs, prev)
		}
		prev = e.CommitTs
		seen++
	}
	if seen < 2 {
		t.Fatalf("expected at least 2 stamped entries, saw %d", seen)
	}
}

func TestCommitTs_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	preReopenMax := db.nextCommitTs.Load()
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := db2.nextCommitTs.Load(); got != preReopenMax {
		t.Fatalf("nextCommitTs after reopen = %d, want %d (must match max persisted CommitTs)", got, preReopenMax)
	}
	mustExec(t, db2, "INSERT INTO t (id) VALUES (3)")
	if got := db2.nextCommitTs.Load(); got <= preReopenMax {
		t.Fatalf("new commit after reopen did not advance counter: %d <= %d", got, preReopenMax)
	}
}

func TestSnapshotAt_FiltersByCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	asOfFirst := m.SnapshotAt(tsAfterFirst)
	if len(asOfFirst.Entries) != 1 {
		t.Fatalf("SnapshotAt(tsAfterFirst).Entries = %d, want 1", len(asOfFirst.Entries))
	}
	full := m.Snapshot()
	if len(full.Entries) != 3 {
		t.Fatalf("Snapshot().Entries = %d, want 3", len(full.Entries))
	}
}

func TestCommitTs_SegmentsCarryStampedCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	segs, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("got %d segments, want 2", len(segs))
	}
	var olderSeen, newerSeen bool
	for _, s := range segs {
		if s.CommitTs == 0 {
			t.Errorf("segment %q has zero CommitTs; engine must stamp from manifest entry", s.Path())
		}
		if s.CommitTs == tsAfterFirst {
			olderSeen = true
		}
		if s.CommitTs > tsAfterFirst {
			newerSeen = true
		}
	}
	if !olderSeen || !newerSeen {
		t.Fatalf("expected one segment at ts=%d and one at ts>%d; got %+v",
			tsAfterFirst, tsAfterFirst, segCommitTimestamps(segs))
	}
}

func TestCommitTs_WALRecordsCarryIt(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	w, _, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	intent := storage.ManifestIntent{TxnID: 1, CommitTs: 42, Table: "t",
		Adds: []storage.ManifestSegmentAdd{{Path: "/x", Rows: 1}}}
	if _, err := w.AppendManifestIntent(intent); err != nil {
		t.Fatalf("AppendManifestIntent: %v", err)
	}
	w.Close()

	w2, records, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer w2.Close()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	got, err := storage.DecodeManifestIntent(records[0])
	if err != nil {
		t.Fatalf("DecodeManifestIntent: %v", err)
	}
	if got.CommitTs != 42 {
		t.Errorf("ManifestIntent.CommitTs = %d, want 42", got.CommitTs)
	}
}

func TestOpenSegmentsAt_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	atFirst, err := db.openSegmentsAt("t", tsAfterFirst)
	if err != nil {
		t.Fatalf("openSegmentsAt(tsAfterFirst): %v", err)
	}
	if len(atFirst) != 1 {
		t.Fatalf("openSegmentsAt(tsAfterFirst) = %d segs, want 1", len(atFirst))
	}
	latest, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	if len(latest) != 3 {
		t.Fatalf("openSegmentsForQuery = %d segs, want 3", len(latest))
	}
}

func TestOpenSegmentsAt_DeletePreservedAtEarlierTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	tsBeforeDelete := db.nextCommitTs.Load()
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	preDelete, err := db.openSegmentsAt("t", tsBeforeDelete)
	if err != nil {
		t.Fatalf("openSegmentsAt(tsBeforeDelete): %v", err)
	}
	var preDV int
	for _, s := range preDelete {
		if s.DV != nil {
			preDV++
		}
	}
	if preDV != 0 {
		t.Fatalf("snapshot before delete had %d segs with DV, want 0", preDV)
	}

	postDelete, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	var postDV int
	for _, s := range postDelete {
		if s.DV != nil {
			postDV++
		}
	}
	if postDV == 0 {
		t.Fatalf("latest snapshot after delete had 0 segs with DV, want >=1")
	}
}

func TestQueryAt_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	rows, err := db.QueryAt(context.Background(), "SELECT id FROM t", tsAfterFirst)
	if err != nil {
		t.Fatalf("QueryAt(tsAfterFirst): %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("QueryAt(tsAfterFirst) got %d rows, want 1", len(rows.Values))
	}

	latest := mustValues(t, db, "SELECT id FROM t")
	if len(latest) != 3 {
		t.Fatalf("latest got %d rows, want 3", len(latest))
	}
}

func TestQueryAt_TsZeroReturnsEmpty(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	rows, err := db.QueryAt(context.Background(), "SELECT id FROM t", 0)
	if err != nil {
		t.Fatalf("QueryAt(0): %v", err)
	}
	if len(rows.Values) != 0 {
		t.Fatalf("QueryAt(0) got %d rows, want 0 (pre-history snapshot)", len(rows.Values))
	}
}

func TestQueryAt_DeletePreservedAtEarlierTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	tsBeforeDelete := db.nextCommitTs.Load()
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	preDelete, err := db.QueryAt(context.Background(), "SELECT id FROM t", tsBeforeDelete)
	if err != nil {
		t.Fatalf("QueryAt(tsBeforeDelete): %v", err)
	}
	if len(preDelete.Values) != 2 {
		t.Fatalf("AS OF before delete got %d rows, want 2", len(preDelete.Values))
	}

	postDelete := mustValues(t, db, "SELECT id FROM t")
	if len(postDelete) != 1 {
		t.Fatalf("latest after delete got %d rows, want 1", len(postDelete))
	}
}

func TestVacuumRetention_DropsFullyDeletedOldSegment(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	view := snapshotOf(t, db, "t")
	pathFirst := db.resolveTablePath("t", view.Entries[0].Path)
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	cutoff := db.nextCommitTs.Load() + 1

	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 1 {
		t.Fatalf("VacuumRetention retired %d, want 1", n)
	}
	if _, err := os.Stat(pathFirst); !os.IsNotExist(err) {
		t.Fatalf("segment file %q should be gone (err=%v)", pathFirst, err)
	}
	rows := mustValues(t, db, "SELECT id FROM t")
	if len(rows) != 1 {
		t.Fatalf("after retention got %d rows, want 1 (the surviving INSERT)", len(rows))
	}
}

func TestVacuumRetention_RespectsPinRegistry(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	pinTs := db.nextCommitTs.Load() - 1
	db.mu.Lock()
	db.pinnedReadTs[pinTs] = 1
	db.mu.Unlock()
	cutoff := db.nextCommitTs.Load() + 100
	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 0 {
		t.Fatalf("pinned reader at ts=%d should block retirement (cutoff=%d), got %d retired",
			pinTs, cutoff, n)
	}
	db.mu.Lock()
	delete(db.pinnedReadTs, pinTs)
	db.mu.Unlock()
	n, err = db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention after unpin: %v", err)
	}
	if n != 1 {
		t.Fatalf("after unpin: expected 1 retired, got %d", n)
	}
}

func TestVacuumRetention_BelowCutoffOnly(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	tsBeforeInsert := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	n, err := db.VacuumRetention(tsBeforeInsert + 1)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 0 {
		t.Fatalf("cutoff not above seg.CommitTs: expected 0, got %d", n)
	}
}

func TestVacuumRetention_HonorsDVOutCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterInsert := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	tsAfterDelete := db.nextCommitTs.Load()

	cutoff := tsAfterInsert + 1
	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention mid-range: %v", err)
	}
	if n != 0 {
		t.Fatalf("cutoff between insert and DV-out: expected 0, got %d", n)
	}

	n, err = db.VacuumRetention(tsAfterDelete + 1)
	if err != nil {
		t.Fatalf("VacuumRetention above DV-out: %v", err)
	}
	if n != 1 {
		t.Fatalf("cutoff above DV-out: expected 1, got %d", n)
	}
}
