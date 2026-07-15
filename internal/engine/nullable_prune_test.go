// Nullable columns produce mixed pages whose values never reach the sidecar sinks.
// These tests pin that pushed predicates and metadata answers stay exact anyway.
package engine

import "testing"

func TestEngine_NullablePushdownPrune(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 7), (3, NULL)")

	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v = 7"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v = 5"), [][]any{{int64(1)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v > 6"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v <= 5"), [][]any{{int64(1)}})
}

func TestEngine_NullableTextMetadataCount(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, cat text)")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'a'), (2, 'a'), (3, NULL), (4, 'b')")

	wantRows(t, mustValues(t, db, "SELECT count(*) FROM t WHERE cat = 'a'"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE cat = 'b'"), [][]any{{int64(4)}})
}

func TestEngine_NullableSumMetadata(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, NULL)")

	wantRows(t, mustValues(t, db, "SELECT sum(v) FROM t"), [][]any{{int64(30)}})
	wantRows(t, mustValues(t, db, "SELECT min(v), max(v) FROM t"), [][]any{{int64(10), int64(20)}})
}
