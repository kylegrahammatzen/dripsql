// commitTarget is the seam between DB's auto-commit path and Tx's staged path.
// DB writes intent+commit straight to WAL and manifest. Tx accumulates per-table
// updates in memory and flushes them all under one commitTs at Tx.Commit.
package engine

import "github.com/kylegrahammatzen/dripsql/internal/storage"

type commitTarget interface {
	commit(table string, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error
}

type autoCommitTarget struct{ db *DB }

func (t autoCommitTarget) commit(table string, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error {
	return t.db.commitManifestTxn(table, adds, dvUpdates)
}
