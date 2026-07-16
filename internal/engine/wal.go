// WAL plumbing for UPDATE's atomic commit, opened at <root>/wal.
// Replays pending intents and deletes orphan segment files whose paths never reached the matching manifest.
package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func (db *DB) openWAL() error {
	w, records, err := storage.OpenWAL(filepath.Join(db.root, "wal"))
	if err != nil {
		return err
	}
	db.wal = w
	groups, err := storage.PendingTxnGroups(records)
	if err != nil {
		return fmt.Errorf("wal: %w", err)
	}
	for _, group := range groups {
		if err := db.recoverTxnGroup(group); err != nil {
			return err
		}
	}
	return nil
}

// recoverTxnGroup handles all intents for one TxnID atomically. When any intent's adds
// already reached their table's manifest the txn partially committed, so the remaining
// tables forward-roll to land it fully, and otherwise every staged file is deleted.
func (db *DB) recoverTxnGroup(group []storage.ManifestIntent) error {
	if len(group) == 0 {
		return nil
	}
	type intentState struct {
		intent    storage.ManifestIntent
		committed bool
	}
	states := make([]intentState, len(group))
	anyCommitted := false
	for i, intent := range group {
		st := intentState{intent: intent}
		if intent.Table == "" || len(intent.Adds) == 0 {
			states[i] = st
			continue
		}
		m, err := db.manifestFor(intent.Table)
		if err != nil {
			return fmt.Errorf("wal recover: manifest for %q: %w", intent.Table, err)
		}
		view := m.Snapshot()
		// Compare basenames so intents and manifest entries match across path formats.
		known := make(map[string]struct{}, len(view.Entries))
		for _, e := range view.Entries {
			known[filepath.Base(e.Path)] = struct{}{}
		}
		for _, a := range intent.Adds {
			if _, ok := known[filepath.Base(a.Path)]; ok {
				st.committed = true
				anyCommitted = true
				break
			}
		}
		states[i] = st
	}
	if anyCommitted {
		// Apply uncommitted intents under the txn's shared (TxnID, CommitTs).
		for _, st := range states {
			if st.committed {
				if err := db.markIntentResolved(st.intent, 0); err != nil {
					return err
				}
				continue
			}
			if err := db.commitManifestTxnAs(st.intent.Table, st.intent.TxnID, st.intent.CommitTs, st.intent.Adds, st.intent.DVUpdates); err != nil {
				return fmt.Errorf("wal recover: forward-roll %q: %w", st.intent.Table, err)
			}
		}
		return nil
	}
	// Nothing committed, so delete every staged file across the group.
	for _, st := range states {
		for _, a := range st.intent.Adds {
			p := db.resolveTablePath(st.intent.Table, a.Path)
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal recover: remove orphan %q: %w", p, err)
			}
		}
		for _, dv := range st.intent.DVUpdates {
			p := db.resolveTablePath(st.intent.Table, dv.DVPath)
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal recover: remove orphan dv %q: %w", p, err)
			}
		}
		if err := db.markIntentResolved(st.intent, 0); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) markIntentResolved(intent storage.ManifestIntent, manifestVersion uint64) error {
	_, err := db.wal.AppendManifestCommit(storage.ManifestCommit{
		TxnID:           intent.TxnID,
		ManifestVersion: manifestVersion,
	})
	return err
}

func (db *DB) commitManifestTxn(tableName string, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error {
	db.nextTxnID++
	txnID := db.nextTxnID
	commitTs := db.nextCommitTs.Add(1)
	if err := db.commitManifestTxnAs(tableName, txnID, commitTs, adds, dvUpdates); err != nil {
		return err
	}
	if db.wal == nil {
		return nil
	}
	return db.wal.TruncateToHeader()
}

func (db *DB) commitManifestTxnAs(tableName string, txnID, commitTs uint64, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error {
	m, err := db.manifestFor(tableName)
	if err != nil {
		return err
	}
	if db.wal == nil {
		return m.Commit(commitTs, adds, dvUpdates)
	}
	intent := storage.ManifestIntent{
		TxnID:     txnID,
		CommitTs:  commitTs,
		Table:     schema.NormalizeName(tableName),
		Adds:      adds,
		DVUpdates: dvUpdates,
	}
	if _, err := db.wal.AppendManifestIntent(intent); err != nil {
		return err
	}
	if err := m.Commit(commitTs, adds, dvUpdates); err != nil {
		return err
	}
	if _, err := db.wal.AppendManifestCommit(storage.ManifestCommit{
		TxnID:           txnID,
		CommitTs:        commitTs,
		ManifestVersion: m.Snapshot().Version,
	}); err != nil {
		return err
	}
	return nil
}
