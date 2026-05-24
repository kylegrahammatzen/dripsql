// Typed WAL record kinds layered over the raw framing in wal.go.
// Each kind has a stable u8 tag, a JSON payload, and Append/Decode helpers.
package storage

import (
	"encoding/json"
	"fmt"
)

const (
	WALKindManifestIntent uint8 = 1
	WALKindManifestCommit uint8 = 2
	WALKindCheckpoint     uint8 = 3
)

// ManifestIntent is logged before any segment file in the transaction lands on disk.
// CommitTs is the monotonic timestamp the manifest record stamps at Commit, so recovery
// can synthesize the matching commit when the manifest already absorbed the paths.
type ManifestIntent struct {
	TxnID     uint64               `json:"txn_id"`
	CommitTs  uint64               `json:"commit_ts,omitempty"`
	Table     string               `json:"table,omitempty"`
	Adds      []ManifestSegmentAdd `json:"adds,omitempty"`
	DVUpdates []ManifestDVUpdate   `json:"dv_updates,omitempty"`
}

// ManifestCommit is logged after Manifest.Commit returns successfully. Pairs an intent
// with proof that the manifest accepted it, so replay can ignore matching intents.
type ManifestCommit struct {
	TxnID          uint64 `json:"txn_id"`
	CommitTs       uint64 `json:"commit_ts,omitempty"`
	ManifestVersion uint64 `json:"manifest_version"`
}

// Checkpoint marks that the WAL prefix up to Offset is no longer required for recovery
// (all referenced state has been flushed). Replay can skip records before Offset.
type Checkpoint struct {
	Offset int64 `json:"offset"`
}

func (w *WAL) AppendManifestIntent(intent ManifestIntent) (int64, error) {
	return w.appendJSON(WALKindManifestIntent, intent)
}

// AppendManifestCommit logs the commit record without fsync. The intent before it is
// fsynced. This commit record is advisory because recovery synthesizes it from the
// manifest if missing.
func (w *WAL) AppendManifestCommit(commit ManifestCommit) (int64, error) {
	payload, err := json.Marshal(commit)
	if err != nil {
		return 0, fmt.Errorf("WAL commit: encode: %w", err)
	}
	return w.AppendNoSync(WALRecord{Type: WALKindManifestCommit, Payload: payload})
}

func (w *WAL) AppendCheckpoint(cp Checkpoint) (int64, error) {
	return w.appendJSON(WALKindCheckpoint, cp)
}

func (w *WAL) appendJSON(kind uint8, v any) (int64, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return 0, fmt.Errorf("WAL %d: encode: %w", kind, err)
	}
	return w.Append(WALRecord{Type: kind, Payload: payload})
}

func DecodeManifestIntent(rec WALRecord) (ManifestIntent, error) {
	var v ManifestIntent
	if rec.Type != WALKindManifestIntent {
		return v, fmt.Errorf("WAL: want intent kind %d, got %d", WALKindManifestIntent, rec.Type)
	}
	if err := json.Unmarshal(rec.Payload, &v); err != nil {
		return v, fmt.Errorf("WAL intent: decode: %w", err)
	}
	return v, nil
}

func DecodeManifestCommit(rec WALRecord) (ManifestCommit, error) {
	var v ManifestCommit
	if rec.Type != WALKindManifestCommit {
		return v, fmt.Errorf("WAL: want commit kind %d, got %d", WALKindManifestCommit, rec.Type)
	}
	if err := json.Unmarshal(rec.Payload, &v); err != nil {
		return v, fmt.Errorf("WAL commit: decode: %w", err)
	}
	return v, nil
}

func DecodeCheckpoint(rec WALRecord) (Checkpoint, error) {
	var v Checkpoint
	if rec.Type != WALKindCheckpoint {
		return v, fmt.Errorf("WAL: want checkpoint kind %d, got %d", WALKindCheckpoint, rec.Type)
	}
	if err := json.Unmarshal(rec.Payload, &v); err != nil {
		return v, fmt.Errorf("WAL checkpoint: decode: %w", err)
	}
	return v, nil
}

// PendingTxns walks the replayed records and returns the intents whose matching
// commit record has not been observed. These are the transactions whose segment
// files may be orphans on disk. Order within a TxnID is preserved (append order).
// Multi-table txns share a TxnID across N intents, callers group by TxnID.
func PendingTxns(records []WALRecord) ([]ManifestIntent, error) {
	intents := map[uint64][]ManifestIntent{}
	order := []uint64{}
	seen := map[uint64]bool{}
	for _, rec := range records {
		switch rec.Type {
		case WALKindManifestIntent:
			v, err := DecodeManifestIntent(rec)
			if err != nil {
				return nil, err
			}
			if !seen[v.TxnID] {
				seen[v.TxnID] = true
				order = append(order, v.TxnID)
			}
			intents[v.TxnID] = append(intents[v.TxnID], v)
		case WALKindManifestCommit:
			v, err := DecodeManifestCommit(rec)
			if err != nil {
				return nil, err
			}
			delete(intents, v.TxnID)
		case WALKindCheckpoint:
			// no-op for pending-txn calculation. Checkpoint affects truncation policy only.
		}
	}
	out := make([]ManifestIntent, 0)
	for _, id := range order {
		out = append(out, intents[id]...)
	}
	return out, nil
}

// PendingTxnGroups returns pending intents grouped by TxnID, preserving WAL append order
// across groups. Multi-table commits register one TxnID containing N per-table intents
// so recovery treats the whole group as atomic.
func PendingTxnGroups(records []WALRecord) ([][]ManifestIntent, error) {
	intents := map[uint64][]ManifestIntent{}
	order := []uint64{}
	seen := map[uint64]bool{}
	for _, rec := range records {
		switch rec.Type {
		case WALKindManifestIntent:
			v, err := DecodeManifestIntent(rec)
			if err != nil {
				return nil, err
			}
			if !seen[v.TxnID] {
				seen[v.TxnID] = true
				order = append(order, v.TxnID)
			}
			intents[v.TxnID] = append(intents[v.TxnID], v)
		case WALKindManifestCommit:
			v, err := DecodeManifestCommit(rec)
			if err != nil {
				return nil, err
			}
			delete(intents, v.TxnID)
		}
	}
	out := make([][]ManifestIntent, 0, len(intents))
	for _, id := range order {
		if g, ok := intents[id]; ok {
			out = append(out, g)
		}
	}
	return out, nil
}
