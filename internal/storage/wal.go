// WAL is a single-writer append-only log of opaque byte records framed with a per-record crc32.
// Open replays records up to the first truncated or corrupt frame, truncating the tail.
package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Each record and the file header occupy a walPageSize slot so a torn write at boundary N plus 1 cannot corrupt the durable slot N.
const (
	walMagic               = "DWAL"
	walVersion      uint16 = 2
	walHeaderLen           = len(walMagic) + 2
	walFrameHdrLen         = 1 + 4
	walFrameTailLen        = 4
	walPageSize            = 4096
)

func walPaddedFrameSize(payloadLen int) int64 {
	raw := walFrameHdrLen + payloadLen + walFrameTailLen
	pad := (walPageSize - (raw % walPageSize)) % walPageSize
	return int64(raw + pad)
}

type WALRecord struct {
	Type    uint8
	Payload []byte
}

type WAL struct {
	path string
	f    *os.File
	mu   sync.Mutex
	size int64
}

func OpenWAL(path string) (*WAL, []WALRecord, error) {
	created, err := openOrCreate(path)
	if err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, err
	}
	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	w := &WAL{path: path, f: f}
	records, err := w.replay()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return w, records, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Append writes the record framed with crc32 then fsyncs. Returns the post-write
// file offset so callers can use it as an LSN.
func (w *WAL) Append(rec WALRecord) (int64, error) {
	return w.append(rec, true)
}

// AppendNoSync writes the record without fsync for advisory records like ManifestCommit,
// whose loss just makes recovery re-check the manifest for the intent's paths.
func (w *WAL) AppendNoSync(rec WALRecord) (int64, error) {
	return w.append(rec, false)
}

func (w *WAL) append(rec WALRecord, sync bool) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return 0, errors.New("WAL: closed")
	}
	if uint64(len(rec.Payload)) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("WAL: payload too large (%d bytes)", len(rec.Payload))
	}
	padded := walPaddedFrameSize(len(rec.Payload))
	buf := make([]byte, padded)
	buf[0] = rec.Type
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(rec.Payload)))
	copy(buf[5:5+len(rec.Payload)], rec.Payload)
	crc := crc32.ChecksumIEEE(buf[:walFrameHdrLen+len(rec.Payload)])
	binary.LittleEndian.PutUint32(buf[walFrameHdrLen+len(rec.Payload):walFrameHdrLen+len(rec.Payload)+walFrameTailLen], crc)
	if _, err := w.f.WriteAt(buf, w.size); err != nil {
		return 0, err
	}
	if sync {
		if err := w.f.Sync(); err != nil {
			return 0, err
		}
	}
	w.size += padded
	return w.size, nil
}

func (w *WAL) replay() ([]WALRecord, error) {
	info, err := w.f.Stat()
	if err != nil {
		return nil, err
	}
	w.size = info.Size()
	if w.size == 0 {
		hdr := make([]byte, walPageSize)
		copy(hdr[:4], walMagic)
		binary.LittleEndian.PutUint16(hdr[4:6], walVersion)
		if _, err := w.f.WriteAt(hdr, 0); err != nil {
			return nil, err
		}
		if err := w.f.Sync(); err != nil {
			return nil, err
		}
		w.size = int64(walPageSize)
		return nil, nil
	}
	if w.size < int64(walPageSize) {
		return nil, w.truncate(0, "WAL: header truncated")
	}
	hdr := make([]byte, walHeaderLen)
	if _, err := w.f.ReadAt(hdr, 0); err != nil {
		return nil, err
	}
	if string(hdr[:4]) != walMagic {
		return nil, fmt.Errorf("WAL: bad magic %q", hdr[:4])
	}
	if v := binary.LittleEndian.Uint16(hdr[4:6]); v != walVersion {
		return nil, fmt.Errorf("WAL: version %d != %d", v, walVersion)
	}
	var (
		records []WALRecord
		pos     = int64(walPageSize)
	)
	for pos < w.size {
		rec, _, ok, err := w.readFrame(pos)
		if err != nil {
			return nil, err
		}
		if !ok {
			if err := w.truncateAt(pos); err != nil {
				return nil, err
			}
			break
		}
		records = append(records, rec)
		pos += walPaddedFrameSize(len(rec.Payload))
	}
	return records, nil
}

// readFrame returns ok=false when the frame is truncated or fails crc, signalling
// the caller to truncate the tail at pos. err is returned only for IO errors.
func (w *WAL) readFrame(pos int64) (WALRecord, int64, bool, error) {
	if pos+int64(walFrameHdrLen) > w.size {
		return WALRecord{}, 0, false, nil
	}
	hdr := make([]byte, walFrameHdrLen)
	if _, err := w.f.ReadAt(hdr, pos); err != nil {
		return WALRecord{}, 0, false, err
	}
	recType := hdr[0]
	length := binary.LittleEndian.Uint32(hdr[1:5])
	end := pos + int64(walFrameHdrLen) + int64(length) + int64(walFrameTailLen)
	if end > w.size {
		return WALRecord{}, 0, false, nil
	}
	payload := make([]byte, length)
	if length > 0 {
		if _, err := w.f.ReadAt(payload, pos+int64(walFrameHdrLen)); err != nil {
			return WALRecord{}, 0, false, err
		}
	}
	tail := make([]byte, walFrameTailLen)
	if _, err := w.f.ReadAt(tail, pos+int64(walFrameHdrLen)+int64(length)); err != nil {
		return WALRecord{}, 0, false, err
	}
	want := binary.LittleEndian.Uint32(tail)
	got := crc32.ChecksumIEEE(append(hdr, payload...))
	if want != got {
		return WALRecord{}, 0, false, nil
	}
	return WALRecord{Type: recType, Payload: payload}, end, true, nil
}

func (w *WAL) truncate(to int64, _ string) error {
	if err := w.f.Truncate(to); err != nil {
		return err
	}
	if _, err := w.f.Seek(to, io.SeekStart); err != nil {
		return err
	}
	w.size = to
	return nil
}

func (w *WAL) truncateAt(pos int64) error {
	return w.truncate(pos, "WAL: tail truncated at pos")
}

func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// TruncateToHeader drops every record, leaving only the magic+version header, and is
// safe once no in-flight intents remain, typically right after a successful commit
// under the single-writer DB mutex. The truncate is not fsynced because stale records
// that survive a crash replay through recovery and resolve harmlessly via manifest checks.
func (w *WAL) TruncateToHeader() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("WAL: closed")
	}
	if w.size == int64(walPageSize) {
		return nil
	}
	if err := w.f.Truncate(int64(walPageSize)); err != nil {
		return err
	}
	w.size = int64(walPageSize)
	return nil
}

const (
	WALKindManifestIntent uint8 = 1
	WALKindManifestCommit uint8 = 2
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
	TxnID           uint64 `json:"txn_id"`
	CommitTs        uint64 `json:"commit_ts,omitempty"`
	ManifestVersion uint64 `json:"manifest_version"`
}

func (w *WAL) AppendManifestIntent(intent ManifestIntent) (int64, error) {
	payload, err := json.Marshal(intent)
	if err != nil {
		return 0, fmt.Errorf("WAL intent: encode: %w", err)
	}
	return w.Append(WALRecord{Type: WALKindManifestIntent, Payload: payload})
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
