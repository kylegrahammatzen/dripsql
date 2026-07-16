// Manifest is the append-only JSONL record of segments comprising a table.
// Each line is `<json>\t<crc32 hex>\n`. A corrupt tail line is dropped on Open and truncated.
package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// ManifestEntry is either a single legacy segment add (Path set) or a transaction record
// that publishes Adds and applies DVUpdates atomically (Path empty). Snapshot resolution
// flattens transaction records into per-segment views with the latest DV path.
//
// CommitTs is the monotonic commit timestamp assigned at commitManifestTxn time. Used by
// SnapshotAt(commit_ts) so future MVCC readers see a consistent snapshot. Zero on legacy
// records written before PR-V1, treated as "earliest" for visibility.
type ManifestEntry struct {
	Version  uint64 `json:"version"`
	CommitTs uint64 `json:"commit_ts,omitempty"`

	Path               string `json:"path,omitempty"`
	Rows               uint32 `json:"rows,omitempty"`
	ContentHash        uint64 `json:"content_hash,omitempty"`
	SchemaGeneration   uint64 `json:"schema_generation,omitempty"`
	DeletionVectorPath string `json:"deletion_vector_path,omitempty"`
	// DVCommitTs is the CommitTs of the record that wrote DeletionVectorPath. Zero when
	// there is no DV. Retention reads it to keep a segment alive for readers pinned
	// between the segment's insert ts and its DV-out ts.
	DVCommitTs uint64 `json:"-"`

	Adds      []ManifestSegmentAdd `json:"adds,omitempty"`
	DVUpdates []ManifestDVUpdate   `json:"dv_updates,omitempty"`
	Removes   []string             `json:"removes,omitempty"`
}

type ManifestSegmentAdd struct {
	Path        string `json:"path"`
	Rows        uint32 `json:"rows"`
	ContentHash uint64 `json:"content_hash,omitempty"`
	// SchemaGeneration is the catalog Generation in effect when the segment was written.
	// Zero on legacy records and on tests that don't pipe a generation in.
	SchemaGeneration uint64 `json:"schema_generation,omitempty"`
}

type ManifestDVUpdate struct {
	SegmentPath string `json:"segment_path"`
	DVPath      string `json:"dv_path"`
	Rows        uint32 `json:"rows"`
}

type ManifestView struct {
	Version uint64
	Entries []ManifestEntry
}

type Manifest struct {
	path     string
	f        *os.File
	mu       sync.Mutex
	records  []ManifestEntry
	version  uint64
	readOnly bool
	// goodEnd is the byte offset just past the last successfully parsed line.
	goodEnd int64
}

func OpenManifest(path string) (*Manifest, error) {
	created, err := openOrCreate(path)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if created {
		if err := syncDir(filepath.Dir(path)); err != nil {
			f.Close()
			return nil, err
		}
	}
	m := &Manifest{path: path, f: f}
	if err := m.load(); err != nil {
		f.Close()
		return nil, err
	}
	return m, nil
}

// OpenManifestReadOnly opens the manifest without creating, truncating, or syncing anything.
// A missing file yields an empty view and a corrupt tail line is dropped in memory only.
func OpenManifestReadOnly(path string) (*Manifest, error) {
	m := &Manifest{path: path, readOnly: true}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	m.f = f
	if err := m.load(); err != nil {
		f.Close()
		return nil, err
	}
	return m, nil
}

// Reload folds in records appended after the last successful read, opening the file if it
// has appeared since. The tail-drop rule applies again, so a line that was partial on the
// previous pass is retried from the same offset.
func (m *Manifest) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		if !m.readOnly {
			return fmt.Errorf("manifest: closed")
		}
		f, err := os.Open(m.path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		m.f = f
	}
	return m.loadFrom(m.goodEnd)
}

// openOrCreate creates the file if absent and reports whether the creation happened.
// A true return means the caller must fsync the parent directory so the new entry
// survives a crash.
func openOrCreate(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err == nil {
		if err := f.Sync(); err != nil {
			f.Close()
			return false, err
		}
		f.Close()
		return true, nil
	}
	if os.IsExist(err) {
		return false, nil
	}
	return false, err
}

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return nil
	}
	err := m.f.Close()
	m.f = nil
	return err
}

func (m *Manifest) Append(entry ManifestEntry) error {
	return m.appendRecord(entry)
}

// Commit writes one atomic manifest record that publishes new segments AND applies DV
// updates in a single fsync'd line. Callers must have already written all segment files
// and DV files to their final paths. The commitTs is the monotonic timestamp assigned
// by the caller's transaction counter so future MVCC readers can pick a snapshot.
func (m *Manifest) Commit(commitTs uint64, adds []ManifestSegmentAdd, dvUpdates []ManifestDVUpdate) error {
	if len(adds) == 0 && len(dvUpdates) == 0 {
		return nil
	}
	return m.appendRecord(ManifestEntry{CommitTs: commitTs, Adds: adds, DVUpdates: dvUpdates})
}

func (m *Manifest) appendRecord(entry ManifestEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return fmt.Errorf("manifest: closed")
	}
	if m.readOnly {
		return fmt.Errorf("manifest: read-only")
	}
	entry.Version = m.version + 1
	line, err := encodeManifestLine(entry)
	if err != nil {
		return err
	}
	offset, err := m.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return m.poison(err)
	}
	if _, err := m.f.Write(line); err != nil {
		return m.rollbackOrPoison(offset, err)
	}
	if err := m.f.Sync(); err != nil {
		return m.rollbackOrPoison(offset, err)
	}
	m.version++
	m.records = append(m.records, entry)
	m.goodEnd = offset + int64(len(line))
	return nil
}

func (m *Manifest) rollbackOrPoison(offset int64, cause error) error {
	if err := m.f.Truncate(offset); err != nil {
		return m.poison(fmt.Errorf("manifest: %w (truncate after: %v)", cause, err))
	}
	if _, err := m.f.Seek(offset, io.SeekStart); err != nil {
		return m.poison(fmt.Errorf("manifest: %w (seek after: %v)", cause, err))
	}
	return cause
}

func (m *Manifest) poison(cause error) error {
	if m.f != nil {
		_ = m.f.Close()
		m.f = nil
	}
	return cause
}

func (m *Manifest) Snapshot() ManifestView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ManifestView{Version: m.version, Entries: m.resolveLocked(m.version)}
}

// SnapshotAt returns the per-segment view containing only records whose CommitTs is at most
// maxCommitTs. Legacy records (CommitTs == 0, written before PR-V1) are treated as committed
// at time 0 and are always visible -- they predate the timestamp regime.
func (m *Manifest) SnapshotAt(maxCommitTs uint64) ManifestView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return ManifestView{Version: m.version, Entries: m.resolveLockedFiltered(m.version, maxCommitTs)}
}

// MaxCommitTs returns the highest CommitTs across all records, or 0 if none have one set.
// Used at DB.Open to bootstrap the global nextCommitTs counter past any persisted state.
func (m *Manifest) MaxCommitTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var maxTs uint64
	for _, rec := range m.records {
		if rec.CommitTs > maxTs {
			maxTs = rec.CommitTs
		}
	}
	return maxTs
}

// resolveLocked flattens records up to and including upTo into per-segment entries.
// Legacy single-segment records become entries directly. Transaction records contribute
// their Adds as synthetic entries and patch DV paths via DVUpdates on prior entries.
func (m *Manifest) resolveLocked(upTo uint64) []ManifestEntry {
	return m.resolveLockedFiltered(upTo, ^uint64(0))
}

func (m *Manifest) resolveLockedFiltered(upTo, maxCommitTs uint64) []ManifestEntry {
	out := make([]ManifestEntry, 0, len(m.records))
	idxByPath := make(map[string]int)
	for _, rec := range m.records {
		if rec.Version > upTo {
			break
		}
		if rec.CommitTs > maxCommitTs {
			continue
		}
		if rec.Path != "" {
			out = append(out, rec)
			idxByPath[rec.Path] = len(out) - 1
			continue
		}
		for _, a := range rec.Adds {
			out = append(out, ManifestEntry{
				Version:          rec.Version,
				CommitTs:         rec.CommitTs,
				Path:             a.Path,
				Rows:             a.Rows,
				ContentHash:      a.ContentHash,
				SchemaGeneration: a.SchemaGeneration,
			})
			idxByPath[a.Path] = len(out) - 1
		}
		for _, dv := range rec.DVUpdates {
			i, ok := idxByPath[dv.SegmentPath]
			if !ok {
				continue
			}
			out[i].DeletionVectorPath = dv.DVPath
			out[i].DVCommitTs = rec.CommitTs
		}
		for _, p := range rec.Removes {
			i, ok := idxByPath[p]
			if !ok {
				continue
			}
			out[i].Path = ""
			delete(idxByPath, p)
		}
	}
	compact := out[:0]
	for _, e := range out {
		if e.Path == "" {
			continue
		}
		compact = append(compact, e)
	}
	return compact
}

func (m *Manifest) Retire(commitTs uint64, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	return m.appendRecord(ManifestEntry{CommitTs: commitTs, Removes: paths})
}

func (m *Manifest) load() error {
	if err := m.loadFrom(0); err != nil {
		return err
	}
	if m.readOnly {
		return nil
	}
	return m.truncateTail(m.goodEnd)
}

// loadFrom parses records starting at offset and advances goodEnd past each complete line.
// A corrupt or partial tail stops the scan and stays on disk for the caller to handle.
func (m *Manifest) loadFrom(offset int64) error {
	if _, err := m.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(m.f)
	m.goodEnd = offset
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			entry, parseErr := decodeManifestLine(line)
			if parseErr != nil {
				return nil
			}
			m.records = append(m.records, entry)
			if entry.Version > m.version {
				m.version = entry.Version
			}
			m.goodEnd += int64(len(line))
		}
		if err != nil {
			return nil
		}
	}
}

func (m *Manifest) truncateTail(offset int64) error {
	end, err := m.f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if end == offset {
		return nil
	}
	if err := m.f.Truncate(offset); err != nil {
		return err
	}
	_, err = m.f.Seek(0, io.SeekEnd)
	return err
}

func encodeManifestLine(entry ManifestEntry) ([]byte, error) {
	body, err := json.Marshal(entry)
	if err != nil {
		return nil, err
	}
	sum := crc32.ChecksumIEEE(body)
	out := make([]byte, 0, len(body)+12)
	out = append(out, body...)
	out = append(out, '\t')
	out = strconv.AppendUint(out, uint64(sum), 16)
	out = append(out, '\n')
	return out, nil
}

func decodeManifestLine(line []byte) (ManifestEntry, error) {
	trimmed := bytes.TrimRight(line, "\n")
	if len(trimmed) == len(line) {
		return ManifestEntry{}, fmt.Errorf("manifest: line missing newline")
	}
	tab := bytes.LastIndexByte(trimmed, '\t')
	if tab < 0 {
		return ManifestEntry{}, fmt.Errorf("manifest: line missing CRC delimiter")
	}
	body := trimmed[:tab]
	crcHex := trimmed[tab+1:]
	want, err := strconv.ParseUint(string(crcHex), 16, 32)
	if err != nil {
		return ManifestEntry{}, fmt.Errorf("manifest: bad CRC hex %q: %w", crcHex, err)
	}
	if got := crc32.ChecksumIEEE(body); got != uint32(want) {
		return ManifestEntry{}, fmt.Errorf("manifest: CRC mismatch %x != %x", got, want)
	}
	var entry ManifestEntry
	if err := json.Unmarshal(body, &entry); err != nil {
		return ManifestEntry{}, fmt.Errorf("manifest: json: %w", err)
	}
	return entry, nil
}
