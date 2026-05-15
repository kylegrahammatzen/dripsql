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
	"strconv"
	"sync"
)

// ManifestEntry is either a single legacy segment add (Path set) or a transaction record
// that publishes Adds and applies DVUpdates atomically (Path empty). Snapshot resolution
// flattens transaction records into per-segment views with the latest DV path.
type ManifestEntry struct {
	Version uint64 `json:"version"`

	Path               string `json:"path,omitempty"`
	Rows               uint32 `json:"rows,omitempty"`
	ContentHash        uint64 `json:"content_hash,omitempty"`
	DeletionVectorPath string `json:"deletion_vector_path,omitempty"`

	Adds      []ManifestSegmentAdd `json:"adds,omitempty"`
	DVUpdates []ManifestDVUpdate   `json:"dv_updates,omitempty"`
}

type ManifestSegmentAdd struct {
	Path        string `json:"path"`
	Rows        uint32 `json:"rows"`
	ContentHash uint64 `json:"content_hash,omitempty"`
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
	path    string
	f       *os.File
	mu      sync.Mutex
	records []ManifestEntry
	version uint64
}

func OpenManifest(path string) (*Manifest, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	m := &Manifest{path: path, f: f}
	if err := m.load(); err != nil {
		f.Close()
		return nil, err
	}
	return m, nil
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
// and DV files to their final paths; this record makes them visible together.
func (m *Manifest) Commit(adds []ManifestSegmentAdd, dvUpdates []ManifestDVUpdate) error {
	if len(adds) == 0 && len(dvUpdates) == 0 {
		return nil
	}
	return m.appendRecord(ManifestEntry{Adds: adds, DVUpdates: dvUpdates})
}

func (m *Manifest) appendRecord(entry ManifestEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.f == nil {
		return fmt.Errorf("manifest: closed")
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

func (m *Manifest) At(version uint64) ManifestView {
	m.mu.Lock()
	defer m.mu.Unlock()
	clamped := min(version, m.version)
	return ManifestView{Version: clamped, Entries: m.resolveLocked(clamped)}
}

// resolveLocked flattens records up to and including upTo into per-segment entries.
// Legacy single-segment records become entries directly. Transaction records contribute
// their Adds as synthetic entries and patch DV paths via DVUpdates on prior entries.
func (m *Manifest) resolveLocked(upTo uint64) []ManifestEntry {
	out := make([]ManifestEntry, 0, len(m.records))
	idxByPath := make(map[string]int)
	for _, rec := range m.records {
		if rec.Version > upTo {
			break
		}
		if rec.Path != "" {
			out = append(out, rec)
			idxByPath[rec.Path] = len(out) - 1
			continue
		}
		for _, a := range rec.Adds {
			out = append(out, ManifestEntry{
				Version:     rec.Version,
				Path:        a.Path,
				Rows:        a.Rows,
				ContentHash: a.ContentHash,
			})
			idxByPath[a.Path] = len(out) - 1
		}
		for _, dv := range rec.DVUpdates {
			i, ok := idxByPath[dv.SegmentPath]
			if !ok {
				continue
			}
			out[i].DeletionVectorPath = dv.DVPath
		}
	}
	return out
}

func (m *Manifest) load() error {
	if _, err := m.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(m.f)
	var goodEnd int64
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			entry, parseErr := decodeManifestLine(line)
			if parseErr != nil {
				break
			}
			m.records = append(m.records, entry)
			if entry.Version > m.version {
				m.version = entry.Version
			}
			goodEnd += int64(len(line))
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			break
		}
	}
	return m.truncateTail(goodEnd)
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
