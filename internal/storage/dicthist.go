// Dict-histogram sidecar (.dh) for SELECT <col>, count(*) GROUP BY <col> SMA.
// Per-segment per-varbytes-column map[value][]byte -> row count, computed once at seal
// time and aggregated across segments at query time. The sidecar lives next to the
// .dsv4 segment; absence is tolerated (metadata SMA falls back to operator scan).
package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	dictHistMagic         = "DHV1"
	dictHistMaxDistinct   = 4096
	dictHistFileExtension = ".dh"
)

// DictHistogram maps a varbytes value to its row count within a segment. Encoded values
// are raw bytes (the same bytes stored in the page payload), not parsed strings.
type DictHistogram map[string]uint64

// DictHistograms is keyed by normalized column name.
type DictHistograms map[string]DictHistogram

// dictHistogramPath returns the sidecar path for a given segment path.
func dictHistogramPath(segPath string) string {
	return segPath + dictHistFileExtension
}

// buildDictHistograms scans batch columns for varbytes kinds and computes per-column
// row-count histograms. Returns nil when no column qualifies. Columns whose distinct
// count exceeds dictHistMaxDistinct are skipped (the SMA wouldn't speed them up).
func buildDictHistograms(pages []types.Batch) DictHistograms {
	if len(pages) == 0 {
		return nil
	}
	out := DictHistograms{}
	for ci, col := range pages[0].Columns {
		if !col.V.Kind.IsVarBytes() {
			continue
		}
		hist := DictHistogram{}
		skip := false
		for _, page := range pages {
			vb := page.Columns[ci].V.Var()
			rows := int(page.Columns[ci].V.Len)
			for r := range rows {
				if page.Columns[ci].V.Valid != nil && !page.Columns[ci].V.Valid.IsValid(r) {
					continue
				}
				key := string(vb.Bytes(r))
				hist[key]++
				if len(hist) > dictHistMaxDistinct {
					skip = true
					break
				}
			}
			if skip {
				break
			}
		}
		if !skip && len(hist) > 0 {
			out[types.NormalizeName(col.Name)] = hist
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// writeDictHistogramSidecar writes the histograms next to the segment via tmp+rename
// followed by a parent dir fsync on POSIX. Empty input is a no-op (no file created).
func writeDictHistogramSidecar(segPath string, h DictHistograms) error {
	if len(h) == 0 {
		return nil
	}
	path := dictHistogramPath(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	buf := encodeDictHistograms(h)
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func encodeDictHistograms(h DictHistograms) []byte {
	w := newWireBuffer(64 + len(h)*64)
	w.Raw([]byte(dictHistMagic))
	w.U32(uint32(len(h)))
	for name, hist := range h {
		w.LenPrefixedString(name)
		w.U32(uint32(len(hist)))
		for value, count := range hist {
			w.U64(count)
			w.U32(uint32(len(value)))
			w.Raw([]byte(value))
		}
	}
	return w.Bytes()
}

// LoadDictHistograms reads a sidecar file if it exists. A missing file returns nil
// without an error; any other I/O or decode error is surfaced.
func LoadDictHistograms(segPath string) (DictHistograms, error) {
	path := dictHistogramPath(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < 4+4 || string(data[:4]) != dictHistMagic {
		return nil, fmt.Errorf("dicthist: bad magic at %s", path)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(DictHistograms, cols)
	for range cols {
		name := r.LenPrefixedString()
		entries := int(r.U32())
		hist := make(DictHistogram, entries)
		for range entries {
			count := r.U64()
			vlen := int(r.U32())
			val := r.Raw(vlen)
			hist[string(val)] = count
		}
		out[types.NormalizeName(name)] = hist
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("dicthist: decode %s: %w", path, err)
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("dicthist: trailing bytes at %s (remaining=%d)", path, r.Remaining())
	}
	_ = binary.LittleEndian
	return out, nil
}
