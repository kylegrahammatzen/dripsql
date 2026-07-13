// Per-segment deletion vector. File at <segment>.dv (or an explicit per-version path)
// holds the still-valid bitmap in little-endian uint64 words. Absent file means all live.
package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func DVPath(segmentPath string) string { return segmentPath + ".dv" }

func loadDVAtPath(dvPath string, rows int) (vector.Validity, error) {
	data, err := os.ReadFile(dvPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	want := vector.ValidityWords(rows) * 8
	if len(data) != want {
		return nil, fmt.Errorf("DV %q is %d bytes, want %d for %d rows", dvPath, len(data), want, rows)
	}
	v := make(vector.Validity, vector.ValidityWords(rows))
	for i := range v {
		base := i * 8
		v[i] = uint64(data[base]) |
			uint64(data[base+1])<<8 |
			uint64(data[base+2])<<16 |
			uint64(data[base+3])<<24 |
			uint64(data[base+4])<<32 |
			uint64(data[base+5])<<40 |
			uint64(data[base+6])<<48 |
			uint64(data[base+7])<<56
	}
	return v, nil
}

// WriteDV serializes v atomically to <segmentPath>.dv. nil v removes the .dv file.
// Used by DELETE which writes the conventional DV path.
func WriteDV(segmentPath string, rows int, v vector.Validity) error {
	dvPath := DVPath(segmentPath)
	if v == nil {
		err := os.Remove(dvPath)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return WriteDVAtPath(dvPath, rows, v)
}

// WriteDVAtPath writes a DV to a specific path (atomic via tmp+rename+dir-fsync). Used by
// UPDATE which stages a versioned DV file alongside its segment before the transaction
// manifest record makes it visible.
func WriteDVAtPath(dvPath string, rows int, v vector.Validity) error {
	want := vector.ValidityWords(rows)
	if len(v) != want {
		return fmt.Errorf("WriteDV: validity has %d words, want %d for %d rows", len(v), want, rows)
	}
	buf := make([]byte, want*8)
	v.MarshalLE(buf)
	tmp := dvPath + ".tmp"
	_ = os.Remove(tmp)
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, dvPath); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(dvPath))
}
