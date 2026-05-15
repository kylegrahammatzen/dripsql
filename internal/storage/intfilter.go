// Sidecar Binary Fuse 8 filter per int column. Proves absence for `WHERE col = N` when
// the value lies inside min/max; false positives fall through to the regular scan.
package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/FastFilter/xorfilter"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	intFilterMagic      = "FFV1"
	intFilterFileSuffix = ".bf"
)

type IntFilter struct {
	fuse *xorfilter.BinaryFuse8
}

type IntFilters map[string]*IntFilter

func intFilterPath(segPath string) string { return segPath + intFilterFileSuffix }

func (f *IntFilter) Contains(v int64) bool {
	if f == nil || f.fuse == nil {
		return true
	}
	return f.fuse.Contains(uint64(v))
}

// PopulateBinaryFuse8 rejects duplicate keys, so we dedupe per column. The reference
// implementation also requires at least one key; columns with all-null pages are skipped.
func buildIntFilters(pages []types.Batch) IntFilters {
	if len(pages) == 0 {
		return nil
	}
	out := IntFilters{}
	for ci, col := range pages[0].Columns {
		if !kindEligibleForIntFilter(col.V.Kind) {
			continue
		}
		seen := make(map[uint64]struct{})
		var keys []uint64
		for _, p := range pages {
			c := p.Columns[ci]
			rows := int(c.V.Len)
			valid := c.V.Valid
			for r := range rows {
				if valid != nil && !valid.IsValid(r) {
					continue
				}
				k := uint64(readInt64Key(c.V, r))
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
		if len(keys) == 0 {
			continue
		}
		fuse, err := xorfilter.PopulateBinaryFuse8(keys)
		if err != nil {
			continue
		}
		out[types.NormalizeName(col.Name)] = &IntFilter{fuse: fuse}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func kindEligibleForIntFilter(k types.VecKind) bool {
	switch k {
	case types.VecInt16, types.VecInt32, types.VecInt64,
		types.VecDate, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		return true
	}
	return false
}

func readInt64Key(v types.Vec, row int) int64 {
	switch v.Kind {
	case types.VecInt16:
		return int64(v.I16()[row])
	case types.VecInt32, types.VecDate:
		return int64(v.I32()[row])
	}
	return v.I64()[row]
}

func writeIntFilterSidecar(segPath string, filters IntFilters) error {
	if len(filters) == 0 {
		return nil
	}
	path := intFilterPath(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	buf, err := encodeIntFilters(filters)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func encodeIntFilters(filters IntFilters) ([]byte, error) {
	w := newWireBuffer(64 + len(filters)*512)
	w.Raw([]byte(intFilterMagic))
	w.U32(uint32(len(filters)))
	for name, f := range filters {
		w.LenPrefixedString(name)
		var body bytes.Buffer
		if err := f.fuse.Save(&body); err != nil {
			return nil, fmt.Errorf("intfilter: save %q: %w", name, err)
		}
		w.U32(uint32(body.Len()))
		w.Raw(body.Bytes())
	}
	return w.Bytes(), nil
}

func LoadIntFilters(segPath string) (IntFilters, error) {
	path := intFilterPath(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < 8 || string(data[:4]) != intFilterMagic {
		return nil, fmt.Errorf("intfilter: bad magic at %s", path)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(IntFilters, cols)
	for range cols {
		name := r.LenPrefixedString()
		bodyLen := int(r.U32())
		body := r.Raw(bodyLen)
		if err := r.Err(); err != nil {
			return nil, fmt.Errorf("intfilter: decode %s: %w", path, err)
		}
		fuse, err := xorfilter.LoadBinaryFuse8(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("intfilter: load %q: %w", name, err)
		}
		out[types.NormalizeName(name)] = &IntFilter{fuse: fuse}
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("intfilter: trailing bytes at %s", path)
	}
	return out, nil
}
