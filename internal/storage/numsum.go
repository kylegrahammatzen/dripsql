// Sidecar per-column int64 sum. Backs sum(col) without decoding pages. Skipped on
// any int64 overflow during accumulation so the metadata path stays exact.
package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	numSumMagic      = "NSV1"
	numSumFileSuffix = ".sm"
)

type NumericSum struct {
	Sum int64
}

type NumericSums map[string]NumericSum

func numSumPath(segPath string) string { return segPath + numSumFileSuffix }

// Skips columns whose total sum would overflow int64 (any per-batch partial sum past the
// bound). The metadata path then falls back to the operator scan for that column.
func buildNumericSums(pages []types.Batch) NumericSums {
	if len(pages) == 0 {
		return nil
	}
	out := NumericSums{}
	for ci, col := range pages[0].Columns {
		if !bloomEligibleKind(col.V.Kind) {
			continue
		}
		var sum int64
		overflow := false
		any := false
		for _, p := range pages {
			c := p.Columns[ci]
			rows := int(c.V.Len)
			valid := c.V.Valid
			for r := range rows {
				if valid != nil && !valid.IsValid(r) {
					continue
				}
				x := readInt64Key(c.V, r)
				if addOverflowsInt64(sum, x) {
					overflow = true
					break
				}
				sum += x
				any = true
			}
			if overflow {
				break
			}
		}
		if overflow || !any {
			continue
		}
		out[types.NormalizeName(col.Name)] = NumericSum{Sum: sum}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func addOverflowsInt64(a, b int64) bool {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return true
	}
	if b < 0 && a < -int64(^uint64(0)>>1)-1-b {
		return true
	}
	return false
}

func writeNumericSumSidecar(segPath string, sums NumericSums) error {
	if len(sums) == 0 {
		return nil
	}
	path := numSumPath(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	buf := encodeNumericSums(sums)
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func encodeNumericSums(sums NumericSums) []byte {
	w := newWireBuffer(16 + len(sums)*32)
	w.Raw([]byte(numSumMagic))
	w.U32(uint32(len(sums)))
	for name, s := range sums {
		w.LenPrefixedString(name)
		w.U64(uint64(s.Sum))
	}
	return w.Bytes()
}

func LoadNumericSums(segPath string) (NumericSums, error) {
	path := numSumPath(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < 8 || string(data[:4]) != numSumMagic {
		return nil, fmt.Errorf("numsum: bad magic at %s", path)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(NumericSums, cols)
	for range cols {
		name := r.LenPrefixedString()
		sum := int64(r.U64())
		out[types.NormalizeName(name)] = NumericSum{Sum: sum}
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("numsum: decode %s: %w", path, err)
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("numsum: trailing bytes at %s", path)
	}
	return out, nil
}
