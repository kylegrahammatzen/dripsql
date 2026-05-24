// Generic sidecar plumbing shared by the .dh, .bf, and .sm sidecars.
// Outer framing (magic, col count, per-col name) is uniform with a per-T body codec.
package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Sidecar[T any] struct {
	Magic      string
	Suffix     string
	BufHint    int
	EncodeBody func(name string, v T, w *wireBuffer) error
	DecodeBody func(name string, r *wireReader) (T, error)
}

func (s Sidecar[T]) Path(segPath string) string { return segPath + s.Suffix }

// Encode returns the body bytes (magic+colCount+entries) for use either as a
// standalone sidecar file or as a section inside a container.
func (s Sidecar[T]) Encode(entries map[string]T) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	hint := s.BufHint
	if hint <= 0 {
		hint = 64
	}
	w := newWireBuffer(hint + len(entries)*64)
	w.Raw([]byte(s.Magic))
	w.U32(uint32(len(entries)))
	for name, v := range entries {
		w.LenPrefixedString(name)
		if err := s.EncodeBody(name, v, w); err != nil {
			return nil, fmt.Errorf("%s: encode body %q: %w", s.Suffix, name, err)
		}
	}
	return w.Bytes(), nil
}

// Decode parses bytes previously produced by Encode. Returns (nil, nil) for empty
// input so callers can treat absent sections as no-op.
func (s Sidecar[T]) Decode(data []byte) (map[string]T, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 8 || string(data[:4]) != s.Magic {
		return nil, fmt.Errorf("%s: bad magic", s.Suffix)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(map[string]T, cols)
	for range cols {
		name := r.LenPrefixedString()
		if err := r.Err(); err != nil {
			return nil, fmt.Errorf("%s: decode: %w", s.Suffix, err)
		}
		v, err := s.DecodeBody(name, r)
		if err != nil {
			return nil, fmt.Errorf("%s: decode body %q: %w", s.Suffix, name, err)
		}
		out[types.NormalizeName(name)] = v
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("%s: decode: %w", s.Suffix, err)
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("%s: trailing bytes (remaining=%d)", s.Suffix, r.Remaining())
	}
	return out, nil
}

// Empty entries produce no file so sidecars stay best-effort.
func (s Sidecar[T]) Write(segPath string, entries map[string]T) error {
	data, err := s.Encode(entries)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	path := s.Path(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Missing file returns (nil, nil). Bad magic or trailing bytes are an error so
// silent corruption never serves stale data.
func (s Sidecar[T]) Read(segPath string) (map[string]T, error) {
	path := s.Path(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return s.Decode(data)
}
