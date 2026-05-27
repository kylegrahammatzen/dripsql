// Catalog atomic IO. Load reads the live file, falls back to .bak, and migrates v1 inline. Save writes via tmp plus fsync plus rename plus parent-dir fsync on POSIX.
package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	fileName    = "catalog.json"
	tmpSuffix   = ".tmp"
	bakSuffix   = ".bak"
	tmpFileName = fileName + tmpSuffix
	bakFileName = fileName + bakSuffix
)

func Load(root string) (*File, error) {
	path := filepath.Join(root, fileName)
	tmpPath := filepath.Join(root, tmpFileName)
	bakPath := filepath.Join(root, bakFileName)

	// A stale .tmp is always garbage left from a crashed save.
	_ = os.Remove(tmpPath)

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if bakRaw, bakErr := os.ReadFile(bakPath); bakErr == nil {
			raw = bakRaw
		} else if os.IsNotExist(bakErr) {
			return newEmpty(), nil
		} else {
			return nil, bakErr
		}
	} else if err != nil {
		return nil, err
	}

	if isV1(raw) {
		f, err := migrateV1(raw)
		if err != nil {
			return nil, err
		}
		if err := Save(root, f); err != nil {
			return nil, fmt.Errorf("catalog migrate: persist v2: %w", err)
		}
		return f, nil
	}

	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("catalog: parse %s: %w", path, err)
	}
	if err := Validate(&f); err != nil {
		return nil, err
	}
	return &f, nil
}

func newEmpty() *File {
	return &File{
		FormatVersion: CurrentFormatVersion,
		Generation:    0,
		NextTableID:   1,
		NextTypeID:    1,
		Types:         []Type{},
		Tables:        []Table{},
	}
}

// Save writes .tmp, fsyncs it, renames the current file to .bak, renames .tmp to .json, and fsyncs the parent on POSIX.
func Save(root string, f *File) error {
	if err := Validate(f); err != nil {
		return err
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	target := filepath.Join(root, fileName)
	tmp := filepath.Join(root, tmpFileName)
	bak := filepath.Join(root, bakFileName)

	if err := writeAndSync(tmp, data); err != nil {
		return err
	}

	if _, err := os.Stat(target); err == nil {
		if err := os.Rename(target, bak); err != nil {
			os.Remove(tmp)
			return fmt.Errorf("catalog save: rename to bak: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		os.Remove(tmp)
		return err
	}

	if err := os.Rename(tmp, target); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("catalog save: rename tmp to target: %w", err)
	}

	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeAndSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}
