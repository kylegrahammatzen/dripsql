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
	// A stale .tmp is always garbage left from a crashed save.
	_ = os.Remove(filepath.Join(root, tmpFileName))

	raw, err := readRaw(root)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return newEmpty(), nil
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
	return parseV2(root, raw)
}

// LoadReadOnly reads the catalog without sweeping tmp files or persisting a migration.
// v1-era catalogs are refused because migrating them requires a Save.
func LoadReadOnly(root string) (*File, error) {
	raw, err := readRaw(root)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return newEmpty(), nil
	}
	if isV1(raw) {
		return nil, fmt.Errorf("catalog: %s is a v1-era catalog, open the database read-write once to migrate it before read-only use", filepath.Join(root, fileName))
	}
	return parseV2(root, raw)
}

// readRaw returns the newest persisted catalog bytes, or nil when no catalog exists yet.
func readRaw(root string) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(root, fileName))
	if err == nil {
		return raw, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	bakRaw, bakErr := os.ReadFile(filepath.Join(root, bakFileName))
	if bakErr == nil {
		return bakRaw, nil
	}
	if os.IsNotExist(bakErr) {
		return nil, nil
	}
	return nil, bakErr
}

func parseV2(root string, raw []byte) (*File, error) {
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("catalog: parse %s: %w", filepath.Join(root, fileName), err)
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
