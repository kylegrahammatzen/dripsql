package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
