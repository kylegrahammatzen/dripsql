package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// Save serializes f and writes it to catalog.json atomically. The prior catalog.json is
// preserved as catalog.json.bak. Pattern is: write .tmp, fsync, rename current to .bak,
// rename .tmp to .json, fsync parent (POSIX only).
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
