package storage

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
)

type RecoveryReport struct {
	TempSegmentsRemoved int
}

func (s *Store) Recover(ctx context.Context) (RecoveryReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if err := s.beginOp(); err != nil {
		return RecoveryReport{}, err
	}
	defer s.endOp()

	var report RecoveryReport
	tablesDir := filepath.Join(s.root, "tables")
	err := filepath.WalkDir(tablesDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".tmp" {
			return nil
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		report.TempSegmentsRemoved++
		return nil
	})
	if os.IsNotExist(err) {
		return report, nil
	}
	return report, err
}
