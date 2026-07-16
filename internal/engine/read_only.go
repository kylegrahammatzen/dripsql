// OpenReadOnly opens a database for replica-style reads without touching a single byte on disk.
// Refresh folds in manifest records and catalog generations the writer published since open.
package engine

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
)

// OpenReadOnly mirrors Open minus every write, so no directories are created, no
// manifest is created or truncated, the WAL is ignored, and v1 catalogs are refused.
// Pending WAL intents reference segments the manifest never published, so skipping
// them yields a consistent snapshot and orphan cleanup stays the writer's job.
func OpenReadOnly(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("engine: database path is required")
	}
	file, err := catalog.LoadReadOnly(path)
	if err != nil {
		return nil, err
	}
	db := newDB(path, file)
	db.hardReadOnly = true
	db.readOnly.Store(true)
	if err := db.bootstrapCommitTs(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Refresh re-reads the catalog and reloads every table's manifest so rows the writer
// committed since open become visible. Segments are immutable and cache keys include
// the DV path, so the segment cache is left alone.
func (db *DB) Refresh() error {
	if err := db.lockOpen(); err != nil {
		return err
	}
	defer db.mu.Unlock()
	file, err := catalog.LoadReadOnly(db.root)
	if err != nil {
		return err
	}
	if file.Generation != db.catalog.Generation {
		db.catalog = file
		db.types = indexTypes(file)
		db.tables = indexTables(file)
		db.version = sql.SchemaVersion(file.Generation)
		db.plans = sql.NewPlanCache(256)
	}
	maxCommitTs := db.nextCommitTs.Load()
	for name := range db.tables {
		m, err := db.manifestFor(name)
		if err != nil {
			return err
		}
		if err := m.Reload(); err != nil {
			return err
		}
		if t := m.MaxCommitTs(); t > maxCommitTs {
			maxCommitTs = t
		}
	}
	db.nextCommitTs.Store(maxCommitTs)
	return nil
}
