// Bulk ingest path. Accepts pre-built vector.Batch pages, validates against catalog,
// writes segment(s), and appends manifest. Zero SQL parsing overhead.
package engine

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// IngestConfig holds a set of batches to ingest into a single table.
// Each batch becomes one page in the resulting segment. All batches must
// have identical schema matching the target table.
type IngestConfig struct {
	Table   string
	Batches []vector.Batch
}

// Ingest writes pre-built batches directly to storage, bypassing SQL parse/plan.
// Returns total rows ingested.
func (db *DB) Ingest(ctx context.Context, cfg IngestConfig) (int64, error) {
	if len(cfg.Batches) == 0 {
		return 0, nil
	}
	ctx = ctxOrBackground(ctx)

	if err := db.lockOpen(); err != nil {
		return 0, err
	}
	defer db.mu.Unlock()

	if db.readOnly.Load() {
		return 0, ErrReadOnly
	}

	// Resolve table and build BoundTableDef for validation + codecs + identity.
	entry, err := db.table(cfg.Table)
	if err != nil {
		return 0, err
	}
	def := db.boundTable(entry)

	// Validate all batches against the table schema.
	if err := validateIngestBatches(def, cfg.Batches); err != nil {
		return 0, err
	}

	var total uint64
	for _, b := range cfg.Batches {
		total += uint64(b.Len)
	}
	if total > uint64(^uint32(0)) {
		return 0, fmt.Errorf("ingest: %d rows exceeds single segment capacity", total)
	}

	add, cleanup, err := db.writeSegmentAdd(def, cfg.Batches, uint32(total))
	if err != nil {
		return 0, err
	}
	defer func() { cleanup() }()
	if err := db.commitManifestTxn(def.Name, []storage.ManifestSegmentAdd{add}, nil); err != nil {
		return 0, err
	}
	cleanup = func() {}
	return int64(total), nil
}

func validateIngestBatches(def sql.BoundTableDef, batches []vector.Batch) error {
	for bi, batch := range batches {
		if batch.Len > vector.StandardBatchRows {
			return fmt.Errorf("ingest: batch %d len %d exceeds StandardBatchRows %d", bi, batch.Len, vector.StandardBatchRows)
		}
		if len(batch.Columns) != len(def.Columns) {
			return fmt.Errorf("ingest: batch %d has %d columns, table %q has %d", bi, len(batch.Columns), def.Name, len(def.Columns))
		}
		for ci, col := range batch.Columns {
			expect := def.Columns[ci]
			if schema.NormalizeName(col.Name) != schema.NormalizeName(expect.Name) {
				return fmt.Errorf("ingest: batch %d column %d name %q, want %q", bi, ci, col.Name, expect.Name)
			}
			if col.Type != expect.Type {
				return fmt.Errorf("ingest: batch %d column %q type %v, want %v", bi, col.Name, col.Type, expect.Type)
			}
			if int(col.V.Len) != batch.Len {
				return fmt.Errorf("ingest: batch %d column %q vec len %d, batch len %d", bi, col.Name, col.V.Len, batch.Len)
			}
			if !expect.Nullable && col.V.Valid != nil && col.V.Valid.NullCount(batch.Len) > 0 {
				return fmt.Errorf("ingest: batch %d column %q carries NULLs into a NOT NULL column", bi, col.Name)
			}
		}
	}
	return nil
}