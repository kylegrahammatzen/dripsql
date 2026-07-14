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

	total := uint64(0)
	for _, b := range cfg.Batches {
		total += uint64(b.Len)
	}

	root := storage.NewSpan("Ingest " + def.Name)
	add, cleanup, err := db.writeSegmentAdd(def, cfg.Batches, uint32(total), root)
	root.End()
	db.publishWriteSpan(root)
	if err != nil {
		return int64(total), err
	}
	defer func() { cleanup() }()
	if err := db.commitManifestTxn(def.Name, []storage.ManifestSegmentAdd{add}, nil); err != nil {
		return int64(total), err
	}
	cleanup = func() {}
	return int64(total), nil
}

func validateIngestBatches(def sql.BoundTableDef, batches []vector.Batch) error {
	if len(batches) == 0 {
		return nil
	}
	first := batches[0]
	if len(first.Columns) != len(def.Columns) {
		return fmt.Errorf("ingest: batch has %d columns, table %q has %d", len(first.Columns), def.Name, len(def.Columns))
	}
	for i, col := range first.Columns {
		expect := def.Columns[i]
		if schema.NormalizeName(col.Name) != schema.NormalizeName(expect.Name) {
			return fmt.Errorf("ingest: column %d name %q != expected %q", i, col.Name, expect.Name)
		}
		if col.Type != expect.Type {
			return fmt.Errorf("ingest: column %q type %v != expected %v", col.Name, col.Type, expect.Type)
		}
		if int(col.V.Len) != first.Len {
			return fmt.Errorf("ingest: column %q vec len %d != batch len %d", col.Name, col.V.Len, first.Len)
		}
		if first.Len > vector.StandardBatchRows {
			return fmt.Errorf("ingest: batch len %d exceeds StandardBatchRows %d", first.Len, vector.StandardBatchRows)
		}
	}
	// Subsequent batches: same shape check (column count, names, types).
	// Row counts can vary per batch (up to StandardBatchRows).
	for bi, batch := range batches[1:] {
		if len(batch.Columns) != len(first.Columns) {
			return fmt.Errorf("ingest: batch %d has %d columns, want %d", bi+1, len(batch.Columns), len(first.Columns))
		}
		for ci, col := range batch.Columns {
			expect := first.Columns[ci]
			if schema.NormalizeName(col.Name) != schema.NormalizeName(expect.Name) {
				return fmt.Errorf("ingest: batch %d column %d name %q != %q", bi+1, ci, col.Name, expect.Name)
			}
			if col.Type != expect.Type {
				return fmt.Errorf("ingest: batch %d column %q type %v != %v", bi+1, col.Name, col.Type, expect.Type)
			}
			if int(col.V.Len) != batch.Len {
				return fmt.Errorf("ingest: batch %d column %q vec len %d != batch len %d", bi+1, col.Name, col.V.Len, batch.Len)
			}
		}
		if batch.Len > vector.StandardBatchRows {
			return fmt.Errorf("ingest: batch %d len %d exceeds StandardBatchRows %d", bi+1, batch.Len, vector.StandardBatchRows)
		}
	}
	return nil
}