package engine

import (
	"context"
	"sort"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// StorageStats reports raw on-disk measurements for a table. Derived numbers
// (compression ratios, overhead, bytes/row) live at the call site so we
// aren't deciding how to spell them inside the engine.
type StorageStats struct {
	Table              string        `json:"table"`
	Rows               int64         `json:"rows"`
	Segments           int           `json:"segments"`
	TableBytes         int64         `json:"table_bytes"`
	ColumnPayloadBytes int64         `json:"column_payload_bytes"`
	PlainEstimate      int64         `json:"plain_estimate_bytes"`
	Columns            []ColumnStats `json:"columns,omitempty"`
}

type ColumnStats struct {
	Name        string   `json:"name"`
	TypeName    string   `json:"type"`
	Encodings   []string `json:"encodings"`
	PlainBytes  int64    `json:"plain_bytes"`
	StoredBytes int64    `json:"stored_bytes"`
}

type ReadStats = storage.ReadStats
type ByteCacheStats = storage.SegmentByteCacheStats

func (db *DB) ReadStats() ReadStats {
	return db.data.ReadStats()
}

func (db *DB) ByteCacheStats() ByteCacheStats {
	return db.data.ByteCacheStats()
}

func (db *DB) ClearByteCache() {
	if db == nil || db.data == nil {
		return
	}
	db.data.ClearByteCache()
}

func ResetReadStats() {
	storage.ResetReadStats()
}

func (db *DB) StorageStats(ctx context.Context, tableName string) (StorageStats, error) {
	entry, err := db.tableForOp(ctx, tableName)
	if err != nil {
		return StorageStats{}, err
	}
	if err := db.data.FlushBuffered(ctx, entry.spec); err != nil {
		return StorageStats{}, err
	}
	segments, err := db.data.ScanSegments(ctx, entry.spec)
	if err != nil {
		return StorageStats{}, err
	}
	stats := StorageStats{Table: entry.spec.Name, Segments: len(segments)}
	type colAgg struct {
		stored    int64
		plain     int64
		encodings map[string]int64
	}
	colAggs := make(map[string]*colAgg, len(entry.spec.Columns))
	for _, col := range entry.spec.Columns {
		colAggs[col.Name] = &colAgg{encodings: make(map[string]int64)}
	}
	for _, segment := range segments {
		stats.Rows += int64(segment.Meta.Rows)
		stats.TableBytes += segment.Size
		for _, col := range segment.Meta.Columns {
			agg, ok := colAggs[col.Name]
			if !ok {
				continue
			}
			for _, page := range col.Pages {
				stats.ColumnPayloadBytes += int64(page.Length)
				agg.stored += int64(page.Length)
				agg.encodings[page.Encoding.String()] += int64(page.Length)
				agg.plain += plainBytesForPage(col.Type, page)
			}
		}
	}
	stats.Columns = make([]ColumnStats, 0, len(entry.spec.Columns))
	for _, col := range entry.spec.Columns {
		agg := colAggs[col.Name]
		stats.PlainEstimate += agg.plain
		stats.Columns = append(stats.Columns, ColumnStats{
			Name:        col.Name,
			TypeName:    col.Type.String(),
			Encodings:   encodingsByDescendingShare(agg.encodings),
			PlainBytes:  agg.plain,
			StoredBytes: agg.stored,
		})
	}
	return stats, nil
}

// plainBytesForPage returns what the page would have taken under plain
// encoding. Varlen pages carry data-byte stats so dict/compressed pages can
// still report an honest plain baseline; without them we fall back to a flat
// 20 bytes/row guess.
func plainBytesForPage(typ types.Type, page storage.PageMeta) int64 {
	rows := int64(page.Rows)
	if typ.Kind != types.KindText && typ.Kind != types.KindBytes && typ.Kind != types.KindJSON {
		return rows * plainColumnBytes(typ)
	}
	if page.Text != nil {
		validityBytes := int64(0)
		if page.NullCount != 0 {
			validityBytes = int64(types.ValidityWords(int(page.Rows)) * 8)
		}
		return int64(page.Text.DataBytes) + rows*4 + 4 + validityBytes
	}
	if page.Encoding == types.EncodingFlat {
		return int64(page.Length)
	}
	return rows * 20
}

func encodingsByDescendingShare(byBytes map[string]int64) []string {
	out := make([]string, 0, len(byBytes))
	for name := range byBytes {
		out = append(out, name)
	}
	sort.Slice(out, func(i, j int) bool { return byBytes[out[i]] > byBytes[out[j]] })
	return out
}

func plainColumnBytes(typ types.Type) int64 {
	switch typ.Kind {
	case types.KindBool:
		return 1
	case types.KindInt16:
		return 2
	case types.KindInt32, types.KindDate, types.KindFloat32, types.KindNamed:
		return 4
	case types.KindInt64, types.KindDecimal, types.KindTimestamp, types.KindTime, types.KindFloat64:
		return 8
	case types.KindUUID:
		return 16
	case types.KindText, types.KindBytes, types.KindJSON:
		return 20
	default:
		return 0
	}
}
