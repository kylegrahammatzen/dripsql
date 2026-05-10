package engine

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Options struct{}

type Result struct {
	Statements   int
	RowsAffected int64
}

type Rows struct {
	Columns []string
	Values  [][]any
}

type StorageStats struct {
	Table              string        `json:"table"`
	Rows               int64         `json:"rows"`
	Segments           int           `json:"segments"`
	TableBytes         int64         `json:"table_bytes"`
	ColumnPayloadBytes int64         `json:"column_payload_bytes"`
	StorageOverhead    int64         `json:"storage_overhead_bytes"`
	PlainEstimate      int64         `json:"plain_estimate_bytes"`
	ColumnCompression  float64       `json:"column_compression"`
	TableCompression   float64       `json:"table_compression"`
	BytesPerRow        float64       `json:"bytes_per_row"`
	Columns            []ColumnStats `json:"columns,omitempty"`
}

type ColumnStats struct {
	Name        string   `json:"name"`
	TypeName    string   `json:"type"`
	Encodings   []string `json:"encodings"`
	PlainBytes  int64    `json:"plain_bytes"`
	StoredBytes int64    `json:"stored_bytes"`
	Compression float64  `json:"compression"`
}

type DB struct {
	path    string
	data    *storage.Store
	types   map[string]typeEntry
	tables  map[string]tableEntry
	version v3sql.SchemaVersion
	plans   *planCache
	closed  bool
}

type typeEntry struct {
	id   v3sql.TypeID
	spec types.TypeSpec
}

type tableEntry struct {
	id   v3sql.TableID
	spec types.TableSpec
}

func Open(ctx context.Context, path string, opts Options) (*DB, error) {
	_ = opts
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("database path is required")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	typesByName, tablesByName, version, err := loadCatalog(path)
	if err != nil {
		return nil, err
	}
	store, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{path: path, data: store, types: typesByName, tables: tablesByName, version: version, plans: newPlanCache()}, nil
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closed = true
	if db.data != nil {
		return db.data.Close()
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, sqlText string, args ...any) (Result, error) {
	if err := db.checkReady(ctx); err != nil {
		return Result{}, err
	}
	if len(args) != 0 {
		return Result{}, fmt.Errorf("Exec arguments are not supported yet")
	}
	stmts, err := v3sql.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	var result Result
	for _, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		switch stmt := stmt.(type) {
		case *v3sql.CreateTypeStmt:
			plan, err := v3sql.BindCreateType(stmt)
			if err != nil {
				return result, err
			}
			if err := db.CreateType(ctx, plan.Spec); err != nil {
				return result, err
			}
		case *v3sql.CreateTableStmt:
			plan, err := v3sql.BindCreateTable(stmt)
			if err != nil {
				return result, err
			}
			if err := db.CreateTable(ctx, plan.Spec); err != nil {
				return result, err
			}
		case *v3sql.InsertStmt:
			bound, err := db.bindInsert(stmt)
			if err != nil {
				return result, err
			}
			batch, err := batchFromInsert(bound)
			if err != nil {
				return result, err
			}
			entry, _ := db.table(bound.Table)
			if err := db.data.AppendBuffered(ctx, entry.spec, batch); err != nil {
				return result, err
			}
			result.RowsAffected += int64(bound.RowCount)
		default:
			return result, fmt.Errorf("unsupported statement %T", stmt)
		}
		result.Statements++
	}
	return result, nil
}

func (db *DB) Query(ctx context.Context, sqlText string, args ...any) (*Rows, error) {
	if err := db.checkReady(ctx); err != nil {
		return nil, err
	}
	if len(args) != 0 {
		return nil, fmt.Errorf("Query arguments are not supported yet")
	}
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	return db.Execute(ctx, plan)
}

// planForQuery parses and binds sqlText, memoizing the result. The cache is
// keyed on (sql, db.version); CreateType/CreateTable bump db.version so a
// stale plan can never reach Execute.
func (db *DB) planForQuery(sqlText string) (v3sql.Plan, error) {
	if plan, ok := db.plans.get(sqlText, db.version); ok {
		return plan, nil
	}
	stmt, err := v3sql.ParseOne(sqlText)
	if err != nil {
		return nil, err
	}
	plan, err := db.bindQuery(stmt)
	if err != nil {
		return nil, err
	}
	db.plans.put(sqlText, db.version, plan)
	return plan, nil
}

func (db *DB) CreateType(ctx context.Context, spec types.TypeSpec) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	name := normalizeName(spec.Name)
	if _, ok := db.types[name]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("type %q already exists", name)
	}
	db.version++
	spec.Name = name
	db.types[name] = typeEntry{id: v3sql.TypeID(len(db.types) + 1), spec: spec}
	if err := db.saveCatalog(); err != nil {
		delete(db.types, name)
		db.version--
		return err
	}
	return nil
}

func (db *DB) CreateTable(ctx context.Context, spec types.TableSpec) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	if err := db.resolveTableTypes(&spec); err != nil {
		return err
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	name := normalizeName(spec.Name)
	if _, ok := db.tables[name]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", name)
	}
	db.version++
	spec.Name = name
	db.tables[name] = tableEntry{id: v3sql.TableID(len(db.tables) + 1), spec: spec}
	if err := db.saveCatalog(); err != nil {
		delete(db.tables, name)
		db.version--
		return err
	}
	return nil
}

// AppendBuffered clones the batch into the table's ingest buffer.
// Use AppendBufferedOwned for the no-copy fast path when the caller
// is done with the batch.
func (db *DB) AppendBuffered(ctx context.Context, tableName string, batch types.Batch) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	entry, ok := db.table(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}
	return db.data.AppendBuffered(ctx, entry.spec, batch)
}

// AppendBufferedOwned takes ownership of batch — the caller MUST NOT
// mutate or reuse it after this call. Skips the per-Vec slice clone
// AppendBuffered performs.
func (db *DB) AppendBufferedOwned(ctx context.Context, tableName string, batch types.Batch) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	entry, ok := db.table(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}
	return db.data.AppendBufferedOwned(ctx, entry.spec, batch)
}

func (db *DB) FlushBuffered(ctx context.Context, tableName string) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	entry, ok := db.table(tableName)
	if !ok {
		return fmt.Errorf("table %q does not exist", tableName)
	}
	return db.data.FlushBuffered(ctx, entry.spec)
}

func (db *DB) StorageStats(ctx context.Context, tableName string) (StorageStats, error) {
	if err := db.checkReady(ctx); err != nil {
		return StorageStats{}, err
	}
	entry, ok := db.table(tableName)
	if !ok {
		return StorageStats{}, fmt.Errorf("table %q does not exist", tableName)
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
	stats.StorageOverhead = stats.TableBytes - stats.ColumnPayloadBytes
	if stats.StorageOverhead < 0 {
		stats.StorageOverhead = 0
	}
	for _, agg := range colAggs {
		stats.PlainEstimate += agg.plain
	}
	if stats.ColumnPayloadBytes > 0 {
		stats.ColumnCompression = float64(stats.PlainEstimate) / float64(stats.ColumnPayloadBytes)
	}
	if stats.TableBytes > 0 {
		stats.TableCompression = float64(stats.PlainEstimate) / float64(stats.TableBytes)
	}
	if stats.Rows > 0 {
		stats.BytesPerRow = float64(stats.TableBytes) / float64(stats.Rows)
	}
	stats.Columns = make([]ColumnStats, 0, len(entry.spec.Columns))
	for _, col := range entry.spec.Columns {
		agg := colAggs[col.Name]
		cs := ColumnStats{Name: col.Name, TypeName: col.Type.String(), PlainBytes: agg.plain, StoredBytes: agg.stored}
		cs.Encodings = encodingsByDescendingShare(agg.encodings)
		if cs.StoredBytes > 0 {
			cs.Compression = float64(cs.PlainBytes) / float64(cs.StoredBytes)
		}
		stats.Columns = append(stats.Columns, cs)
	}
	return stats, nil
}

// plainBytesForPage returns the bytes a page would have taken under plain
// encoding. For fixed-width kinds it's rows × element bytes. For varlen text
// the only honest baseline is per-page content length: a Flat page already is
// plain, dictionary pages get rows × avg(dict value) + offset overhead, and
// constant pages get rows × constant length. Falls back to a rough 20-byte
// per row only when no page-level signal exists.
func plainBytesForPage(typ types.Type, page storage.PageMeta) int64 {
	rows := int64(page.Rows)
	if typ.Kind != types.KindText && typ.Kind != types.KindBytes && typ.Kind != types.KindJSON {
		return rows * plainColumnBytes(typ)
	}
	if page.Encoding == types.EncodingFlat {
		return int64(page.Length)
	}
	if page.Text != nil && len(page.Text.Values) > 0 {
		var total int64
		for _, v := range page.Text.Values {
			total += int64(len(v))
		}
		avg := total / int64(len(page.Text.Values))
		return rows*(avg+4) + 4
	}
	return rows * 20
}

func encodingsByDescendingShare(byBytes map[string]int64) []string {
	if len(byBytes) == 0 {
		return nil
	}
	type pair struct {
		name  string
		bytes int64
	}
	pairs := make([]pair, 0, len(byBytes))
	for name, bytes := range byBytes {
		pairs = append(pairs, pair{name, bytes})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].bytes > pairs[j].bytes })
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.name
	}
	return out
}

func plainEstimate(table types.TableSpec, rows int64) int64 {
	var perRow int64
	for _, col := range table.Columns {
		perRow += plainColumnBytes(col.Type)
	}
	return rows * perRow
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

func (db *DB) bindQuery(stmt v3sql.Stmt) (v3sql.Plan, error) {
	switch stmt := stmt.(type) {
	case *v3sql.SelectStmt:
		entry, ok := db.table(stmt.Table)
		if !ok {
			return nil, fmt.Errorf("table %q does not exist", stmt.Table)
		}
		return v3sql.BindSelect(stmt, db.boundTable(entry))
	case *v3sql.ExplainStmt:
		selectStmt, ok := stmt.Inner.(*v3sql.SelectStmt)
		if !ok {
			return nil, fmt.Errorf("EXPLAIN supports SELECT only")
		}
		entry, ok := db.table(selectStmt.Table)
		if !ok {
			return nil, fmt.Errorf("table %q does not exist", selectStmt.Table)
		}
		return v3sql.BindExplain(stmt, db.boundTable(entry))
	default:
		return nil, fmt.Errorf("Query only supports SELECT and EXPLAIN statements")
	}
}

func (db *DB) bindInsert(stmt *v3sql.InsertStmt) (v3sql.InsertValues, error) {
	entry, ok := db.table(stmt.Table)
	if !ok {
		return v3sql.InsertValues{}, fmt.Errorf("table %q does not exist", stmt.Table)
	}
	return v3sql.BindInsertValues(stmt, db.boundTable(entry))
}

func (db *DB) boundTable(entry tableEntry) v3sql.BoundTableDef {
	cols := make([]v3sql.BoundColumnDef, len(entry.spec.Columns))
	for i, col := range entry.spec.Columns {
		cols[i] = v3sql.BoundColumnDef{ID: v3sql.ColumnID(i + 1), Name: col.Name, Type: col.Type, Nullable: col.Nullable}
		if col.Type.Kind == types.KindNamed {
			if typ, ok := db.types[normalizeName(col.Type.Name)]; ok {
				cols[i].Labels = append([]string(nil), typ.spec.EnumLabels...)
			}
		}
	}
	return v3sql.BoundTableDef{ID: entry.id, Name: entry.spec.Name, Columns: cols, Options: entry.spec.Options, Version: db.version}
}

func (db *DB) table(name string) (tableEntry, bool) {
	entry, ok := db.tables[normalizeName(name)]
	return entry, ok
}

func (db *DB) resolveTableTypes(spec *types.TableSpec) error {
	spec.Name = normalizeName(spec.Name)
	for i := range spec.Columns {
		col := &spec.Columns[i]
		col.Name = normalizeName(col.Name)
		if col.Type.Kind == types.KindNamed {
			name := normalizeName(col.Type.Name)
			if _, ok := db.types[name]; !ok {
				return fmt.Errorf("unknown type %q", col.Type.Name)
			}
			col.Type = types.Named(name)
		}
	}
	return nil
}

func (db *DB) checkReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil || db.data == nil {
		return fmt.Errorf("database is nil")
	}
	if db.closed {
		return fmt.Errorf("database is closed")
	}
	return nil
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
