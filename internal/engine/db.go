// DB wires the parser/binder, plan cache, exec operators, and storage manifests behind Open/Exec/Query.
// One sync.Mutex serializes catalog and table state. The plan cache carries its own internal lock.
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// Sentinels callers branch on with errors.Is.
var (
	ErrClosed   = errors.New("dripsql: database is closed")
	ErrReadOnly = errors.New("dripsql: database is read-only")
	ErrTxDone   = errors.New("dripsql: transaction already committed or rolled back")
)

type DB struct {
	root          string
	mu            sync.Mutex
	types         map[string]typeEntry
	tables        map[string]tableEntry
	version       sql.SchemaVersion
	plans         *sql.PlanCache
	manifests     map[string]*storage.Manifest
	segCount      map[string]uint64
	segments      *segmentCache
	closed        bool
	lastWriteSpan atomic.Pointer[storage.Span]
	wal           *storage.WAL
	nextTxnID     uint64
	nextCommitTs  atomic.Uint64
	pinnedReadTs  map[uint64]int

	autoRetention atomic.Bool
	retentionLag  atomic.Uint64
	cacheSize     atomic.Int64
	readOnly      atomic.Bool
}

func (db *DB) SetAutoRetention(on bool) { db.autoRetention.Store(on) }

func (db *DB) SetRetentionLag(lag uint64) { db.retentionLag.Store(lag) }

// Values below 1 fall back to the compiled-in default so callers cannot wedge the cache at zero.
func (db *DB) SetCacheSize(n int) {
	if n < 1 {
		n = defaultSegCacheLimit
	}
	db.cacheSize.Store(int64(n))
}

func (db *DB) SetReadOnly(on bool) { db.readOnly.Store(on) }

// Default keeps us well below the Windows default-handle ceiling without thrashing on typical workloads.
const defaultSegCacheLimit = 256

func (db *DB) segCacheLimit() int {
	if n := db.cacheSize.Load(); n > 0 {
		return int(n)
	}
	return defaultSegCacheLimit
}

// Returns the most recent statement's write-phase span tree, or nil. The
// pointer is replaced atomically. The tree it points to is never mutated
// after publish, so concurrent readers are safe.
func (db *DB) LastWriteSpan() *storage.Span {
	return db.lastWriteSpan.Load()
}

func (db *DB) publishWriteSpan(s *storage.Span) {
	if s == nil {
		return
	}
	db.lastWriteSpan.Store(s)
}

type Result struct {
	Statements   int
	RowsAffected int64
}

// Seam between DB's auto-commit path and Tx's staged path; DB binds db.commitManifestTxn, Tx binds tx.commit.
type commitFn func(table string, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error

// lockOpen acquires db.mu and rejects when the DB has been closed; on success the caller must defer db.mu.Unlock.
func (db *DB) lockOpen() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrClosed
	}
	return nil
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

type Rows struct {
	Columns []string
	Values  [][]any
}

func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("engine: database path is required")
	}
	if err := os.MkdirAll(filepath.Join(path, "segments"), 0o755); err != nil {
		return nil, err
	}
	typesByName, tablesByName, version, err := loadCatalog(path)
	if err != nil {
		return nil, err
	}
	db := &DB{
		root:         path,
		types:        typesByName,
		tables:       tablesByName,
		version:      version,
		plans:        sql.NewPlanCache(256),
		manifests:    make(map[string]*storage.Manifest),
		segCount:     make(map[string]uint64),
		pinnedReadTs: make(map[uint64]int),
	}
	db.segments = newSegmentCache(db.segCacheLimit)
	var maxCommitTs uint64
	for name := range tablesByName {
		m, err := db.manifestFor(name)
		if err != nil {
			db.Close()
			return nil, err
		}
		if t := m.MaxCommitTs(); t > maxCommitTs {
			maxCommitTs = t
		}
	}
	db.nextCommitTs.Store(maxCommitTs)
	if err := db.openWAL(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	firstErr := db.segments.close()
	for _, m := range db.manifests {
		if err := m.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.manifests = nil
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		db.wal = nil
	}
	return firstErr
}

func (db *DB) tableDir(name string) string {
	return filepath.Join(db.root, "segments", schema.NormalizeName(name))
}

func (db *DB) manifestFor(name string) (*storage.Manifest, error) {
	key := schema.NormalizeName(name)
	if m, ok := db.manifests[key]; ok {
		return m, nil
	}
	dir := db.tableDir(key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m, err := storage.OpenManifest(filepath.Join(dir, "manifest"))
	if err != nil {
		return nil, err
	}
	db.manifests[key] = m
	view := m.Snapshot()
	for _, entry := range view.Entries {
		base := filepath.Base(entry.Path)
		if id, ok := parseSegmentID(base); ok && id >= db.segCount[key] {
			db.segCount[key] = id + 1
		}
	}
	return m, nil
}

func parseSegmentID(filename string) (uint64, bool) {
	base, ok := strings.CutSuffix(filename, ".dsv4")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseUint(base, 10, 64)
	return id, err == nil
}

func (db *DB) nextSegmentPath(name string) string {
	key := schema.NormalizeName(name)
	id := db.segCount[key]
	db.segCount[key] = id + 1
	return filepath.Join(db.tableDir(key), fmt.Sprintf("%06d.dsv4", id))
}

func (db *DB) table(name string) (tableEntry, error) {
	entry, ok := db.tables[schema.NormalizeName(name)]
	if !ok {
		return tableEntry{}, fmt.Errorf("table %q does not exist", name)
	}
	return entry, nil
}

func columnCodecs(def sql.BoundTableDef) map[string]schema.Encoding {
	var out map[string]schema.Encoding
	for _, c := range def.Columns {
		if c.Codec == schema.EncInvalid {
			continue
		}
		if out == nil {
			out = make(map[string]schema.Encoding, len(def.Columns))
		}
		out[schema.NormalizeName(c.Name)] = c.Codec
	}
	return out
}

func (db *DB) boundTable(entry tableEntry) sql.BoundTableDef {
	cols := make([]sql.BoundColumnDef, len(entry.spec.Columns))
	for i, col := range entry.spec.Columns {
		var labels []string
		if col.Type.Kind == schema.KindNamed {
			if t, ok := db.types[schema.NormalizeName(col.Type.Name)]; ok {
				labels = append([]string(nil), t.spec.EnumLabels...)
			}
		}
		cols[i] = sql.BoundColumnDef{
			ID:       sql.ColumnID(i + 1),
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
			Labels:   labels,
			Codec:    col.Codec,
		}
	}
	return sql.BoundTableDef{
		ID:      entry.id,
		Name:    entry.spec.Name,
		Columns: cols,
		Options: entry.spec.Options,
		Path:    db.tableDir(entry.spec.Name),
		Version: db.version,
	}
}

func (db *DB) boundTableByName(name string) (sql.BoundTableDef, error) {
	entry, err := db.table(name)
	if err != nil {
		return sql.BoundTableDef{}, err
	}
	return db.boundTable(entry), nil
}

func (db *DB) registerType(spec schema.TypeSpec) error {
	key := schema.NormalizeName(spec.Name)
	if _, ok := db.types[key]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("type %q already exists", spec.Name)
	}
	id := sql.TypeID(len(db.types) + 1)
	db.types[key] = typeEntry{id: id, spec: spec}
	db.version++
	if err := saveCatalog(db.root, db.types, db.tables, db.version); err != nil {
		delete(db.types, key)
		db.version--
		return err
	}
	return nil
}

func (db *DB) registerTable(spec schema.TableSpec) error {
	key := schema.NormalizeName(spec.Name)
	if _, ok := db.tables[key]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", spec.Name)
	}
	id := sql.TableID(len(db.tables) + 1)
	db.tables[key] = tableEntry{id: id, spec: spec}
	db.version++
	if err := saveCatalog(db.root, db.types, db.tables, db.version); err != nil {
		delete(db.tables, key)
		db.version--
		return err
	}
	if _, err := db.manifestFor(spec.Name); err != nil {
		delete(db.tables, key)
		db.version--
		_ = saveCatalog(db.root, db.types, db.tables, db.version)
		return err
	}
	return nil
}

func (db *DB) resolveTableTypes(spec *schema.TableSpec) error {
	spec.Name = schema.NormalizeName(spec.Name)
	for i := range spec.Columns {
		col := &spec.Columns[i]
		col.Name = schema.NormalizeName(col.Name)
		if col.Type.Kind == schema.KindNamed {
			name := schema.NormalizeName(col.Type.Name)
			if _, ok := db.types[name]; !ok {
				return fmt.Errorf("unknown type %q", col.Type.Name)
			}
			col.Type = schema.Named(name)
		}
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, sqlText string, args ...any) (Result, error) {
	ctx = ctxOrBackground(ctx)
	stmts, err := sql.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	if err := db.lockOpen(); err != nil {
		return Result{}, err
	}
	defer db.mu.Unlock()
	if db.readOnly.Load() {
		return Result{}, ErrReadOnly
	}
	var result Result
	for _, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		affected, err := db.execStmt(ctx, stmt, args)
		if err != nil {
			return result, err
		}
		result.Statements++
		result.RowsAffected += affected
	}
	return result, nil
}

func (db *DB) execStmt(ctx context.Context, stmt sql.Stmt, args []any) (int64, error) {
	plan, err := db.planner().Plan(stmt)
	if err != nil {
		return 0, err
	}
	plan, err = sql.BindParameters(plan, args)
	if err != nil {
		return 0, err
	}
	switch plan.Kind {
	case sql.PlanCreateType:
		spec := plan.TypeSpec
		spec.Name = schema.NormalizeName(spec.Name)
		if err := spec.Validate(); err != nil {
			return 0, err
		}
		return 0, db.registerType(spec)
	case sql.PlanCreateTable:
		spec := plan.TableSpec
		if err := db.resolveTableTypes(&spec); err != nil {
			return 0, err
		}
		if err := spec.Validate(); err != nil {
			return 0, err
		}
		return 0, db.registerTable(spec)
	case sql.PlanInsert:
		return db.insert(ctx, plan, db.commitManifestTxn)
	case sql.PlanDelete:
		return db.delete(ctx, plan, db.commitManifestTxn)
	case sql.PlanUpdate:
		return db.update(ctx, plan, db.commitManifestTxn)
	}
	return 0, fmt.Errorf("engine: unsupported plan kind %v", plan.Kind)
}

func (db *DB) planner() *sql.Planner {
	return sql.NewPlanner(db.boundTableByName)
}

func (db *DB) Query(ctx context.Context, sqlText string, args ...any) (*Rows, error) {
	ctx = ctxOrBackground(ctx)
	if err := db.lockOpen(); err != nil {
		return nil, err
	}
	defer db.mu.Unlock()
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	bound, err := sql.BindParameters(plan, args)
	if err != nil {
		return nil, err
	}
	return db.runQuery(ctx, bound)
}

func (db *DB) QueryAt(ctx context.Context, sqlText string, readTs uint64, args ...any) (*Rows, error) {
	ctx = ctxOrBackground(ctx)
	if err := db.lockOpen(); err != nil {
		return nil, err
	}
	defer db.mu.Unlock()
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	bound, err := sql.BindParameters(plan, args)
	if err != nil {
		return nil, err
	}
	return db.runQueryWith(ctx, bound, func(d sql.BoundTableDef) ([]*storage.Segment, error) {
		ts := readTs
		if d.AsOf != 0 {
			ts = d.AsOf
		}
		return db.openSegmentsAt(d.Name, ts)
	})
}

// planForQuery resolves a SELECT into a bound *Plan. Hits the plan cache before parsing so
// repeated queries skip the AST + binder allocations entirely. db.version invalidates stale
// plans when the catalog changes, so a cache hit is still safe.
func (db *DB) planForQuery(sqlText string) (*sql.Plan, error) {
	if cached, ok := db.plans.Get(sqlText, db.version); ok {
		return cached, nil
	}
	stmt, err := sql.ParseOne(sqlText)
	if err != nil {
		return nil, err
	}
	switch stmt.(type) {
	case *sql.SelectStmt, *sql.ExplainStmt:
	default:
		return nil, fmt.Errorf("engine: Query supports SELECT or EXPLAIN only")
	}
	plan, err := db.planner().Plan(stmt)
	if err != nil {
		return nil, err
	}
	db.plans.Put(sqlText, db.version, plan)
	return plan, nil
}
