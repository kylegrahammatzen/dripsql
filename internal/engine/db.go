// DB wires the parser/binder, plan cache, exec operators, and storage manifests behind Open/Exec/Query.
// One sync.Mutex serializes catalog and table state. The plan cache carries its own internal lock.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type DB struct {
	root      string
	mu        sync.Mutex
	types     map[string]typeEntry
	tables    map[string]tableEntry
	version   sql.SchemaVersion
	plans     *sql.PlanCache
	manifests map[string]*storage.Manifest
	segCount  map[string]uint64
	segCache  map[segCacheKey]*storage.Segment
	closed    bool
}

type segCacheKey struct {
	path   string
	dvPath string
}

type Result struct {
	Statements   int
	RowsAffected int64
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
		root:      path,
		types:     typesByName,
		tables:    tablesByName,
		version:   version,
		plans:     sql.NewPlanCache(256),
		manifests: make(map[string]*storage.Manifest),
		segCount:  make(map[string]uint64),
		segCache:  make(map[segCacheKey]*storage.Segment),
	}
	for name := range tablesByName {
		if _, err := db.manifestFor(name); err != nil {
			db.Close()
			return nil, err
		}
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
	var firstErr error
	for _, s := range db.segCache {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.segCache = nil
	for _, m := range db.manifests {
		if err := m.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.manifests = nil
	return firstErr
}

func (db *DB) tableDir(name string) string {
	return filepath.Join(db.root, "segments", types.NormalizeName(name))
}

func (db *DB) manifestFor(name string) (*storage.Manifest, error) {
	key := types.NormalizeName(name)
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
	key := types.NormalizeName(name)
	id := db.segCount[key]
	db.segCount[key] = id + 1
	return filepath.Join(db.tableDir(key), fmt.Sprintf("%06d.dsv4", id))
}

func (db *DB) table(name string) (tableEntry, error) {
	entry, ok := db.tables[types.NormalizeName(name)]
	if !ok {
		return tableEntry{}, fmt.Errorf("table %q does not exist", name)
	}
	return entry, nil
}

func columnCodecs(def sql.BoundTableDef) map[string]types.Encoding {
	var out map[string]types.Encoding
	for _, c := range def.Columns {
		if c.Codec == types.EncodingAuto {
			continue
		}
		if out == nil {
			out = make(map[string]types.Encoding, len(def.Columns))
		}
		out[types.NormalizeName(c.Name)] = c.Codec
	}
	return out
}

func (db *DB) boundTable(entry tableEntry) sql.BoundTableDef {
	cols := make([]sql.BoundColumnDef, len(entry.spec.Columns))
	for i, col := range entry.spec.Columns {
		var labels []string
		if col.Type.Kind == types.KindNamed {
			if t, ok := db.types[types.NormalizeName(col.Type.Name)]; ok {
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

func (db *DB) registerType(spec types.TypeSpec) error {
	key := types.NormalizeName(spec.Name)
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

func (db *DB) registerTable(spec types.TableSpec) error {
	key := types.NormalizeName(spec.Name)
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

func (db *DB) resolveTableTypes(spec *types.TableSpec) error {
	spec.Name = types.NormalizeName(spec.Name)
	for i := range spec.Columns {
		col := &spec.Columns[i]
		col.Name = types.NormalizeName(col.Name)
		if col.Type.Kind == types.KindNamed {
			name := types.NormalizeName(col.Type.Name)
			if _, ok := db.types[name]; !ok {
				return fmt.Errorf("unknown type %q", col.Type.Name)
			}
			col.Type = types.Named(name)
		}
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, sqlText string) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("engine: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stmts, err := sql.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return Result{}, fmt.Errorf("engine: database is closed")
	}
	var result Result
	for _, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		affected, err := db.execStmt(ctx, stmt)
		if err != nil {
			return result, err
		}
		result.Statements++
		result.RowsAffected += affected
	}
	return result, nil
}

func (db *DB) execStmt(ctx context.Context, stmt sql.Stmt) (int64, error) {
	plan, err := db.planner().Plan(stmt)
	if err != nil {
		return 0, err
	}
	switch plan.Kind {
	case sql.PlanCreateType:
		spec := plan.TypeSpec
		spec.Name = types.NormalizeName(spec.Name)
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
		return db.insert(ctx, plan)
	case sql.PlanDelete:
		return db.delete(ctx, plan)
	case sql.PlanUpdate:
		return db.update(ctx, plan)
	}
	return 0, fmt.Errorf("engine: unsupported plan kind %v", plan.Kind)
}

func (db *DB) planner() *sql.Planner {
	return sql.NewPlanner(db.boundTableByName)
}

func (db *DB) Query(ctx context.Context, sqlText string) (*Rows, error) {
	if db == nil {
		return nil, fmt.Errorf("engine: nil DB")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, fmt.Errorf("engine: database is closed")
	}
	plan, err := db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	return db.runQuery(ctx, plan)
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
