// DB wires the parser/binder, plan cache, exec operators, and storage manifests behind Open/Exec/Query.
// One sync.Mutex serializes catalog and table state. The plan cache carries its own internal lock.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
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

func (db *DB) checkOpen() error {
	if db == nil {
		return fmt.Errorf("engine: nil DB")
	}
	if db.closed {
		return fmt.Errorf("engine: database is closed")
	}
	return nil
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

func primaryFromSelect(stmt *sql.SelectStmt) (name string, joined bool, err error) {
	node := stmt.From
	joined = false
	for {
		switch n := node.(type) {
		case *sql.TableName:
			return n.Name, joined, nil
		case *sql.JoinExpr:
			joined = true
			node = n.Left
		default:
			return "", false, fmt.Errorf("engine: unsupported FROM node %T", node)
		}
	}
}

func parseSegmentID(filename string) (uint64, bool) {
	ext := filepath.Ext(filename)
	if ext != ".dsv4" {
		return 0, false
	}
	var id uint64
	if _, err := fmt.Sscanf(filename, "%d.dsv4", &id); err != nil {
		return 0, false
	}
	return id, true
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
	if err := db.checkOpen(); err != nil {
		return Result{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stmts, err := sql.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	var result Result
	db.mu.Lock()
	defer db.mu.Unlock()
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
	if err := db.checkOpen(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.mu.Lock()
	plan, def, err := db.planAndDef(sqlText)
	db.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return db.runQuery(ctx, plan, def)
}

func (db *DB) planAndDef(sqlText string) (*sql.Plan, sql.BoundTableDef, error) {
	stmt, err := sql.ParseOne(sqlText)
	if err != nil {
		return nil, sql.BoundTableDef{}, err
	}
	selectStmt, ok := stmt.(*sql.SelectStmt)
	if !ok {
		return nil, sql.BoundTableDef{}, fmt.Errorf("engine: Query supports SELECT only")
	}
	primaryName, _, err := primaryFromSelect(selectStmt)
	if err != nil {
		return nil, sql.BoundTableDef{}, err
	}
	def, err := db.boundTableByName(primaryName)
	if err != nil {
		return nil, sql.BoundTableDef{}, err
	}
	if cached, ok := db.plans.Get(sqlText, db.version); ok {
		return cached, def, nil
	}
	plan, err := db.planner().Plan(selectStmt)
	if err != nil {
		return nil, sql.BoundTableDef{}, err
	}
	db.plans.Put(sqlText, db.version, plan)
	return plan, def, nil
}
