// DB wires the parser/binder, plan cache, exec operators, and storage manifests behind Open/Exec/Query.
// One sync.Mutex serializes catalog and table state. The plan cache carries its own internal lock.
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// Sentinels callers branch on with errors.Is.
var (
	ErrClosed   = errors.New("database is closed")
	ErrReadOnly = errors.New("database is read-only")
	ErrTxDone   = errors.New("transaction already committed or rolled back")
)

type DB struct {
	root          string
	mu            sync.Mutex
	catalog       *catalog.File
	types         map[string]*catalog.Type
	tables        map[string]*catalog.Table
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
	file, err := catalog.Load(path)
	if err != nil {
		return nil, err
	}
	db := &DB{
		root:         path,
		catalog:      file,
		types:        indexTypes(file),
		tables:       indexTables(file),
		version:      sql.SchemaVersion(file.Generation),
		plans:        sql.NewPlanCache(256),
		manifests:    make(map[string]*storage.Manifest),
		segCount:     make(map[string]uint64),
		pinnedReadTs: make(map[uint64]int),
	}
	db.segments = newSegmentCache(db.segCacheLimit)
	var maxCommitTs uint64
	for name := range db.tables {
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
	key := schema.NormalizeName(name)
	if t, ok := db.tables[key]; ok && !t.LegacyPath {
		return filepath.Join(db.root, "tables", fmt.Sprintf("%016x", uint64(t.TableID)))
	}
	return filepath.Join(db.root, "segments", key)
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

func (db *DB) table(name string) (*catalog.Table, error) {
	entry, ok := db.tables[schema.NormalizeName(name)]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	return entry, nil
}

type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func (db *DB) Tables() []string {
	if err := db.lockOpen(); err != nil {
		return nil
	}
	defer db.mu.Unlock()
	out := make([]string, 0, len(db.tables))
	for _, e := range db.tables {
		out = append(out, e.Name)
	}
	sort.Strings(out)
	return out
}

func (db *DB) TableSchema(name string) ([]ColumnInfo, error) {
	if err := db.lockOpen(); err != nil {
		return nil, err
	}
	defer db.mu.Unlock()
	entry, ok := db.tables[schema.NormalizeName(name)]
	if !ok {
		return nil, fmt.Errorf("table %q does not exist", name)
	}
	out := make([]ColumnInfo, 0, len(entry.Columns))
	for _, c := range entry.Columns {
		if c.DroppedAtGeneration != nil {
			continue
		}
		out = append(out, ColumnInfo{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
	}
	return out, nil
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

func (db *DB) boundTable(entry *catalog.Table) sql.BoundTableDef {
	cols := make([]sql.BoundColumnDef, 0, len(entry.Columns))
	for _, col := range entry.Columns {
		if col.DroppedAtGeneration != nil {
			continue
		}
		t, err := schema.ParseType(col.Type)
		if err != nil {
			continue
		}
		var labels []string
		if t.Kind == schema.KindNamed {
			if tt, ok := db.types[schema.NormalizeName(t.Name)]; ok {
				labels = append([]string(nil), tt.Labels...)
			}
		}
		cols = append(cols, sql.BoundColumnDef{
			ID:       sql.ColumnID(col.ColumnID),
			Ordinal:  col.Ordinal,
			Name:     col.Name,
			Type:     t,
			Nullable: col.Nullable,
			Labels:   labels,
			Codec:    codecForColumn(entry.StoragePolicy, col.ColumnID),
		})
	}
	return sql.BoundTableDef{
		ID:      sql.TableID(entry.TableID),
		Name:    entry.Name,
		Columns: cols,
		Options: optionsFromPolicy(entry.StoragePolicy, entry.Columns),
		Path:    db.tableDir(entry.Name),
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

func (db *DB) registerType(spec schema.TypeSpec, ifNotExists bool) error {
	key := schema.NormalizeName(spec.Name)
	if _, ok := db.types[key]; ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("type %q already exists", spec.Name)
	}
	newGen := db.catalog.Generation + 1
	newType := catalog.Type{
		TypeID:              db.catalog.NextTypeID,
		Name:                spec.Name,
		Kind:                "enum",
		Labels:              append([]string{}, spec.EnumLabels...),
		CreatedAtGeneration: newGen,
		UpdatedAtGeneration: newGen,
	}
	db.catalog.Types = append(db.catalog.Types, newType)
	db.catalog.NextTypeID++
	db.catalog.Generation = newGen
	db.types[key] = &db.catalog.Types[len(db.catalog.Types)-1]
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		db.catalog.Types = db.catalog.Types[:len(db.catalog.Types)-1]
		db.catalog.NextTypeID--
		db.catalog.Generation--
		delete(db.types, key)
		db.types = indexTypes(db.catalog)
		db.version = sql.SchemaVersion(db.catalog.Generation)
		return err
	}
	return nil
}

func (db *DB) registerTable(spec schema.TableSpec, ifNotExists bool) error {
	key := schema.NormalizeName(spec.Name)
	if _, ok := db.tables[key]; ok {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", spec.Name)
	}
	newGen := db.catalog.Generation + 1
	tab, err := buildTable(db.catalog, spec, newGen)
	if err != nil {
		return err
	}
	db.catalog.Tables = append(db.catalog.Tables, tab)
	db.catalog.NextTableID++
	db.catalog.Generation = newGen
	db.tables[key] = &db.catalog.Tables[len(db.catalog.Tables)-1]
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		db.catalog.Tables = db.catalog.Tables[:len(db.catalog.Tables)-1]
		db.catalog.NextTableID--
		db.catalog.Generation--
		delete(db.tables, key)
		db.tables = indexTables(db.catalog)
		db.version = sql.SchemaVersion(db.catalog.Generation)
		return err
	}
	if _, err := db.manifestFor(spec.Name); err != nil {
		db.catalog.Tables = db.catalog.Tables[:len(db.catalog.Tables)-1]
		db.catalog.NextTableID--
		db.catalog.Generation--
		delete(db.tables, key)
		db.tables = indexTables(db.catalog)
		db.version = sql.SchemaVersion(db.catalog.Generation)
		_ = catalog.Save(db.root, db.catalog)
		return err
	}
	return nil
}

func (db *DB) alterTable(p *sql.AlterPayload) error {
	if p == nil {
		return fmt.Errorf("ALTER TABLE: nil payload")
	}
	tab, ok := db.tables[schema.NormalizeName(p.Table)]
	if !ok {
		return fmt.Errorf("table %q does not exist", p.Table)
	}
	switch {
	case p.Rename != nil:
		return db.renameColumn(tab, p)
	case p.Add != nil:
		return db.addColumn(tab, p.Add)
	case p.Drop != nil:
		return db.dropColumn(tab, p.Drop)
	case p.SetType != nil:
		return db.alterColumnType(tab, p.SetType)
	}
	return fmt.Errorf("ALTER TABLE: unsupported operation")
}

// wideningCasts enumerates the type widenings the scan path can perform on read. Any
// other change requires a segment rewrite and is rejected here.
var wideningCasts = map[[2]schema.Kind]struct{}{
	{schema.KindInt32, schema.KindInt64}:     {},
	{schema.KindFloat32, schema.KindFloat64}: {},
}

func (db *DB) renameColumn(tab *catalog.Table, p *sql.AlterPayload) error {
	from := schema.NormalizeName(p.Rename.From)
	to := schema.NormalizeName(p.Rename.To)
	var target *catalog.Column
	for i := range tab.Columns {
		c := &tab.Columns[i]
		if c.DroppedAtGeneration != nil {
			continue
		}
		if schema.NormalizeName(c.Name) == to {
			return fmt.Errorf("column %q already exists in table %q", p.Rename.To, p.Table)
		}
		if schema.NormalizeName(c.Name) == from {
			target = c
		}
	}
	if target == nil {
		return fmt.Errorf("column %q does not exist in table %q", p.Rename.From, p.Table)
	}
	prevName := target.Name
	prevGen := db.catalog.Generation
	prevSchemaVersion := tab.SchemaVersion
	prevUpdated := tab.UpdatedAtGeneration
	newGen := prevGen + 1
	target.Name = to
	tab.SchemaVersion++
	tab.UpdatedAtGeneration = newGen
	db.catalog.Generation = newGen
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		target.Name = prevName
		tab.SchemaVersion = prevSchemaVersion
		tab.UpdatedAtGeneration = prevUpdated
		db.catalog.Generation = prevGen
		db.version = sql.SchemaVersion(prevGen)
		return err
	}
	return nil
}

func (db *DB) addColumn(tab *catalog.Table, p *sql.AlterAddColumn) error {
	name := schema.NormalizeName(p.Name)
	for i := range tab.Columns {
		c := &tab.Columns[i]
		if c.DroppedAtGeneration != nil {
			continue
		}
		if schema.NormalizeName(c.Name) == name {
			return fmt.Errorf("column %q already exists in table %q", p.Name, tab.Name)
		}
	}
	typ, err := schema.ParseType(p.Type)
	if err != nil {
		return fmt.Errorf("ALTER TABLE ADD COLUMN: %w", err)
	}
	if typ.Kind == schema.KindNamed {
		if _, ok := db.types[schema.NormalizeName(typ.Name)]; !ok {
			return fmt.Errorf("ALTER TABLE ADD COLUMN: unknown type %q", typ.Name)
		}
	}
	typeStr, err := schema.TypeString(typ)
	if err != nil {
		return err
	}
	prevGen := db.catalog.Generation
	prevSchemaVersion := tab.SchemaVersion
	prevUpdated := tab.UpdatedAtGeneration
	prevNextColumnID := tab.NextColumnID
	prevCols := tab.Columns
	newGen := prevGen + 1
	colID := tab.NextColumnID
	ordinal := 0
	for _, c := range tab.Columns {
		if c.DroppedAtGeneration == nil && c.Ordinal >= ordinal {
			ordinal = c.Ordinal + 1
		}
	}
	tab.Columns = append(tab.Columns, catalog.Column{
		ColumnID:          colID,
		Name:              name,
		Type:              typeStr,
		Nullable:          true,
		Ordinal:           ordinal,
		AddedAtGeneration: newGen,
	})
	tab.NextColumnID = colID + 1
	tab.SchemaVersion++
	tab.UpdatedAtGeneration = newGen
	db.catalog.Generation = newGen
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		tab.Columns = prevCols
		tab.NextColumnID = prevNextColumnID
		tab.SchemaVersion = prevSchemaVersion
		tab.UpdatedAtGeneration = prevUpdated
		db.catalog.Generation = prevGen
		db.version = sql.SchemaVersion(prevGen)
		return err
	}
	return nil
}

func (db *DB) dropColumn(tab *catalog.Table, p *sql.AlterDropColumn) error {
	name := schema.NormalizeName(p.Name)
	var target *catalog.Column
	activeCount := 0
	for i := range tab.Columns {
		c := &tab.Columns[i]
		if c.DroppedAtGeneration != nil {
			continue
		}
		activeCount++
		if schema.NormalizeName(c.Name) == name {
			target = c
		}
	}
	if target == nil {
		return fmt.Errorf("column %q does not exist in table %q", p.Name, tab.Name)
	}
	if activeCount <= 1 {
		return fmt.Errorf("cannot drop the last active column %q in table %q", p.Name, tab.Name)
	}
	for _, id := range tab.PrimaryKey {
		if id == target.ColumnID {
			return fmt.Errorf("cannot drop column %q referenced by primary key", p.Name)
		}
	}
	for _, idx := range tab.Indexes {
		for _, id := range idx.Columns {
			if id == target.ColumnID {
				return fmt.Errorf("cannot drop column %q referenced by index %q", p.Name, idx.Name)
			}
		}
	}
	prevGen := db.catalog.Generation
	prevSchemaVersion := tab.SchemaVersion
	prevUpdated := tab.UpdatedAtGeneration
	prevDropped := target.DroppedAtGeneration
	newGen := prevGen + 1
	target.DroppedAtGeneration = &newGen
	tab.SchemaVersion++
	tab.UpdatedAtGeneration = newGen
	db.catalog.Generation = newGen
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		target.DroppedAtGeneration = prevDropped
		tab.SchemaVersion = prevSchemaVersion
		tab.UpdatedAtGeneration = prevUpdated
		db.catalog.Generation = prevGen
		db.version = sql.SchemaVersion(prevGen)
		return err
	}
	return nil
}

func (db *DB) alterColumnType(tab *catalog.Table, p *sql.AlterColumnType) error {
	name := schema.NormalizeName(p.Name)
	var target *catalog.Column
	for i := range tab.Columns {
		c := &tab.Columns[i]
		if c.DroppedAtGeneration != nil {
			continue
		}
		if schema.NormalizeName(c.Name) == name {
			target = c
			break
		}
	}
	if target == nil {
		return fmt.Errorf("column %q does not exist in table %q", p.Name, tab.Name)
	}
	newType, err := schema.ParseType(p.Type)
	if err != nil {
		return fmt.Errorf("ALTER COLUMN: %w", err)
	}
	if newType.Kind == schema.KindNamed {
		if _, ok := db.types[schema.NormalizeName(newType.Name)]; !ok {
			return fmt.Errorf("ALTER COLUMN: unknown named type %q", newType.Name)
		}
	}
	oldType, err := schema.ParseType(target.Type)
	if err != nil {
		return fmt.Errorf("ALTER COLUMN: existing type %q: %w", target.Type, err)
	}
	if newType == oldType {
		return nil
	}
	if _, ok := wideningCasts[[2]schema.Kind{oldType.Kind, newType.Kind}]; !ok {
		return fmt.Errorf("ALTER COLUMN: cast from %s to %s is not a supported widening", oldType, newType)
	}
	newTypeStr, err := schema.TypeString(newType)
	if err != nil {
		return err
	}
	prevType := target.Type
	prevGen := db.catalog.Generation
	prevSchemaVersion := tab.SchemaVersion
	prevUpdated := tab.UpdatedAtGeneration
	newGen := prevGen + 1
	target.Type = newTypeStr
	tab.SchemaVersion++
	tab.UpdatedAtGeneration = newGen
	db.catalog.Generation = newGen
	db.version = sql.SchemaVersion(newGen)
	if err := catalog.Save(db.root, db.catalog); err != nil {
		target.Type = prevType
		tab.SchemaVersion = prevSchemaVersion
		tab.UpdatedAtGeneration = prevUpdated
		db.catalog.Generation = prevGen
		db.version = sql.SchemaVersion(prevGen)
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
		return 0, db.registerType(spec, plan.IfNotExists)
	case sql.PlanCreateTable:
		spec := plan.TableSpec
		if err := db.resolveTableTypes(&spec); err != nil {
			return 0, err
		}
		if err := spec.Validate(); err != nil {
			return 0, err
		}
		return 0, db.registerTable(spec, plan.IfNotExists)
	case sql.PlanAlterTable:
		return 0, db.alterTable(plan.Alter)
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
