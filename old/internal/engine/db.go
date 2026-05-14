package engine

import (
	"context"
	"fmt"
	"os"
	"strings"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Result struct {
	Statements   int
	RowsAffected int64
}

type Rows struct {
	Columns []string
	Values  [][]any
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

func Open(ctx context.Context, path string) (*DB, error) {
	ctx = readyContext(ctx)
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
		affected, err := db.execStmt(ctx, stmt)
		if err != nil {
			return result, err
		}
		result.Statements++
		result.RowsAffected += affected
	}
	return result, nil
}

func (db *DB) execStmt(ctx context.Context, stmt v3sql.Stmt) (int64, error) {
	switch stmt := stmt.(type) {
	case *v3sql.CreateTypeStmt:
		plan, err := v3sql.BindCreateType(stmt)
		if err != nil {
			return 0, err
		}
		return 0, db.CreateType(ctx, plan.Spec)
	case *v3sql.CreateTableStmt:
		plan, err := v3sql.BindCreateTable(stmt)
		if err != nil {
			return 0, err
		}
		return 0, db.CreateTable(ctx, plan.Spec)
	case *v3sql.InsertStmt:
		return db.execInsert(ctx, stmt)
	default:
		return 0, fmt.Errorf("unsupported statement %T", stmt)
	}
}

func (db *DB) execInsert(ctx context.Context, stmt *v3sql.InsertStmt) (int64, error) {
	bound, err := db.bindInsert(stmt)
	if err != nil {
		return 0, err
	}
	batch, err := batchFromInsert(bound)
	if err != nil {
		return 0, err
	}
	entry, err := db.table(bound.Table)
	if err != nil {
		return 0, err
	}
	if err := db.data.AppendBuffered(ctx, entry.spec, batch); err != nil {
		return 0, err
	}
	return int64(bound.RowCount), nil
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
	spec.Name = normalizeName(spec.Name)
	if err := spec.Validate(); err != nil {
		return err
	}
	return db.registerType(spec)
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
	return db.registerTable(spec)
}

func (db *DB) registerType(spec types.TypeSpec) error {
	if _, ok := db.types[spec.Name]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("type %q already exists", spec.Name)
	}
	id := v3sql.TypeID(len(db.types) + 1)
	db.version++
	db.types[spec.Name] = typeEntry{id: id, spec: spec}
	if err := db.saveCatalog(); err != nil {
		delete(db.types, spec.Name)
		db.version--
		return err
	}
	return nil
}

func (db *DB) registerTable(spec types.TableSpec) error {
	if _, ok := db.tables[spec.Name]; ok {
		if spec.IfNotExists {
			return nil
		}
		return fmt.Errorf("table %q already exists", spec.Name)
	}
	id := v3sql.TableID(len(db.tables) + 1)
	db.version++
	db.tables[spec.Name] = tableEntry{id: id, spec: spec}
	if err := db.saveCatalog(); err != nil {
		delete(db.tables, spec.Name)
		db.version--
		return err
	}
	return nil
}

// tableForOp resolves a table through checkReady + lookup. Append*/Flush*
// methods share this entry path.
func (db *DB) tableForOp(ctx context.Context, name string) (tableEntry, error) {
	if err := db.checkReady(ctx); err != nil {
		return tableEntry{}, err
	}
	return db.table(name)
}

// boundTableForName looks up a table and returns its BoundTableDef.
func (db *DB) boundTableForName(name string) (v3sql.BoundTableDef, error) {
	entry, err := db.table(name)
	if err != nil {
		return v3sql.BoundTableDef{}, err
	}
	return db.boundTable(entry), nil
}

// AppendBuffered clones the batch into the table's ingest buffer. Use
// AppendBufferedOwned for the no-copy fast path when the caller is done with
// the batch.
func (db *DB) AppendBuffered(ctx context.Context, tableName string, batch types.Batch) error {
	entry, err := db.tableForOp(ctx, tableName)
	if err != nil {
		return err
	}
	return db.data.AppendBuffered(ctx, entry.spec, batch)
}

// AppendBufferedOwned takes ownership of batch — the caller MUST NOT mutate
// or reuse it after this call.
func (db *DB) AppendBufferedOwned(ctx context.Context, tableName string, batch types.Batch) error {
	entry, err := db.tableForOp(ctx, tableName)
	if err != nil {
		return err
	}
	return db.data.AppendBufferedOwned(ctx, entry.spec, batch)
}

func (db *DB) FlushBuffered(ctx context.Context, tableName string) error {
	entry, err := db.tableForOp(ctx, tableName)
	if err != nil {
		return err
	}
	return db.data.FlushBuffered(ctx, entry.spec)
}

func (db *DB) bindQuery(stmt v3sql.Stmt) (v3sql.Plan, error) {
	switch stmt := stmt.(type) {
	case *v3sql.SelectStmt:
		table, err := db.boundTableForName(stmt.Table)
		if err != nil {
			return nil, err
		}
		return v3sql.BindSelect(stmt, table)
	case *v3sql.ExplainStmt:
		selectStmt, ok := stmt.Inner.(*v3sql.SelectStmt)
		if !ok {
			return nil, fmt.Errorf("EXPLAIN supports SELECT only")
		}
		table, err := db.boundTableForName(selectStmt.Table)
		if err != nil {
			return nil, err
		}
		return v3sql.BindExplain(stmt, table)
	default:
		return nil, fmt.Errorf("Query only supports SELECT and EXPLAIN statements")
	}
}

func (db *DB) bindInsert(stmt *v3sql.InsertStmt) (v3sql.InsertValues, error) {
	table, err := db.boundTableForName(stmt.Table)
	if err != nil {
		return v3sql.InsertValues{}, err
	}
	return v3sql.BindInsertValues(stmt, table)
}

func (db *DB) boundTable(entry tableEntry) v3sql.BoundTableDef {
	cols := make([]v3sql.BoundColumnDef, len(entry.spec.Columns))
	for i, col := range entry.spec.Columns {
		var labels []string
		if col.Type.Kind == types.KindNamed {
			if typ, ok := db.types[normalizeName(col.Type.Name)]; ok {
				labels = append([]string(nil), typ.spec.EnumLabels...)
			}
		}
		cols[i] = v3sql.BoundColumnDef{ID: v3sql.ColumnID(i + 1), Name: col.Name, Type: col.Type, Nullable: col.Nullable, Labels: labels}
	}
	return v3sql.BoundTableDef{ID: entry.id, Name: entry.spec.Name, Columns: cols, Options: entry.spec.Options, Version: db.version}
}

// table looks up a table by name and returns a "does not exist" error when
// missing. This is the only table-lookup entry point in engine.
func (db *DB) table(name string) (tableEntry, error) {
	entry, ok := db.tables[normalizeName(name)]
	if !ok {
		return tableEntry{}, fmt.Errorf("table %q does not exist", name)
	}
	return entry, nil
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

func readyContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (db *DB) checkReady(ctx context.Context) error {
	if db == nil || db.data == nil {
		return fmt.Errorf("database is nil")
	}
	if db.closed {
		return fmt.Errorf("database is closed")
	}
	return readyContext(ctx).Err()
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
