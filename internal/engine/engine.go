// Package engine is the internal DripSQL database entrypoint.
package engine

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/binder"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sql/parser"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

const storageRebuildMessage = "storage/vector execution is being rebuilt"

type Options struct{}

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
	catalog *catalog.Catalog
	store   catalogStore
	data    *storage.Store
	closed  bool
}

func Open(ctx context.Context, path string, opts Options) (*DB, error) {
	_ = opts
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
	store, cat, err := loadCatalog(path)
	if err != nil {
		return nil, err
	}
	data, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{path: path, catalog: cat, store: store, data: data}, nil
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
	stmts, err := parser.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}

	var result Result
	for _, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		switch stmt := stmt.(type) {
		case *ast.CreateTypeStmt:
			spec, err := binder.BindCreateType(stmt)
			if err != nil {
				return result, err
			}
			if err := db.CreateType(ctx, spec); err != nil {
				return result, err
			}
		case *ast.CreateTableStmt:
			spec, err := binder.BindCreateTable(stmt)
			if err != nil {
				return result, err
			}
			if err := db.CreateTable(ctx, spec); err != nil {
				return result, err
			}
		case *ast.InsertStmt:
			def, ok := db.catalog.Table(stmt.Table)
			if !ok {
				return result, fmt.Errorf("table %q does not exist", stmt.Table)
			}
			bound, err := binder.BindInsertValues(stmt, def)
			if err != nil {
				return result, err
			}
			batch, err := batchFromInsert(bound)
			if err != nil {
				return result, err
			}
			if err := db.data.AppendBuffered(ctx, def, batch); err != nil {
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
	stmts, err := parser.Parse(sqlText)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one SELECT statement, got %d", len(stmts))
	}
	switch stmt := stmts[0].(type) {
	case *ast.ExplainStmt:
		return db.executeExplain(ctx, stmt)
	case *ast.SelectStmt:
		def, ok := db.catalog.Table(stmt.Table)
		if !ok {
			return nil, fmt.Errorf("table %q does not exist", stmt.Table)
		}
		logicalPlan, err := binder.BindSelect(stmt, def)
		if err != nil {
			return nil, err
		}
		var scratch storage.QueryScratch
		switch logicalPlan.Kind {
		case logical.QueryAggregate:
			return db.executeAggregateQuery(ctx, def, logicalPlan, &scratch)
		case logical.QueryScan:
			return db.executeScanQuery(ctx, def, logicalPlan)
		default:
			return nil, fmt.Errorf("SELECT execution is not implemented while %s", storageRebuildMessage)
		}
	default:
		return nil, fmt.Errorf("Query only supports SELECT and EXPLAIN statements")
	}
}

func (db *DB) CreateType(ctx context.Context, spec schema.TypeSpec) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	oldStore := cloneCatalogStore(db.store)
	def, existed, err := db.catalog.CreateType(spec)
	if err != nil {
		return err
	}
	if existed {
		return nil
	}
	db.store.Types = append(db.store.Types, typeSpecFromDef(def))
	if err := db.saveCatalog(); err != nil {
		db.restoreCatalog(oldStore)
		return err
	}
	return err
}

func (db *DB) CreateTable(ctx context.Context, spec schema.TableSpec) error {
	if err := db.checkReady(ctx); err != nil {
		return err
	}
	oldStore := cloneCatalogStore(db.store)
	def, existed, err := db.catalog.CreateTable(spec)
	if err != nil {
		return err
	}
	if existed {
		return nil
	}
	db.store.Tables = append(db.store.Tables, tableSpecFromDef(def))
	if err := db.saveCatalog(); err != nil {
		db.restoreCatalog(oldStore)
		return err
	}
	return nil
}

func findTableColumn(def catalog.TableDef, name string) (catalog.ColumnDef, bool) {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, col := range def.Columns {
		if strings.ToLower(strings.TrimSpace(col.Name)) == name {
			return col, true
		}
	}
	return catalog.ColumnDef{}, false
}

func (db *DB) Table(name string) (catalog.TableDef, bool) {
	return db.catalog.Table(name)
}

func (db *DB) Type(name string) (catalog.TypeDef, bool) {
	return db.catalog.Type(name)
}

// RawStore exposes the underlying storage.Store for callers that need direct
// segment-write access (the cmd/bench loader builds vector.Batch values and
// calls AppendBatch directly to bypass parser/binder cost during load
// benchmarking). Not for general use.
func (db *DB) RawStore() *storage.Store {
	return db.data
}

func (db *DB) checkReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if db == nil || db.catalog == nil {
		return fmt.Errorf("database is not open")
	}
	if db.closed {
		return fmt.Errorf("database is closed")
	}
	return nil
}
