// Package dripsql is the public embedded API for the DripSQL columnar engine.
// Open returns a DB and callers use Exec, Query, and BeginTx with no driver indirection.
package dripsql

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

// MemoryPath is the reserved sentinel for an in-memory database once support lands.
const MemoryPath = ":memory:"

// Open returns a DB backed by the directory at path.
// Empty string returns an error so a missing config value cannot silently open a default location.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("dripsql.Open: empty path; pass a directory path")
	}
	if path == MemoryPath {
		return nil, fmt.Errorf("dripsql.Open: MemoryPath is reserved but not yet implemented")
	}
	e, err := engine.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{e: e}, nil
}

// DB wraps the internal engine.DB so callers never import internal/ packages.
type DB struct{ e *engine.DB }

func (db *DB) Close() error { return db.e.Close() }

func (db *DB) Exec(ctx context.Context, sql string) (Result, error) {
	r, err := db.e.Exec(ctx, sql)
	return Result{Statements: r.Statements, RowsAffected: r.RowsAffected}, err
}

func (db *DB) Query(ctx context.Context, sql string) (*Rows, error) {
	rs, err := db.e.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &Rows{rs: rs}, nil
}

func (db *DB) QueryAt(ctx context.Context, sql string, readTs uint64) (*Rows, error) {
	rs, err := db.e.QueryAt(ctx, sql, readTs)
	if err != nil {
		return nil, err
	}
	return &Rows{rs: rs}, nil
}

func (db *DB) BeginTx(ctx context.Context) (*Tx, error) {
	t, err := db.e.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{t: t}, nil
}

func (db *DB) Compact(ctx context.Context, table string) (int, error) {
	return db.e.Compact(ctx, table)
}

func (db *DB) Vacuum() (int, error) { return db.e.Vacuum() }

// Result reports counts from a single Exec call.
type Result struct {
	Statements   int
	RowsAffected int64
}

// Rows is the materialized result set; Columns plus Values give the full payload.
type Rows struct{ rs *engine.Rows }

func (r *Rows) Columns() []string { return r.rs.Columns }
func (r *Rows) Values() [][]any   { return r.rs.Values }

// Tx wraps a multi-statement transaction; the txn holds the writer lock until Commit or Rollback.
type Tx struct{ t *engine.Tx }

func (tx *Tx) Exec(ctx context.Context, sql string) (Result, error) {
	r, err := tx.t.Exec(ctx, sql)
	return Result{Statements: r.Statements, RowsAffected: r.RowsAffected}, err
}

func (tx *Tx) Query(ctx context.Context, sql string) (*Rows, error) {
	rs, err := tx.t.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return &Rows{rs: rs}, nil
}

func (tx *Tx) Commit() error   { return tx.t.Commit() }
func (tx *Tx) Rollback() error { return tx.t.Rollback() }
