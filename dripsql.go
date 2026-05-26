// Package dripsql is the embedded API for the DripSQL columnar engine.
// Runnable examples live under examples/embed, examples/transactions, and examples/readonly.
package dripsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

// MemoryPath is reserved for a future in-memory backend and currently returns an error from Open.
const MemoryPath = ":memory:"

// Sentinels callers branch on with errors.Is.
var (
	ErrClosed      = engine.ErrClosed
	ErrReadOnly    = engine.ErrReadOnly
	ErrTxDone      = engine.ErrTxDone
	ErrNoRows      = errors.New("dripsql: query returned no rows")
	ErrTooManyRows = errors.New("dripsql: query returned more than one row")
)

// Open opens or creates the database directory at path.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("dripsql.Open: empty path")
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

// DB is a single-database handle whose methods are safe for concurrent use.
type DB struct{ e *engine.DB }

// Close releases the WAL, segment cache, and catalog handles.
func (db *DB) Close() error { return db.e.Close() }

// Exec runs one or more non-query statements separated by semicolons and binds ? placeholders from args in order.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	r, err := db.e.Exec(ctx, sql, args...)
	return Result{Statements: r.Statements, RowsAffected: r.RowsAffected}, err
}

// Query runs a SELECT or EXPLAIN and binds ? placeholders from args in order before returning a single-pass cursor that the caller must Close.
func (db *DB) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	rs, err := db.e.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rs), nil
}

// QueryAt runs Query against the snapshot pinned at the given commit timestamp and binds args the same way.
func (db *DB) QueryAt(ctx context.Context, sql string, readTs uint64, args ...any) (*Rows, error) {
	rs, err := db.e.QueryAt(ctx, sql, readTs, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rs), nil
}

// BeginTx starts a multi-statement transaction that holds the writer lock until Commit or Rollback.
func (db *DB) BeginTx(ctx context.Context) (*Tx, error) {
	t, err := db.e.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{t: t}, nil
}

// Update runs fn inside a transaction that commits on nil return and rolls back on error or panic.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) (err error) {
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	return fn(tx)
}

// QueryRow runs sql expecting exactly one result row and returns a single-shot scanner that errors on zero or multiple rows.
func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	rows, err := db.Query(ctx, sql, args...)
	return &Row{rows: rows, err: err}
}

// Compact rewrites half-or-more-deleted segments for table into fresh segments under one atomic manifest swap.
func (db *DB) Compact(ctx context.Context, table string) (int, error) {
	return db.e.Compact(ctx, table)
}

// Vacuum removes deletion-vector files no longer referenced and runs retention retirement when SetAutoRetention is on.
func (db *DB) Vacuum() (int, error) { return db.e.Vacuum() }

// SetAutoRetention makes Vacuum also retire fully-dead segments below the configured lag.
func (db *DB) SetAutoRetention(on bool) { db.e.SetAutoRetention(on) }

// SetRetentionLag sets the commit-timestamp distance Vacuum keeps before retiring fully-dead segments.
func (db *DB) SetRetentionLag(lag uint64) { db.e.SetRetentionLag(lag) }

// SetCacheSize caps the number of open segment file handles and resets to the engine default when n is below 1.
func (db *DB) SetCacheSize(n int) { db.e.SetCacheSize(n) }

// SetReadOnly when true makes Exec and BeginTx return a read-only error while leaving reads unaffected.
func (db *DB) SetReadOnly(on bool) { db.e.SetReadOnly(on) }

// Result reports the statement count and total RowsAffected from one Exec call.
type Result struct {
	Statements   int
	RowsAffected int64
}

// Row is a one-shot scanner for the single result row of QueryRow.
type Row struct {
	rows *Rows
	err  error
}

// Scan copies the single row into dst pointers and returns ErrNoRows or ErrTooManyRows when the result does not have exactly one row.
func (r *Row) Scan(dst ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}
	if err := r.rows.Scan(dst...); err != nil {
		return err
	}
	if r.rows.Next() {
		return ErrTooManyRows
	}
	return r.rows.Err()
}

// Rows is the single-pass cursor returned by Query and must always be Closed.
type Rows struct {
	rs      *engine.Rows
	cursor  int
	current []any
	closed  bool
	err     error
}

func newRows(rs *engine.Rows) *Rows {
	return &Rows{rs: rs, cursor: -1}
}

// Columns returns the result-set column names in select order; the returned slice is owned by the caller.
func (r *Rows) Columns() []string {
	return append([]string(nil), r.rs.Columns...)
}

// Next advances to the next row and reports whether one is available.
func (r *Rows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	r.cursor++
	if r.cursor >= len(r.rs.Values) {
		r.current = nil
		return false
	}
	r.current = r.rs.Values[r.cursor]
	return true
}

// Scan copies the current row into dst pointers which may be any of *int family, *uint family, *float32, *float64, *bool, *string, *[]byte, or *any.
func (r *Rows) Scan(dst ...any) error {
	if r.current == nil {
		return fmt.Errorf("dripsql.Rows.Scan: no current row, call Next first")
	}
	if len(dst) != len(r.current) {
		return fmt.Errorf("dripsql.Rows.Scan: got %d destinations for %d columns", len(dst), len(r.current))
	}
	for i, d := range dst {
		if err := scanInto(d, r.current[i]); err != nil {
			return fmt.Errorf("dripsql.Rows.Scan: column %d: %w", i, err)
		}
	}
	return nil
}

// Err returns the deferred error from iteration or nil if Next exhausted cleanly.
func (r *Rows) Err() error { return r.err }

// All drains the unread rows into a fresh slice so iteration after Next picks up at the next row not the current one.
func (r *Rows) All() ([][]any, error) {
	if r.closed {
		return nil, fmt.Errorf("dripsql.Rows.All: cursor already closed")
	}
	start := r.cursor + 1
	if start < 0 {
		start = 0
	}
	if start > len(r.rs.Values) {
		start = len(r.rs.Values)
	}
	remaining := r.rs.Values[start:]
	out := make([][]any, len(remaining))
	for i, row := range remaining {
		copied := make([]any, len(row))
		copy(copied, row)
		out[i] = copied
	}
	r.cursor = len(r.rs.Values)
	r.current = nil
	return out, nil
}

// Close releases the cursor and is safe to call more than once.
func (r *Rows) Close() error {
	r.closed = true
	r.current = nil
	return nil
}

// Tx is a multi-statement transaction that holds the writer lock until Commit or Rollback.
type Tx struct{ t *engine.Tx }

// Exec runs DDL or DML inside the transaction and binds ? placeholders from args in order.
func (tx *Tx) Exec(ctx context.Context, sql string, args ...any) (Result, error) {
	r, err := tx.t.Exec(ctx, sql, args...)
	return Result{Statements: r.Statements, RowsAffected: r.RowsAffected}, err
}

// Query runs a SELECT or EXPLAIN inside the transaction and binds args the same way as DB.Query.
func (tx *Tx) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	rs, err := tx.t.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rs), nil
}

// QueryRow runs sql inside the transaction expecting exactly one result row.
func (tx *Tx) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	rows, err := tx.Query(ctx, sql, args...)
	return &Row{rows: rows, err: err}
}

// Commit publishes the transaction's writes atomically and releases the writer lock.
func (tx *Tx) Commit() error { return tx.t.Commit() }

// Rollback discards the transaction's writes and releases the writer lock and is safe to call after Commit as a no-op.
func (tx *Tx) Rollback() error { return tx.t.Rollback() }
