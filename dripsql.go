// Package dripsql is the embedded API for the DripSQL columnar engine.
// Runnable examples live under examples/embed, examples/transactions, and examples/readonly.
package dripsql

import (
	"context"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// MemoryPath is reserved for a future in-memory backend and currently returns an error from Open.
const MemoryPath = ":memory:"

// Sentinels callers branch on with errors.Is.
var (
	ErrClosed      = engine.ErrClosed
	ErrReadOnly    = engine.ErrReadOnly
	ErrTxDone      = engine.ErrTxDone
	ErrNoRows      = errors.New("query returned no rows")
	ErrTooManyRows = errors.New("query returned more than one row")
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

// OpenReadOnly opens an existing database directory without creating or modifying any file, for replicas and inspection.
func OpenReadOnly(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("dripsql.OpenReadOnly: empty path")
	}
	e, err := engine.OpenReadOnly(path)
	if err != nil {
		return nil, err
	}
	return &DB{e: e}, nil
}

// DB is a single-database handle whose methods are safe for concurrent use.
type DB struct{ e *engine.DB }

// Refresh picks up catalog and manifest changes committed by a writer since open, meant for read-only replicas on shared storage.
func (db *DB) Refresh() error { return db.e.Refresh() }

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

// QueryChunks runs a SELECT and returns dense columnar chunks so large results avoid per-cell boxing.
// Chunk data is query-owned and stays valid after the call.
func (db *DB) QueryChunks(ctx context.Context, sql string, args ...any) ([]Chunk, error) {
	br, err := db.e.QueryBatches(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	chunks := make([]Chunk, len(br.Batches))
	for i := range br.Batches {
		chunks[i] = Chunk{batch: br.Batches[i]}
	}
	return chunks, nil
}

// Chunk is one dense columnar result block with typed column accessors.
type Chunk struct {
	batch vector.Batch
}

func (c Chunk) Len() int { return c.batch.Len }

// Columns returns the chunk's column names in select order; the returned slice is owned by the caller.
func (c Chunk) Columns() []string {
	out := make([]string, len(c.batch.Columns))
	for i := range c.batch.Columns {
		out[i] = c.batch.Columns[i].Name
	}
	return out
}

// Int64s returns the raw int64 column slice or nil when the column is absent or not int64 shaped.
func (c Chunk) Int64s(col string) []int64 {
	v, ok := c.batch.ColumnByName(col)
	if !ok {
		return nil
	}
	switch v.V.Kind {
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		return v.V.I64()
	}
	return nil
}

// Float64s returns the raw float64 column slice or nil when the column is absent or not float64.
func (c Chunk) Float64s(col string) []float64 {
	v, ok := c.batch.ColumnByName(col)
	if !ok || v.V.Kind != vector.VecFloat64 {
		return nil
	}
	return v.V.F64()
}

// Strings materializes a text column into fresh strings or nil when the column is absent or not text shaped.
func (c Chunk) Strings(col string) []string {
	v, ok := c.batch.ColumnByName(col)
	if !ok {
		return nil
	}
	switch v.V.Kind {
	case vector.VecText, vector.VecBytes, vector.VecJSON:
	default:
		return nil
	}
	vb := v.V.Var()
	out := make([]string, c.batch.Len)
	for i := range out {
		out[i] = vb.String(i)
	}
	return out
}

// Ints32 returns the raw int32 column slice or nil when the column is absent or not int32 shaped.
func (c Chunk) Ints32(col string) []int32 {
	v, ok := c.batch.ColumnByName(col)
	if !ok {
		return nil
	}
	switch v.V.Kind {
	case vector.VecInt32, vector.VecDate:
		return v.V.I32()
	}
	return nil
}

// Ints16 returns the raw int16 column slice or nil when the column is absent or not int16.
func (c Chunk) Ints16(col string) []int16 {
	v, ok := c.batch.ColumnByName(col)
	if !ok || v.V.Kind != vector.VecInt16 {
		return nil
	}
	return v.V.I16()
}

// Bools materializes a bool column from its bit-packed backing into a fresh slice or nil when the column is absent or not bool.
func (c Chunk) Bools(col string) []bool {
	v, ok := c.batch.ColumnByName(col)
	if !ok || v.V.Kind != vector.VecBool {
		return nil
	}
	bits := v.V.BoolBits()
	out := make([]bool, c.batch.Len)
	for i := range out {
		out[i] = bits[i>>3]&(1<<(i&7)) != 0
	}
	return out
}

// Dates returns the raw int32 date column slice in the vector layer's day encoding or nil when the column is absent or not date.
func (c Chunk) Dates(col string) []int32 {
	v, ok := c.batch.ColumnByName(col)
	if !ok || v.V.Kind != vector.VecDate {
		return nil
	}
	return v.V.I32()
}

// IsNull reports whether the cell at row in col is NULL.
func (c Chunk) IsNull(col string, row int) bool {
	v, ok := c.batch.ColumnByName(col)
	if !ok {
		return true
	}
	return v.V.Valid != nil && !v.V.Valid.IsValid(row)
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

// View runs fn inside a read-only transaction whose snapshot is pinned for the call's lifetime and is permitted even when SetReadOnly is on.
func (db *DB) View(ctx context.Context, fn func(*ReadTx) error) (err error) {
	tx, err := db.e.BeginReadTx(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		_ = tx.Rollback()
	}()
	return fn(&ReadTx{t: tx})
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

// SetReadOnly when true makes writes return a read-only error while leaving reads unaffected, and clearing it on an OpenReadOnly database is refused.
func (db *DB) SetReadOnly(on bool) error { return db.e.SetReadOnly(on) }

// Tables returns the catalog table names in sorted order.
func (db *DB) Tables() []string { return db.e.Tables() }

// TableSchema returns column metadata for name or an error if the table is unknown.
func (db *DB) TableSchema(name string) ([]ColumnInfo, error) { return db.e.TableSchema(name) }

// LastCommitTs returns the highest commit timestamp written so far, suitable as the readTs argument to QueryAt or the integer in `AS OF` SQL.
func (db *DB) LastCommitTs() uint64 { return db.e.LastCommitTs() }

// ColumnInfo describes one column returned by TableSchema.
type ColumnInfo = engine.ColumnInfo

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

// Scan copies the single row into dst pointers and returns ErrNoRows or ErrTooManyRows without mutating dst when the result does not have exactly one row.
func (r *Row) Scan(dst ...any) error {
	if r == nil || r.rows == nil {
		if r != nil && r.err != nil {
			return r.err
		}
		return ErrNoRows
	}
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
	first := r.rows.current
	if r.rows.Next() {
		return ErrTooManyRows
	}
	if err := r.rows.Err(); err != nil {
		return err
	}
	r.rows.current = first
	return r.rows.Scan(dst...)
}

// Rows is a single-pass cursor over a materialized result set and must always be Closed.
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
	if r == nil || r.rs == nil {
		return nil
	}
	return append([]string(nil), r.rs.Columns...)
}

// Next advances to the next row and reports whether one is available.
func (r *Rows) Next() bool {
	if r == nil || r.closed || r.err != nil || r.rs == nil {
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

// All drains the unread rows into a fresh slice with each cell deep-copied so caller-owned bytes cannot alias engine memory.
func (r *Rows) All() ([][]any, error) {
	if r.closed || r.rs == nil {
		return nil, fmt.Errorf("dripsql.Rows.All: cursor already closed")
	}
	start := min(max(0, r.cursor+1), len(r.rs.Values))
	remaining := r.rs.Values[start:]
	out := make([][]any, len(remaining))
	for i, row := range remaining {
		copied := make([]any, len(row))
		for j, v := range row {
			copied[j] = cloneCell(v)
		}
		out[i] = copied
	}
	r.cursor = len(r.rs.Values)
	r.current = nil
	return out, nil
}

// Close releases the cursor and the backing result set so a large result does not pin memory until the Rows is collected.
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.current = nil
	r.rs = nil
	return nil
}

func cloneCell(v any) any {
	if b, ok := v.([]byte); ok {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	return v
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

// ReadTx is the read-only handle View passes to its callback so the type system rejects writes at compile time.
type ReadTx struct{ t *engine.Tx }

// Query runs a SELECT or EXPLAIN inside the read transaction and binds args the same way as DB.Query.
func (rtx *ReadTx) Query(ctx context.Context, sql string, args ...any) (*Rows, error) {
	rs, err := rtx.t.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return newRows(rs), nil
}

// QueryRow runs sql inside the read transaction expecting exactly one result row.
func (rtx *ReadTx) QueryRow(ctx context.Context, sql string, args ...any) *Row {
	rows, err := rtx.Query(ctx, sql, args...)
	return &Row{rows: rows, err: err}
}
