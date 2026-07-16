// PR-V3: multi-statement transactions. BeginTx holds db.mu for the txn's lifetime and
// stages writes in memory until Commit applies them under one commit_ts.
package engine

import (
	"context"
	"fmt"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

type Tx struct {
	db      *DB
	readTs  uint64
	pending map[string]*tablePending
	files   []string
	opened  []*storage.Segment
	done    bool
}

type tablePending struct {
	adds      []storage.ManifestSegmentAdd
	dvUpdates []storage.ManifestDVUpdate
}

func (db *DB) BeginTx(ctx context.Context) (*Tx, error) {
	return db.beginTx(ctx, false)
}

// BeginReadTx pins a snapshot for read-only use and is permitted in read-only mode since it cannot stage writes.
func (db *DB) BeginReadTx(ctx context.Context) (*Tx, error) {
	return db.beginTx(ctx, true)
}

func (db *DB) beginTx(ctx context.Context, readOnly bool) (*Tx, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := db.lockOpen(); err != nil {
		return nil, err
	}
	if !readOnly && db.readOnly.Load() {
		db.mu.Unlock()
		return nil, ErrReadOnly
	}
	readTs := db.nextCommitTs.Load()
	db.pinnedReadTs[readTs]++
	return &Tx{
		db:      db,
		readTs:  readTs,
		pending: make(map[string]*tablePending),
	}, nil
}

func (tx *Tx) commit(table string, adds []storage.ManifestSegmentAdd, dvUpdates []storage.ManifestDVUpdate) error {
	key := schema.NormalizeName(table)
	p, ok := tx.pending[key]
	if !ok {
		p = &tablePending{}
		tx.pending[key] = p
	}
	p.adds = append(p.adds, adds...)
	p.dvUpdates = append(p.dvUpdates, dvUpdates...)
	// Rollback removes by filesystem path, so resolve the stored strings up front.
	for _, a := range adds {
		tx.files = append(tx.files, tx.db.resolveTablePath(table, a.Path))
	}
	for _, d := range dvUpdates {
		tx.files = append(tx.files, tx.db.resolveTablePath(table, d.DVPath))
	}
	return nil
}

func (tx *Tx) Exec(ctx context.Context, sqlText string, args ...any) (Result, error) {
	if tx.done {
		return Result{}, ErrTxDone
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stmts, err := sql.Parse(sqlText)
	if err != nil {
		return Result{}, err
	}
	var result Result
	for _, stmt := range stmts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		affected, err := tx.execStmt(ctx, stmt, args)
		if err != nil {
			return result, err
		}
		result.Statements++
		result.RowsAffected += affected
	}
	return result, nil
}

func (tx *Tx) execStmt(ctx context.Context, stmt sql.Stmt, args []any) (int64, error) {
	plan, err := tx.db.planner().Plan(stmt)
	if err != nil {
		return 0, err
	}
	plan, err = sql.BindParameters(plan, args)
	if err != nil {
		return 0, err
	}
	switch plan.Kind {
	case sql.PlanInsert:
		return tx.db.insert(ctx, plan, tx.commit)
	case sql.PlanDelete:
		return tx.db.delete(ctx, plan, tx.commit)
	case sql.PlanUpdate:
		return tx.db.update(ctx, plan, tx.commit)
	}
	return 0, fmt.Errorf("engine: %v not supported inside a transaction", plan.Kind)
}

func (tx *Tx) Query(ctx context.Context, sqlText string, args ...any) (*Rows, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	if ctx == nil {
		ctx = context.Background()
	}
	plan, err := tx.db.planForQuery(sqlText)
	if err != nil {
		return nil, err
	}
	bound, err := sql.BindParameters(plan, args)
	if err != nil {
		return nil, err
	}
	return tx.db.runQueryWith(ctx, bound, tx.resolveSegments)
}

// resolveSegments returns base SnapshotAt(readTs) with pending overlays applied:
// pending DVUpdates replace the manifest entry's DV path for the matching segment,
// and pending Adds append as fresh segments. Overlay segments get a CommitTs of 0
// (always visible to this txn's reader).
func (tx *Tx) resolveSegments(d sql.BoundTableDef) ([]*storage.Segment, error) {
	key := schema.NormalizeName(d.Name)
	p := tx.pending[key]
	m, err := tx.db.manifestFor(d.Name)
	if err != nil {
		return nil, err
	}
	view := m.SnapshotAt(tx.readTs)
	// Overrides key on resolved paths so stored-format differences cannot miss a match.
	dvOverride := make(map[string]string)
	if p != nil {
		for _, dv := range p.dvUpdates {
			dvOverride[tx.db.resolveTablePath(d.Name, dv.SegmentPath)] = dv.DVPath
		}
	}
	segs := make([]*storage.Segment, 0, len(view.Entries)+(func() int {
		if p == nil {
			return 0
		}
		return len(p.adds)
	}()))
	for _, entry := range view.Entries {
		segPath := tx.db.resolveTablePath(d.Name, entry.Path)
		dvPath := entry.DeletionVectorPath
		if pdv, ok := dvOverride[segPath]; ok {
			dvPath = pdv
		}
		seg, err := storage.OpenSegmentWithDV(segPath, tx.db.resolveTablePath(d.Name, dvPath))
		if err != nil {
			return nil, fmt.Errorf("tx: open segment %q: %w", segPath, err)
		}
		seg.CommitTs = entry.CommitTs
		tx.opened = append(tx.opened, seg)
		segs = append(segs, seg)
	}
	if p != nil {
		for _, a := range p.adds {
			addPath := tx.db.resolveTablePath(d.Name, a.Path)
			seg, err := storage.OpenSegmentWithDV(addPath, "")
			if err != nil {
				return nil, fmt.Errorf("tx: open pending segment %q: %w", addPath, err)
			}
			tx.opened = append(tx.opened, seg)
			segs = append(segs, seg)
		}
	}
	return segs, nil
}

func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
	}
	defer tx.finish()
	if len(tx.pending) == 0 {
		return nil
	}
	// One TxnID + CommitTs for every participating table so recovery can group the
	// intents into one logical transaction. Cross-table atomicity hinges on this:
	// a partial recovery sees the same TxnID across all tables' intents and forward-
	// rolls or unwinds them as a unit.
	tx.db.nextTxnID++
	txnID := tx.db.nextTxnID
	commitTs := tx.db.nextCommitTs.Add(1)
	for table, p := range tx.pending {
		if err := tx.db.commitManifestTxnAs(table, txnID, commitTs, p.adds, p.dvUpdates); err != nil {
			return err
		}
	}
	if tx.db.wal != nil {
		if err := tx.db.wal.TruncateToHeader(); err != nil {
			return err
		}
	}
	tx.files = nil
	return nil
}

func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}
	defer tx.finish()
	for _, f := range tx.files {
		_ = os.Remove(f)
	}
	tx.files = nil
	tx.pending = nil
	return nil
}

func (tx *Tx) finish() {
	tx.done = true
	for _, s := range tx.opened {
		_ = s.Close()
	}
	tx.opened = nil
	if tx.db.pinnedReadTs[tx.readTs] > 1 {
		tx.db.pinnedReadTs[tx.readTs]--
	} else {
		delete(tx.db.pinnedReadTs, tx.readTs)
	}
	tx.db.mu.Unlock()
}
