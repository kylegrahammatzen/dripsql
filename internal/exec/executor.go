// BuildOperator turns a bound *sql.Plan into a chain of Operators by walking its Rel tree.
// SegmentsFn resolves a BoundTableDef to storage segments at run time, keeping the catalog mapping out of exec.
package exec

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Operator is the pull-based interface every exec node implements, where Next returns ok=false when exhausted and row visibility lives in batch.Sel with nil meaning all rows are visible.
var (
	ErrOperatorAlreadyOpen = errors.New("operator already open")
	ErrOperatorNotOpen     = errors.New("operator not open")
	ErrOperatorClosed      = errors.New("operator closed")
)

type Operator interface {
	Open(ctx context.Context) error
	Next() (batch vector.Batch, ok bool, err error)
	Close() error
}

type operatorState uint8

const (
	operatorNew operatorState = iota
	operatorOpen
	operatorClosed
)

func (s *operatorState) open() error {
	switch *s {
	case operatorOpen:
		return ErrOperatorAlreadyOpen
	case operatorClosed:
		return ErrOperatorClosed
	default:
		*s = operatorOpen
		return nil
	}
}

func (s operatorState) requireOpen() error {
	switch s {
	case operatorOpen:
		return nil
	case operatorClosed:
		return ErrOperatorClosed
	default:
		return ErrOperatorNotOpen
	}
}

func (s *operatorState) close() {
	*s = operatorClosed
}

type SegmentsFn func(def sql.BoundTableDef) ([]*storage.Segment, error)

func BuildOperator(plan *sql.Plan, segments SegmentsFn) (Operator, error) {
	if plan == nil {
		return nil, fmt.Errorf("BuildOperator: nil plan")
	}
	if segments == nil {
		return nil, fmt.Errorf("BuildOperator: nil SegmentsFn")
	}
	return buildPlan(plan, segments, nil)
}

func buildPlan(plan *sql.Plan, segments SegmentsFn, outer *correlatedOuter) (Operator, error) {
	switch plan.Kind {
	case sql.PlanQuery:
		if plan.Rel == nil {
			return nil, fmt.Errorf("BuildOperator: query plan missing Rel")
		}
		if outer == nil {
			outer = newCorrelatedOuter()
		}
		if err := materializeSubqueries(plan.Rel, segments); err != nil {
			return nil, err
		}
		return buildRelWithOuter(plan.Rel, segments, outer)
	case sql.PlanExplain:
		return nil, fmt.Errorf("BuildOperator: EXPLAIN execution belongs to the engine layer")
	}
	return nil, fmt.Errorf("BuildOperator: unsupported plan kind %v", plan.Kind)
}

func materializeSubqueries(rel *sql.Rel, segments SegmentsFn) error {
	if rel == nil {
		return nil
	}
	for _, in := range rel.Inputs {
		if err := materializeSubqueries(in, segments); err != nil {
			return err
		}
	}
	if rel.Where != nil {
		out, err := materializeSubqueriesExpr(*rel.Where, segments)
		if err != nil {
			return err
		}
		rel.Where = &out
	}
	predOut, err := materializeSubqueriesExpr(rel.Predicate, segments)
	if err != nil {
		return err
	}
	rel.Predicate = predOut
	for i := range rel.Projection {
		out, err := materializeSubqueriesExpr(rel.Projection[i].Expr, segments)
		if err != nil {
			return err
		}
		rel.Projection[i].Expr = out
	}
	for i := range rel.Outputs {
		out, err := materializeSubqueriesExpr(rel.Outputs[i].Expr, segments)
		if err != nil {
			return err
		}
		rel.Outputs[i].Expr = out
	}
	if rel.Having != nil {
		out, err := materializeSubqueriesExpr(*rel.Having, segments)
		if err != nil {
			return err
		}
		rel.Having = &out
	}
	return nil
}

func materializeSubqueriesExpr(expr sql.BoundExpr, segments SegmentsFn) (sql.BoundExpr, error) {
	switch expr.Op {
	case sql.ExprSubquery:
		if expr.SubPlan == nil || expr.SubPlan.Rel == nil {
			return sql.BoundExpr{}, fmt.Errorf("subquery materialize: empty subplan")
		}
		if len(expr.SubPlan.OuterRefs) > 0 {
			// A correlated node stays in place because the row-by-row evaluator builds and drains the inner plan per outer row.
			if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
				return sql.BoundExpr{}, err
			}
			return expr, nil
		}
		if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
			return sql.BoundExpr{}, err
		}
		op, err := buildRel(expr.SubPlan.Rel, segments)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("subquery build: %w", err)
		}
		val, err := drainScalar(op)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("subquery drain: %w", err)
		}
		return sql.BoundExpr{Op: sql.ExprLiteral, Type: expr.Type, Literal: val}, nil
	case sql.ExprInSubquery:
		if expr.SubPlan == nil || expr.SubPlan.Rel == nil {
			return sql.BoundExpr{}, fmt.Errorf("IN subquery materialize: empty subplan")
		}
		if len(expr.SubPlan.OuterRefs) > 0 {
			if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
				return sql.BoundExpr{}, err
			}
			out, err := materializeSubqueriesExpr(expr.Args[0], segments)
			if err != nil {
				return sql.BoundExpr{}, err
			}
			expr.Args[0] = out
			return expr, nil
		}
		if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
			return sql.BoundExpr{}, err
		}
		op, err := buildRel(expr.SubPlan.Rel, segments)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("IN subquery build: %w", err)
		}
		vals, err := drainValues(op)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("IN subquery drain: %w", err)
		}
		target := expr.Args[0]
		out, err := materializeSubqueriesExpr(target, segments)
		if err != nil {
			return sql.BoundExpr{}, err
		}
		args := make([]sql.BoundExpr, 0, len(vals)+1)
		args = append(args, out)
		for _, v := range vals {
			args = append(args, sql.BoundExpr{Op: sql.ExprLiteral, Type: out.Type, Literal: v})
		}
		return sql.BoundExpr{Op: sql.ExprIn, Type: expr.Type, Args: args, Not: expr.Not}, nil
	case sql.ExprExists:
		if expr.SubPlan == nil || expr.SubPlan.Rel == nil {
			return sql.BoundExpr{}, fmt.Errorf("EXISTS subquery materialize: empty subplan")
		}
		if len(expr.SubPlan.OuterRefs) > 0 {
			if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
				return sql.BoundExpr{}, err
			}
			return expr, nil
		}
		if err := materializeSubqueries(expr.SubPlan.Rel, segments); err != nil {
			return sql.BoundExpr{}, err
		}
		op, err := buildRel(expr.SubPlan.Rel, segments)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("EXISTS subquery build: %w", err)
		}
		any, err := drainAny(op)
		if err != nil {
			return sql.BoundExpr{}, fmt.Errorf("EXISTS subquery drain: %w", err)
		}
		result := any
		if expr.Not {
			result = !any
		}
		return sql.BoundExpr{Op: sql.ExprLiteral, Type: expr.Type, Literal: result}, nil
	}
	for i := range expr.Args {
		out, err := materializeSubqueriesExpr(expr.Args[i], segments)
		if err != nil {
			return sql.BoundExpr{}, err
		}
		expr.Args[i] = out
	}
	return expr, nil
}

func drainValues(op Operator) ([]any, error) {
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()
	var out []any
	for {
		batch, ok, err := op.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		if len(batch.Columns) != 1 {
			return nil, fmt.Errorf("IN subquery must return one column, got %d", len(batch.Columns))
		}
		col := &batch.Columns[0]
		if err := vector.ForVisible(batch, func(row int) error {
			v, verr := col.ValueAt(row)
			if verr != nil {
				return verr
			}
			out = append(out, v)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func drainAny(op Operator) (bool, error) {
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		return false, err
	}
	defer op.Close()
	for {
		batch, ok, err := op.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if batch.VisibleLen() > 0 {
			return true, nil
		}
	}
}

func drainScalar(op Operator) (any, error) {
	ctx := context.Background()
	if err := op.Open(ctx); err != nil {
		return nil, err
	}
	defer op.Close()
	var captured any
	rows := 0
	for {
		batch, ok, err := op.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		visible := batch.VisibleLen()
		if visible == 0 {
			continue
		}
		if len(batch.Columns) != 1 {
			return nil, fmt.Errorf("scalar subquery must return one column, got %d", len(batch.Columns))
		}
		col := &batch.Columns[0]
		iterErr := vector.ForVisible(batch, func(row int) error {
			rows++
			if rows > 1 {
				return fmt.Errorf("scalar subquery returned more than one row")
			}
			v, verr := col.ValueAt(row)
			if verr != nil {
				return verr
			}
			captured = v
			return nil
		})
		if iterErr != nil {
			return nil, iterErr
		}
	}
	return captured, nil
}

func buildRel(rel *sql.Rel, segments SegmentsFn) (Operator, error) {
	return buildRelWithOuter(rel, segments, nil)
}

func buildRelWithOuter(rel *sql.Rel, segments SegmentsFn, outer *correlatedOuter) (Operator, error) {
	if rel == nil {
		return nil, fmt.Errorf("buildRel: nil rel")
	}
	switch rel.Op {
	case sql.RelScan:
		return buildScan(rel, segments, nil, outer)
	case sql.RelCTE:
		if len(rel.Inputs) != 1 || rel.Inputs[0] == nil {
			return nil, fmt.Errorf("BuildOperator: RelCTE missing inner rel")
		}
		return buildRelWithOuter(rel.Inputs[0], segments, outer)
	case sql.RelUnion:
		if len(rel.Inputs) != 2 || rel.Inputs[0] == nil || rel.Inputs[1] == nil {
			return nil, fmt.Errorf("BuildOperator: RelUnion needs two inputs")
		}
		left, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		right, err := buildRelWithOuter(rel.Inputs[1], segments, outer)
		if err != nil {
			_ = left.Close()
			return nil, err
		}
		return &UnionOp{Left: left, Right: right}, nil
	case sql.RelWindow:
		if len(rel.Inputs) != 1 || rel.Inputs[0] == nil {
			return nil, fmt.Errorf("BuildOperator: RelWindow missing inner rel")
		}
		source, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		return &WindowOp{Source: source, Funcs: rel.WindowFuncs}, nil
	case sql.RelProject:
		for _, o := range rel.Projection {
			if err := checkExecExpr(o.Expr); err != nil {
				return nil, err
			}
		}
		source, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		if isIdentityProject(rel) {
			return source, nil
		}
		return &ProjectOp{Source: source, Outputs: rel.Projection, outer: outer, subBuild: makeSubBuilder(segments, outer)}, nil
	case sql.RelLimit:
		source, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		n := rel.Limit
		if n < 0 {
			n = -1
		}
		return &LimitOp{Source: source, N: n, Offset: rel.Offset}, nil
	case sql.RelAggregate:
		for _, expr := range rel.GroupBy {
			if err := checkExecExpr(expr); err != nil {
				return nil, err
			}
		}
		if rel.Having != nil {
			if err := checkExecExpr(*rel.Having); err != nil {
				return nil, err
			}
		}
		source, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		wireDictGroupKey(source, rel)
		return &AggregateOp{
			Source:     source,
			GroupBy:    rel.GroupBy,
			Aggregates: rel.Aggregates,
			Hidden:     rel.Hidden,
			Having:     rel.Having,
		}, nil
	case sql.RelFilter:
		if err := checkExecExpr(rel.Predicate); err != nil {
			return nil, err
		}
		source, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		return &FilterOp{Source: source, Predicate: rel.Predicate, outer: outer, subBuild: makeSubBuilder(segments, outer)}, nil
	case sql.RelJoin:
		if len(rel.JoinKeys) == 0 {
			return nil, fmt.Errorf("BuildOperator: join missing keys")
		}
		leftKeys := make([]sql.BoundExpr, len(rel.JoinKeys))
		rightKeys := make([]sql.BoundExpr, len(rel.JoinKeys))
		for i, k := range rel.JoinKeys {
			if err := checkExecExpr(k.Left); err != nil {
				return nil, err
			}
			if err := checkExecExpr(k.Right); err != nil {
				return nil, err
			}
			leftKeys[i] = k.Left
			rightKeys[i] = k.Right
		}
		left, err := buildRelWithOuter(rel.Inputs[0], segments, outer)
		if err != nil {
			return nil, err
		}
		right, err := buildRelWithOuter(rel.Inputs[1], segments, outer)
		if err != nil {
			return nil, err
		}
		return &HashJoinOp{
			Kind:         rel.JoinKind,
			Left:         left,
			Right:        right,
			LeftKeys:     leftKeys,
			RightKeys:    rightKeys,
			LeftOutputs:  rel.Inputs[0].Outputs,
			RightOutputs: rel.Inputs[1].Outputs,
		}, nil
	case sql.RelSort:
		for _, key := range rel.SortKeys {
			if err := checkExecExpr(key.Expr); err != nil {
				return nil, err
			}
		}
		if op, ok, err := buildLateTopKScan(rel, segments); err != nil {
			return nil, err
		} else if ok {
			return op, nil
		}
		source, err := buildSortInput(rel, segments, outer)
		if err != nil {
			return nil, err
		}
		return &SortOp{Source: source, Keys: rel.SortKeys, K: rel.K, Offset: rel.Offset}, nil
	}
	return nil, fmt.Errorf("BuildOperator: unsupported rel op %v", rel.Op)
}

func buildLateTopKScan(sortRel *sql.Rel, segments SegmentsFn) (Operator, bool, error) {
	child := sortRel.Inputs[0]
	if child.Op != sql.RelScan || child.Where != nil || child.Alias != "" || sortRel.K <= 0 || len(sortRel.SortKeys) != 1 {
		return nil, false, nil
	}
	if sortRel.Offset < 0 || sortRel.K+sortRel.Offset > vector.StandardBatchRows {
		return nil, false, nil
	}
	key := sortRel.SortKeys[0]
	if key.Expr.Op != sql.ExprColumn || !intLikeType(key.Expr.Type) {
		return nil, false, nil
	}
	keyName := scanColumnName(child, key.Expr.ColumnID)
	if keyName == "" {
		return nil, false, nil
	}
	segs, err := segments(child.Table)
	if err != nil {
		return nil, false, fmt.Errorf("BuildOperator: resolve segments: %w", err)
	}
	names, _, kinds, _, types, labels, err := scanColumnMetadata(child)
	if err != nil {
		return nil, false, err
	}
	needed := append(append([]string(nil), names...), keyName)
	for _, seg := range segs {
		for _, name := range needed {
			if findTopKColumn(seg, name) < 0 {
				return nil, false, nil
			}
		}
	}
	return &TopKScanOp{
		Segments: segs,
		Columns:  names,
		Kinds:    kinds,
		Types:    types,
		Labels:   labels,
		Key:      keyName,
		Desc:     key.Desc,
		K:        sortRel.K,
		Offset:   sortRel.Offset,
	}, true, nil
}

func isIdentityProject(rel *sql.Rel) bool {
	child := rel.Inputs[0]
	if len(rel.Projection) != len(child.Outputs) {
		return false
	}
	for i, out := range rel.Projection {
		if out.Expr.Op != sql.ExprColumn {
			return false
		}
		ck := child.Outputs[i]
		if ck.Expr.Op != sql.ExprColumn {
			return false
		}
		if !sameFoldName(out.Expr.Column, ck.Expr.Column) {
			return false
		}
		if out.Alias != "" && !sameFoldName(out.Alias, ck.Alias) {
			return false
		}
	}
	return true
}

func sameFoldName(a, b string) bool {
	return schema.NormalizeName(a) == schema.NormalizeName(b)
}

// checkExecExpr rejects trees holding ExprInvalid nodes the row-by-row eval cannot run.
func checkExecExpr(expr sql.BoundExpr) error {
	invalid := false
	sql.WalkExpr(expr, func(e sql.BoundExpr) bool {
		if e.Op == sql.ExprInvalid {
			invalid = true
		}
		return !invalid
	})
	if invalid {
		return fmt.Errorf("exec: invalid expression")
	}
	return nil
}

// buildSortInput pushes top-K into ScanOpts when RelSort sits directly over RelScan with a single int-like SortKey and K > 0, keeping SortOp above for correctness.
func buildSortInput(sortRel *sql.Rel, segments SegmentsFn, outer *correlatedOuter) (Operator, error) {
	child := sortRel.Inputs[0]
	if child.Op == sql.RelScan && child.Where == nil && sortRel.K > 0 && len(sortRel.SortKeys) == 1 {
		key := sortRel.SortKeys[0]
		if key.Expr.Op == sql.ExprColumn {
			colName := scanColumnName(child, key.Expr.ColumnID)
			if colName != "" && intLikeType(key.Expr.Type) {
				return buildScan(child, segments, &storage.TopKPushdown{
					Column: colName,
					Desc:   key.Desc,
					K:      sortRel.K,
					Offset: sortRel.Offset,
				}, outer)
			}
		}
	}
	return buildRelWithOuter(child, segments, outer)
}

func scanColumnName(scan *sql.Rel, id sql.ColumnID) string {
	for _, c := range scan.Table.Columns {
		if c.ID == id {
			return c.Name
		}
	}
	return ""
}

func intLikeType(t schema.Type) bool {
	switch t.Kind {
	case schema.KindInt16, schema.KindInt32, schema.KindInt64,
		schema.KindDate, schema.KindTimestamp, schema.KindTime, schema.KindDecimal:
		return true
	}
	return false
}

func scanColumnMetadata(rel *sql.Rel) ([]string, []uint64, []vector.VecKind, []storage.ScanDefault, []schema.Type, [][]string, error) {
	names := make([]string, 0, len(rel.Columns))
	ids := make([]uint64, 0, len(rel.Columns))
	kinds := make([]vector.VecKind, 0, len(rel.Columns))
	defaults := make([]storage.ScanDefault, 0, len(rel.Columns))
	types := make([]schema.Type, 0, len(rel.Columns))
	labels := make([][]string, 0, len(rel.Columns))
	for _, id := range rel.Columns {
		found := false
		for _, c := range rel.Table.Columns {
			if c.ID == id {
				names = append(names, c.Name)
				ids = append(ids, uint64(c.ID))
				k, err := vector.VecKindOf(c.Type)
				if err != nil {
					return nil, nil, nil, nil, nil, nil, fmt.Errorf("BuildOperator: column %q vec kind: %w", c.Name, err)
				}
				kinds = append(kinds, k)
				defaults = append(defaults, storage.ScanDefault{
					Set:   c.Default.Set,
					Null:  c.Default.Null,
					I64:   c.Default.I64,
					F64:   c.Default.F64,
					Bytes: c.Default.Bytes,
					Bool:  c.Default.Bool,
				})
				types = append(types, c.Type)
				labels = append(labels, c.Labels)
				found = true
				break
			}
		}
		if !found {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("BuildOperator: unknown ColumnID %d in scan for table %q", id, rel.Table.Name)
		}
	}
	return names, ids, kinds, defaults, types, labels, nil
}

// wireDictGroupKey asks the scan for dictionary codes on varbytes group keys sitting directly on it so the aggregate groups by code instead of by string, except columns the storage predicate reads stay materialized because Pred.Apply needs them.
func wireDictGroupKey(source Operator, rel *sql.Rel) {
	scan, ok := source.(*ScanOp)
	if !ok {
		return
	}
	var dictCols []string
	for _, g := range rel.GroupBy {
		if g.Op != sql.ExprColumn {
			continue
		}
		vk, err := vector.VecKindOf(g.Type)
		if err != nil || !vk.IsVarBytes() {
			continue
		}
		inPred := false
		if scan.Opts.Pred != nil {
			for _, p := range scan.Opts.Pred.Columns() {
				if strings.EqualFold(p, g.Column) {
					inPred = true
					break
				}
			}
		}
		if !inPred {
			dictCols = append(dictCols, g.Column)
		}
	}
	scan.Opts.DictCodeColumns = dictCols
}

func buildScan(rel *sql.Rel, segments SegmentsFn, topK *storage.TopKPushdown, outer *correlatedOuter) (Operator, error) {
	if rel.Where != nil {
		if err := checkExecExpr(*rel.Where); err != nil {
			return nil, err
		}
	}
	segs, err := segments(rel.Table)
	if err != nil {
		return nil, fmt.Errorf("BuildOperator: resolve segments: %w", err)
	}
	names, ids, kinds, defaults, _, _, err := scanColumnMetadata(rel)
	if err != nil {
		return nil, err
	}
	opts := storage.ScanOpts{Segments: segs, Columns: names, ColumnIDs: ids, ColumnKinds: kinds, ColumnDefaults: defaults, TopK: topK}
	var residual *sql.BoundExpr
	if rel.Where != nil {
		push, res := splitWhere(*rel.Where)
		if push != nil {
			opts.Pred = push
			if res == nil && len(rel.PredicateOnly) > 0 {
				drop := make(map[sql.ColumnID]struct{}, len(rel.PredicateOnly))
				for _, id := range rel.PredicateOnly {
					drop[id] = struct{}{}
				}
				kept := make([]string, 0, len(names))
				keptIDs := make([]uint64, 0, len(names))
				keptKinds := make([]vector.VecKind, 0, len(names))
				keptDefaults := make([]storage.ScanDefault, 0, len(names))
				for i, id := range rel.Columns {
					if _, ok := drop[id]; !ok {
						kept = append(kept, names[i])
						keptIDs = append(keptIDs, ids[i])
						keptKinds = append(keptKinds, kinds[i])
						keptDefaults = append(keptDefaults, defaults[i])
					}
				}
				opts.Columns = kept
				opts.ColumnIDs = keptIDs
				opts.ColumnKinds = keptKinds
				opts.ColumnDefaults = keptDefaults
			}
		}
		residual = res
	}
	parallelism := 1
	if topK == nil && len(segs) > 1 {
		parallelism = runtime.GOMAXPROCS(0)
	}
	var op Operator = &ScanOp{Opts: opts, ColumnAlias: rel.Alias, Parallelism: parallelism}
	if residual != nil {
		op = &FilterOp{Source: op, Predicate: *residual, outer: outer, subBuild: makeSubBuilder(segments, outer)}
	}
	return op, nil
}
