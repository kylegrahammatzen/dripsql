// Plan is the top-level bound form of any SQL statement. Relational SELECT bodies live under
// Plan.Rel as a tree of Rel nodes; DDL/DML payload fields hold the spec or assignment data.
// Catalog identifiers are assigned by the catalog, not by Bind.
package sql

import (
	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type TableID uint64
type ColumnID uint64
type TypeID uint64
type SchemaVersion uint64

type TypeDef struct {
	ID      TypeID
	Name    string
	Labels  []string
	Version SchemaVersion
}

type BoundColumnDef struct {
	ID       ColumnID
	Name     string
	Type     schema.Type
	Labels   []string
	Nullable bool
	Codec    schema.Encoding
}

type BoundTableDef struct {
	ID      TableID
	Name    string
	Columns []BoundColumnDef
	Options schema.TableOptions
	Path    string
	Version SchemaVersion
	// AsOf carries the parsed `AS OF <commit_ts>` clause from the table reference. Zero
	// means use the statement-level snapshot. The engine resolver uses this per-reference
	// so different tables in one join can scan different snapshots.
	AsOf uint64
}

type PlanKind uint8

const (
	PlanInvalid PlanKind = iota
	PlanCreateType
	PlanCreateTable
	PlanInsert
	PlanDelete
	PlanUpdate
	PlanQuery
	PlanExplain
)

type RelOp uint8

const (
	RelInvalid RelOp = iota
	RelScan
	RelFilter
	RelJoin
	RelAggregate
	RelProject
	RelSort
	RelLimit
	RelCTE
	RelWindow
	RelUnion
)

type JoinKey struct {
	Left  BoundExpr
	Right BoundExpr
}

// Rel is the relational tree node. Every Rel carries its output schema in Outputs and its
// child rels in Inputs (left then right). Op-specific fields below are populated only for
// the matching op; constructors enforce shape so consumers can rely on it.
type Rel struct {
	Op      RelOp
	Outputs []BoundOutput
	Inputs  []*Rel

	Table   BoundTableDef
	Alias   string
	Columns []ColumnID
	// PredicateOnly lists Column IDs that are referenced only by Where, not by any
	// downstream output. When the executor pushes Where into storage.Predicate, those
	// columns do not need to be emitted; storage decodes them only as far as the
	// predicate needs and can skip materialization entirely via EvalEncoded.
	PredicateOnly []ColumnID
	Where         *BoundExpr

	Predicate BoundExpr

	JoinKind JoinKind
	JoinKeys []JoinKey

	GroupBy    []BoundExpr
	Aggregates []AggSpec
	Hidden     []AggSpec
	Having     *BoundExpr

	Projection []BoundOutput

	SortKeys []SortKey
	K        int64

	Offset int64
	Limit  int64

	WindowFuncs []WindowFunc
}

type WindowFunc struct {
	Func      WindowFuncKind
	Alias     string
	Arg       *BoundExpr
	Partition []BoundExpr
	OrderBy   []SortKey
	Frame     *WindowFrameBounds
}

// Nil Frame means default semantics: full-partition without ORDER BY, running
// aggregate with ORDER BY. IsRange switches to value-distance on the order key.
type WindowFrameBounds struct {
	IsRange        bool
	StartUnbounded bool
	StartCurrent   bool
	StartPreceding int64
	EndUnbounded   bool
	EndCurrent     bool
	EndFollowing   int64
}

type WindowFuncKind uint8

const (
	WindowInvalid WindowFuncKind = iota
	WindowRowNumber
	WindowRank
	WindowDenseRank
	WindowSum
	WindowCount
	WindowMin
	WindowMax
	WindowAvg
)

func (w WindowFuncKind) String() string {
	switch w {
	case WindowRowNumber:
		return "row_number"
	case WindowRank:
		return "rank"
	case WindowDenseRank:
		return "dense_rank"
	case WindowSum:
		return "sum"
	case WindowCount:
		return "count"
	case WindowMin:
		return "min"
	case WindowMax:
		return "max"
	case WindowAvg:
		return "avg"
	}
	return "window_unknown"
}

func WindowFuncByName(name string) (WindowFuncKind, bool) {
	switch name {
	case "row_number":
		return WindowRowNumber, true
	case "rank":
		return WindowRank, true
	case "dense_rank":
		return WindowDenseRank, true
	case "sum":
		return WindowSum, true
	case "count":
		return WindowCount, true
	case "min":
		return WindowMin, true
	case "max":
		return WindowMax, true
	case "avg":
		return WindowAvg, true
	}
	return WindowInvalid, false
}

func (w WindowFuncKind) IsAggregate() bool {
	switch w {
	case WindowSum, WindowCount, WindowMin, WindowMax, WindowAvg:
		return true
	}
	return false
}

// Plan is the bound statement. Kind selects which payload is meaningful.
type Plan struct {
	Kind PlanKind

	Rel *Rel

	Inner   *Plan
	Analyze bool

	TypeSpec  schema.TypeSpec
	TableSpec schema.TableSpec

	Table       BoundTableDef
	Values      InsertValues
	Assignments []BoundAssignment
	Where       *BoundExpr

	// OuterRefs lists outer column names referenced inside this plan (only set on
	// subquery plans). When non-empty the executor cannot materialize the result
	// once; it must re-run the plan per outer row with the named values bound in.
	OuterRefs []string
}

type BoundAssignment struct {
	Column BoundColumnDef
	Value  Value
}

func scanRel(table BoundTableDef, alias string, cols []ColumnID, where *BoundExpr) *Rel {
	outputs := make([]BoundOutput, 0, len(cols))
	for _, id := range cols {
		for _, c := range table.Columns {
			if c.ID == id {
				name := c.Name
				if alias != "" {
					name = alias + "." + schema.NormalizeName(c.Name)
				}
				outputs = append(outputs, BoundOutput{Expr: BoundExpr{Op: ExprColumn, Type: c.Type, Column: name, ColumnID: c.ID}})
				break
			}
		}
	}
	return &Rel{Op: RelScan, Outputs: outputs, Table: table, Alias: alias, Columns: cols, Where: where}
}

func filterRel(src *Rel, pred BoundExpr) *Rel {
	return &Rel{Op: RelFilter, Outputs: src.Outputs, Inputs: []*Rel{src}, Predicate: pred}
}

func joinRel(kind JoinKind, left, right *Rel, keys []JoinKey) *Rel {
	outputs := make([]BoundOutput, 0, len(left.Outputs)+len(right.Outputs))
	outputs = append(outputs, left.Outputs...)
	outputs = append(outputs, right.Outputs...)
	return &Rel{Op: RelJoin, Outputs: outputs, Inputs: []*Rel{left, right}, JoinKind: kind, JoinKeys: keys}
}

func aggregateRel(src *Rel, group []BoundExpr, agg, hidden []AggSpec, having *BoundExpr, outputs []BoundOutput) *Rel {
	return &Rel{Op: RelAggregate, Outputs: outputs, Inputs: []*Rel{src}, GroupBy: group, Aggregates: agg, Hidden: hidden, Having: having}
}

func projectRel(src *Rel, projection []BoundOutput) *Rel {
	return &Rel{Op: RelProject, Outputs: projection, Inputs: []*Rel{src}, Projection: projection}
}

func sortRel(src *Rel, keys []SortKey, k, offset int64) *Rel {
	return &Rel{Op: RelSort, Outputs: src.Outputs, Inputs: []*Rel{src}, SortKeys: keys, K: k, Offset: offset}
}

func limitRel(src *Rel, n, offset int64) *Rel {
	return &Rel{Op: RelLimit, Outputs: src.Outputs, Inputs: []*Rel{src}, Limit: n, Offset: offset}
}

func unionRel(left, right *Rel) *Rel {
	return &Rel{Op: RelUnion, Outputs: left.Outputs, Inputs: []*Rel{left, right}}
}

type AggregateFunc uint8

const (
	AggregateInvalid AggregateFunc = iota
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
	AggregateAvg
)

type AggSpec struct {
	Func      AggregateFunc
	ArgColumn ColumnID
	ArgName   string
	Star      bool
	Alias     string
}

type SortKey struct {
	Name string
	Expr BoundExpr
	Desc bool
}

type BoundOutput struct {
	Alias string
	Expr  BoundExpr
}

// ExprOp identifies a BoundExpr node. Each op fixes the meaning of Args:
//
//	ExprColumn, ExprLiteral: leaf; Args empty.
//	Unary ops (ExprNot, ExprLower, ExprUpper, ExprLength): Args = [operand].
//	Binary comparison/arithmetic/concat/coalesce/JSON/AND/OR: Args = [left, right].
//	ExprSubstring: Args = [text, start, length?].
//	ExprBetween: Args = [target, low, high].
//	ExprIn: Args = [target, val1, val2, ...]; Not negates the membership test.
type ExprOp uint8

const (
	ExprInvalid ExprOp = iota
	ExprColumn
	ExprLiteral
	ExprAnd
	ExprOr
	ExprNot
	ExprEqual
	ExprNotEqual
	ExprLess
	ExprLessEqual
	ExprGreater
	ExprGreaterEqual
	ExprAdd
	ExprSubtract
	ExprMultiply
	ExprDivide
	ExprModulo
	ExprIntDivide
	ExprConcat
	ExprLower
	ExprUpper
	ExprLength
	ExprCoalesce
	ExprSubstring
	ExprJSONGet
	ExprJSONGetText
	ExprBetween
	ExprIn
	ExprAbs
	ExprNullIf
	// ExprCase: Args = [when1, then1, when2, then2, ..., elseExpr]. Args has even length
	// when no ELSE is present (binder synthesizes a NULL literal so length stays odd).
	// Type is the common kind across all THEN/ELSE branches.
	ExprCase
	ExprSubquery
	ExprInSubquery
	ExprExists
)

type BoundExpr struct {
	Op       ExprOp
	Type     schema.Type
	Args     []BoundExpr
	Column   string
	ColumnID ColumnID
	Literal  any
	Not      bool
	SubPlan  *Plan
	// Outer marks an ExprColumn that resolves to a parent scope rather than the
	// current batch. The evaluator reads its value from a runtime outer-row table
	// keyed by Column name. Only meaningful for Op == ExprColumn.
	Outer bool
}
