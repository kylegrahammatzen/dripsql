// Plan is the top-level bound form of any SQL statement plus the PlanCache that engines memoize bound Plans through.
// Relational SELECT bodies live under Plan.Rel as a tree of Rel nodes and DDL/DML payload fields hold the spec or assignment data.
package sql

import (
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type TableID uint64
type ColumnID uint64
type TypeID uint64
type SchemaVersion uint64

type AlterPayload struct {
	Table   string
	Rename  *AlterRenameColumn
	Add     *AlterAddColumn
	Drop    *AlterDropColumn
	SetType *AlterColumnType
}

type TypeDef struct {
	ID      TypeID
	Name    string
	Labels  []string
	Version SchemaVersion
}

type BoundColumnDef struct {
	ID       ColumnID
	Ordinal  int
	Name     string
	Type     schema.Type
	Labels   []string
	Nullable bool
	Codec    schema.Encoding
	Default  BoundDefault
}

type BoundDefault struct {
	Set   bool
	Null  bool
	I64   int64
	F64   float64
	Bytes []byte
	Bool  bool
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
	PlanAlterTable
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

	TypeSpec    schema.TypeSpec
	TableSpec   schema.TableSpec
	Alter       *AlterPayload
	IfNotExists bool

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

func joinRel(kind JoinKind, left, right *Rel, keys []JoinKey) *Rel {
	outputs := make([]BoundOutput, 0, len(left.Outputs)+len(right.Outputs))
	outputs = append(outputs, left.Outputs...)
	outputs = append(outputs, right.Outputs...)
	return &Rel{Op: RelJoin, Outputs: outputs, Inputs: []*Rel{left, right}, JoinKind: kind, JoinKeys: keys}
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
	// ArgExpr carries a computed argument, the planner projects it as a hidden column and fills ArgName.
	ArgExpr *BoundExpr
	Star    bool
	Alias   string
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
	// ExprParameter is a positional ? placeholder bound to args[Parameter-1] at execution time.
	ExprParameter
	// ExprIsNull and ExprIsNotNull take Args = [operand] and are never unknown under three-valued logic.
	ExprIsNull
	ExprIsNotNull
)

type BoundExpr struct {
	Op        ExprOp
	Type      schema.Type
	Args      []BoundExpr
	Column    string
	ColumnID  ColumnID
	Literal   any
	Parameter int
	Not       bool
	SubPlan   *Plan
	// Outer marks an ExprColumn that resolves to a parent scope rather than the
	// current batch. The evaluator reads its value from a runtime outer-row table
	// keyed by Column name. Only meaningful for Op == ExprColumn.
	Outer bool
}

// PlanCache memoizes bound Plans keyed by SQL text plus the catalog SchemaVersion they were bound against.
// Stale-version hits miss so DDL invalidates wholesale.
type PlanCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]planCacheEntry
}

type planCacheEntry struct {
	version SchemaVersion
	plan    *Plan
}

func NewPlanCache(max int) *PlanCache {
	if max <= 0 {
		max = 256
	}
	return &PlanCache{max: max, entries: make(map[string]planCacheEntry)}
}

func (c *PlanCache) Get(sql string, version SchemaVersion) (*Plan, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[sql]
	if !ok || e.version != version {
		return nil, false
	}
	return e.plan, true
}

func (c *PlanCache) Put(sql string, version SchemaVersion, plan *Plan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[sql]; ok && existing.version == version {
		return
	}
	if len(c.entries) >= c.max {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
	c.entries[sql] = planCacheEntry{version: version, plan: plan}
}

func (c *PlanCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}
