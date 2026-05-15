// Plan is the top-level bound form of any SQL statement. Relational SELECT bodies live under
// Plan.Rel as a tree of Rel nodes; DDL/DML payload fields hold the spec or assignment data.
// Catalog identifiers are assigned by the catalog, not by Bind.
package sql

import "github.com/kylegrahammatzen/dripsql/internal/types"

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
	Type     types.Type
	Labels   []string
	Nullable bool
}

type BoundTableDef struct {
	ID      TableID
	Name    string
	Columns []BoundColumnDef
	Options types.TableOptions
	Path    string
	Version SchemaVersion
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
}

// Plan is the bound statement. Kind selects which payload is meaningful.
type Plan struct {
	Kind PlanKind

	Rel *Rel

	Inner   *Plan
	Analyze bool

	TypeSpec  types.TypeSpec
	TableSpec types.TableSpec

	Table       BoundTableDef
	Values      InsertValues
	Assignments []BoundAssignment
	Where       *BoundExpr
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
					name = alias + "." + types.NormalizeName(c.Name)
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

type AggregateFunc uint8

const (
	AggregateInvalid AggregateFunc = iota
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
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
//   ExprColumn, ExprLiteral: leaf; Args empty.
//   Unary ops (ExprNot, ExprLower, ExprUpper, ExprLength): Args = [operand].
//   Binary comparison/arithmetic/concat/coalesce/JSON/AND/OR: Args = [left, right].
//   ExprSubstring: Args = [text, start, length?].
//   ExprBetween: Args = [target, low, high].
//   ExprIn: Args = [target, val1, val2, ...]; Not negates the membership test.
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
)

type BoundExpr struct {
	Op       ExprOp
	Type     types.Type
	Args     []BoundExpr
	Column   string
	ColumnID ColumnID
	Literal  any
	Not      bool
}
