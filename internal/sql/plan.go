package sql

import "github.com/kylegrahammatzen/dripsql/internal/types"

// TableID identifies a table in the bound catalog snapshot.
type TableID uint64

// ColumnID identifies a column in the bound catalog snapshot.
type ColumnID uint64

// TypeID identifies a user-defined type in the bound catalog snapshot.
type TypeID uint64

// SchemaVersion identifies the catalog version used to bind a plan.
type SchemaVersion uint64

// TypeDef is the bound form of a catalog type definition.
type TypeDef struct {
	ID      TypeID
	Name    string
	Labels  []string
	Version SchemaVersion
}

// BoundColumnDef is the bound form of a table column definition.
type BoundColumnDef struct {
	ID       ColumnID
	Name     string
	Type     types.Type
	Labels   []string
	Nullable bool
}

// BoundTableDef is the bound form of a table definition.
type BoundTableDef struct {
	ID      TableID
	Name    string
	Columns []BoundColumnDef
	Options types.TableOptions
	Path    string
	Version SchemaVersion
}

// Plan is a bound relational plan node produced by the binder.
type Plan interface{ planNode() }

// CreateTypePlan describes CREATE TYPE ... AS ENUM (...).
type CreateTypePlan struct {
	Spec types.TypeSpec
}

// CreateTablePlan describes CREATE TABLE ... before it is installed in the catalog.
type CreateTablePlan struct {
	Spec types.TableSpec
}

// InsertPlan describes INSERT INTO ... VALUES (...).
type InsertPlan struct {
	Table   BoundTableDef
	Columns []ColumnID
	Values  [][]Value
}

// ScanPlan reads selected columns from a table, optionally with a pushdownable filter.
type ScanPlan struct {
	Table   BoundTableDef
	Columns []ColumnID
	Where   *BoundExpr
}

// AggregatePlan groups and aggregates rows from Source.
type AggregatePlan struct {
	Source     Plan
	GroupBy    []BoundExpr
	Aggregates []AggSpec
	Having     *BoundExpr
	Hidden     []AggSpec
}

// ProjectPlan evaluates output expressions from Source.
type ProjectPlan struct {
	Source Plan
	Exprs  []BoundOutput
}

// SortPlan orders rows from Source.
type SortPlan struct {
	Source Plan
	Keys   []SortKey
}

// LimitPlan applies LIMIT/OFFSET to Source.
type LimitPlan struct {
	Source Plan
	N      int64
	Offset int64
}

// ExplainPlan wraps a SELECT-shaped plan for EXPLAIN or EXPLAIN ANALYZE.
type ExplainPlan struct {
	Inner   Plan
	Analyze bool
}

func (*CreateTypePlan) planNode()  {}
func (*CreateTablePlan) planNode() {}
func (*InsertPlan) planNode()      {}
func (*ScanPlan) planNode()        {}
func (*AggregatePlan) planNode()   {}
func (*ProjectPlan) planNode()     {}
func (*SortPlan) planNode()        {}
func (*LimitPlan) planNode()       {}
func (*ExplainPlan) planNode()     {}

// AggregateFunc names the aggregate functions supported by the current SQL surface.
type AggregateFunc uint8

const (
	AggregateInvalid AggregateFunc = iota
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
)

// AggSpec describes one aggregate output or hidden aggregate used by HAVING.
type AggSpec struct {
	Func      AggregateFunc
	ArgColumn ColumnID
	ArgName   string
	Star      bool
	Alias     string
}

// SortKey describes one ORDER BY expression.
type SortKey struct {
	Name string
	Expr BoundExpr
	Desc bool
}

// BoundOutput describes one projected output expression.
type BoundOutput struct {
	Alias string
	Expr  BoundExpr
}

// FilterOp is the canonical comparison operation set used by predicate binding.
type FilterOp uint8

const (
	FilterEqual FilterOp = iota
	FilterNotEqual
	FilterLess
	FilterLessEqual
	FilterGreater
	FilterGreaterEqual
	FilterBetween
	FilterIn
	FilterNotIn
)

// BoundExprKind classifies a bound scalar expression.
type BoundExprKind uint8

const (
	BoundExprInvalid BoundExprKind = iota
	BoundExprColumn
	BoundExprLiteral
	BoundExprBinary
	BoundExprUnary
	BoundExprBetween
	BoundExprIn
)

// BoundOp is the canonical scalar operation set used after binding.
type BoundOp uint8

const (
	BoundOpInvalid BoundOp = iota
	BoundOpEqual
	BoundOpNotEqual
	BoundOpLess
	BoundOpLessEqual
	BoundOpGreater
	BoundOpGreaterEqual
	BoundOpAdd
	BoundOpSubtract
	BoundOpMultiply
	BoundOpDivide
	BoundOpModulo
	BoundOpIntDivide
	BoundOpConcat
	BoundOpAnd
	BoundOpOr
	BoundOpNot
	BoundOpLower
	BoundOpUpper
	BoundOpJSONGet
	BoundOpJSONGetText
	BoundOpLength
	BoundOpCoalesce
)

// BoundExpr is a type-checked scalar expression used by plans and operators.
type BoundExpr struct {
	Kind     BoundExprKind
	Type     types.Type
	Column   string
	ColumnID ColumnID
	Literal  any
	Op       BoundOp
	Left     *BoundExpr
	Right    *BoundExpr
	Args     []BoundExpr
	Not      bool
}
