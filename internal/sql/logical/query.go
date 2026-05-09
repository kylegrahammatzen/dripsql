// Package logical contains bound relational plan nodes.
package logical

import (
	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

type QueryKind uint8

const (
	QueryInvalid QueryKind = iota
	QueryAggregate
	QueryScan
)

type AggregateFunc uint8

const (
	AggregateInvalid AggregateFunc = iota
	AggregateCount
	AggregateSum
	AggregateMin
	AggregateMax
)

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

type ExprKind uint8

const (
	ExprInvalid ExprKind = iota
	ExprColumn
	ExprLiteral
	ExprBinary
	ExprUnary
	ExprBetween
	ExprIn
)

type Op uint8

const (
	OpInvalid Op = iota
	OpEqual
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpModulo
	OpIntDivide
	OpConcat
	OpAnd
	OpOr
	OpNot
	OpLower
	OpUpper
)

type Expr struct {
	Kind    ExprKind
	Type    sqltype.Type
	Column  string
	Literal any
	Op      Op
	Left    *Expr
	Right   *Expr
	Args    []Expr
	Not     bool
}

type OutputExpr struct {
	Alias string
	Expr  Expr
}

type Query struct {
	Kind             QueryKind
	Table            catalog.TableDef
	SelectOutputs    []OutputExpr
	Aggregate        AggregateFunc
	AggregateColumn  string
	AggregateAlias   string
	HasFilter        bool
	WhereExpr        *Expr
	GroupColumn      string
	GroupType        sqltype.Kind
	GroupAlias       string
	HasHaving        bool
	HavingFilterExpr *Expr
	HavingAggregates []HavingAggregate
	HiddenColumns    []string
	GroupExpr        *Expr
	OrderColumn      string
	OrderExpr        *Expr
	OrderDesc        bool
	HasLimit         bool
	Limit            uint64
	HasOffset        bool
	Offset           uint64
}

type HavingAggregate struct {
	Column    string
	Aggregate AggregateFunc
	ArgColumn string
}
