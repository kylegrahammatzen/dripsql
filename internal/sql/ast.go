// AST node definitions for DripSQL statements and expressions.
// Stmt, Expr, and TableExpr are sealed via unexported marker methods so binders type-switch on concrete types.
package sql

import "errors"

var (
	errNestedJoinRight = errors.New("nested JOIN right side not supported")
	errUnsupportedFrom = errors.New("unsupported FROM clause")
)

type Stmt interface{ isStmt() }

type CreateTypeStmt struct {
	Name        string
	IfNotExists bool
	EnumLabels  []string
}

type CreateTableStmt struct {
	Name        string
	IfNotExists bool
	Columns     []ColumnDef
	Options     []TableOption
}

type AlterTableStmt struct {
	Table   string
	Rename  *AlterRenameColumn
	Add     *AlterAddColumn
	Drop    *AlterDropColumn
	SetType *AlterColumnType
}

type AlterRenameColumn struct {
	From string
	To   string
}

type AlterAddColumn struct {
	Name       string
	Type       string
	NotNull    bool
	HasDefault bool
	Default    Value
}

type AlterDropColumn struct {
	Name string
}

type AlterColumnType struct {
	Name string
	Type string
}

type InsertStmt struct {
	Table   string
	Columns []string
	Values  [][]Value
}

type DeleteStmt struct {
	Table string
	Where Expr
}

type UpdateStmt struct {
	Table       string
	Assignments []Assignment
	Where       Expr
}

type Assignment struct {
	Column string
	Value  Value
}

type ExplainStmt struct {
	Analyze bool
	Inner   Stmt
}

type ColumnDef struct {
	Name    string
	Type    string
	NotNull bool
	Codec   string
}

type TableOption struct {
	Name  string
	Value Value
}

type SelectStmt struct {
	With     []CTE
	From     TableExpr
	Distinct bool
	Select   []SelectExpr
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	OrderBy  []OrderExpr
	Limit    *int64
	Offset   *int64

	Union *UnionTail
}

type UnionTail struct {
	All   bool
	Right *SelectStmt
}

type CTE struct {
	Name  string
	Query *SelectStmt
}

type TableExpr interface{ isTableExpr() }

type TableName struct {
	Name  string
	Alias string
	// AsOf carries the snapshot CommitTs from `AS OF <int>`. Zero means no clause.
	AsOf uint64
}

type JoinExpr struct {
	Kind  JoinKind
	Left  TableExpr
	Right TableExpr
	On    Expr
}

func (*TableName) isTableExpr() {}
func (*JoinExpr) isTableExpr()  {}

// flattenFrom walks a left-deep FROM tree into the primary table plus joins in source order, rejecting nested-source joins since JoinExpr.Right is always *TableName in the current grammar.
func (s *SelectStmt) flattenFrom() (*TableName, []*JoinExpr, error) {
	var rev []*JoinExpr
	node := s.From
	for {
		switch n := node.(type) {
		case *TableName:
			out := make([]*JoinExpr, len(rev))
			for i, j := range rev {
				out[len(rev)-1-i] = j
			}
			return n, out, nil
		case *JoinExpr:
			if _, ok := n.Right.(*TableName); !ok {
				return nil, nil, errNestedJoinRight
			}
			rev = append(rev, n)
			node = n.Left
		default:
			return nil, nil, errUnsupportedFrom
		}
	}
}

type JoinKind uint8

const (
	JoinInner JoinKind = iota
	JoinLeft
	JoinRight
	JoinFull
)

func (*CreateTypeStmt) isStmt()  {}
func (*CreateTableStmt) isStmt() {}
func (*AlterTableStmt) isStmt()  {}
func (*InsertStmt) isStmt()      {}
func (*DeleteStmt) isStmt()      {}
func (*UpdateStmt) isStmt()      {}
func (*ExplainStmt) isStmt()     {}
func (*SelectStmt) isStmt()      {}

type SelectExpr struct {
	Expr  Expr
	Alias string
}

type OrderExpr struct {
	Name string
	Expr Expr
	Desc bool
}

type Expr interface{ isExpr() }

type ColumnRef struct {
	Qualifier string
	Name      string
}

type StarRef struct{}

type FuncCall struct {
	Name string
	Args []Expr
	Star bool

	Over *WindowSpec
}

type WindowSpec struct {
	Partition []Expr
	OrderBy   []OrderExpr
	Frame     *WindowFrame
}

type WindowFrame struct {
	IsRange        bool
	StartUnbounded bool
	StartCurrent   bool
	StartPreceding int64
	EndUnbounded   bool
	EndCurrent     bool
	EndFollowing   int64
}

type Literal struct {
	Value Value
}

// Placeholder is a positional ? parameter whose value is bound at execution time.
type Placeholder struct {
	Index int
}

type BinaryExpr struct {
	Left  Expr
	Op    BinaryOp
	Right Expr
}

type BetweenExpr struct {
	Expr Expr
	Low  Expr
	High Expr
}

type InExpr struct {
	Expr   Expr
	Values []Expr
	Not    bool
}

type AndExpr struct {
	Left  Expr
	Right Expr
}

type OrExpr struct {
	Left  Expr
	Right Expr
}

type NotExpr struct {
	Expr Expr
}

type IsNullExpr struct {
	Expr Expr
	Not  bool
}

type WhenClause struct {
	When Expr
	Then Expr
}

type SubqueryExpr struct {
	Query *SelectStmt
}

type ExistsExpr struct {
	Query *SelectStmt
	Not   bool
}

type CaseExpr struct {
	When []WhenClause
	Else Expr
}

func (*ColumnRef) isExpr()    {}
func (*StarRef) isExpr()      {}
func (*FuncCall) isExpr()     {}
func (*Literal) isExpr()      {}
func (*Placeholder) isExpr()  {}
func (*BinaryExpr) isExpr()   {}
func (*BetweenExpr) isExpr()  {}
func (*InExpr) isExpr()       {}
func (*AndExpr) isExpr()      {}
func (*OrExpr) isExpr()       {}
func (*CaseExpr) isExpr()     {}
func (*SubqueryExpr) isExpr() {}
func (*ExistsExpr) isExpr()   {}
func (*NotExpr) isExpr()      {}
func (*IsNullExpr) isExpr()   {}

type BinaryOp uint8

const (
	BinaryEqual BinaryOp = iota
	BinaryNotEqual
	BinaryLess
	BinaryLessEqual
	BinaryGreater
	BinaryGreaterEqual
	BinaryAdd
	BinarySubtract
	BinaryMultiply
	BinaryDivide
	BinaryModulo
	BinaryIntDivide
	BinaryConcat
	BinaryJSONGet
	BinaryJSONGetText
)

type ValueKind uint8

const (
	ValueInvalid ValueKind = iota
	ValueIdent
	ValueInt
	ValueFloat
	ValueString
	ValueBool
	ValueNull
	ValueEnum
)

type Value struct {
	Kind   ValueKind
	Int    int64
	Float  float64
	String string
	Bool   bool
	Enum   uint32
}
