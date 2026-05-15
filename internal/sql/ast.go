// AST node definitions for DripSQL statements and expressions.
// Stmt, Expr, TableExpr are sealed via unexported marker methods; binders type-switch on concrete types.
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
	From    TableExpr
	Select  []SelectExpr
	Where   Expr
	GroupBy []Expr
	Having  Expr
	OrderBy []OrderExpr
	Limit   *int64
	Offset  *int64
}

type TableExpr interface{ isTableExpr() }

type TableName struct {
	Name  string
	Alias string
}

type JoinExpr struct {
	Kind  JoinKind
	Left  TableExpr
	Right TableExpr
	On    Expr
}

func (*TableName) isTableExpr() {}
func (*JoinExpr) isTableExpr()  {}

// flattenFrom walks a left-deep FROM tree and returns (primary, joins-in-source-order).
// JoinExpr.Right is always *TableName in current grammar; nested-source joins are rejected.
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
}

type Literal struct {
	Value Value
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

func (*ColumnRef) isExpr()   {}
func (*StarRef) isExpr()     {}
func (*FuncCall) isExpr()    {}
func (*Literal) isExpr()     {}
func (*BinaryExpr) isExpr()  {}
func (*BetweenExpr) isExpr() {}
func (*InExpr) isExpr()      {}
func (*AndExpr) isExpr()     {}
func (*OrExpr) isExpr()      {}
func (*NotExpr) isExpr()     {}

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
