package sql

type Stmt interface{ stmtNode() }

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

type ExplainStmt struct {
	Analyze bool
	Inner   Stmt
}

type SelectStmt struct {
	Table   string
	Select  []SelectExpr
	Where   Expr
	GroupBy []Expr
	Having  Expr
	OrderBy []OrderExpr
	Limit   *int64
	Offset  *int64
}

func (*CreateTypeStmt) stmtNode()  {}
func (*CreateTableStmt) stmtNode() {}
func (*InsertStmt) stmtNode()      {}
func (*SelectStmt) stmtNode()      {}
func (*ExplainStmt) stmtNode()     {}

type SelectExpr struct {
	Expr  Expr
	Alias string
}

type OrderExpr struct {
	Name string
	Expr Expr
	Desc bool
}

type Expr interface{ exprNode() }

type ColumnRef struct {
	Name string
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

func (*ColumnRef) exprNode()   {}
func (*StarRef) exprNode()     {}
func (*FuncCall) exprNode()    {}
func (*Literal) exprNode()     {}
func (*BinaryExpr) exprNode()  {}
func (*BetweenExpr) exprNode() {}
func (*InExpr) exprNode()      {}
func (*AndExpr) exprNode()     {}
func (*OrExpr) exprNode()      {}
func (*NotExpr) exprNode()     {}

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
	BinaryJSONGet     // -> returns JSON (object key or array index)
	BinaryJSONGetText // ->> returns text (object key or array index, coerced)
)

type ColumnDef struct {
	Name    string
	Type    string
	NotNull bool
}

type TableOption struct {
	Name  string
	Value Value
}

type ValueKind uint8

const (
	ValueInvalid ValueKind = iota
	ValueIdent
	ValueInt
	ValueFloat
	ValueString
	ValueBool
	ValueNull
)

type Value struct {
	Kind   ValueKind
	Int    int64
	Float  float64
	String string
	Bool   bool
}
