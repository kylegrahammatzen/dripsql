// Parser entry points, shared helpers, and full statement grammar (SELECT, INSERT, UPDATE,
// DELETE, EXPLAIN, CREATE TYPE, CREATE TABLE). Expression grammar is in parse_expr.go.
package sql

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type parser struct {
	lex       lexer
	buf       [2]token
	has       [2]bool
	paramSeen int
}

type parserState struct {
	pos int
	buf [2]token
	has [2]bool
}

func Parse(sqlText string) ([]Stmt, error) {
	p := &parser{lex: lexer{sql: sqlText}}
	var stmts []Stmt
	for {
		if err := p.skipSemicolons(); err != nil {
			return nil, err
		}
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.typ == tokEOF {
			return stmts, nil
		}
		stmt, err := p.parseStmt()
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, stmt)
		if err := p.consumeStatementEnd(); err != nil {
			return nil, err
		}
	}
}

func ParseOne(sqlText string) (Stmt, error) {
	stmts, err := Parse(sqlText)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one statement, got %d", len(stmts))
	}
	return stmts[0], nil
}

func (p *parser) parseStmt() (Stmt, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return nil, err
	}
	switch tok.lit {
	case "with":
		return p.parseWithSelect()
	case "select":
		return p.parseSelect()
	case "create":
		return p.parseCreate()
	case "alter":
		return p.parseAlter()
	case "insert":
		return p.parseInsert()
	case "delete":
		return p.parseDelete()
	case "update":
		return p.parseUpdate()
	case "explain":
		return p.parseExplain()
	default:
		return nil, p.errorAt(tok, "unsupported statement %q", tok.lit)
	}
}

func (p *parser) parseAlter() (Stmt, error) {
	if err := p.expectWord("table"); err != nil {
		return nil, err
	}
	tableName, err := p.parseName()
	if err != nil {
		return nil, err
	}
	op, err := p.expect(tokIdent)
	if err != nil {
		return nil, err
	}
	switch op.lit {
	case "rename":
		if err := p.expectWord("column"); err != nil {
			return nil, err
		}
		from, err := p.parseName()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("to"); err != nil {
			return nil, err
		}
		to, err := p.parseName()
		if err != nil {
			return nil, err
		}
		return &AlterTableStmt{Table: tableName, Rename: &AlterRenameColumn{From: from, To: to}}, nil
	case "add":
		if err := p.expectWord("column"); err != nil {
			return nil, err
		}
		name, err := p.parseName()
		if err != nil {
			return nil, err
		}
		typeName, err := p.parseName()
		if err != nil {
			return nil, err
		}
		add := &AlterAddColumn{Name: name, Type: typeName}
		// Optional NOT NULL and DEFAULT modifiers in either order.
		for {
			tok, err := p.peek()
			if err != nil {
				return nil, err
			}
			if tok.typ != tokIdent {
				break
			}
			switch tok.lit {
			case "not":
				if _, err := p.next(); err != nil {
					return nil, err
				}
				if err := p.expectWord("null"); err != nil {
					return nil, err
				}
				add.NotNull = true
			case "default":
				if _, err := p.next(); err != nil {
					return nil, err
				}
				val, err := p.parseValue()
				if err != nil {
					return nil, err
				}
				add.HasDefault = true
				add.Default = val
			default:
				return &AlterTableStmt{Table: tableName, Add: add}, nil
			}
		}
		return &AlterTableStmt{Table: tableName, Add: add}, nil
	case "drop":
		if err := p.expectWord("column"); err != nil {
			return nil, err
		}
		name, err := p.parseName()
		if err != nil {
			return nil, err
		}
		return &AlterTableStmt{Table: tableName, Drop: &AlterDropColumn{Name: name}}, nil
	case "alter":
		if err := p.expectWord("column"); err != nil {
			return nil, err
		}
		name, err := p.parseName()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("type"); err != nil {
			return nil, err
		}
		typeName, err := p.parseName()
		if err != nil {
			return nil, err
		}
		return &AlterTableStmt{Table: tableName, SetType: &AlterColumnType{Name: name, Type: typeName}}, nil
	}
	return nil, p.errorAt(op, "unsupported ALTER TABLE op %q", op.lit)
}

func (p *parser) parseCreate() (Stmt, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return nil, err
	}
	switch tok.lit {
	case "type":
		return p.parseCreateType()
	case "table":
		return p.parseCreateTable()
	default:
		return nil, p.errorAt(tok, "unsupported CREATE target %q", tok.lit)
	}
}

func (p *parser) parseIfNotExists() (bool, error) {
	if ok, err := p.maybeWord("if"); err != nil || !ok {
		return false, err
	}
	if err := p.expectWord("not"); err != nil {
		return false, err
	}
	if err := p.expectWord("exists"); err != nil {
		return false, err
	}
	return true, nil
}

func (p *parser) next() (token, error) {
	if p.has[0] {
		tok := p.buf[0]
		// Shift slot 1 down if it was filled by a peek2.
		p.buf[0] = p.buf[1]
		p.has[0] = p.has[1]
		p.has[1] = false
		return tok, nil
	}
	return p.lex.next()
}

func (p *parser) peek() (token, error) {
	if p.has[0] {
		return p.buf[0], nil
	}
	tok, err := p.lex.next()
	if err != nil {
		return token{}, err
	}
	p.buf[0] = tok
	p.has[0] = true
	return tok, nil
}

// peek2 returns the token after peek without consuming either, needed to disambiguate `AS <alias>` from `AS OF <int>`.
func (p *parser) peek2() (token, error) {
	if _, err := p.peek(); err != nil {
		return token{}, err
	}
	if p.has[1] {
		return p.buf[1], nil
	}
	tok, err := p.lex.next()
	if err != nil {
		return token{}, err
	}
	p.buf[1] = tok
	p.has[1] = true
	return tok, nil
}

func (p *parser) expect(typ tokenType) (token, error) {
	tok, err := p.next()
	if err != nil {
		return token{}, err
	}
	if tok.typ != typ {
		return token{}, p.errorAt(tok, "expected %s", tokenName(typ))
	}
	return tok, nil
}

func (p *parser) expectWord(word string) error {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return err
	}
	if tok.lit != word {
		return p.errorAt(tok, "expected %s", word)
	}
	return nil
}

func (p *parser) maybeWord(word string) (bool, error) {
	tok, err := p.peek()
	if err != nil {
		return false, err
	}
	if tok.typ == tokIdent && tok.lit == word {
		_, _ = p.next()
		return true, nil
	}
	return false, nil
}

func (p *parser) maybe(typ tokenType) (bool, error) {
	tok, err := p.peek()
	if err != nil {
		return false, err
	}
	if tok.typ == typ {
		_, _ = p.next()
		return true, nil
	}
	return false, nil
}

func (p *parser) mark() parserState {
	return parserState{pos: p.lex.pos, buf: p.buf, has: p.has}
}

func (p *parser) restore(state parserState) {
	p.lex.pos = state.pos
	p.buf = state.buf
	p.has = state.has
}

func (p *parser) skipSemicolons() error {
	for {
		tok, err := p.peek()
		if err != nil {
			return err
		}
		if tok.typ != tokSemicolon {
			return nil
		}
		_, _ = p.next()
	}
}

func (p *parser) consumeStatementEnd() error {
	tok, err := p.peek()
	if err != nil {
		return err
	}
	if tok.typ == tokSemicolon {
		_, _ = p.next()
		return nil
	}
	if tok.typ == tokEOF {
		return nil
	}
	return p.errorAt(tok, "expected statement end")
}

func (p *parser) errorAt(tok token, format string, args ...any) error {
	return fmt.Errorf("at byte %d: %s", tok.pos, fmt.Sprintf(format, args...))
}

func (p *parser) parseName() (string, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return "", err
	}
	return tok.lit, nil
}

func (p *parser) parseValue() (Value, error) {
	tok, err := p.next()
	if err != nil {
		return Value{}, err
	}
	negate := false
	if tok.typ == tokMinus {
		negate = true
		tok, err = p.next()
		if err != nil {
			return Value{}, err
		}
	}
	switch tok.typ {
	case tokString:
		if negate {
			return Value{}, p.errorAt(tok, "cannot negate string literal")
		}
		return Value{Kind: ValueString, String: tok.lit}, nil
	case tokInt:
		value, err := strconv.ParseInt(tok.lit, 10, 64)
		if err != nil {
			return Value{}, p.errorAt(tok, "invalid integer literal")
		}
		if negate {
			value = -value
		}
		return Value{Kind: ValueInt, Int: value}, nil
	case tokFloat:
		value, err := strconv.ParseFloat(tok.lit, 64)
		if err != nil {
			return Value{}, p.errorAt(tok, "invalid float literal")
		}
		if negate {
			value = -value
		}
		return Value{Kind: ValueFloat, Float: value}, nil
	case tokIdent:
		if !tok.quoted {
			switch tok.lit {
			case "true":
				if negate {
					return Value{}, p.errorAt(tok, "cannot negate bool literal")
				}
				return Value{Kind: ValueBool, Bool: true}, nil
			case "false":
				if negate {
					return Value{}, p.errorAt(tok, "cannot negate bool literal")
				}
				return Value{Kind: ValueBool, Bool: false}, nil
			case "null":
				if negate {
					return Value{}, p.errorAt(tok, "cannot negate null")
				}
				return Value{Kind: ValueNull}, nil
			}
		}
	}
	return Value{}, p.errorAt(tok, "expected literal")
}

func tokenName(typ tokenType) string {
	switch typ {
	case tokEOF:
		return "end of input"
	case tokIdent:
		return "identifier"
	case tokString:
		return "string literal"
	case tokInt:
		return "integer literal"
	case tokFloat:
		return "float literal"
	case tokComma:
		return ","
	case tokLParen:
		return "("
	case tokRParen:
		return ")"
	case tokSemicolon:
		return ";"
	case tokEqual:
		return "="
	case tokNotEqual:
		return "!="
	case tokLess:
		return "<"
	case tokLessEqual:
		return "<="
	case tokGreater:
		return ">"
	case tokGreaterEqual:
		return ">="
	case tokStar:
		return "*"
	case tokPlus:
		return "+"
	case tokMinus:
		return "-"
	case tokSlash:
		return "/"
	case tokPercent:
		return "%"
	case tokConcat:
		return "||"
	case tokJSONGet:
		return "->"
	case tokJSONGetText:
		return "->>"
	case tokDot:
		return "."
	default:
		return "token"
	}
}

// DDL recursive descent for CREATE TYPE AS ENUM and CREATE TABLE, supporting only enum types and column-level NOT NULL so other constraints raise a clear error.

func (p *parser) parseCreateType() (*CreateTypeStmt, error) {
	ifNotExists, err := p.parseIfNotExists()
	if err != nil {
		return nil, err
	}
	name, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if err := p.expectWord("as"); err != nil {
		return nil, err
	}
	if err := p.expectWord("enum"); err != nil {
		return nil, fmt.Errorf("unsupported CREATE TYPE: only ENUM types are supported")
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var labels []string
	for {
		label, err := p.expect(tokString)
		if err != nil {
			return nil, err
		}
		labels = append(labels, label.lit)
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		switch tok.typ {
		case tokComma:
			_, _ = p.next()
		case tokRParen:
			_, _ = p.next()
			return &CreateTypeStmt{Name: name, IfNotExists: ifNotExists, EnumLabels: labels}, nil
		default:
			return nil, p.errorAt(tok, "expected , or )")
		}
	}
}

func (p *parser) parseCreateTable() (*CreateTableStmt, error) {
	ifNotExists, err := p.parseIfNotExists()
	if err != nil {
		return nil, err
	}
	name, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}

	var columns []ColumnDef
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.typ == tokRParen {
			if len(columns) == 0 {
				return nil, p.errorAt(tok, "CREATE TABLE requires at least one column")
			}
			return nil, p.errorAt(tok, "trailing comma in column list")
		}
		if tok.typ == tokIdent && isUnsupportedTableConstraint(tok.lit) {
			return nil, p.errorAt(tok, "unsupported CREATE TABLE constraint %q", tok.lit)
		}
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)

		tok, err = p.peek()
		if err != nil {
			return nil, err
		}
		switch tok.typ {
		case tokComma:
			_, _ = p.next()
		case tokRParen:
			_, _ = p.next()
		default:
			return nil, p.errorAt(tok, "expected , or )")
		}
		if tok.typ == tokRParen {
			break
		}
	}

	options, err := p.parseOptionalTableOptions()
	if err != nil {
		return nil, err
	}
	return &CreateTableStmt{Name: name, IfNotExists: ifNotExists, Columns: columns, Options: options}, nil
}

func (p *parser) parseColumnDef() (ColumnDef, error) {
	name, err := p.parseName()
	if err != nil {
		return ColumnDef{}, err
	}
	typ, err := p.parseType()
	if err != nil {
		return ColumnDef{}, err
	}
	col := ColumnDef{Name: name, Type: typ}
	for {
		tok, err := p.peek()
		if err != nil {
			return ColumnDef{}, err
		}
		if tok.typ == tokComma || tok.typ == tokRParen {
			return col, nil
		}
		if tok.typ != tokIdent {
			return ColumnDef{}, p.errorAt(tok, "unexpected column constraint")
		}
		switch tok.lit {
		case "not":
			_, _ = p.next()
			if err := p.expectWord("null"); err != nil {
				return ColumnDef{}, err
			}
			col.NotNull = true
		case "null":
			_, _ = p.next()
			col.NotNull = false
		case "primary", "unique", "check", "default", "references", "constraint", "foreign":
			return ColumnDef{}, p.errorAt(tok, "unsupported column constraint %q", tok.lit)
		case "with":
			_, _ = p.next()
			opts, err := p.parseColumnOptions()
			if err != nil {
				return ColumnDef{}, err
			}
			for _, o := range opts {
				switch schema.NormalizeName(o.Name) {
				case "codec":
					if o.Value.Kind != ValueString {
						return ColumnDef{}, p.errorAt(tok, "codec value must be a string literal")
					}
					col.Codec = o.Value.String
				default:
					return ColumnDef{}, p.errorAt(tok, "unknown column option %q", o.Name)
				}
			}
		default:
			return ColumnDef{}, p.errorAt(tok, "unexpected token %q in column definition", tok.lit)
		}
	}
}

func (p *parser) parseType() (string, error) {
	var parts []string
	for {
		tok, err := p.peek()
		if err != nil {
			return "", err
		}
		if tok.typ != tokIdent {
			break
		}
		// A "with" followed by "(" is the column option marker, while a bare "with" inside multi-word types like `timestamp with time zone` keeps flowing into the type name.
		if tok.lit == "with" {
			state := p.mark()
			_, _ = p.next()
			next, err := p.peek()
			p.restore(state)
			if err == nil && next.typ == tokLParen {
				break
			}
		}
		if tok.lit != "with" && (tok.lit == "not" || tok.lit == "null" || tok.lit == "default" || tok.lit == "references" || isUnsupportedTableConstraint(tok.lit)) {
			break
		}
		_, _ = p.next()
		parts = append(parts, tok.lit)
	}
	if len(parts) == 0 {
		tok, err := p.peek()
		if err != nil {
			return "", err
		}
		return "", p.errorAt(tok, "expected column type")
	}
	return strings.Join(parts, " "), nil
}

func (p *parser) parseOptionalTableOptions() ([]TableOption, error) {
	if ok, err := p.maybeWord("with"); err != nil || !ok {
		return nil, err
	}
	return p.parseColumnOptions()
}

// parseColumnOptions reads `( name = scalar [, ...] )` starting at the opening paren, serving both table-level WITH (keyword already consumed) and per-column WITH.
func (p *parser) parseColumnOptions() ([]TableOption, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var options []TableOption
	for {
		name, err := p.parseName()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokEqual); err != nil {
			return nil, err
		}
		value, err := p.parseOptionScalar()
		if err != nil {
			return nil, err
		}
		options = append(options, TableOption{Name: name, Value: value})
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		switch tok.typ {
		case tokComma:
			_, _ = p.next()
		case tokRParen:
			_, _ = p.next()
			return options, nil
		default:
			return nil, p.errorAt(tok, "expected , or )")
		}
	}
}

func (p *parser) parseOptionScalar() (Value, error) {
	tok, err := p.next()
	if err != nil {
		return Value{}, err
	}
	switch tok.typ {
	case tokIdent:
		if tok.lit == "true" {
			return Value{Kind: ValueBool, Bool: true}, nil
		}
		if tok.lit == "false" {
			return Value{Kind: ValueBool, Bool: false}, nil
		}
		return Value{Kind: ValueIdent, String: tok.lit}, nil
	case tokString:
		return Value{Kind: ValueString, String: tok.lit}, nil
	case tokInt:
		value, err := strconv.ParseInt(tok.lit, 10, 64)
		if err != nil {
			return Value{}, p.errorAt(tok, "invalid integer literal")
		}
		return Value{Kind: ValueInt, Int: value}, nil
	default:
		return Value{}, p.errorAt(tok, "expected table option value")
	}
}

func isUnsupportedTableConstraint(word string) bool {
	switch word {
	case "primary", "unique", "check", "constraint", "foreign", "family", "index":
		return true
	default:
		return false
	}
}

// INSERT recursive descent where the column list is optional and the row count is implicit in the VALUES tuples.

func (p *parser) parseInsert() (*InsertStmt, error) {
	if err := p.expectWord("into"); err != nil {
		return nil, err
	}
	tableName, err := p.parseName()
	if err != nil {
		return nil, err
	}

	var columns []string
	if ok, err := p.maybe(tokLParen); err != nil {
		return nil, err
	} else if ok {
		for {
			name, err := p.parseName()
			if err != nil {
				return nil, err
			}
			columns = append(columns, name)
			tok, err := p.peek()
			if err != nil {
				return nil, err
			}
			switch tok.typ {
			case tokComma:
				_, _ = p.next()
			case tokRParen:
				_, _ = p.next()
				goto columnsDone
			default:
				return nil, p.errorAt(tok, "expected , or )")
			}
		}
	}

columnsDone:
	if err := p.expectWord("values"); err != nil {
		return nil, err
	}

	var rows [][]Value
	for {
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		var row []Value
		for {
			value, err := p.parseValue()
			if err != nil {
				return nil, err
			}
			row = append(row, value)

			tok, err := p.peek()
			if err != nil {
				return nil, err
			}
			switch tok.typ {
			case tokComma:
				_, _ = p.next()
			case tokRParen:
				_, _ = p.next()
				goto rowDone
			default:
				return nil, p.errorAt(tok, "expected , or )")
			}
		}

	rowDone:
		rows = append(rows, row)
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.typ != tokComma {
			return &InsertStmt{Table: tableName, Columns: columns, Values: rows}, nil
		}
		_, _ = p.next()
	}
}

// DELETE FROM table with an optional WHERE, where no aliases or joins exist yet and an empty WHERE deletes every row.

func (p *parser) parseDelete() (*DeleteStmt, error) {
	if err := p.expectWord("from"); err != nil {
		return nil, err
	}
	tableName, err := p.parseName()
	if err != nil {
		return nil, err
	}
	where, err := p.parseOptionalWhere()
	if err != nil {
		return nil, err
	}
	return &DeleteStmt{Table: tableName, Where: where}, nil
}

// UPDATE table SET assignments with literal-only values, where an empty WHERE rewrites every row in the table.

func (p *parser) parseUpdate() (*UpdateStmt, error) {
	tableName, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if err := p.expectWord("set"); err != nil {
		return nil, err
	}
	assignments, err := p.parseAssignments()
	if err != nil {
		return nil, err
	}
	where, err := p.parseOptionalWhere()
	if err != nil {
		return nil, err
	}
	return &UpdateStmt{Table: tableName, Assignments: assignments, Where: where}, nil
}

func (p *parser) parseAssignments() ([]Assignment, error) {
	var out []Assignment
	for {
		col, err := p.parseName()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokEqual); err != nil {
			return nil, err
		}
		val, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out = append(out, Assignment{Column: col, Value: val})
		ok, err := p.maybe(tokComma)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("UPDATE SET requires at least one assignment")
	}
	return out, nil
}

// EXPLAIN and EXPLAIN ANALYZE wrap an inner SelectStmt, and only SELECT is allowed for now.

func (p *parser) parseExplain() (Stmt, error) {
	analyze, err := p.maybeWord("analyze")
	if err != nil {
		return nil, err
	}
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.typ != tokIdent {
		return nil, p.errorAt(tok, "EXPLAIN requires a SELECT statement")
	}
	if tok.lit != "select" {
		return nil, p.errorAt(tok, "EXPLAIN supports SELECT only, got %q", tok.lit)
	}
	_, _ = p.next()
	inner, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	return &ExplainStmt{Analyze: analyze, Inner: inner}, nil
}

// SELECT recursive descent covering projection, WHERE, GROUP BY, HAVING, ORDER BY, and LIMIT/OFFSET, with JSON path ops binding tighter than mul/div/mod which bind tighter than concat/plus/minus.

func (p *parser) parseWithSelect() (*SelectStmt, error) {
	ctes, err := p.parseCTEs()
	if err != nil {
		return nil, err
	}
	if err := p.expectWord("select"); err != nil {
		return nil, err
	}
	stmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.With = ctes
	return stmt, nil
}

func (p *parser) parseCTEs() ([]CTE, error) {
	var out []CTE
	seen := make(map[string]struct{})
	for {
		name, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		key := schema.NormalizeName(name)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate CTE name %q", name)
		}
		seen[key] = struct{}{}
		if err := p.expectWord("as"); err != nil {
			return nil, err
		}
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		if err := p.expectWord("select"); err != nil {
			return nil, err
		}
		inner, err := p.parseSelect()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		out = append(out, CTE{Name: name, Query: inner})
		more, err := p.maybe(tokComma)
		if err != nil {
			return nil, err
		}
		if !more {
			return out, nil
		}
	}
}

func (p *parser) expectIdent() (string, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return "", err
	}
	return tok.lit, nil
}

func (p *parser) parseSelect() (*SelectStmt, error) {
	stmt, err := p.parseSelectBody()
	if err != nil {
		return nil, err
	}
	if ok, err := p.maybeWord("union"); err != nil {
		return nil, err
	} else if ok {
		all, err := p.maybeWord("all")
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("select"); err != nil {
			return nil, err
		}
		right, err := p.parseSelectBody()
		if err != nil {
			return nil, err
		}
		stmt.Union = &UnionTail{All: all, Right: right}
	}
	orderBy, err := p.parseOptionalOrderBy()
	if err != nil {
		return nil, err
	}
	limit, err := p.parseOptionalLimit()
	if err != nil {
		return nil, err
	}
	offset, err := p.parseOptionalOffset()
	if err != nil {
		return nil, err
	}
	stmt.OrderBy = orderBy
	stmt.Limit = limit
	stmt.Offset = offset
	return stmt, nil
}

func (p *parser) parseSelectBody() (*SelectStmt, error) {
	distinct, err := p.maybeWord("distinct")
	if err != nil {
		return nil, err
	}
	selectExprs, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}

	if err := p.expectWord("from"); err != nil {
		return nil, err
	}

	from, err := p.parseFrom()
	if err != nil {
		return nil, err
	}

	where, err := p.parseOptionalWhere()
	if err != nil {
		return nil, err
	}

	groupBy, err := p.parseOptionalGroupBy()
	if err != nil {
		return nil, err
	}
	having, err := p.parseOptionalHaving()
	if err != nil {
		return nil, err
	}
	return &SelectStmt{
		From:     from,
		Distinct: distinct,
		Select:   selectExprs,
		Where:    where,
		GroupBy:  groupBy,
		Having:   having,
	}, nil
}

// parseFrom reads a base table plus a left-deep chain of JOINs, each wrapping the current tree as Left and the new TableName as Right.
func (p *parser) parseFrom() (TableExpr, error) {
	base, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	var node TableExpr = base
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.typ != tokIdent || tok.quoted {
			return node, nil
		}
		kind := JoinInner
		switch tok.lit {
		case "join", "inner":
			_, _ = p.next()
			if tok.lit == "inner" {
				if err := p.expectWord("join"); err != nil {
					return nil, err
				}
			}
		case "left":
			_, _ = p.next()
			if _, err := p.maybeWord("outer"); err != nil {
				return nil, err
			}
			if err := p.expectWord("join"); err != nil {
				return nil, err
			}
			kind = JoinLeft
		case "right":
			_, _ = p.next()
			if _, err := p.maybeWord("outer"); err != nil {
				return nil, err
			}
			if err := p.expectWord("join"); err != nil {
				return nil, err
			}
			kind = JoinRight
		case "full":
			_, _ = p.next()
			if _, err := p.maybeWord("outer"); err != nil {
				return nil, err
			}
			if err := p.expectWord("join"); err != nil {
				return nil, err
			}
			kind = JoinFull
		default:
			return node, nil
		}
		right, err := p.parseTableName()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("on"); err != nil {
			return nil, err
		}
		on, err := p.parsePredicateOr()
		if err != nil {
			return nil, err
		}
		node = &JoinExpr{Kind: kind, Left: node, Right: right, On: on}
	}
}

func (p *parser) parseTableName() (*TableName, error) {
	name, err := p.parseName()
	if err != nil {
		return nil, err
	}
	// Disambiguate `AS <alias>` from `AS OF <int>` by peeking two tokens ahead.
	first, err := p.peek()
	if err != nil {
		return nil, err
	}
	tn := &TableName{Name: name}
	if first.typ == tokIdent && !first.quoted && first.lit == "as" {
		second, err := p.peek2()
		if err != nil {
			return nil, err
		}
		if second.typ == tokIdent && !second.quoted && second.lit == "of" {
			if _, err := p.next(); err != nil { // consume "as"
				return nil, err
			}
			if _, err := p.next(); err != nil { // consume "of"
				return nil, err
			}
			ts, err := p.parseAsOfTimestamp()
			if err != nil {
				return nil, err
			}
			tn.AsOf = ts
			return tn, nil
		}
	}
	alias, err := p.parseOptionalAlias(aliasStopsTable...)
	if err != nil {
		return nil, err
	}
	tn.Alias = alias
	// `<table> [<alias>] AS OF <int>` after a bare alias.
	if next, err := p.peek(); err == nil && next.typ == tokIdent && !next.quoted && next.lit == "as" {
		if second, err := p.peek2(); err == nil && second.typ == tokIdent && !second.quoted && second.lit == "of" {
			if _, err := p.next(); err != nil {
				return nil, err
			}
			if _, err := p.next(); err != nil {
				return nil, err
			}
			ts, err := p.parseAsOfTimestamp()
			if err != nil {
				return nil, err
			}
			tn.AsOf = ts
		}
	}
	return tn, nil
}

func (p *parser) parseAsOfTimestamp() (uint64, error) {
	tok, err := p.next()
	if err != nil {
		return 0, err
	}
	if tok.typ != tokInt {
		return 0, p.errorAt(tok, "expected integer commit_ts after AS OF, got %s", tokenName(tok.typ))
	}
	ts, err := strconv.ParseUint(tok.lit, 10, 64)
	if err != nil {
		return 0, p.errorAt(tok, "AS OF: invalid uint64 %q: %v", tok.lit, err)
	}
	if ts == 0 {
		return 0, p.errorAt(tok, "AS OF 0 is invalid; commit_ts begins at 1")
	}
	return ts, nil
}

func (p *parser) parseSelectList() ([]SelectExpr, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	if tok.typ == tokStar {
		_, _ = p.next()
		return []SelectExpr{{Expr: &StarRef{}}}, nil
	}

	expr, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	alias, err := p.parseOptionalAlias(aliasStopsColumn...)
	if err != nil {
		return nil, err
	}
	exprs := []SelectExpr{{Expr: expr, Alias: alias}}

	for {
		ok, err := p.maybe(tokComma)
		if err != nil || !ok {
			return exprs, err
		}
		expr, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		alias, err := p.parseOptionalAlias(aliasStopsColumn...)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, SelectExpr{Expr: expr, Alias: alias})
	}
}

// parseScalarExpr drives one precedence-climbing loop instead of a function per level, with add/sub/concat lowest, mul/div/mod above, and JSON path ops binding tightest.
func (p *parser) parseScalarExpr() (Expr, string, error) {
	return p.parseScalarPrecedence(1)
}

func (p *parser) parseScalarPrecedence(minPrec int) (Expr, string, error) {
	left, name, err := p.parseScalarPrimary()
	if err != nil {
		return nil, "", err
	}
	for {
		op, prec, ok, err := p.peekScalarOp()
		if err != nil || !ok || prec < minPrec {
			return left, name, err
		}
		if _, err := p.next(); err != nil {
			return nil, "", err
		}
		right, _, err := p.parseScalarPrecedence(prec + 1)
		if err != nil {
			return nil, "", err
		}
		left = &BinaryExpr{Left: left, Op: op, Right: right}
		name = ""
	}
}

func (p *parser) peekScalarOp() (BinaryOp, int, bool, error) {
	tok, err := p.peek()
	if err != nil {
		return 0, 0, false, err
	}
	switch tok.typ {
	case tokJSONGetText:
		return BinaryJSONGetText, 3, true, nil
	case tokJSONGet:
		return BinaryJSONGet, 3, true, nil
	case tokStar:
		return BinaryMultiply, 2, true, nil
	case tokSlash:
		return BinaryDivide, 2, true, nil
	case tokPercent:
		return BinaryModulo, 2, true, nil
	case tokConcat:
		return BinaryConcat, 1, true, nil
	case tokPlus:
		return BinaryAdd, 1, true, nil
	case tokMinus:
		return BinarySubtract, 1, true, nil
	case tokIdent:
		if !tok.quoted {
			switch tok.lit {
			case "mod":
				return BinaryModulo, 2, true, nil
			case "div":
				return BinaryIntDivide, 2, true, nil
			}
		}
	}
	return 0, 0, false, nil
}

func (p *parser) parseScalarPrimary() (Expr, string, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, "", err
	}
	if tok.typ == tokMinus {
		_, _ = p.next()
		inner, _, err := p.parseScalarPrimary()
		if err != nil {
			return nil, "", err
		}
		if lit, ok := inner.(*Literal); ok {
			switch lit.Value.Kind {
			case ValueInt:
				return &Literal{Value: Value{Kind: ValueInt, Int: -lit.Value.Int}}, "", nil
			case ValueFloat:
				return &Literal{Value: Value{Kind: ValueFloat, Float: -lit.Value.Float}}, "", nil
			}
		}
		return &BinaryExpr{Left: &Literal{Value: Value{Kind: ValueInt, Int: 0}}, Op: BinarySubtract, Right: inner}, "", nil
	}
	if tok.typ == tokString || tok.typ == tokInt || tok.typ == tokFloat ||
		(tok.typ == tokIdent && !tok.quoted && (tok.lit == "true" || tok.lit == "false" || tok.lit == "null")) {
		value, err := p.parseValue()
		if err != nil {
			return nil, "", err
		}
		return &Literal{Value: value}, "", nil
	}
	if tok.typ == tokPlaceholder {
		_, _ = p.next()
		p.paramSeen++
		return &Placeholder{Index: p.paramSeen}, "", nil
	}
	if tok.typ == tokIdent && !tok.quoted && tok.lit == "case" {
		_, _ = p.next()
		ce, err := p.parseCaseTail()
		if err != nil {
			return nil, "", err
		}
		return ce, "", nil
	}
	if ok, err := p.maybe(tokLParen); err != nil || ok {
		if err != nil {
			return nil, "", err
		}
		next, perr := p.peek()
		if perr != nil {
			return nil, "", perr
		}
		if next.typ == tokIdent && !next.quoted && next.lit == "select" {
			_, _ = p.next()
			inner, err := p.parseSelect()
			if err != nil {
				return nil, "", err
			}
			if _, err := p.expect(tokRParen); err != nil {
				return nil, "", err
			}
			return &SubqueryExpr{Query: inner}, "", nil
		}
		expr, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, "", err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, "", err
		}
		return expr, "", nil
	}
	name, err := p.parseName()
	if err != nil {
		return nil, "", err
	}
	if ok, err := p.maybe(tokLParen); err != nil || ok {
		if err != nil {
			return nil, "", err
		}
		expr, err := p.parseScalarCallArgs(name)
		return expr, "", err
	}
	if ok, err := p.maybe(tokDot); err != nil || ok {
		if err != nil {
			return nil, "", err
		}
		colName, err := p.parseName()
		if err != nil {
			return nil, "", err
		}
		return &ColumnRef{Qualifier: name, Name: colName}, colName, nil
	}
	return &ColumnRef{Name: name}, name, nil
}

func (p *parser) parseCaseTail() (*CaseExpr, error) {
	out := &CaseExpr{}
	for {
		if err := p.expectWord("when"); err != nil {
			return nil, err
		}
		whenExpr, err := p.parsePredicateOr()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("then"); err != nil {
			return nil, err
		}
		thenExpr, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		out.When = append(out.When, WhenClause{When: whenExpr, Then: thenExpr})
		next, err := p.peek()
		if err != nil {
			return nil, err
		}
		if next.typ != tokIdent || next.quoted {
			return nil, p.errorAt(next, "expected WHEN, ELSE, or END in CASE")
		}
		if next.lit == "when" {
			continue
		}
		if next.lit == "else" {
			_, _ = p.next()
			elseExpr, _, err := p.parseScalarExpr()
			if err != nil {
				return nil, err
			}
			out.Else = elseExpr
			if err := p.expectWord("end"); err != nil {
				return nil, err
			}
			return out, nil
		}
		if next.lit == "end" {
			_, _ = p.next()
			return out, nil
		}
		return nil, p.errorAt(next, "expected WHEN, ELSE, or END in CASE; got %q", next.lit)
	}
}

func (p *parser) parseScalarCallArgs(funcName string) (Expr, error) {
	call, err := p.parseScalarCallBody(funcName)
	if err != nil {
		return nil, err
	}
	over, err := p.maybeParseOver()
	if err != nil {
		return nil, err
	}
	if over != nil {
		call.Over = over
	}
	return call, nil
}

func (p *parser) parseScalarCallBody(funcName string) (*FuncCall, error) {
	if ok, err := p.maybe(tokRParen); err != nil || ok {
		return &FuncCall{Name: funcName}, err
	}
	if ok, err := p.maybe(tokStar); err != nil || ok {
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return &FuncCall{Name: funcName, Star: true}, nil
	}
	args := make([]Expr, 0, 1)
	for {
		arg, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if ok, err := p.maybe(tokComma); err != nil || !ok {
			if err != nil {
				return nil, err
			}
			break
		}
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return &FuncCall{Name: funcName, Args: args}, nil
}

func (p *parser) maybeParseOver() (*WindowSpec, error) {
	ok, err := p.maybeWord("over")
	if err != nil || !ok {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	spec := &WindowSpec{}
	if ok, err := p.maybeWord("partition"); err != nil {
		return nil, err
	} else if ok {
		if err := p.expectWord("by"); err != nil {
			return nil, err
		}
		for {
			expr, _, err := p.parseScalarExpr()
			if err != nil {
				return nil, err
			}
			spec.Partition = append(spec.Partition, expr)
			if more, err := p.maybe(tokComma); err != nil || !more {
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}
	if ok, err := p.maybeWord("order"); err != nil {
		return nil, err
	} else if ok {
		if err := p.expectWord("by"); err != nil {
			return nil, err
		}
		for {
			expr, name, err := p.parseScalarExpr()
			if err != nil {
				return nil, err
			}
			desc := false
			if d, err := p.maybeWord("desc"); err != nil {
				return nil, err
			} else if d {
				desc = true
			} else if asc, err := p.maybeWord("asc"); err != nil {
				return nil, err
			} else if asc {
				desc = false
			}
			spec.OrderBy = append(spec.OrderBy, OrderExpr{Name: name, Expr: expr, Desc: desc})
			if more, err := p.maybe(tokComma); err != nil || !more {
				if err != nil {
					return nil, err
				}
				break
			}
		}
	}
	if ok, err := p.maybeWord("rows"); err != nil {
		return nil, err
	} else if ok {
		frame, err := p.parseWindowFrame(false)
		if err != nil {
			return nil, err
		}
		spec.Frame = frame
	} else if ok, err := p.maybeWord("range"); err != nil {
		return nil, err
	} else if ok {
		frame, err := p.parseWindowFrame(true)
		if err != nil {
			return nil, err
		}
		spec.Frame = frame
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return spec, nil
}

func (p *parser) parseWindowFrame(isRange bool) (*WindowFrame, error) {
	if err := p.expectWord("between"); err != nil {
		return nil, err
	}
	frame := &WindowFrame{IsRange: isRange}
	if err := p.parseFrameBound(frame, true); err != nil {
		return nil, err
	}
	if err := p.expectWord("and"); err != nil {
		return nil, err
	}
	if err := p.parseFrameBound(frame, false); err != nil {
		return nil, err
	}
	return frame, nil
}

func (p *parser) parseFrameBound(frame *WindowFrame, isStart bool) error {
	if ok, err := p.maybeWord("unbounded"); err != nil {
		return err
	} else if ok {
		if isStart {
			if err := p.expectWord("preceding"); err != nil {
				return err
			}
			frame.StartUnbounded = true
			return nil
		}
		if err := p.expectWord("following"); err != nil {
			return err
		}
		frame.EndUnbounded = true
		return nil
	}
	if ok, err := p.maybeWord("current"); err != nil {
		return err
	} else if ok {
		if err := p.expectWord("row"); err != nil {
			return err
		}
		if isStart {
			frame.StartCurrent = true
		} else {
			frame.EndCurrent = true
		}
		return nil
	}
	tok, err := p.expect(tokInt)
	if err != nil {
		return err
	}
	n, err := strconv.ParseInt(tok.lit, 10, 64)
	if err != nil {
		return p.errorAt(tok, "frame bound: %v", err)
	}
	if isStart {
		if err := p.expectWord("preceding"); err != nil {
			return err
		}
		frame.StartPreceding = n
		return nil
	}
	if err := p.expectWord("following"); err != nil {
		return err
	}
	frame.EndFollowing = n
	return nil
}

// parseOptionalAlias accepts an optional alias where `as` forces the next token to be a name and a bare ident counts as an alias unless it is one of the stops keywords that legitimately follow.
func (p *parser) parseOptionalAlias(stops ...string) (string, error) {
	ok, err := p.maybeWord("as")
	if err != nil {
		return "", err
	}
	if ok {
		return p.parseName()
	}
	tok, err := p.peek()
	if err != nil || tok.typ != tokIdent || tok.quoted {
		return "", err
	}
	if slices.Contains(stops, tok.lit) {
		return "", nil
	}
	_, _ = p.next()
	return tok.lit, nil
}

// Stop sets used at each alias call site.
var (
	aliasStopsTable  = []string{"join", "inner", "left", "right", "full", "outer", "on", "where", "group", "having", "order", "limit", "offset", "union"}
	aliasStopsColumn = []string{"from"}
)

func (p *parser) parseOptionalWhere() (Expr, error) {
	ok, err := p.maybeWord("where")
	if err != nil || !ok {
		return nil, err
	}
	return p.parsePredicateOr()
}

func (p *parser) parseOptionalHaving() (Expr, error) {
	ok, err := p.maybeWord("having")
	if err != nil || !ok {
		return nil, err
	}
	return p.parsePredicateOr()
}

func (p *parser) parsePredicateAfterLeft(left Expr) (Expr, error) {
	if ok, err := p.maybeWord("between"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		low, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("and"); err != nil {
			return nil, err
		}
		high, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: left, Low: low, High: high}, nil
	}

	if ok, err := p.maybeWord("is"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		not, err := p.maybeWord("not")
		if err != nil {
			return nil, err
		}
		if err := p.expectWord("null"); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: left, Not: not}, nil
	}

	inNot := false
	if ok, err := p.maybeWord("not"); err != nil {
		return nil, err
	} else if ok {
		inNot = true
	}
	if ok, err := p.maybeWord("in"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		next, perr := p.peek()
		if perr != nil {
			return nil, perr
		}
		if next.typ == tokIdent && !next.quoted && next.lit == "select" {
			_, _ = p.next()
			inner, err := p.parseSelect()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokRParen); err != nil {
				return nil, err
			}
			return &InExpr{Expr: left, Values: []Expr{&SubqueryExpr{Query: inner}}, Not: inNot}, nil
		}
		values := make([]Expr, 0, 2)
		for {
			value, _, err := p.parseScalarExpr()
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			if ok, err := p.maybe(tokComma); err != nil || !ok {
				if err != nil {
					return nil, err
				}
				break
			}
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return &InExpr{Expr: left, Values: values, Not: inNot}, nil
	} else if inNot {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		return nil, p.errorAt(tok, "expected IN after NOT")
	}

	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	var op BinaryOp
	switch tok.typ {
	case tokEqual:
		op = BinaryEqual
	case tokNotEqual:
		op = BinaryNotEqual
	case tokLess:
		op = BinaryLess
	case tokLessEqual:
		op = BinaryLessEqual
	case tokGreater:
		op = BinaryGreater
	case tokGreaterEqual:
		op = BinaryGreaterEqual
	default:
		return nil, p.errorAt(tok, "expected predicate operator")
	}
	if _, err := p.next(); err != nil {
		return nil, err
	}
	right, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

func (p *parser) parseOptionalGroupBy() ([]Expr, error) {
	ok, err := p.maybeWord("group")
	if err != nil || !ok {
		return nil, err
	}
	if err := p.expectWord("by"); err != nil {
		return nil, err
	}
	keys := make([]Expr, 0, 1)
	for {
		expr, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		keys = append(keys, expr)
		if ok, err := p.maybe(tokComma); err != nil || !ok {
			return keys, err
		}
	}
}

func (p *parser) parseOptionalOrderBy() ([]OrderExpr, error) {
	ok, err := p.maybeWord("order")
	if err != nil || !ok {
		return nil, err
	}
	if err := p.expectWord("by"); err != nil {
		return nil, err
	}
	orderBy := make([]OrderExpr, 0, 1)
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		expr, name, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		if _, ok := expr.(*Literal); ok {
			return nil, p.errorAt(tok, "expected identifier")
		}
		desc := false
		if ok, err := p.maybeWord("asc"); err != nil || ok {
			orderBy = append(orderBy, OrderExpr{Name: name, Expr: expr, Desc: desc})
			if err != nil {
				return nil, err
			}
			if ok, err := p.maybe(tokComma); err != nil || !ok {
				if err != nil {
					return nil, err
				}
				return orderBy, nil
			}
			continue
		}
		if ok, err := p.maybeWord("desc"); err != nil || ok {
			if err != nil {
				return nil, err
			}
			desc = true
		}
		orderBy = append(orderBy, OrderExpr{Name: name, Expr: expr, Desc: desc})
		if ok, err := p.maybe(tokComma); err != nil || !ok {
			if err != nil {
				return nil, err
			}
			return orderBy, nil
		}
	}
}

func (p *parser) parseOptionalLimit() (*int64, error) {
	return p.parseOptionalIntClause("limit")
}

func (p *parser) parseOptionalOffset() (*int64, error) {
	return p.parseOptionalIntClause("offset")
}

func (p *parser) parseOptionalIntClause(word string) (*int64, error) {
	ok, err := p.maybeWord(word)
	if err != nil || !ok {
		return nil, err
	}
	tok, err := p.expect(tokInt)
	if err != nil {
		return nil, err
	}
	limit, err := strconv.ParseInt(tok.lit, 10, 64)
	if err != nil {
		return nil, p.errorAt(tok, "invalid integer literal")
	}
	return &limit, nil
}

func (p *parser) parsePredicateOr() (Expr, error) {
	expr, err := p.parsePredicateAnd()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("or")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parsePredicateAnd()
		if err != nil {
			return nil, err
		}
		expr = &OrExpr{Left: expr, Right: right}
	}
}

func (p *parser) parsePredicateAnd() (Expr, error) {
	expr, err := p.parsePredicateNot()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("and")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parsePredicateNot()
		if err != nil {
			return nil, err
		}
		expr = &AndExpr{Left: expr, Right: right}
	}
}

func (p *parser) parsePredicateNot() (Expr, error) {
	if ok, err := p.maybeWord("not"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		next, perr := p.peek()
		if perr != nil {
			return nil, perr
		}
		if next.typ == tokIdent && !next.quoted && next.lit == "exists" {
			_, _ = p.next()
			inner, err := p.parseExistsTail()
			if err != nil {
				return nil, err
			}
			return &ExistsExpr{Query: inner, Not: true}, nil
		}
		expr, err := p.parsePredicateNot()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: expr}, nil
	}
	if next, err := p.peek(); err == nil && next.typ == tokIdent && !next.quoted && next.lit == "exists" {
		_, _ = p.next()
		inner, err := p.parseExistsTail()
		if err != nil {
			return nil, err
		}
		return &ExistsExpr{Query: inner}, nil
	}
	return p.parsePredicatePrimary()
}

func (p *parser) parseExistsTail() (*SelectStmt, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	if err := p.expectWord("select"); err != nil {
		return nil, err
	}
	inner, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return inner, nil
}

func (p *parser) parsePredicatePrimary() (Expr, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	if tok.typ == tokLParen {
		state := p.mark()
		_, _ = p.next()
		expr, err := p.parsePredicateOr()
		if err == nil {
			if _, err := p.expect(tokRParen); err == nil {
				return expr, nil
			}
		}
		p.restore(state)
	}
	left, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	return p.parsePredicateAfterLeft(left)
}
