// Package parser contains DripSQL's small SQL parser.
package parser

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

type parser struct {
	lex lexer
	buf token
	has bool
}

type parserState struct {
	pos int
	buf token
	has bool
}

func Parse(sql string) ([]ast.Stmt, error) {
	p := &parser{lex: lexer{sql: sql}}
	var stmts []ast.Stmt
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

func ParseOne(sql string) (ast.Stmt, error) {
	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, fmt.Errorf("expected one statement, got %d", len(stmts))
	}
	return stmts[0], nil
}

func (p *parser) parseStmt() (ast.Stmt, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return nil, err
	}
	switch tok.lit {
	case "create":
		return p.parseCreate()
	case "insert":
		return p.parseInsert()
	case "select":
		return p.parseSelect()
	case "explain":
		return p.parseExplain()
	default:
		return nil, p.errorAt(tok, "unsupported statement %q", tok.lit)
	}
}

func (p *parser) parseExplain() (ast.Stmt, error) {
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
	return &ast.ExplainStmt{Analyze: analyze, Inner: inner}, nil
}

func (p *parser) parseCreate() (ast.Stmt, error) {
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

func (p *parser) next() (token, error) {
	if p.has {
		p.has = false
		return p.buf, nil
	}
	return p.lex.next()
}

func (p *parser) peek() (token, error) {
	if p.has {
		return p.buf, nil
	}
	tok, err := p.lex.next()
	if err != nil {
		return token{}, err
	}
	p.buf = tok
	p.has = true
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
	default:
		return "token"
	}
}
