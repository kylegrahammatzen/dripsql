package parser

import (
	"strconv"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

func (p *parser) parseInsert() (*ast.InsertStmt, error) {
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

	var rows [][]ast.Value
	for {
		if _, err := p.expect(tokLParen); err != nil {
			return nil, err
		}
		var row []ast.Value
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
			return &ast.InsertStmt{Table: tableName, Columns: columns, Values: rows}, nil
		}
		_, _ = p.next()
	}
}

func (p *parser) parseValue() (ast.Value, error) {
	tok, err := p.next()
	if err != nil {
		return ast.Value{}, err
	}
	switch tok.typ {
	case tokString:
		return ast.Value{Kind: ast.ValueString, String: tok.lit}, nil
	case tokInt:
		value, err := strconv.ParseInt(tok.lit, 10, 64)
		if err != nil {
			return ast.Value{}, p.errorAt(tok, "invalid integer literal")
		}
		return ast.Value{Kind: ast.ValueInt, Int: value}, nil
	case tokFloat:
		value, err := strconv.ParseFloat(tok.lit, 64)
		if err != nil {
			return ast.Value{}, p.errorAt(tok, "invalid float literal")
		}
		return ast.Value{Kind: ast.ValueFloat, Float: value}, nil
	case tokIdent:
		switch tok.lit {
		case "true":
			return ast.Value{Kind: ast.ValueBool, Bool: true}, nil
		case "false":
			return ast.Value{Kind: ast.ValueBool, Bool: false}, nil
		case "null":
			return ast.Value{Kind: ast.ValueNull}, nil
		}
	}
	return ast.Value{}, p.errorAt(tok, "expected literal")
}
