package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

func (p *parser) parseCreateType() (*ast.CreateTypeStmt, error) {
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
			return &ast.CreateTypeStmt{Name: name, IfNotExists: ifNotExists, EnumLabels: labels}, nil
		default:
			return nil, p.errorAt(tok, "expected , or )")
		}
	}
}

func (p *parser) parseCreateTable() (*ast.CreateTableStmt, error) {
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

	var columns []ast.ColumnDef
	for {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		if tok.typ == tokRParen {
			_, _ = p.next()
			break
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
	return &ast.CreateTableStmt{Name: name, IfNotExists: ifNotExists, Columns: columns, Options: options}, nil
}

func (p *parser) parseColumnDef() (ast.ColumnDef, error) {
	name, err := p.parseName()
	if err != nil {
		return ast.ColumnDef{}, err
	}
	typ, err := p.parseType()
	if err != nil {
		return ast.ColumnDef{}, err
	}
	col := ast.ColumnDef{Name: name, Type: typ}
	for {
		tok, err := p.peek()
		if err != nil {
			return ast.ColumnDef{}, err
		}
		if tok.typ == tokComma || tok.typ == tokRParen {
			return col, nil
		}
		if tok.typ != tokIdent {
			return ast.ColumnDef{}, p.errorAt(tok, "unexpected column constraint")
		}
		switch tok.lit {
		case "not":
			_, _ = p.next()
			if err := p.expectWord("null"); err != nil {
				return ast.ColumnDef{}, err
			}
			col.NotNull = true
		case "null":
			_, _ = p.next()
			col.NotNull = false
		case "primary", "unique", "check", "default", "references", "constraint", "foreign":
			return ast.ColumnDef{}, p.errorAt(tok, "unsupported column constraint %q", tok.lit)
		default:
			return ast.ColumnDef{}, p.errorAt(tok, "unexpected token %q in column definition", tok.lit)
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
		if tok.typ != tokIdent || isColumnConstraintStart(tok.lit) {
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

func (p *parser) parseOptionalTableOptions() ([]ast.TableOption, error) {
	if ok, err := p.maybeWord("with"); err != nil || !ok {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var options []ast.TableOption
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
		options = append(options, ast.TableOption{Name: name, Value: value})

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

func (p *parser) parseOptionScalar() (ast.Value, error) {
	tok, err := p.next()
	if err != nil {
		return ast.Value{}, err
	}
	switch tok.typ {
	case tokIdent:
		if tok.lit == "true" {
			return ast.Value{Kind: ast.ValueBool, Bool: true}, nil
		}
		if tok.lit == "false" {
			return ast.Value{Kind: ast.ValueBool, Bool: false}, nil
		}
		return ast.Value{Kind: ast.ValueIdent, String: tok.lit}, nil
	case tokString:
		return ast.Value{Kind: ast.ValueString, String: tok.lit}, nil
	case tokInt:
		value, err := strconv.ParseInt(tok.lit, 10, 64)
		if err != nil {
			return ast.Value{}, p.errorAt(tok, "invalid integer literal")
		}
		return ast.Value{Kind: ast.ValueInt, Int: value}, nil
	default:
		return ast.Value{}, p.errorAt(tok, "expected table option value")
	}
}

func (p *parser) parseName() (string, error) {
	tok, err := p.expect(tokIdent)
	if err != nil {
		return "", err
	}
	return tok.lit, nil
}

func isColumnConstraintStart(word string) bool {
	return word == "not" || word == "null" || word == "default" || word == "references" || isUnsupportedTableConstraint(word)
}

func isUnsupportedTableConstraint(word string) bool {
	switch word {
	case "primary", "unique", "check", "constraint", "foreign", "family", "index":
		return true
	default:
		return false
	}
}
