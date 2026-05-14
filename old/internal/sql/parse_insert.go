package sql

import "strconv"

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

func (p *parser) parseValue() (Value, error) {
	tok, err := p.next()
	if err != nil {
		return Value{}, err
	}
	switch tok.typ {
	case tokString:
		return Value{Kind: ValueString, String: tok.lit}, nil
	case tokInt:
		value, err := strconv.ParseInt(tok.lit, 10, 64)
		if err != nil {
			return Value{}, p.errorAt(tok, "invalid integer literal")
		}
		return Value{Kind: ValueInt, Int: value}, nil
	case tokFloat:
		value, err := strconv.ParseFloat(tok.lit, 64)
		if err != nil {
			return Value{}, p.errorAt(tok, "invalid float literal")
		}
		return Value{Kind: ValueFloat, Float: value}, nil
	case tokIdent:
		switch tok.lit {
		case "true":
			return Value{Kind: ValueBool, Bool: true}, nil
		case "false":
			return Value{Kind: ValueBool, Bool: false}, nil
		case "null":
			return Value{Kind: ValueNull}, nil
		}
	}
	return Value{}, p.errorAt(tok, "expected literal")
}
