package parser

import (
	"strconv"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

func (p *parser) parseSelect() (*ast.SelectStmt, error) {
	selectExprs, groupColumn, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}

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

	groupBy, err := p.parseOptionalGroupBy(groupColumn)
	if err != nil {
		return nil, err
	}
	having, err := p.parseOptionalHaving()
	if err != nil {
		return nil, err
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

	return &ast.SelectStmt{
		Table:   tableName,
		Select:  selectExprs,
		Where:   where,
		GroupBy: groupBy,
		Having:  having,
		OrderBy: orderBy,
		Limit:   limit,
		Offset:  offset,
	}, nil
}

func (p *parser) parseSelectList() ([]ast.SelectExpr, string, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, "", err
	}

	if tok.typ == tokIdent && isAggregateName(tok.lit) {
		expr, err := p.parseAggregateCall()
		if err != nil {
			return nil, "", err
		}
		alias, err := p.parseOptionalAlias()
		if err != nil {
			return nil, "", err
		}
		return []ast.SelectExpr{{Expr: expr, Alias: alias}}, "", nil
	}
	if tok.typ == tokStar {
		_, _ = p.next()
		return []ast.SelectExpr{{Expr: &ast.StarRef{}}}, "", nil
	}

	firstExpr, groupColumn, err := p.parseSelectItem()
	if err != nil {
		return nil, "", err
	}
	groupAlias, err := p.parseOptionalAlias()
	if err != nil {
		return nil, "", err
	}

	if ok, err := p.maybe(tokComma); err != nil || !ok {
		if err != nil {
			return nil, "", err
		}
		return []ast.SelectExpr{{Expr: firstExpr, Alias: groupAlias}}, "", nil
	}

	if tok, err := p.peek(); err != nil || tok.typ != tokIdent || !isAggregateName(tok.lit) {
		if err != nil {
			return nil, "", err
		}
		exprs := []ast.SelectExpr{{Expr: firstExpr, Alias: groupAlias}}
		for {
			expr, _, err := p.parseSelectItem()
			if err != nil {
				return nil, "", err
			}
			alias, err := p.parseOptionalAlias()
			if err != nil {
				return nil, "", err
			}
			exprs = append(exprs, ast.SelectExpr{Expr: expr, Alias: alias})
			ok, err := p.maybe(tokComma)
			if err != nil || !ok {
				return exprs, "", err
			}
		}
	}

	countExpr, err := p.parseAggregateCall()
	if err != nil {
		return nil, "", err
	}
	aggregateAlias, err := p.parseOptionalAlias()
	if err != nil {
		return nil, "", err
	}
	return []ast.SelectExpr{{Expr: firstExpr, Alias: groupAlias}, {Expr: countExpr, Alias: aggregateAlias}}, groupColumn, nil
}

func (p *parser) parseSelectItem() (ast.Expr, string, error) {
	return p.parseScalarExpr()
}

func (p *parser) parseScalarExpr() (ast.Expr, string, error) {
	expr, name, err := p.parseScalarTerm()
	if err != nil {
		return nil, "", err
	}
	for {
		if ok, err := p.maybe(tokConcat); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarTerm()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryConcat, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybe(tokPlus); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarTerm()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryAdd, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybe(tokMinus); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarTerm()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinarySubtract, Right: right}
			name = ""
			continue
		}
		return expr, name, nil
	}
}

func (p *parser) parseScalarTerm() (ast.Expr, string, error) {
	expr, name, err := p.parseScalarPrimary()
	if err != nil {
		return nil, "", err
	}
	for {
		if ok, err := p.maybe(tokStar); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarPrimary()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryMultiply, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybe(tokSlash); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarPrimary()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryDivide, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybe(tokPercent); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarPrimary()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryModulo, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybeWord("mod"); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarPrimary()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryModulo, Right: right}
			name = ""
			continue
		}
		if ok, err := p.maybeWord("div"); err != nil || ok {
			if err != nil {
				return nil, "", err
			}
			right, _, err := p.parseScalarPrimary()
			if err != nil {
				return nil, "", err
			}
			expr = &ast.BinaryExpr{Left: expr, Op: ast.BinaryIntDivide, Right: right}
			name = ""
			continue
		}
		return expr, name, nil
	}
}

func (p *parser) parseScalarPrimary() (ast.Expr, string, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, "", err
	}
	if tok.typ == tokString || tok.typ == tokInt || tok.typ == tokFloat || tok.lit == "true" || tok.lit == "false" || tok.lit == "null" {
		value, err := p.parseValue()
		if err != nil {
			return nil, "", err
		}
		return &ast.Literal{Value: value}, "", nil
	}
	if ok, err := p.maybe(tokLParen); err != nil || ok {
		if err != nil {
			return nil, "", err
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
		if isAggregateName(name) {
			expr, err := p.parseAggregateCallArgs(name)
			return expr, "", err
		}
		expr, err := p.parseScalarCallArgs(name)
		return expr, "", err
	}
	return &ast.ColumnRef{Name: name}, name, nil
}

func (p *parser) parseScalarCallArgs(funcName string) (ast.Expr, error) {
	args := make([]ast.Expr, 0, 1)
	if ok, err := p.maybe(tokRParen); err != nil || ok {
		return &ast.FuncCall{Name: funcName}, err
	}
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
	return &ast.FuncCall{Name: funcName, Args: args}, nil
}

func (p *parser) parseOptionalAlias() (string, error) {
	ok, err := p.maybeWord("as")
	if err != nil {
		return "", err
	}
	if ok {
		return p.parseName()
	}
	tok, err := p.peek()
	if err != nil || tok.typ != tokIdent || isSelectClauseKeyword(tok.lit) {
		return "", err
	}
	_, _ = p.next()
	return tok.lit, nil
}

func isSelectClauseKeyword(word string) bool {
	switch word {
	case "from", "where", "group", "having", "order", "limit", "offset", "by", "asc", "desc", "between", "in", "not":
		return true
	default:
		return false
	}
}

func (p *parser) parseOptionalWhere() (ast.Expr, error) {
	ok, err := p.maybeWord("where")
	if err != nil || !ok {
		return nil, err
	}
	return p.parseWhereOr()
}

func (p *parser) parseWhereOr() (ast.Expr, error) {
	expr, err := p.parseWhereAnd()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("or")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parseWhereAnd()
		if err != nil {
			return nil, err
		}
		expr = &ast.OrExpr{Left: expr, Right: right}
	}
}

func (p *parser) parseWhereAnd() (ast.Expr, error) {
	expr, err := p.parseWhereNot()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("and")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parseWhereNot()
		if err != nil {
			return nil, err
		}
		expr = &ast.AndExpr{Left: expr, Right: right}
	}
}

func (p *parser) parseWhereNot() (ast.Expr, error) {
	if ok, err := p.maybeWord("not"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		expr, err := p.parseWhereNot()
		if err != nil {
			return nil, err
		}
		return &ast.NotExpr{Expr: expr}, nil
	}
	return p.parseWherePrimary()
}

func (p *parser) parseWherePrimary() (ast.Expr, error) {
	if tok, err := p.peek(); err != nil {
		return nil, err
	} else if tok.typ == tokLParen {
		state := p.mark()
		_, _ = p.next()
		expr, err := p.parseWhereOr()
		if err == nil {
			if _, err := p.expect(tokRParen); err == nil {
				return expr, nil
			}
		}
		p.restore(state)
	}
	return p.parseWherePredicate()
}

func (p *parser) parseOptionalHaving() (ast.Expr, error) {
	ok, err := p.maybeWord("having")
	if err != nil || !ok {
		return nil, err
	}
	return p.parseHavingOr()

}

func (p *parser) parseHavingOr() (ast.Expr, error) {
	expr, err := p.parseHavingAnd()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("or")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parseHavingAnd()
		if err != nil {
			return nil, err
		}
		expr = &ast.OrExpr{Left: expr, Right: right}
	}
}

func (p *parser) parseHavingAnd() (ast.Expr, error) {
	expr, err := p.parseHavingNot()
	if err != nil {
		return nil, err
	}
	for {
		ok, err := p.maybeWord("and")
		if err != nil || !ok {
			return expr, err
		}
		right, err := p.parseHavingNot()
		if err != nil {
			return nil, err
		}
		expr = &ast.AndExpr{Left: expr, Right: right}
	}
}

func (p *parser) parseHavingNot() (ast.Expr, error) {
	if ok, err := p.maybeWord("not"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		expr, err := p.parseHavingNot()
		if err != nil {
			return nil, err
		}
		return &ast.NotExpr{Expr: expr}, nil
	}
	return p.parseHavingPrimary()
}

func (p *parser) parseHavingPrimary() (ast.Expr, error) {
	if tok, err := p.peek(); err != nil {
		return nil, err
	} else if tok.typ == tokLParen {
		state := p.mark()
		_, _ = p.next()
		expr, err := p.parseHavingOr()
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

func (p *parser) parseOptionalPredicateClause(word string) (ast.Expr, error) {
	ok, err := p.maybeWord(word)
	if err != nil || !ok {
		return nil, err
	}

	left, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	return p.parsePredicateAfterLeft(left)
}

func (p *parser) parseWherePredicate() (ast.Expr, error) {
	left, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	return p.parsePredicateAfterLeft(left)
}

func (p *parser) parsePredicateAfterLeft(left ast.Expr) (ast.Expr, error) {
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
		return &ast.BetweenExpr{Expr: left, Low: low, High: high}, nil
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
		values := make([]ast.Expr, 0, 2)
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
		return &ast.InExpr{Expr: left, Values: values, Not: inNot}, nil
	} else if inNot {
		tok, err := p.peek()
		if err != nil {
			return nil, err
		}
		return nil, p.errorAt(tok, "expected IN after NOT")
	}

	if ok, err := p.maybe(tokNotEqual); err != nil || ok {
		if err != nil {
			return nil, err
		}
		right, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: ast.BinaryNotEqual, Right: right}, nil
	}

	if op, ok, err := p.parseComparisonOp(); err != nil || ok {
		if err != nil {
			return nil, err
		}
		right, _, err := p.parseScalarExpr()
		if err != nil {
			return nil, err
		}
		return &ast.BinaryExpr{Left: left, Op: op, Right: right}, nil
	}

	if _, err := p.expect(tokEqual); err != nil {
		return nil, err
	}
	right, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	return &ast.BinaryExpr{Left: left, Op: ast.BinaryEqual, Right: right}, nil
}

func (p *parser) parseHavingOperand() (ast.Expr, error) {
	name, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if !isAggregateName(name) {
		return &ast.ColumnRef{Name: name}, nil
	}
	if ok, err := p.maybe(tokLParen); err != nil || ok {
		if err != nil {
			return nil, err
		}
		return p.parseAggregateCallArgs(name)
	}
	return &ast.ColumnRef{Name: name}, nil
}

func (p *parser) parseComparisonOp() (ast.BinaryOp, bool, error) {
	if ok, err := p.maybe(tokLess); err != nil || ok {
		return ast.BinaryLess, ok, err
	}
	if ok, err := p.maybe(tokLessEqual); err != nil || ok {
		return ast.BinaryLessEqual, ok, err
	}
	if ok, err := p.maybe(tokGreater); err != nil || ok {
		return ast.BinaryGreater, ok, err
	}
	if ok, err := p.maybe(tokGreaterEqual); err != nil || ok {
		return ast.BinaryGreaterEqual, ok, err
	}
	return ast.BinaryEqual, false, nil
}

func (p *parser) parseOptionalGroupBy(groupColumn string) ([]ast.Expr, error) {
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}

	ok, err := p.maybeWord("group")
	if err != nil {
		return nil, err
	}

	if !ok {
		if groupColumn != "" {
			return nil, p.errorAt(tok, "selected column %q requires GROUP BY", groupColumn)
		}
		return nil, nil
	}

	if err := p.expectWord("by"); err != nil {
		return nil, err
	}

	expr, _, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}

	if groupColumn == "" {
		return []ast.Expr{expr}, nil
	}

	if col, ok := expr.(*ast.ColumnRef); !ok || col.Name != groupColumn {
		return nil, p.errorAt(tok, "GROUP BY expression does not match selected column %q", groupColumn)
	}

	return []ast.Expr{expr}, nil
}

func (p *parser) parseOptionalOrderBy() ([]ast.OrderExpr, error) {
	ok, err := p.maybeWord("order")
	if err != nil || !ok {
		return nil, err
	}
	if err := p.expectWord("by"); err != nil {
		return nil, err
	}
	tok, err := p.peek()
	if err != nil {
		return nil, err
	}
	expr, name, err := p.parseScalarExpr()
	if err != nil {
		return nil, err
	}
	if _, ok := expr.(*ast.Literal); ok {
		return nil, p.errorAt(tok, "expected identifier")
	}
	desc := false
	if ok, err := p.maybeWord("asc"); err != nil || ok {
		return []ast.OrderExpr{{Name: name, Expr: expr}}, err
	}
	if ok, err := p.maybeWord("desc"); err != nil || ok {
		if err != nil {
			return nil, err
		}
		desc = true
	}
	return []ast.OrderExpr{{Name: name, Expr: expr, Desc: desc}}, nil
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

func (p *parser) parseAggregateCall() (ast.Expr, error) {
	funcName, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	return p.parseAggregateCallArgs(funcName)
}

func (p *parser) parseAggregateCallArgs(funcName string) (ast.Expr, error) {
	if ok, err := p.maybe(tokStar); err != nil || ok {
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return &ast.FuncCall{Name: funcName, Star: true}, nil
	}
	column, err := p.parseName()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return &ast.FuncCall{Name: funcName, Args: []ast.Expr{&ast.ColumnRef{Name: column}}}, nil
}

func isAggregateName(name string) bool {
	return name == "count" || name == "sum" || name == "min" || name == "max"
}
