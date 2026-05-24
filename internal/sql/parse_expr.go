// Predicate-level expression grammar shared by WHERE, JOIN ON, and HAVING.
// Or > And > Not > Primary, where Primary is a parenthesized predicate or a scalar+predicate-tail.
package sql

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
