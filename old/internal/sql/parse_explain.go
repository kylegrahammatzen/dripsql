package sql

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
