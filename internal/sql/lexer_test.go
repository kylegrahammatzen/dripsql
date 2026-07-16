// Lexer smoke tests covering token type and literal per opcode, identifier casefolding,
// string and quoted-ident escapes, number forms, comment skipping, and error positions.
package sql

import "testing"

func tokenize(t *testing.T, src string) []token {
	t.Helper()
	l := lexer{sql: src}
	var out []token
	for {
		tk, err := l.next()
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		out = append(out, tk)
		if tk.typ == tokEOF {
			return out
		}
	}
}

func TestLexer_Punctuation(t *testing.T) {
	toks := tokenize(t, "(),; * + - / %")
	want := []tokenType{tokLParen, tokRParen, tokComma, tokSemicolon, tokStar, tokPlus, tokMinus, tokSlash, tokPercent, tokEOF}
	if len(toks) != len(want) {
		t.Fatalf("got %d tokens, want %d", len(toks), len(want))
	}
	for i, w := range want {
		if toks[i].typ != w {
			t.Fatalf("tok %d type %d, want %d", i, toks[i].typ, w)
		}
	}
}

func TestLexer_Comparisons(t *testing.T) {
	toks := tokenize(t, "= != <> < <= > >=")
	want := []tokenType{tokEqual, tokNotEqual, tokNotEqual, tokLess, tokLessEqual, tokGreater, tokGreaterEqual, tokEOF}
	for i, w := range want {
		if toks[i].typ != w {
			t.Fatalf("tok %d type %d, want %d", i, toks[i].typ, w)
		}
	}
}

func TestLexer_JSONOps(t *testing.T) {
	toks := tokenize(t, "-> ->>")
	if toks[0].typ != tokJSONGet || toks[0].lit != "->" {
		t.Fatalf("tok 0 = %+v, want ->", toks[0])
	}
	if toks[1].typ != tokJSONGetText || toks[1].lit != "->>" {
		t.Fatalf("tok 1 = %+v, want ->>", toks[1])
	}
}

func TestLexer_Concat(t *testing.T) {
	toks := tokenize(t, "a || b")
	if len(toks) != 4 || toks[1].typ != tokConcat || toks[1].lit != "||" {
		t.Fatalf("concat: %+v", toks)
	}
}

func TestLexer_IdentCaseFolded(t *testing.T) {
	toks := tokenize(t, "SELECT FROM Users")
	for i, tk := range toks[:3] {
		if tk.typ != tokIdent {
			t.Fatalf("tok %d typ %d, want ident", i, tk.typ)
		}
	}
	if toks[0].lit != "select" || toks[1].lit != "from" || toks[2].lit != "users" {
		t.Fatalf("idents not lowered: %+v", toks)
	}
}

func TestLexer_QuotedIdentPreservesCase(t *testing.T) {
	toks := tokenize(t, `"MixedCase" "with""quote"`)
	if toks[0].lit != "MixedCase" {
		t.Fatalf("quoted ident lit = %q", toks[0].lit)
	}
	if toks[1].lit != `with"quote` {
		t.Fatalf("escaped quote ident lit = %q", toks[1].lit)
	}
}

func TestLexer_String_EscapesAndUnterminated(t *testing.T) {
	toks := tokenize(t, "'hello' 'with''apos'")
	if toks[0].typ != tokString || toks[0].lit != "hello" {
		t.Fatalf("tok 0 = %+v", toks[0])
	}
	if toks[1].lit != "with'apos" {
		t.Fatalf("escaped apos lit = %q", toks[1].lit)
	}
	l := lexer{sql: "'unterminated"}
	if _, err := l.next(); err == nil {
		t.Fatal("unterminated string must error")
	}
}

func TestLexer_Numbers(t *testing.T) {
	toks := tokenize(t, "0 42 3.14 1e10 2.5E-3")
	want := []tokenType{tokInt, tokInt, tokFloat, tokFloat, tokFloat, tokEOF}
	for i, w := range want {
		if toks[i].typ != w {
			t.Fatalf("tok %d typ %d, want %d (%+v)", i, toks[i].typ, w, toks[i])
		}
	}
}

func TestLexer_LineComment(t *testing.T) {
	toks := tokenize(t, "a -- comment\nb")
	want := []tokenType{tokIdent, tokIdent, tokEOF}
	for i, w := range want {
		if toks[i].typ != w {
			t.Fatalf("tok %d typ %d, want %d", i, toks[i].typ, w)
		}
	}
}

func TestLexer_RejectsUnknownChar(t *testing.T) {
	l := lexer{sql: "@"}
	if _, err := l.next(); err == nil {
		t.Fatal("unknown char must error")
	}
}
