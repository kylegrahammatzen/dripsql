// Hand-written lexer over a SQL source string. ASCII fast-path dispatches operators by byte;
// non-ASCII bytes fall back to UTF-8 decode for identifier-start validation.
package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type tokenType uint8

const (
	tokEOF tokenType = iota
	tokIdent
	tokString
	tokInt
	tokFloat
	tokComma
	tokLParen
	tokRParen
	tokSemicolon
	tokEqual
	tokNotEqual
	tokLess
	tokLessEqual
	tokGreater
	tokGreaterEqual
	tokStar
	tokPlus
	tokMinus
	tokSlash
	tokPercent
	tokConcat
	tokJSONGet
	tokJSONGetText
	tokDot
)

type token struct {
	typ    tokenType
	lit    string
	pos    int
	quoted bool
}

type lexer struct {
	sql string
	pos int
}

func (l *lexer) next() (token, error) {
	l.skipSpaceAndComments()
	if l.pos >= len(l.sql) {
		return token{typ: tokEOF, pos: l.pos}, nil
	}
	start := l.pos
	b := l.sql[l.pos]

	if b < 0x80 {
		if t, ok, err := l.scanASCIIOperator(b, start); ok || err != nil {
			return t, err
		}
		if b == '\'' {
			return l.scanDelimited('\'', tokString, false, "string literal")
		}
		if b == '"' {
			return l.scanDelimited('"', tokIdent, true, "quoted identifier")
		}
		if b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') {
			return l.scanIdent()
		}
		if b >= '0' && b <= '9' {
			return l.scanNumber()
		}
	}

	r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
	if r == utf8.RuneError && size == 1 {
		return token{}, fmt.Errorf("invalid utf-8 at byte %d", l.pos)
	}
	if unicode.IsLetter(r) {
		return l.scanIdent()
	}
	return token{}, fmt.Errorf("unexpected character %q at byte %d", r, start)
}

// scanASCIIOperator handles ASCII punctuation and operators. Returns (token, true, nil) on a
// recognized operator. Returns (_, false, nil) if the byte starts an identifier, string, number,
// or unrecognized input — the caller dispatches further.
func (l *lexer) scanASCIIOperator(b byte, start int) (token, bool, error) {
	switch b {
	case ',':
		l.pos++
		return token{typ: tokComma, lit: ",", pos: start}, true, nil
	case '(':
		l.pos++
		return token{typ: tokLParen, lit: "(", pos: start}, true, nil
	case ')':
		l.pos++
		return token{typ: tokRParen, lit: ")", pos: start}, true, nil
	case ';':
		l.pos++
		return token{typ: tokSemicolon, lit: ";", pos: start}, true, nil
	case '=':
		l.pos++
		return token{typ: tokEqual, lit: "=", pos: start}, true, nil
	case '*':
		l.pos++
		return token{typ: tokStar, lit: "*", pos: start}, true, nil
	case '.':
		l.pos++
		return token{typ: tokDot, lit: ".", pos: start}, true, nil
	case '+':
		l.pos++
		return token{typ: tokPlus, lit: "+", pos: start}, true, nil
	case '/':
		l.pos++
		return token{typ: tokSlash, lit: "/", pos: start}, true, nil
	case '%':
		l.pos++
		return token{typ: tokPercent, lit: "%", pos: start}, true, nil
	case '!':
		if l.peekByte(1) == '=' {
			l.pos += 2
			return token{typ: tokNotEqual, lit: "!=", pos: start}, true, nil
		}
		return token{}, false, fmt.Errorf("unexpected character '!' at byte %d", start)
	case '<':
		if l.peekByte(1) == '>' {
			l.pos += 2
			return token{typ: tokNotEqual, lit: "<>", pos: start}, true, nil
		}
		if l.peekByte(1) == '=' {
			l.pos += 2
			return token{typ: tokLessEqual, lit: "<=", pos: start}, true, nil
		}
		l.pos++
		return token{typ: tokLess, lit: "<", pos: start}, true, nil
	case '>':
		if l.peekByte(1) == '=' {
			l.pos += 2
			return token{typ: tokGreaterEqual, lit: ">=", pos: start}, true, nil
		}
		l.pos++
		return token{typ: tokGreater, lit: ">", pos: start}, true, nil
	case '-':
		if l.peekByte(1) == '>' && l.peekByte(2) == '>' {
			l.pos += 3
			return token{typ: tokJSONGetText, lit: "->>", pos: start}, true, nil
		}
		if l.peekByte(1) == '>' {
			l.pos += 2
			return token{typ: tokJSONGet, lit: "->", pos: start}, true, nil
		}
		l.pos++
		return token{typ: tokMinus, lit: "-", pos: start}, true, nil
	case '|':
		if l.peekByte(1) == '|' {
			l.pos += 2
			return token{typ: tokConcat, lit: "||", pos: start}, true, nil
		}
		return token{}, false, fmt.Errorf("unexpected character '|' at byte %d", start)
	}
	return token{}, false, nil
}

func (l *lexer) peekByte(off int) byte {
	if l.pos+off >= len(l.sql) {
		return 0
	}
	return l.sql[l.pos+off]
}

func (l *lexer) skipSpaceAndComments() {
	for l.pos < len(l.sql) {
		b := l.sql[l.pos]
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			l.pos++
			continue
		}
		if b < 0x80 {
			if b == '-' && l.peekByte(1) == '-' {
				l.pos += 2
				for l.pos < len(l.sql) && l.sql[l.pos] != '\n' && l.sql[l.pos] != '\r' {
					l.pos++
				}
				continue
			}
			return
		}
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if !unicode.IsSpace(r) {
			return
		}
		l.pos += size
	}
}

func (l *lexer) scanIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.sql) {
		b := l.sql[l.pos]
		if b < 0x80 {
			if b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') {
				l.pos++
				continue
			}
			break
		}
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		l.pos += size
	}
	return token{typ: tokIdent, lit: strings.ToLower(l.sql[start:l.pos]), pos: start}, nil
}

// scanDelimited reads a delimited literal where the same delimiter doubled is an escape.
// Returns a token of `typ`. If `quoted` is true the token is marked as a quoted identifier.
func (l *lexer) scanDelimited(delim byte, typ tokenType, quoted bool, label string) (token, error) {
	start := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if r == utf8.RuneError && size == 1 {
			return token{}, fmt.Errorf("invalid utf-8 in %s at byte %d", label, l.pos)
		}
		if size == 1 && byte(r) == delim {
			if l.peekByte(1) == delim {
				b.WriteByte(delim)
				l.pos += 2
				continue
			}
			l.pos++
			return token{typ: typ, lit: b.String(), pos: start, quoted: quoted}, nil
		}
		b.WriteRune(r)
		l.pos += size
	}
	return token{}, fmt.Errorf("unterminated %s at byte %d", label, start)
}

func (l *lexer) scanNumber() (token, error) {
	start := l.pos
	l.scanDigits()
	typ := tokInt
	if l.pos < len(l.sql) && l.sql[l.pos] == '.' {
		typ = tokFloat
		l.pos++
		fractionStart := l.pos
		l.scanDigits()
		if l.pos == fractionStart {
			return token{}, fmt.Errorf("invalid float literal %q at byte %d", l.sql[start:l.pos], start)
		}
	}
	if l.pos < len(l.sql) && (l.sql[l.pos] == 'e' || l.sql[l.pos] == 'E') {
		typ = tokFloat
		l.pos++
		if l.pos < len(l.sql) && (l.sql[l.pos] == '+' || l.sql[l.pos] == '-') {
			l.pos++
		}
		exponentStart := l.pos
		l.scanDigits()
		if l.pos == exponentStart {
			return token{}, fmt.Errorf("invalid float literal %q at byte %d", l.sql[start:l.pos], start)
		}
	}
	lit := l.sql[start:l.pos]
	if typ == tokFloat {
		if _, err := strconv.ParseFloat(lit, 64); err != nil {
			return token{}, fmt.Errorf("invalid float literal %q at byte %d", lit, start)
		}
		return token{typ: tokFloat, lit: lit, pos: start}, nil
	}
	if _, err := strconv.ParseInt(lit, 10, 64); err != nil {
		return token{}, fmt.Errorf("invalid integer literal %q at byte %d", lit, start)
	}
	return token{typ: tokInt, lit: lit, pos: start}, nil
}

func (l *lexer) scanDigits() {
	for l.pos < len(l.sql) && l.sql[l.pos] >= '0' && l.sql[l.pos] <= '9' {
		l.pos++
	}
}
