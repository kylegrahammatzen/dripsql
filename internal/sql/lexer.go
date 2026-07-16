// Hand-written lexer over a SQL source string.
// An ASCII fast path dispatches operators by byte and non-ASCII bytes fall back to UTF-8 decode for identifier starts.
package sql

import (
	"fmt"
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
	tokPlaceholder
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

// scanASCIIOperator handles ASCII punctuation, returning (token, true, nil) on a recognized operator and (_, false, nil) when the byte starts an identifier, string, number, or unrecognized input for the caller to dispatch.
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
	case '?':
		l.pos++
		return token{typ: tokPlaceholder, lit: "?", pos: start}, true, nil
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

// scanIdent reads an identifier and lowercases only when needed, returning pure-lowercase ASCII source slices directly, lowercasing mixed-case ASCII byte-wise without Unicode tables, and routing non-ASCII through strings.ToLower.
func (l *lexer) scanIdent() (token, error) {
	start := l.pos
	ascii, upper := true, false
	for l.pos < len(l.sql) {
		b := l.sql[l.pos]
		if b < 0x80 {
			if b == '_' || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') {
				l.pos++
				continue
			}
			if b >= 'A' && b <= 'Z' {
				upper = true
				l.pos++
				continue
			}
			break
		}
		ascii = false
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			break
		}
		l.pos += size
	}
	lit := l.sql[start:l.pos]
	switch {
	case !ascii:
		lit = strings.ToLower(lit)
	case upper:
		buf := []byte(lit)
		for i, c := range buf {
			if c >= 'A' && c <= 'Z' {
				buf[i] = c + ('a' - 'A')
			}
		}
		lit = string(buf)
	}
	return token{typ: tokIdent, lit: lit, pos: start}, nil
}

// scanDelimited reads a delimited literal of typ where a doubled delimiter escapes, slicing the source directly unless escapes or non-ASCII bytes force a string builder.
func (l *lexer) scanDelimited(delim byte, typ tokenType, quoted bool, label string) (token, error) {
	start := l.pos
	l.pos++
	partStart := l.pos
	var b strings.Builder
	escaped := false
	for l.pos < len(l.sql) {
		c := l.sql[l.pos]
		if c < utf8.RuneSelf {
			if c != delim {
				l.pos++
				continue
			}
			if l.pos+1 < len(l.sql) && l.sql[l.pos+1] == delim {
				if !escaped {
					b.Grow(len(l.sql) - partStart)
					escaped = true
				}
				b.WriteString(l.sql[partStart:l.pos])
				b.WriteByte(delim)
				l.pos += 2
				partStart = l.pos
				continue
			}
			end := l.pos
			l.pos++
			if !escaped {
				return token{typ: typ, lit: l.sql[partStart:end], pos: start, quoted: quoted}, nil
			}
			b.WriteString(l.sql[partStart:end])
			return token{typ: typ, lit: b.String(), pos: start, quoted: quoted}, nil
		}
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if r == utf8.RuneError && size == 1 {
			return token{}, fmt.Errorf("invalid utf-8 in %s at byte %d", label, l.pos)
		}
		l.pos += size
	}
	return token{}, fmt.Errorf("unterminated %s at byte %d", label, start)
}

// scanNumber validates the shape of an integer or float literal and slices the source.
// Numeric conversion is deferred to parseValue / parseOptionalIntClause so the lexer pays
// strconv only once per literal instead of twice.
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
	return token{typ: typ, lit: l.sql[start:l.pos], pos: start}, nil
}

func (l *lexer) scanDigits() {
	for l.pos < len(l.sql) && l.sql[l.pos] >= '0' && l.sql[l.pos] <= '9' {
		l.pos++
	}
}
