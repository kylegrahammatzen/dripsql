package parser

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
)

type token struct {
	typ tokenType
	lit string
	pos int
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
	r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
	if r == utf8.RuneError && size == 1 {
		return token{}, fmt.Errorf("invalid utf-8 at byte %d", l.pos)
	}

	switch r {
	case ',':
		l.pos += size
		return token{typ: tokComma, lit: ",", pos: start}, nil
	case '(':
		l.pos += size
		return token{typ: tokLParen, lit: "(", pos: start}, nil
	case ')':
		l.pos += size
		return token{typ: tokRParen, lit: ")", pos: start}, nil
	case ';':
		l.pos += size
		return token{typ: tokSemicolon, lit: ";", pos: start}, nil
	case '=':
		l.pos += size
		return token{typ: tokEqual, lit: "=", pos: start}, nil
	case '!':
		if strings.HasPrefix(l.sql[l.pos+size:], "=") {
			l.pos += size + 1
			return token{typ: tokNotEqual, lit: "!=", pos: start}, nil
		}
	case '<':
		if strings.HasPrefix(l.sql[l.pos+size:], ">") {
			l.pos += size + 1
			return token{typ: tokNotEqual, lit: "<>", pos: start}, nil
		}
		if strings.HasPrefix(l.sql[l.pos+size:], "=") {
			l.pos += size + 1
			return token{typ: tokLessEqual, lit: "<=", pos: start}, nil
		}
		l.pos += size
		return token{typ: tokLess, lit: "<", pos: start}, nil
	case '>':
		if strings.HasPrefix(l.sql[l.pos+size:], "=") {
			l.pos += size + 1
			return token{typ: tokGreaterEqual, lit: ">=", pos: start}, nil
		}
		l.pos += size
		return token{typ: tokGreater, lit: ">", pos: start}, nil
	case '*':
		l.pos += size
		return token{typ: tokStar, lit: "*", pos: start}, nil
	case '+':
		l.pos += size
		return token{typ: tokPlus, lit: "+", pos: start}, nil
	case '-':
		l.pos += size
		return token{typ: tokMinus, lit: "-", pos: start}, nil
	case '/':
		l.pos += size
		return token{typ: tokSlash, lit: "/", pos: start}, nil
	case '%':
		l.pos += size
		return token{typ: tokPercent, lit: "%", pos: start}, nil
	case '|':
		if strings.HasPrefix(l.sql[l.pos+size:], "|") {
			l.pos += size + 1
			return token{typ: tokConcat, lit: "||", pos: start}, nil
		}
	case '\'':
		return l.scanString()
	case '"':
		return l.scanQuotedIdent()
	}

	if isIdentStart(r) {
		return l.scanIdent()
	}
	if unicode.IsDigit(r) {
		return l.scanNumber()
	}
	return token{}, fmt.Errorf("unexpected character %q at byte %d", r, start)
}

func (l *lexer) skipSpaceAndComments() {
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if unicode.IsSpace(r) {
			l.pos += size
			continue
		}
		if strings.HasPrefix(l.sql[l.pos:], "--") {
			l.pos += 2
			for l.pos < len(l.sql) {
				r, size = utf8.DecodeRuneInString(l.sql[l.pos:])
				l.pos += size
				if r == '\n' || r == '\r' {
					break
				}
			}
			continue
		}
		return
	}
}

func (l *lexer) scanIdent() (token, error) {
	start := l.pos
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if !isIdentPart(r) {
			break
		}
		l.pos += size
	}
	return token{typ: tokIdent, lit: strings.ToLower(l.sql[start:l.pos]), pos: start}, nil
}

func (l *lexer) scanQuotedIdent() (token, error) {
	start := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if r == '"' {
			if strings.HasPrefix(l.sql[l.pos+size:], "\"") {
				b.WriteRune('"')
				l.pos += size + 1
				continue
			}
			l.pos += size
			return token{typ: tokIdent, lit: b.String(), pos: start}, nil
		}
		b.WriteRune(r)
		l.pos += size
	}
	return token{}, fmt.Errorf("unterminated quoted identifier at byte %d", start)
}

func (l *lexer) scanString() (token, error) {
	start := l.pos
	l.pos++
	var b strings.Builder
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if r == '\'' {
			if strings.HasPrefix(l.sql[l.pos+size:], "'") {
				b.WriteRune('\'')
				l.pos += size + 1
				continue
			}
			l.pos += size
			return token{typ: tokString, lit: b.String(), pos: start}, nil
		}
		b.WriteRune(r)
		l.pos += size
	}
	return token{}, fmt.Errorf("unterminated string literal at byte %d", start)
}

func (l *lexer) scanNumber() (token, error) {
	start := l.pos
	for l.pos < len(l.sql) {
		r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
		if !unicode.IsDigit(r) {
			break
		}
		l.pos += size
	}
	typ := tokInt
	if l.pos < len(l.sql) && l.sql[l.pos] == '.' {
		typ = tokFloat
		l.pos++
		fractionStart := l.pos
		for l.pos < len(l.sql) {
			r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
			if !unicode.IsDigit(r) {
				break
			}
			l.pos += size
		}
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
		for l.pos < len(l.sql) {
			r, size := utf8.DecodeRuneInString(l.sql[l.pos:])
			if !unicode.IsDigit(r) {
				break
			}
			l.pos += size
		}
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

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
