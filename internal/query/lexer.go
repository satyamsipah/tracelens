package query

import (
	"fmt"
	"strings"
)

// TokenType classifies a lexed token.
type TokenType int

const (
	TokEOF TokenType = iota
	TokIllegal

	TokLBrace
	TokRBrace
	TokLParen
	TokRParen
	TokComma
	TokPipe
	TokMinus

	TokEq       // =
	TokNeq      // !=
	TokRegexEq  // =~
	TokRegexNeq // !~
	TokEqEq     // ==
	TokGT
	TokGTE
	TokLT
	TokLTE

	TokIdent
	TokString
	TokNumber
)

// Token is one lexed unit, with a byte-offset-derived line/column for error
// messages that point at the exact offending character.
type Token struct {
	Type TokenType
	Lit  string
	Line int
	Col  int
}

// Lexer is a hand-written scanner over the DSL's byte source. It has no
// dependency on the parser and can be driven standalone (the fuzz target
// does exactly that as its first pass).
type Lexer struct {
	src       string
	pos       int // current byte offset
	line, col int
	tokenLine int
	tokenCol  int
}

// NewLexer builds a scanner over src.
func NewLexer(src string) *Lexer {
	return &Lexer{src: src, line: 1, col: 1}
}

func (l *Lexer) peekByte() byte {
	if l.pos >= len(l.src) {
		return 0
	}
	return l.src[l.pos]
}

func (l *Lexer) peekByteAt(off int) byte {
	if l.pos+off >= len(l.src) {
		return 0
	}
	return l.src[l.pos+off]
}

func (l *Lexer) advance() byte {
	b := l.src[l.pos]
	l.pos++
	if b == '\n' {
		l.line++
		l.col = 1
	} else {
		l.col++
	}
	return b
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.src) {
		switch l.src[l.pos] {
		case ' ', '\t', '\r', '\n':
			l.advance()
		default:
			return
		}
	}
}

// Next returns the next token, or a TokEOF at end of input, or a TokIllegal
// carrying a human-readable reason in Lit.
func (l *Lexer) Next() Token {
	l.skipWhitespace()
	l.tokenLine, l.tokenCol = l.line, l.col

	if l.pos >= len(l.src) {
		return l.tok(TokEOF, "")
	}

	b := l.peekByte()
	switch {
	case b == '{':
		l.advance()
		return l.tok(TokLBrace, "{")
	case b == '}':
		l.advance()
		return l.tok(TokRBrace, "}")
	case b == '(':
		l.advance()
		return l.tok(TokLParen, "(")
	case b == ')':
		l.advance()
		return l.tok(TokRParen, ")")
	case b == ',':
		l.advance()
		return l.tok(TokComma, ",")
	case b == '|':
		l.advance()
		return l.tok(TokPipe, "|")
	case b == '-':
		l.advance()
		return l.tok(TokMinus, "-")
	case b == '=':
		l.advance()
		switch l.peekByte() {
		case '~':
			l.advance()
			return l.tok(TokRegexEq, "=~")
		case '=':
			l.advance()
			return l.tok(TokEqEq, "==")
		default:
			return l.tok(TokEq, "=")
		}
	case b == '!':
		l.advance()
		switch l.peekByte() {
		case '=':
			l.advance()
			return l.tok(TokNeq, "!=")
		case '~':
			l.advance()
			return l.tok(TokRegexNeq, "!~")
		default:
			return l.tok(TokIllegal, "unexpected '!': expected '!=' or '!~'")
		}
	case b == '>':
		l.advance()
		if l.peekByte() == '=' {
			l.advance()
			return l.tok(TokGTE, ">=")
		}
		return l.tok(TokGT, ">")
	case b == '<':
		l.advance()
		if l.peekByte() == '=' {
			l.advance()
			return l.tok(TokLTE, "<=")
		}
		return l.tok(TokLT, "<")
	case b == '"':
		return l.scanString()
	case isDigit(b):
		return l.scanNumber()
	case isIdentStart(b):
		return l.scanIdent()
	default:
		l.advance()
		return l.tok(TokIllegal, fmt.Sprintf("unexpected character %q", b))
	}
}

func (l *Lexer) tok(t TokenType, lit string) Token {
	return Token{Type: t, Lit: lit, Line: l.tokenLine, Col: l.tokenCol}
}

func (l *Lexer) scanString() Token {
	l.advance() // opening quote
	var sb strings.Builder
	for {
		if l.pos >= len(l.src) {
			return l.tok(TokIllegal, "unterminated string literal")
		}
		b := l.peekByte()
		if b == '"' {
			l.advance()
			return l.tok(TokString, sb.String())
		}
		if b == '\\' {
			l.advance()
			if l.pos >= len(l.src) {
				return l.tok(TokIllegal, "unterminated escape in string literal")
			}
			esc := l.advance()
			switch esc {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '"':
				sb.WriteByte('"')
			case '\\':
				sb.WriteByte('\\')
			default:
				sb.WriteByte(esc)
			}
			continue
		}
		sb.WriteByte(b)
		l.advance()
	}
}

// scanNumber consumes one or more immediately-adjacent digit[.digit]+unit
// segments as a SINGLE token, so a compound duration like "1h30m" lexes as
// one NUMBER token while "1h 500ms" (separated by whitespace) lexes as two --
// letting the parser treat "concatenated, no gap" as the only rule for
// compound durations, rather than tracking whitespace itself.
func (l *Lexer) scanNumber() Token {
	start := l.pos
	for {
		l.scanDigitRun()
		unitStart := l.pos
		for l.pos < len(l.src) && isUnitLetter(l.peekByte()) {
			l.advance()
		}
		// Only continue the loop (compound duration) if a unit was consumed
		// AND another digit immediately follows with no gap.
		if l.pos == unitStart || !isDigit(l.peekByte()) {
			break
		}
	}
	return l.tok(TokNumber, l.src[start:l.pos])
}

func (l *Lexer) scanDigitRun() {
	for l.pos < len(l.src) && isDigit(l.peekByte()) {
		l.advance()
	}
	if l.peekByte() == '.' && isDigit(l.peekByteAt(1)) {
		l.advance()
		for l.pos < len(l.src) && isDigit(l.peekByte()) {
			l.advance()
		}
	}
}

func (l *Lexer) scanIdent() Token {
	start := l.pos
	for l.pos < len(l.src) && isIdentPart(l.peekByte()) {
		l.advance()
	}
	return l.tok(TokIdent, l.src[start:l.pos])
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func isUnitLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isIdentStart(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_'
}

// isIdentPart deliberately excludes '-': OTel attribute keys use dots and
// underscores (http.status_code, user_agent.original), and a bare hyphen
// must stay its own TokMinus token so "now-1h" lexes as IDENT("now"),
// MINUS, NUMBER("1h") rather than swallowing into one identifier.
func isIdentPart(b byte) bool {
	return isIdentStart(b) || isDigit(b) || b == '.'
}
