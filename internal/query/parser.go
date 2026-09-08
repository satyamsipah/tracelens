package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseError points at the exact line and column of the offending token, so
// a caller (the query API, a CLI) can render a caret under the bad input
// rather than a bare error string.
type ParseError struct {
	Line, Col int
	Msg       string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("query:%d:%d: %s", e.Line, e.Col, e.Msg)
}

// Parser is a recursive-descent parser over the token stream produced by
// Lexer. It holds no state beyond the current and lookahead token, so it is
// cheap to construct per query.
type Parser struct {
	lex  *Lexer
	cur  Token
	peek Token
	now  func() time.Time
}

// NewParser builds a parser over src. now is injectable so tests can pin
// "now"-relative time ranges to a fixed instant; production callers pass
// time.Now.
func NewParser(src string, now func() time.Time) *Parser {
	p := &Parser{lex: NewLexer(src), now: now}
	p.cur = p.lex.Next()
	p.peek = p.lex.Next()
	return p
}

// Parse parses src with time.Now as the clock. This is the entry point most
// callers want.
func Parse(src string) (Query, error) {
	return NewParser(src, time.Now).Parse()
}

func (p *Parser) advance() {
	p.cur = p.peek
	p.peek = p.lex.Next()
}

func (p *Parser) errf(tok Token, format string, args ...any) error {
	return &ParseError{Line: tok.Line, Col: tok.Col, Msg: fmt.Sprintf(format, args...)}
}

func (p *Parser) expect(t TokenType, what string) (Token, error) {
	if p.cur.Type != t {
		return Token{}, p.errf(p.cur, "unexpected %s, expected %s", describe(p.cur), what)
	}
	tok := p.cur
	p.advance()
	return tok, nil
}

func (p *Parser) curIsKeyword(kw string) bool {
	return p.cur.Type == TokIdent && strings.EqualFold(p.cur.Lit, kw)
}

func describe(t Token) string {
	switch t.Type {
	case TokEOF:
		return "end of query"
	case TokIllegal:
		return fmt.Sprintf("invalid input (%s)", t.Lit)
	case TokString:
		return fmt.Sprintf("string %q", t.Lit)
	default:
		return fmt.Sprintf("%q", t.Lit)
	}
}

// Parse parses one complete query: a TraceQuery or a SelectQuery, and
// requires the whole input be consumed -- trailing garbage is an error
// rather than a silent partial parse.
func (p *Parser) Parse() (Query, error) {
	if p.cur.Type == TokIllegal {
		return nil, p.errf(p.cur, "%s", p.cur.Lit)
	}

	var q Query
	var err error
	if p.curIsKeyword("trace") {
		q, err = p.parseTraceQuery()
	} else {
		q, err = p.parseSelectQuery()
	}
	if err != nil {
		return nil, err
	}
	if p.cur.Type != TokEOF {
		return nil, p.errf(p.cur, "unexpected trailing input starting at %s", describe(p.cur))
	}
	return q, nil
}

func (p *Parser) parseTraceQuery() (Query, error) {
	p.advance() // "trace"
	if _, err := p.expect(TokLParen, "'('"); err != nil {
		return nil, err
	}
	idTok, err := p.expect(TokString, "a quoted trace id")
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokRParen, "')'"); err != nil {
		return nil, err
	}
	if idTok.Lit == "" {
		return nil, p.errf(idTok, "trace id must not be empty")
	}
	return TraceQuery{TraceID: idTok.Lit}, nil
}

func (p *Parser) parseSelectQuery() (Query, error) {
	matchers, err := p.parseSelector()
	if err != nil {
		return nil, err
	}

	tr := TimeRange{Start: p.now().Add(-time.Hour), End: p.now(), Explicit: false}
	if p.curIsKeyword("since") || p.curIsKeyword("range") {
		tr, err = p.parseTimeRange()
		if err != nil {
			return nil, err
		}
	}
	if !tr.Start.Before(tr.End) {
		return nil, fmt.Errorf("query: time range start (%s) must be before end (%s)", tr.Start, tr.End)
	}

	var stages []Stage
	for p.cur.Type == TokPipe {
		p.advance()
		stage, err := p.parseStage()
		if err != nil {
			return nil, err
		}
		stages = append(stages, stage)
	}

	return SelectQuery{Selector: matchers, Range: tr, Stages: stages}, nil
}

func (p *Parser) parseSelector() ([]LabelMatcher, error) {
	if _, err := p.expect(TokLBrace, "'{'"); err != nil {
		return nil, err
	}
	if p.cur.Type == TokRBrace {
		p.advance()
		return nil, nil
	}

	var matchers []LabelMatcher
	for {
		nameTok, err := p.expect(TokIdent, "a label name")
		if err != nil {
			return nil, err
		}
		op, err := p.parseLabelOp()
		if err != nil {
			return nil, err
		}
		// A value is a quoted string, or a bare identifier for the common
		// case of matching an enum-like value without ceremony (status=error,
		// as in the canonical example) -- both lex distinctly, so there is
		// no ambiguity in accepting either here.
		var valTok Token
		switch p.cur.Type {
		case TokString, TokIdent:
			valTok = p.cur
			p.advance()
		default:
			return nil, p.errf(p.cur, "unexpected %s, expected a value", describe(p.cur))
		}
		matchers = append(matchers, LabelMatcher{Name: nameTok.Lit, Op: op, Value: valTok.Lit})

		if p.cur.Type == TokComma {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(TokRBrace, "'}' or ','"); err != nil {
		return nil, err
	}
	return matchers, nil
}

func (p *Parser) parseLabelOp() (LabelOp, error) {
	switch p.cur.Type {
	case TokEq:
		p.advance()
		return OpEq, nil
	case TokNeq:
		p.advance()
		return OpNeq, nil
	case TokRegexEq:
		p.advance()
		return OpRegexMatch, nil
	case TokRegexNeq:
		p.advance()
		return OpRegexNotMatch, nil
	default:
		return 0, p.errf(p.cur, "unexpected %s, expected a label operator (=, !=, =~, !~)", describe(p.cur))
	}
}

func (p *Parser) parseTimeRange() (TimeRange, error) {
	if p.curIsKeyword("since") {
		p.advance()
		d, err := p.parseDurationToken()
		if err != nil {
			return TimeRange{}, err
		}
		end := p.now()
		return TimeRange{Start: end.Add(-d), End: end, Explicit: true}, nil
	}

	// "range"
	p.advance()
	if _, err := p.expect(TokLParen, "'('"); err != nil {
		return TimeRange{}, err
	}
	start, err := p.parseTimeLiteral()
	if err != nil {
		return TimeRange{}, err
	}
	if _, err := p.expect(TokComma, "','"); err != nil {
		return TimeRange{}, err
	}
	end, err := p.parseTimeLiteral()
	if err != nil {
		return TimeRange{}, err
	}
	if _, err := p.expect(TokRParen, "')'"); err != nil {
		return TimeRange{}, err
	}
	return TimeRange{Start: start, End: end, Explicit: true}, nil
}

func (p *Parser) parseTimeLiteral() (time.Time, error) {
	if p.cur.Type == TokString {
		tok := p.cur
		p.advance()
		t, err := time.Parse(time.RFC3339, tok.Lit)
		if err != nil {
			return time.Time{}, p.errf(tok, "invalid RFC3339 timestamp %q: %v", tok.Lit, err)
		}
		return t, nil
	}
	if p.curIsKeyword("now") {
		p.advance()
		t := p.now()
		if p.cur.Type == TokMinus {
			p.advance()
			d, err := p.parseDurationToken()
			if err != nil {
				return time.Time{}, err
			}
			t = t.Add(-d)
		}
		return t, nil
	}
	return time.Time{}, p.errf(p.cur, "unexpected %s, expected a quoted RFC3339 timestamp or \"now\"", describe(p.cur))
}

// parseDurationToken consumes one NUMBER token and requires it to carry a
// unit suffix (the lexer attaches units directly, so "1h30m" is already one
// token) -- a bare number here is ambiguous with no default unit worth
// guessing, so it is a parse error rather than a silent assumption.
func (p *Parser) parseDurationToken() (time.Duration, error) {
	tok, err := p.expect(TokNumber, "a duration (e.g. 500ms, 5m, 1h)")
	if err != nil {
		return 0, err
	}
	if !hasUnitSuffix(tok.Lit) {
		return 0, p.errf(tok, "%q is missing a duration unit (ns, us, ms, s, m, h)", tok.Lit)
	}
	d, err := time.ParseDuration(tok.Lit)
	if err != nil {
		return 0, p.errf(tok, "invalid duration %q: %v", tok.Lit, err)
	}
	return d, nil
}

func (p *Parser) parseStage() (Stage, error) {
	if isAggKeyword(p.cur) {
		return p.parseAggregation()
	}
	if p.curIsKeyword("sort") {
		return p.parseSort()
	}
	if p.curIsKeyword("limit") {
		return p.parseLimit()
	}
	if p.cur.Type == TokIdent {
		return p.parseNumericFilter()
	}
	return nil, p.errf(p.cur, "unexpected %s, expected a filter, aggregation, sort or limit stage", describe(p.cur))
}

func (p *Parser) parseNumericFilter() (Stage, error) {
	fieldTok := p.cur
	p.advance()
	op, err := p.parseCmpOp()
	if err != nil {
		return nil, err
	}
	numTok, err := p.expect(TokNumber, "a number")
	if err != nil {
		return nil, err
	}
	val, unit, err := splitNumberUnit(numTok.Lit)
	if err != nil {
		return nil, p.errf(numTok, "%v", err)
	}
	return NumericFilter{Field: fieldTok.Lit, Op: op, Value: val, Unit: unit}, nil
}

func (p *Parser) parseCmpOp() (CmpOp, error) {
	switch p.cur.Type {
	case TokGT:
		p.advance()
		return CmpGT, nil
	case TokGTE:
		p.advance()
		return CmpGTE, nil
	case TokLT:
		p.advance()
		return CmpLT, nil
	case TokLTE:
		p.advance()
		return CmpLTE, nil
	case TokEqEq:
		p.advance()
		return CmpEQ, nil
	case TokNeq:
		p.advance()
		return CmpNEQ, nil
	default:
		return 0, p.errf(p.cur, "unexpected %s, expected a comparison operator (>, >=, <, <=, ==, !=)", describe(p.cur))
	}
}

var aggKeywords = map[string]AggFunc{
	"count": AggCount, "sum": AggSum, "avg": AggAvg,
	"min": AggMin, "max": AggMax,
	"p50": AggP50, "p95": AggP95, "p99": AggP99,
}

func isAggKeyword(t Token) bool {
	if t.Type != TokIdent {
		return false
	}
	_, ok := aggKeywords[strings.ToLower(t.Lit)]
	return ok
}

func (p *Parser) parseAggregation() (Stage, error) {
	var exprs []AggExpr
	for {
		expr, err := p.parseAggExpr()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if p.cur.Type == TokComma {
			p.advance()
			continue
		}
		break
	}

	var groupBy []string
	if p.curIsKeyword("by") {
		p.advance()
		if _, err := p.expect(TokLParen, "'('"); err != nil {
			return nil, err
		}
		for {
			fieldTok, err := p.expect(TokIdent, "a group-by field")
			if err != nil {
				return nil, err
			}
			groupBy = append(groupBy, fieldTok.Lit)
			if p.cur.Type == TokComma {
				p.advance()
				continue
			}
			break
		}
		if _, err := p.expect(TokRParen, "')'"); err != nil {
			return nil, err
		}
	}
	return Aggregation{Exprs: exprs, GroupBy: groupBy}, nil
}

func (p *Parser) parseAggExpr() (AggExpr, error) {
	fnTok := p.cur
	fn := aggKeywords[strings.ToLower(fnTok.Lit)]
	p.advance()

	if fn == AggCount && p.cur.Type != TokLParen {
		return AggExpr{Func: fn}, nil
	}
	if _, err := p.expect(TokLParen, "'('"); err != nil {
		return AggExpr{}, err
	}
	fieldTok, err := p.expect(TokIdent, "a field name")
	if err != nil {
		return AggExpr{}, err
	}
	if _, err := p.expect(TokRParen, "')'"); err != nil {
		return AggExpr{}, err
	}
	return AggExpr{Func: fn, Field: fieldTok.Lit}, nil
}

func (p *Parser) parseSort() (Stage, error) {
	p.advance() // "sort"
	if !p.curIsKeyword("by") {
		return nil, p.errf(p.cur, "unexpected %s, expected \"by\"", describe(p.cur))
	}
	p.advance()
	if _, err := p.expect(TokLParen, "'('"); err != nil {
		return nil, err
	}
	var fields []SortField
	for {
		fieldTok, err := p.expect(TokIdent, "a sort field")
		if err != nil {
			return nil, err
		}
		desc := false
		if p.curIsKeyword("asc") {
			p.advance()
		} else if p.curIsKeyword("desc") {
			desc = true
			p.advance()
		}
		fields = append(fields, SortField{Field: fieldTok.Lit, Desc: desc})
		if p.cur.Type == TokComma {
			p.advance()
			continue
		}
		break
	}
	if _, err := p.expect(TokRParen, "')'"); err != nil {
		return nil, err
	}
	return SortStage{Fields: fields}, nil
}

func (p *Parser) parseLimit() (Stage, error) {
	p.advance() // "limit"
	numTok, err := p.expect(TokNumber, "a limit count")
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(numTok.Lit)
	if err != nil || n < 0 {
		return nil, p.errf(numTok, "invalid limit %q: must be a non-negative integer", numTok.Lit)
	}
	return LimitStage{N: n}, nil
}

// splitNumberUnit separates a lexed NUMBER token's digit run from its
// trailing unit letters, e.g. "500ms" -> (500, "ms"), "20" -> (20, "").
func splitNumberUnit(lit string) (float64, string, error) {
	i := 0
	for i < len(lit) && (isDigit(lit[i]) || lit[i] == '.') {
		i++
	}
	numPart, unitPart := lit[:i], lit[i:]
	val, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid number %q", lit)
	}
	return val, unitPart, nil
}

func hasUnitSuffix(lit string) bool {
	_, unit, err := splitNumberUnit(lit)
	return err == nil && unit != ""
}
