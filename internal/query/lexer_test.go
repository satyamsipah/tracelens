package query

import "testing"

func TestLexerShouldTokenizeEveryPunctuationForm(t *testing.T) {
	src := `{}(),|- = != =~ !~ == >= <= > <`
	want := []TokenType{
		TokLBrace, TokRBrace, TokLParen, TokRParen, TokComma, TokPipe, TokMinus,
		TokEq, TokNeq, TokRegexEq, TokRegexNeq, TokEqEq, TokGTE, TokLTE, TokGT, TokLT,
		TokEOF,
	}
	lex := NewLexer(src)
	for i, wantType := range want {
		tok := lex.Next()
		if tok.Type != wantType {
			t.Fatalf("token %d: got type %d (%q), want %d", i, tok.Type, tok.Lit, wantType)
		}
	}
}

func TestLexerCompoundNumberAdjacencyRule(t *testing.T) {
	tests := []struct {
		src  string
		want []string // expected NUMBER token literals, in order
	}{
		{"500ms", []string{"500ms"}},
		{"1h30m", []string{"1h30m"}},
		{"1h 500ms", []string{"1h", "500ms"}},
		{"20", []string{"20"}},
		{"1.5s", []string{"1.5s"}},
	}
	for _, tt := range tests {
		lex := NewLexer(tt.src)
		var got []string
		for {
			tok := lex.Next()
			if tok.Type == TokEOF {
				break
			}
			if tok.Type != TokNumber {
				t.Fatalf("%s: unexpected token type %d (%q)", tt.src, tok.Type, tok.Lit)
			}
			got = append(got, tok.Lit)
		}
		if len(got) != len(tt.want) {
			t.Fatalf("%s: got tokens %v, want %v", tt.src, got, tt.want)
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("%s: token %d = %q, want %q", tt.src, i, got[i], tt.want[i])
			}
		}
	}
}

func TestLexerIdentAllowsDottedAttributeKeys(t *testing.T) {
	lex := NewLexer("http.status_code")
	tok := lex.Next()
	if tok.Type != TokIdent || tok.Lit != "http.status_code" {
		t.Fatalf("got %+v", tok)
	}
}

func TestLexerRelativeTimeMinusIsSeparateFromIdent(t *testing.T) {
	lex := NewLexer("now-1h")
	types := []TokenType{TokIdent, TokMinus, TokNumber, TokEOF}
	for i, want := range types {
		tok := lex.Next()
		if tok.Type != want {
			t.Fatalf("token %d: got type %d (%q), want %d", i, tok.Type, tok.Lit, want)
		}
	}
}

func TestLexerStringEscapes(t *testing.T) {
	lex := NewLexer(`"a\"b\\c\td\ne"`)
	tok := lex.Next()
	if tok.Type != TokString {
		t.Fatalf("got type %d", tok.Type)
	}
	want := "a\"b\\c\td\ne"
	if tok.Lit != want {
		t.Errorf("got %q, want %q", tok.Lit, want)
	}
}

func TestLexerUnterminatedStringIsIllegal(t *testing.T) {
	lex := NewLexer(`"abc`)
	tok := lex.Next()
	if tok.Type != TokIllegal {
		t.Fatalf("got %+v, want TokIllegal", tok)
	}
}

func TestLexerLineAndColumnTracking(t *testing.T) {
	lex := NewLexer("{}\n  x")
	lex.Next() // {
	lex.Next() // }
	tok := lex.Next()
	if tok.Line != 2 || tok.Col != 3 {
		t.Errorf("got line=%d col=%d, want line=2 col=3", tok.Line, tok.Col)
	}
}
