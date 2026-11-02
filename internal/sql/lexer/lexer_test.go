package lexer

import "testing"

func TestLexerTracksQuotedNamesStringsCommentsAndPositions(t *testing.T) {
	tokens, err := All("-- c\nSELECT \"MiX\", 'a''b', 12.5 FROM t /*x*/ WHERE a<>1;")
	if err != nil {
		t.Fatal(err)
	}
	if tokens[0].Kind != KeywordSelect || tokens[0].Span.Start.Line != 2 {
		t.Fatalf("select token=%+v", tokens[0])
	}
	if tokens[1].Kind != QuotedIdentifier || tokens[1].Text != "MiX" {
		t.Fatalf("quoted=%+v", tokens[1])
	}
	if tokens[3].Kind != String || tokens[3].Text != "a'b" {
		t.Fatalf("string=%+v", tokens[3])
	}
	if tokens[5].Kind != Number || tokens[5].Raw != "12.5" {
		t.Fatalf("number=%+v", tokens[5])
	}
	found := false
	for _, tok := range tokens {
		if tok.Kind == Neq {
			found = true
		}
	}
	if !found {
		t.Fatal("expected <> token")
	}
}

func TestLexerRejectsUnterminatedInput(t *testing.T) {
	for _, sql := range []string{"SELECT 'x", "SELECT \"x", "SELECT 1 /* x"} {
		if _, err := All(sql); err == nil {
			t.Fatalf("expected error for %q", sql)
		}
	}
}
