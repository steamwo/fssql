package lexer

import (
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func (l *Lexer) scanParameter(start ast.Position) (Token, error) {
	marker := l.src[l.offset]
	l.advanceByte()
	if marker == '?' {
		return l.tokenFrom(start, Parameter), nil
	}
	for l.offset < len(l.src) {
		r, size := utf8.DecodeRuneInString(l.src[l.offset:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			break
		}
		l.advanceN(size, r)
	}
	if l.offset == start.Offset+1 {
		return Token{}, fmt.Errorf("sql lexer: expected parameter name or number at %d:%d", start.Line, start.Column)
	}
	return l.tokenFrom(start, Parameter), nil
}
