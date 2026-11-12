package parser

import (
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

func (p *Parser) parseSetStatement() (ast.Statement, bool, error) {
	start := p.peek().Span.Start
	reset := p.peekWord("reset")
	p.pos++
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, true, err
	}
	s := &ast.SetStmt{Name: name, Reset: reset}
	if !reset {
		if !p.accept(lexer.Eq) && !p.acceptWord("to") {
			return nil, true, p.errorf("SET requires = or TO")
		}
		s.Value, err = p.parseExpr(0)
		if err != nil {
			return nil, true, err
		}
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return s, true, nil
}
