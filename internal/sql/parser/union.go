package parser

import (
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

func (p *Parser) parseSelectStatement() (*ast.SelectStmt, bool, error) {
	left, ok, err := p.parseSelectCore()
	if err != nil || !ok {
		return left, ok, err
	}
	for p.accept(lexer.KeywordUnion) {
		if len(left.OrderBy) != 0 || left.Limit != nil || left.Offset != nil {
			return nil, false, p.errorf("ORDER BY/LIMIT before UNION ALL requires a derived table")
		}
		if !p.acceptWord("all") {
			return nil, false, p.errorf("only UNION ALL is supported")
		}
		right, ok, err := p.parseSelectCore()
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, p.errorf("UNION ALL requires SELECT")
		}
		with := left.With
		left.With = nil
		outer := &ast.SelectStmt{Span: ast.Span{Start: left.Span.Start, End: right.Span.End}, With: with, UnionLeft: left, UnionRight: right, OrderBy: right.OrderBy, Limit: right.Limit, Offset: right.Offset}
		right.OrderBy = nil
		right.Limit = nil
		right.Offset = nil
		left = outer
	}
	return left, true, nil
}
