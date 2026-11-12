package parser

import (
	"fmt"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

func (p *Parser) parseSelectStatement() (*ast.SelectStmt, bool, error) {
	q, grouped, ok, err := p.parseSetExpression(1)
	if err != nil || !ok {
		return q, ok, err
	}
	// A suffix after parentheses belongs to the entire grouped query. Keep any
	// inner limit/order intact by introducing a normal derived-table boundary.
	if p.peek().Kind == lexer.KeywordOrder || p.peek().Kind == lexer.KeywordLimit || p.peek().Kind == lexer.KeywordOffset {
		if !grouped && (len(q.OrderBy) > 0 || q.Limit != nil || q.Offset != nil) {
			return nil, false, p.errorf("duplicate or out-of-order query suffix")
		}
		if grouped {
			p.internalSeq++
			alias := ast.Identifier{Name: fmt.Sprintf("__fssql_set_%d", p.internalSeq)}
			q = &ast.SelectStmt{Span: q.Span, Projection: []ast.SelectItem{{Star: true}}, From: &ast.DerivedTable{Query: q, Alias: alias}}
		}
		if p.accept(lexer.KeywordOrder) {
			if err := p.expect(lexer.KeywordBy, "BY"); err != nil {
				return nil, false, err
			}
			q.OrderBy, err = p.parseOrderTerms()
			if err != nil {
				return nil, false, err
			}
		}
		if p.accept(lexer.KeywordLimit) {
			q.Limit, err = p.parseExpr(0)
			if err != nil {
				return nil, false, err
			}
		}
		if p.accept(lexer.KeywordOffset) {
			q.Offset, err = p.parseExpr(0)
			if err != nil {
				return nil, false, err
			}
		}
		q.Span.End = p.previous().Span.End
	}
	return q, true, nil
}

func (p *Parser) parseSetExpression(minimum int) (*ast.SelectStmt, bool, bool, error) {
	left, grouped, ok, err := p.parseSetPrimary()
	if err != nil || !ok {
		return left, grouped, ok, err
	}
	for {
		kind := p.peek().Kind
		precedence := 1
		if kind == lexer.KeywordIntersect {
			precedence = 2
		} else if kind != lexer.KeywordUnion && kind != lexer.KeywordExcept {
			break
		}
		if precedence < minimum {
			break
		}
		if !grouped && (len(left.OrderBy) > 0 || left.Limit != nil || left.Offset != nil) {
			return nil, false, false, p.errorf("ORDER BY/LIMIT before a set operation requires parentheses")
		}
		p.pos++
		all := p.acceptWord("all")
		if !all {
			p.accept(lexer.KeywordDistinct)
		}
		byName := false
		if p.accept(lexer.KeywordBy) {
			if kind != lexer.KeywordUnion || !p.acceptWord("name") { return nil,false,false,p.errorf("BY NAME is only supported for UNION") }
			byName = true
		}
		op := ast.UnionDistinct
		switch kind {
		case lexer.KeywordUnion:
			if all {
				op = ast.UnionAll
			}
		case lexer.KeywordIntersect:
			op = ast.IntersectDistinct
			if all {
				op = ast.IntersectAll
			}
		case lexer.KeywordExcept:
			op = ast.ExceptDistinct
			if all {
				op = ast.ExceptAll
			}
		}
		right, rightGrouped, ok, err := p.parseSetExpression(precedence + 1)
		if err != nil {
			return nil, false, false, err
		}
		if !ok {
			return nil, false, false, p.errorf("set operation requires SELECT")
		}
		outer := &ast.SelectStmt{Span: ast.Span{Start: left.Span.Start, End: right.Span.End}, UnionLeft: left, UnionRight: right, SetOperation: op, ByName:byName}
		if !grouped {
			outer.With = left.With
			left.With = nil
		}
		if !rightGrouped {
			outer.OrderBy, outer.Limit, outer.Offset = right.OrderBy, right.Limit, right.Offset
			right.OrderBy, right.Limit, right.Offset = nil, nil, nil
		}
		left, grouped = outer, false
	}
	return left, grouped, true, nil
}

func (p *Parser) parseSetPrimary() (*ast.SelectStmt, bool, bool, error) {
	if p.peek().Kind != lexer.LParen {
		q, ok, err := p.parseSelectCore()
		return q, false, ok, err
	}
	mark := p.mark()
	start := p.peek().Span.Start
	p.pos++
	q, ok, err := p.parseSelectStatement()
	if err != nil {
		return nil, false, false, err
	}
	if !ok {
		p.restore(mark)
		return nil, false, false, nil
	}
	if err := p.expect(lexer.RParen, ")"); err != nil {
		return nil, false, false, err
	}
	q.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return q, true, true, nil
}
