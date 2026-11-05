package parser

import (
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

func (p *Parser) parsePredicate(left ast.Expr, minBP int) (ast.Expr, bool, error) {
	not := p.peek().Kind == lexer.KeywordNot
	offset := 0
	if not {
		offset = 1
	}
	in := p.peekNWord(offset, "in")
	between := p.peekNWord(offset, "between")
	like := p.peekNWord(offset, "like")
	if minBP > 30 || (!in && !between && !like) {
		return left, false, nil
	}
	p.pos += offset + 1
	args := []ast.Expr{left}
	name := "__fssql_in"
	if in {
		if err := p.expect(lexer.LParen, "("); err != nil {
			return nil, true, err
		}
		if p.peek().Kind == lexer.KeywordSelect || p.peek().Kind == lexer.KeywordWith {
			query, ok, err := p.parseSelectStatement()
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, true, p.errorf("IN requires SELECT")
			}
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, true, err
			}
			return &ast.InSubqueryExpr{Span: ast.Span{Start: left.NodeSpan().Start, End: p.previous().Span.End}, Value: left, Query: query, Not: not}, true, nil
		}
		for {
			arg, err := p.parseExpr(0)
			if err != nil {
				return nil, true, err
			}
			args = append(args, arg)
			if !p.accept(lexer.Comma) {
				break
			}
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, true, err
		}
	} else if between {
		name = "__fssql_between"
		lower, err := p.parseExpr(31)
		if err != nil {
			return nil, true, err
		}
		if err := p.expect(lexer.KeywordAnd, "AND"); err != nil {
			return nil, true, err
		}
		upper, err := p.parseExpr(31)
		if err != nil {
			return nil, true, err
		}
		args = append(args, lower, upper)
	} else {
		name = "__fssql_like"
		pattern, err := p.parseExpr(31)
		if err != nil {
			return nil, true, err
		}
		args = append(args, pattern)
		if p.acceptWord("escape") {
			escape, err := p.parseExpr(31)
			if err != nil {
				return nil, true, err
			}
			args = append(args, escape)
		}
	}
	span := ast.Span{Start: left.NodeSpan().Start, End: p.previous().Span.End}
	var result ast.Expr = phase4Call(name, span, args...)
	if not {
		result = &ast.UnaryExpr{Span: span, Op: "NOT", Expr: result}
	}
	return result, true, nil
}
