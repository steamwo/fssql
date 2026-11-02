package parser

import (
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

// Parser 使用 PEG 风格 ordered-choice/回溯解析 statement，表达式单独使用 Pratt parser。
type Parser struct {
	tokens []lexer.Token // tokens 是一次 SQL statement 的有限 token 序列。
	pos    int           // pos 是当前 PEG/Pratt 游标。
}

// Parse 解析单条 Stable v1 SQL statement。
func Parse(sql string) (ast.Statement, error) {
	tokens, err := lexer.All(sql)
	if err != nil {
		return nil, err
	}
	p := &Parser{tokens: tokens}
	stmt, ok, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, p.errorf("expected Stable v1 statement")
	}
	p.accept(lexer.Semicolon)
	if p.peek().Kind != lexer.EOF {
		return nil, p.errorf("unexpected token %q", p.peek().Raw)
	}
	return stmt, nil
}

// mark 返回 PEG-style ordered choice 的回溯位置。
func (p *Parser) mark() int { return p.pos }

// restore 恢复 ordered choice 的回溯位置。
func (p *Parser) restore(mark int) { p.pos = mark }

// parseSelectStatement 解析 [WITH ...] SELECT ...。
func (p *Parser) parseSelectStatement() (*ast.SelectStmt, bool, error) {
	mark := p.mark()
	start := p.peek().Span.Start
	var with *ast.WithClause
	if p.accept(lexer.KeywordWith) {
		w, err := p.parseWithClause(start)
		if err != nil {
			return nil, false, err
		}
		with = w
	}
	if !p.accept(lexer.KeywordSelect) {
		p.restore(mark)
		return nil, false, nil
	}
	s := &ast.SelectStmt{With: with}
	s.Distinct = p.accept(lexer.KeywordDistinct)
	items, err := p.parseSelectList()
	if err != nil {
		return nil, false, err
	}
	s.Projection = items
	if p.accept(lexer.KeywordFrom) {
		from, err := p.parseTableExpr()
		if err != nil {
			return nil, false, err
		}
		s.From = from
	}
	if p.accept(lexer.KeywordWhere) {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, false, err
		}
		s.Where = e
	}
	if p.accept(lexer.KeywordGroup) {
		if err := p.expect(lexer.KeywordBy, "BY"); err != nil {
			return nil, false, err
		}
		xs, err := p.parseExprList()
		if err != nil {
			return nil, false, err
		}
		s.GroupBy = xs
	}
	if p.accept(lexer.KeywordHaving) {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, false, err
		}
		s.Having = e
	}
	if p.accept(lexer.KeywordOrder) {
		if err := p.expect(lexer.KeywordBy, "BY"); err != nil {
			return nil, false, err
		}
		terms, err := p.parseOrderTerms()
		if err != nil {
			return nil, false, err
		}
		s.OrderBy = terms
	}
	if p.accept(lexer.KeywordLimit) {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, false, err
		}
		s.Limit = e
	}
	if p.accept(lexer.KeywordOffset) {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, false, err
		}
		s.Offset = e
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return s, true, nil
}

// parseWithClause 解析 non-recursive CTE；RECURSIVE 未进入 Stable v1。
func (p *Parser) parseWithClause(start ast.Position) (*ast.WithClause, error) {
	w := &ast.WithClause{}
	for {
		name, err := p.parseIdentifier()
		if err != nil {
			return nil, err
		}
		cte := ast.CTE{Name: name}
		cteStart := name.Span.Start
		if p.accept(lexer.LParen) {
			for {
				id, err := p.parseIdentifier()
				if err != nil {
					return nil, err
				}
				cte.Columns = append(cte.Columns, id)
				if !p.accept(lexer.Comma) {
					break
				}
			}
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, err
			}
		}
		if err := p.expect(lexer.KeywordAs, "AS"); err != nil {
			return nil, err
		}
		if err := p.expect(lexer.LParen, "("); err != nil {
			return nil, err
		}
		q, ok, err := p.parseSelectStatement()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, p.errorf("expected SELECT in CTE")
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, err
		}
		cte.Query = q
		cte.Span = ast.Span{Start: cteStart, End: p.previous().Span.End}
		w.CTEs = append(w.CTEs, cte)
		if !p.accept(lexer.Comma) {
			break
		}
	}
	w.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return w, nil
}

// parseSelectList 解析 SELECT 投影列表。
func (p *Parser) parseSelectList() ([]ast.SelectItem, error) {
	var out []ast.SelectItem
	for {
		start := p.peek().Span.Start
		item := ast.SelectItem{}
		if p.accept(lexer.Star) {
			item.Star = true
		} else {
			e, err := p.parseExpr(0)
			if err != nil {
				return nil, err
			}
			item.Expr = e
		}
		if p.accept(lexer.KeywordAs) {
			id, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			item.Alias = &id
		} else if p.peek().Kind == lexer.Identifier || p.peek().Kind == lexer.QuotedIdentifier {
			id, _ := p.parseIdentifier()
			item.Alias = &id
		}
		item.Span = ast.Span{Start: start, End: p.previous().Span.End}
		out = append(out, item)
		if !p.accept(lexer.Comma) {
			return out, nil
		}
	}
}

// parseExprList 解析逗号分隔的表达式列表。
func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var out []ast.Expr
	for {
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
		if !p.accept(lexer.Comma) {
			return out, nil
		}
	}
}

// parseOrderTerms 解析 ORDER BY 列表及 NULLS FIRST/LAST。
func (p *Parser) parseOrderTerms() ([]ast.OrderTerm, error) {
	var out []ast.OrderTerm
	for {
		start := p.peek().Span.Start
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		t := ast.OrderTerm{Expr: e}
		if p.accept(lexer.KeywordDesc) {
			t.Descending = true
		} else {
			p.accept(lexer.KeywordAsc)
		}
		if p.accept(lexer.KeywordNulls) {
			first := false
			if p.accept(lexer.KeywordFirst) {
				first = true
			} else if !p.accept(lexer.KeywordLast) {
				return nil, p.errorf("expected FIRST or LAST")
			}
			t.NullsFirst = &first
		}
		t.Span = ast.Span{Start: start, End: p.previous().Span.End}
		out = append(out, t)
		if !p.accept(lexer.Comma) {
			return out, nil
		}
	}
}

// parseTableExpr 解析表名/表函数以及 Stable JOIN 链。
func (p *Parser) parseTableExpr() (ast.TableExpr, error) {
	left, err := p.parseTablePrimary()
	if err != nil {
		return nil, err
	}
	for {
		mark := p.mark()
		jt := ast.JoinInner
		explicit := false
		switch p.peek().Kind {
		case lexer.KeywordInner:
			explicit = true
			p.pos++
			jt = ast.JoinInner
		case lexer.KeywordLeft:
			explicit = true
			p.pos++
			jt = ast.JoinLeft
			p.accept(lexer.KeywordOuter)
		case lexer.KeywordRight:
			explicit = true
			p.pos++
			jt = ast.JoinRight
			p.accept(lexer.KeywordOuter)
		case lexer.KeywordFull:
			explicit = true
			p.pos++
			jt = ast.JoinFull
			p.accept(lexer.KeywordOuter)
		case lexer.KeywordCross:
			explicit = true
			p.pos++
			jt = ast.JoinCross
		case lexer.KeywordSemi:
			explicit = true
			p.pos++
			jt = ast.JoinSemi
		case lexer.KeywordAnti:
			explicit = true
			p.pos++
			jt = ast.JoinAnti
		}
		if !p.accept(lexer.KeywordJoin) {
			if explicit {
				return nil, p.errorf("expected JOIN")
			}
			p.restore(mark)
			return left, nil
		}
		right, err := p.parseTablePrimary()
		if err != nil {
			return nil, err
		}
		j := &ast.JoinExpr{Left: left, Right: right, Type: jt}
		j.Span.Start = left.NodeSpan().Start
		if jt != ast.JoinCross {
			if p.accept(lexer.KeywordOn) {
				e, err := p.parseExpr(0)
				if err != nil {
					return nil, err
				}
				j.Condition = e
			} else if p.accept(lexer.KeywordUsing) {
				if err := p.expect(lexer.LParen, "("); err != nil {
					return nil, err
				}
				for {
					id, err := p.parseIdentifier()
					if err != nil {
						return nil, err
					}
					j.Using = append(j.Using, id)
					if !p.accept(lexer.Comma) {
						break
					}
				}
				if err := p.expect(lexer.RParen, ")"); err != nil {
					return nil, err
				}
			} else {
				return nil, p.errorf("JOIN requires ON or USING")
			}
		}
		j.Span.End = p.previous().Span.End
		left = j
	}
}

// parseTablePrimary 解析普通表名或表函数调用。
func (p *Parser) parseTablePrimary() (ast.TableExpr, error) {
	name, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	start := name.Span.Start
	if p.accept(lexer.LParen) {
		call := &ast.CallExpr{Function: name}
		if !p.accept(lexer.RParen) {
			for {
				e, err := p.parseExpr(0)
				if err != nil {
					return nil, err
				}
				call.Args = append(call.Args, e)
				if !p.accept(lexer.Comma) {
					break
				}
			}
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, err
			}
		}
		call.Span = ast.Span{Start: start, End: p.previous().Span.End}
		t := &ast.TableFunction{Call: call}
		t.Alias = p.parseOptionalAlias()
		t.Span = ast.Span{Start: start, End: p.previous().Span.End}
		return t, nil
	}
	t := &ast.TableName{Name: name}
	t.Alias = p.parseOptionalAlias()
	t.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return t, nil
}

// parseOptionalAlias 解析显式或普通裸 alias。
func (p *Parser) parseOptionalAlias() *ast.Identifier {
	if p.accept(lexer.KeywordAs) {
		id, err := p.parseIdentifier()
		if err == nil {
			return &id
		}
		return nil
	}
	if p.peek().Kind == lexer.Identifier || p.peek().Kind == lexer.QuotedIdentifier {
		id, _ := p.parseIdentifier()
		return &id
	}
	return nil
}

// parseExpr 是 Pratt parser；binding power 描述 Stable v1 布尔、比较和算术运算。
func (p *Parser) parseExpr(minBP int) (ast.Expr, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}
	for {
		if p.peek().Kind == lexer.KeywordIs {
			if 30 < minBP {
				return left, nil
			}
			start := left.NodeSpan().Start
			p.pos++
			not := p.accept(lexer.KeywordNot)
			if err := p.expect(lexer.KeywordNull, "NULL"); err != nil {
				return nil, err
			}
			left = &ast.IsNullExpr{Span: ast.Span{Start: start, End: p.previous().Span.End}, Expr: left, Not: not}
			continue
		}
		lbp, rbp, op, ok := infixBinding(p.peek().Kind)
		if !ok || lbp < minBP {
			return left, nil
		}
		p.pos++
		right, err := p.parseExpr(rbp)
		if err != nil {
			return nil, err
		}
		left = &ast.BinaryExpr{Span: ast.Span{Start: left.NodeSpan().Start, End: right.NodeSpan().End}, Left: left, Op: op, Right: right}
	}
}

// parsePrefix 解析 Pratt 前缀表达式、函数调用及子查询。
func (p *Parser) parsePrefix() (ast.Expr, error) {
	t := p.peek()
	switch t.Kind {
	case lexer.KeywordNot, lexer.Plus, lexer.Minus:
		p.pos++
		e, err := p.parseExpr(70)
		if err != nil {
			return nil, err
		}
		return &ast.UnaryExpr{Span: ast.Span{Start: t.Span.Start, End: e.NodeSpan().End}, Op: strings.ToUpper(t.Raw), Expr: e}, nil
	case lexer.KeywordNull:
		p.pos++
		return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralNull, Value: "NULL"}, nil
	case lexer.KeywordTrue, lexer.KeywordFalse:
		p.pos++
		return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralBool, Value: strings.ToUpper(t.Raw)}, nil
	case lexer.String:
		p.pos++
		return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}, nil
	case lexer.Number:
		p.pos++
		kind := ast.LiteralInteger
		if strings.Contains(t.Raw, ".") {
			kind = ast.LiteralDecimal
		}
		return &ast.LiteralExpr{Span: t.Span, Kind: kind, Value: t.Raw}, nil
	case lexer.LParen:
		start := t.Span.Start
		p.pos++
		if q, ok, err := p.parseSelectStatement(); err != nil {
			return nil, err
		} else if ok {
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, err
			}
			return &ast.SubqueryExpr{Span: ast.Span{Start: start, End: p.previous().Span.End}, Query: q}, nil
		}
		e, err := p.parseExpr(0)
		if err != nil {
			return nil, err
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, err
		}
		return e, nil
	case lexer.Identifier, lexer.QuotedIdentifier:
		name, err := p.parseQualifiedName()
		if err != nil {
			return nil, err
		}
		if !p.accept(lexer.LParen) {
			return &ast.NameExpr{Span: name.Span, Name: name}, nil
		}
		call := &ast.CallExpr{Function: name}
		if p.accept(lexer.KeywordDistinct) {
			call.Distinct = true
		}
		if p.accept(lexer.Star) {
			call.Star = true
		} else if !p.accept(lexer.RParen) {
			for {
				e, err := p.parseExpr(0)
				if err != nil {
					return nil, err
				}
				call.Args = append(call.Args, e)
				if !p.accept(lexer.Comma) {
					break
				}
			}
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, err
			}
			call.Span = ast.Span{Start: name.Span.Start, End: p.previous().Span.End}
			return call, nil
		}
		if call.Star {
			if err := p.expect(lexer.RParen, ")"); err != nil {
				return nil, err
			}
		}
		call.Span = ast.Span{Start: name.Span.Start, End: p.previous().Span.End}
		return call, nil
	default:
		return nil, p.errorf("expected expression, got %q", t.Raw)
	}
}

// infixBinding 返回 Pratt 左/右 binding power 与规范化操作符。
func infixBinding(k lexer.Kind) (int, int, string, bool) {
	switch k {
	case lexer.KeywordOr:
		return 10, 11, "OR", true
	case lexer.KeywordAnd:
		return 20, 21, "AND", true
	case lexer.Eq:
		return 30, 31, "=", true
	case lexer.Neq:
		return 30, 31, "<>", true
	case lexer.Lt:
		return 30, 31, "<", true
	case lexer.Lte:
		return 30, 31, "<=", true
	case lexer.Gt:
		return 30, 31, ">", true
	case lexer.Gte:
		return 30, 31, ">=", true
	case lexer.Plus:
		return 40, 41, "+", true
	case lexer.Minus:
		return 40, 41, "-", true
	case lexer.Star:
		return 50, 51, "*", true
	case lexer.Slash:
		return 50, 51, "/", true
	case lexer.Percent:
		return 50, 51, "%", true
	default:
		return 0, 0, "", false
	}
}

// parseQualifiedName 解析由点分隔的限定名称。
func (p *Parser) parseQualifiedName() (ast.QualifiedName, error) {
	start := p.peek().Span.Start
	id, err := p.parseIdentifier()
	if err != nil {
		return ast.QualifiedName{}, err
	}
	parts := []ast.Identifier{id}
	for p.accept(lexer.Dot) {
		next, err := p.parseIdentifier()
		if err != nil {
			return ast.QualifiedName{}, err
		}
		parts = append(parts, next)
	}
	return ast.QualifiedName{Span: ast.Span{Start: start, End: parts[len(parts)-1].Span.End}, Parts: parts}, nil
}

// parseIdentifier 解析普通或双引号标识符。
func (p *Parser) parseIdentifier() (ast.Identifier, error) {
	t := p.peek()
	if t.Kind != lexer.Identifier && t.Kind != lexer.QuotedIdentifier {
		return ast.Identifier{}, p.errorf("expected identifier")
	}
	p.pos++
	return ast.Identifier{Span: t.Span, Raw: t.Raw, Name: t.Text, Quoted: t.Kind == lexer.QuotedIdentifier}, nil
}

// accept 在当前 token 类型匹配时消费 token。
func (p *Parser) accept(k lexer.Kind) bool {
	if p.peek().Kind == k {
		p.pos++
		return true
	}
	return false
}

// expect 要求并消费指定 token，否则返回带源位置的语法错误。
func (p *Parser) expect(k lexer.Kind, label string) error {
	if !p.accept(k) {
		return p.errorf("expected %s", label)
	}
	return nil
}

// peek 返回当前 token，不移动游标。
func (p *Parser) peek() lexer.Token {
	if p.pos >= len(p.tokens) {
		return p.tokens[len(p.tokens)-1]
	}
	return p.tokens[p.pos]
}

// previous 返回最近消费的 token。
func (p *Parser) previous() lexer.Token {
	if p.pos == 0 {
		return p.tokens[0]
	}
	return p.tokens[p.pos-1]
}

// errorf 构造带当前行列的 parser 错误。
func (p *Parser) errorf(format string, args ...any) error {
	t := p.peek()
	return fmt.Errorf("sql parser %d:%d: %s", t.Span.Start.Line, t.Span.Start.Column, fmt.Sprintf(format, args...))
}
