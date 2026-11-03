package parser

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

type Parser struct {
	tokens      []lexer.Token
	pos         int
	internalSeq uint64
	pendingCTEs []ast.CTE
}

func Parse(sql string) (ast.Statement, error) {
	tokens, err := lexer.All(sql)
	if err != nil { return nil, err }
	p := &Parser{tokens: tokens}
	stmt, ok, err := p.parseStatement()
	if err != nil { return nil, err }
	if !ok { return nil, p.errorf("expected Stable v1 statement") }
	p.accept(lexer.Semicolon)
	if p.peek().Kind != lexer.EOF { return nil, p.errorf("unexpected token %q", p.peek().Raw) }
	return stmt, nil
}

func (p *Parser) mark() int { return p.pos }
func (p *Parser) restore(mark int) { p.pos = mark }

func (p *Parser) parseSelectStatement() (*ast.SelectStmt, bool, error) {
	mark := p.mark()
	pendingBase := len(p.pendingCTEs)
	start := p.peek().Span.Start
	var with *ast.WithClause
	if p.accept(lexer.KeywordWith) {
		w, err := p.parseWithClause(start)
		if err != nil { return nil, false, err }
		with = w
	}
	if !p.accept(lexer.KeywordSelect) {
		p.restore(mark)
		return nil, false, nil
	}
	s := &ast.SelectStmt{With: with}
	s.Distinct = p.accept(lexer.KeywordDistinct)
	items, err := p.parseSelectList()
	if err != nil { return nil, false, err }
	s.Projection = items
	if p.accept(lexer.KeywordFrom) {
		from, err := p.parseTableExpr()
		if err != nil { return nil, false, err }
		s.From = from
	}
	if p.accept(lexer.KeywordWhere) {
		e, err := p.parseExpr(0); if err != nil { return nil, false, err }; s.Where = e
	}
	if p.accept(lexer.KeywordGroup) {
		if err := p.expect(lexer.KeywordBy, "BY"); err != nil { return nil, false, err }
		xs, err := p.parseExprList(); if err != nil { return nil, false, err }; s.GroupBy = xs
	}
	if p.accept(lexer.KeywordHaving) {
		e, err := p.parseExpr(0); if err != nil { return nil, false, err }; s.Having = e
	}
	if p.accept(lexer.KeywordOrder) {
		if err := p.expect(lexer.KeywordBy, "BY"); err != nil { return nil, false, err }
		terms, err := p.parseOrderTerms(); if err != nil { return nil, false, err }; s.OrderBy = terms
	}
	if p.accept(lexer.KeywordLimit) {
		e, err := p.parseExpr(0); if err != nil { return nil, false, err }; s.Limit = e
	}
	if p.accept(lexer.KeywordOffset) {
		e, err := p.parseExpr(0); if err != nil { return nil, false, err }; s.Offset = e
	}
	if len(p.pendingCTEs) > pendingBase {
		generated := append([]ast.CTE(nil), p.pendingCTEs[pendingBase:]...)
		p.pendingCTEs = p.pendingCTEs[:pendingBase]
		if s.With == nil { s.With = &ast.WithClause{} }
		s.With.CTEs = append(s.With.CTEs, generated...)
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	if s.With != nil { s.With.Span = ast.Span{Start: start, End: s.Span.End} }
	return s, true, nil
}

func (p *Parser) parseWithClause(start ast.Position) (*ast.WithClause, error) {
	w := &ast.WithClause{}
	for {
		name, err := p.parseIdentifier(); if err != nil { return nil, err }
		cte := ast.CTE{Name: name}; cteStart := name.Span.Start
		if p.accept(lexer.LParen) {
			for { id, err := p.parseIdentifier(); if err != nil { return nil, err }; cte.Columns = append(cte.Columns, id); if !p.accept(lexer.Comma) { break } }
			if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
		}
		if err := p.expect(lexer.KeywordAs, "AS"); err != nil { return nil, err }
		if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }
		q, ok, err := p.parseSelectStatement(); if err != nil { return nil, err }; if !ok { return nil, p.errorf("expected SELECT in CTE") }
		if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
		cte.Query = q; cte.Span = ast.Span{Start: cteStart, End: p.previous().Span.End}; w.CTEs = append(w.CTEs, cte)
		if !p.accept(lexer.Comma) { break }
	}
	w.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return w, nil
}

func (p *Parser) parseSelectList() ([]ast.SelectItem, error) {
	var out []ast.SelectItem
	for {
		start := p.peek().Span.Start; item := ast.SelectItem{}
		if p.accept(lexer.Star) { item.Star = true } else { e, err := p.parseExpr(0); if err != nil { return nil, err }; item.Expr = e }
		if p.accept(lexer.KeywordAs) { id, err := p.parseIdentifier(); if err != nil { return nil, err }; item.Alias = &id
		} else if p.peek().Kind == lexer.Identifier || p.peek().Kind == lexer.QuotedIdentifier { id, _ := p.parseIdentifier(); item.Alias = &id }
		item.Span = ast.Span{Start: start, End: p.previous().Span.End}; out = append(out, item)
		if !p.accept(lexer.Comma) { return out, nil }
	}
}

func (p *Parser) parseExprList() ([]ast.Expr, error) {
	var out []ast.Expr
	for { e, err := p.parseExpr(0); if err != nil { return nil, err }; out = append(out, e); if !p.accept(lexer.Comma) { return out, nil } }
}

func (p *Parser) parseOrderTerms() ([]ast.OrderTerm, error) {
	var out []ast.OrderTerm
	for {
		start := p.peek().Span.Start; e, err := p.parseExpr(0); if err != nil { return nil, err }; t := ast.OrderTerm{Expr: e}
		if p.accept(lexer.KeywordDesc) { t.Descending = true } else { p.accept(lexer.KeywordAsc) }
		if p.accept(lexer.KeywordNulls) { first := false; if p.accept(lexer.KeywordFirst) { first = true } else if !p.accept(lexer.KeywordLast) { return nil, p.errorf("expected FIRST or LAST") }; t.NullsFirst = &first }
		t.Span = ast.Span{Start: start, End: p.previous().Span.End}; out = append(out, t); if !p.accept(lexer.Comma) { return out, nil }
	}
}

func (p *Parser) parseTableExpr() (ast.TableExpr, error) {
	left, err := p.parseTablePrimary(); if err != nil { return nil, err }
	for {
		mark := p.mark(); jt := ast.JoinInner; explicit := false
		switch p.peek().Kind {
		case lexer.KeywordInner: explicit = true; p.pos++; jt = ast.JoinInner
		case lexer.KeywordLeft: explicit = true; p.pos++; jt = ast.JoinLeft; p.accept(lexer.KeywordOuter)
		case lexer.KeywordRight: explicit = true; p.pos++; jt = ast.JoinRight; p.accept(lexer.KeywordOuter)
		case lexer.KeywordFull: explicit = true; p.pos++; jt = ast.JoinFull; p.accept(lexer.KeywordOuter)
		case lexer.KeywordCross: explicit = true; p.pos++; jt = ast.JoinCross
		case lexer.KeywordSemi: explicit = true; p.pos++; jt = ast.JoinSemi
		case lexer.KeywordAnti: explicit = true; p.pos++; jt = ast.JoinAnti
		}
		if !p.accept(lexer.KeywordJoin) { if explicit { return nil, p.errorf("expected JOIN") }; p.restore(mark); return left, nil }
		right, err := p.parseTablePrimary(); if err != nil { return nil, err }
		j := &ast.JoinExpr{Left: left, Right: right, Type: jt}; j.Span.Start = left.NodeSpan().Start
		if jt != ast.JoinCross {
			if p.accept(lexer.KeywordOn) { e, err := p.parseExpr(0); if err != nil { return nil, err }; j.Condition = e
			} else if p.accept(lexer.KeywordUsing) {
				if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }
				for { id, err := p.parseIdentifier(); if err != nil { return nil, err }; j.Using = append(j.Using, id); if !p.accept(lexer.Comma) { break } }
				if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
			} else { return nil, p.errorf("JOIN requires ON or USING") }
		}
		j.Span.End = p.previous().Span.End; left = j
	}
}

func (p *Parser) parseTablePrimary() (ast.TableExpr, error) {
	start := p.peek().Span.Start
	var out ast.TableExpr
	if p.accept(lexer.LParen) {
		q, ok, err := p.parseSelectStatement(); if err != nil { return nil, err }; if !ok { return nil, p.errorf("derived table requires SELECT") }
		if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
		alias := p.parseOptionalAlias(); if alias == nil { return nil, p.errorf("derived table requires alias") }
		name := p.generatedName("derived")
		cte := ast.CTE{Name: name, Query: q, Span: ast.Span{Start: start, End: p.previous().Span.End}}
		p.pendingCTEs = append(p.pendingCTEs, cte)
		qn := ast.QualifiedName{Span: name.Span, Parts: []ast.Identifier{name}}
		out = &ast.TableName{Span: ast.Span{Start: start, End: p.previous().Span.End}, Name: qn, Alias: alias}
	} else {
		name, err := p.parseQualifiedName(); if err != nil { return nil, err }; start = name.Span.Start
		if p.accept(lexer.LParen) {
			call := &ast.CallExpr{Function: name}
			if !p.accept(lexer.RParen) {
				for { e, err := p.parseExpr(0); if err != nil { return nil, err }; call.Args = append(call.Args, e); if !p.accept(lexer.Comma) { break } }
				if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
			}
			call.Span = ast.Span{Start: start, End: p.previous().Span.End}; if err := foldStaticTableArguments(call); err != nil { return nil, err }
			t := &ast.TableFunction{Call: call}; t.Alias = p.parseOptionalAlias(); t.Span = ast.Span{Start: start, End: p.previous().Span.End}; out = t
		} else {
			t := &ast.TableName{Name: name}; t.Alias = p.parseOptionalAlias(); t.Span = ast.Span{Start: start, End: p.previous().Span.End}; out = t
		}
	}
	return p.parseSampleSuffix(out)
}

func (p *Parser) parseSampleSuffix(source ast.TableExpr) (ast.TableExpr, error) {
	if p.peek().Kind != lexer.KeywordUsing || !p.peekNWord(1, "sample") { return source, nil }
	p.pos += 2
	method := "reservoir"
	var amount uint64
	var percentBits uint64
	if p.peekWord("reservoir") || p.peekWord("bernoulli") {
		method = strings.ToLower(p.peek().Text); p.pos++
		if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }
		t := p.peek(); if t.Kind != lexer.Number { return nil, p.errorf("sample amount must be numeric") }; p.pos++
		if method == "reservoir" {
			if strings.Contains(t.Raw, ".") { return nil, p.errorf("reservoir sample requires integer rows") }
			v, err := strconv.ParseUint(t.Raw, 10, 64); if err != nil { return nil, p.errorf("invalid reservoir size") }; amount = v
			if !p.acceptWord("rows") { return nil, p.errorf("reservoir sample requires ROWS") }
		} else {
			v, err := strconv.ParseFloat(t.Raw, 64); if err != nil || v < 0 || v > 100 { return nil, p.errorf("bernoulli percentage must be in [0,100]") }; percentBits = math.Float64bits(v)
			if !p.acceptWord("percent") { return nil, p.errorf("bernoulli sample requires PERCENT") }
		}
		if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
	} else {
		t := p.peek(); if t.Kind != lexer.Number || strings.Contains(t.Raw, ".") { return nil, p.errorf("sample row count must be an integer") }; p.pos++
		v, err := strconv.ParseUint(t.Raw, 10, 64); if err != nil { return nil, p.errorf("invalid sample row count") }; amount = v
		if !p.acceptWord("rows") { return nil, p.errorf("fixed sample requires ROWS") }
	}
	hasSeed := false; seed := uint64(0)
	if p.acceptWord("repeatable") {
		hasSeed = true; if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }
		t := p.peek(); if t.Kind != lexer.Number || strings.Contains(t.Raw, ".") { return nil, p.errorf("REPEATABLE seed must be an integer") }; p.pos++
		v, err := strconv.ParseUint(t.Raw, 10, 64); if err != nil { return nil, p.errorf("invalid REPEATABLE seed") }; seed = v
		if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
	}
	code := "r"; value := amount
	if method == "bernoulli" { code = "b"; value = percentBits }
	seedCode := "n"; if hasSeed { seedCode = "s" }
	name := p.generatedName(fmt.Sprintf("sample_%s_%x_%s_%x", code, value, seedCode, seed))
	q := &ast.SelectStmt{Projection: []ast.SelectItem{{Star: true, Span: source.NodeSpan()}}, From: source, Span: source.NodeSpan()}
	p.pendingCTEs = append(p.pendingCTEs, ast.CTE{Name: name, Query: q, Span: source.NodeSpan()})
	qn := ast.QualifiedName{Span: name.Span, Parts: []ast.Identifier{name}}
	return &ast.TableName{Span: ast.Span{Start: source.NodeSpan().Start, End: p.previous().Span.End}, Name: qn}, nil
}

func (p *Parser) generatedName(kind string) ast.Identifier {
	p.internalSeq++
	name := fmt.Sprintf("__fssql_%s_%d", kind, p.internalSeq)
	span := p.previous().Span
	return ast.Identifier{Span: span, Raw: name, Name: name}
}

func (p *Parser) parseOptionalAlias() *ast.Identifier {
	if p.accept(lexer.KeywordAs) { id, err := p.parseIdentifier(); if err == nil { return &id }; return nil }
	if p.peek().Kind == lexer.Identifier || p.peek().Kind == lexer.QuotedIdentifier { id, _ := p.parseIdentifier(); return &id }
	return nil
}

func (p *Parser) parseExpr(minBP int) (ast.Expr, error) {
	left, err := p.parsePrefix(); if err != nil { return nil, err }
	for {
		if p.peek().Kind == lexer.KeywordIs {
			if 30 < minBP { return left, nil }
			start := left.NodeSpan().Start; p.pos++; not := p.accept(lexer.KeywordNot)
			if p.accept(lexer.KeywordDistinct) {
				if err := p.expect(lexer.KeywordFrom, "FROM"); err != nil { return nil, err }
				right, err := p.parseExpr(31); if err != nil { return nil, err }
				span := ast.Span{Start: start, End: right.NodeSpan().End}
				eq := &ast.BinaryExpr{Span: span, Left: left, Op: "=", Right: right}
				falseLit := &ast.LiteralExpr{Span: span, Kind: ast.LiteralBool, Value: "FALSE"}
				eqSafe := phase4Call("coalesce", span, eq, falseLit)
				bothNull := &ast.BinaryExpr{Span: span, Left: &ast.IsNullExpr{Span: span, Expr: left}, Op: "AND", Right: &ast.IsNullExpr{Span: span, Expr: right}}
				value := ast.Expr(&ast.BinaryExpr{Span: span, Left: eqSafe, Op: "OR", Right: bothNull})
				if !not { value = &ast.UnaryExpr{Span: span, Op: "NOT", Expr: value} }
				left = value; continue
			}
			if err := p.expect(lexer.KeywordNull, "NULL"); err != nil { return nil, err }
			left = &ast.IsNullExpr{Span: ast.Span{Start: start, End: p.previous().Span.End}, Expr: left, Not: not}; continue
		}
		lbp, rbp, op, ok := infixBinding(p.peek().Kind); if !ok || lbp < minBP { return left, nil }; p.pos++
		right, err := p.parseExpr(rbp); if err != nil { return nil, err }; span := ast.Span{Start: left.NodeSpan().Start, End: right.NodeSpan().End}
		if op == "||" { left = phase4Call("__fssql_concat", span, left, right) } else { left = &ast.BinaryExpr{Span: span, Left: left, Op: op, Right: right} }
	}
}

func (p *Parser) parsePrefix() (ast.Expr, error) {
	t := p.peek()
	switch t.Kind {
	case lexer.KeywordNot, lexer.Plus, lexer.Minus:
		p.pos++; e, err := p.parseExpr(70); if err != nil { return nil, err }; return &ast.UnaryExpr{Span: ast.Span{Start: t.Span.Start, End: e.NodeSpan().End}, Op: strings.ToUpper(t.Raw), Expr: e}, nil
	case lexer.KeywordNull:
		p.pos++; return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralNull, Value: "NULL"}, nil
	case lexer.KeywordTrue, lexer.KeywordFalse:
		p.pos++; return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralBool, Value: strings.ToUpper(t.Raw)}, nil
	case lexer.String:
		p.pos++; return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}, nil
	case lexer.Number:
		p.pos++; kind := ast.LiteralInteger; if strings.Contains(t.Raw, ".") { kind = ast.LiteralDecimal }; return &ast.LiteralExpr{Span: t.Span, Kind: kind, Value: t.Raw}, nil
	case lexer.LParen:
		start := t.Span.Start; p.pos++; if q, ok, err := p.parseSelectStatement(); err != nil { return nil, err } else if ok { if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }; return &ast.SubqueryExpr{Span: ast.Span{Start: start, End: p.previous().Span.End}, Query: q}, nil }
		e, err := p.parseExpr(0); if err != nil { return nil, err }; if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }; return e, nil
	case lexer.Identifier, lexer.QuotedIdentifier:
		if t.Kind == lexer.Identifier && strings.EqualFold(t.Text, "case") { return p.parsePhase4Case() }
		if t.Kind == lexer.Identifier && strings.EqualFold(t.Text, "cast") { return p.parsePhase4Cast() }
		name, err := p.parseQualifiedName(); if err != nil { return nil, err }
		if !p.accept(lexer.LParen) { return &ast.NameExpr{Span: name.Span, Name: name}, nil }
		call := &ast.CallExpr{Function: name}
		if p.accept(lexer.KeywordDistinct) { call.Distinct = true }
		closed := false
		if p.accept(lexer.Star) { call.Star = true
		} else if p.accept(lexer.RParen) { closed = true
		} else {
			for { e, err := p.parseExpr(0); if err != nil { return nil, err }; call.Args = append(call.Args, e); if !p.accept(lexer.Comma) { break } }
		}
		if !closed { if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err } }
		call.Span = ast.Span{Start: name.Span.Start, End: p.previous().Span.End}
		if p.acceptWord("over") { return p.parseWindowCall(call) }
		return call, nil
	default:
		return nil, p.errorf("expected expression, got %q", t.Raw)
	}
}

func (p *Parser) parseWindowCall(call *ast.CallExpr) (ast.Expr, error) {
	if call == nil || len(call.Function.Parts) != 1 { return nil, p.errorf("window function must be unqualified") }
	name := strings.ToLower(call.Function.Parts[0].Name)
	synthetic := ""
	switch name {
	case "row_number": if call.Star || len(call.Args) != 0 { return nil, p.errorf("ROW_NUMBER takes no arguments") }; synthetic = "__fssql_window_row_number"
	case "count": if !call.Star || len(call.Args) != 0 { return nil, p.errorf("phase 5 window supports COUNT(*) only") }; synthetic = "__fssql_window_count"
	case "sum": if call.Star || len(call.Args) != 1 { return nil, p.errorf("SUM window requires one argument") }; synthetic = "__fssql_window_sum"
	default: return nil, p.errorf("window function %s is not supported in phase 5", name)
	}
	if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }
	var partition []ast.Expr; var order []ast.OrderTerm
	if p.acceptWord("partition") { if err := p.expect(lexer.KeywordBy, "BY"); err != nil { return nil, err }; xs, err := p.parseWindowExprList(); if err != nil { return nil, err }; partition = xs }
	if p.accept(lexer.KeywordOrder) { if err := p.expect(lexer.KeywordBy, "BY"); err != nil { return nil, err }; xs, err := p.parseWindowOrderTerms(); if err != nil { return nil, err }; order = xs }
	frame := "none"
	if p.acceptWord("rows") {
		if name == "row_number" { return nil, p.errorf("ROW_NUMBER frame is not supported") }
		if !p.acceptWord("between") || !p.acceptWord("unbounded") || !p.acceptWord("preceding") || !p.accept(lexer.KeywordAnd) { return nil, p.errorf("only ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING is supported") }
		t := p.peek(); if t.Kind != lexer.Number || t.Raw != "1" { return nil, p.errorf("only 1 PRECEDING frame end is supported") }; p.pos++
		if !p.acceptWord("preceding") { return nil, p.errorf("expected PRECEDING") }; frame = "prefix1"
	} else if p.peekWord("range") || p.peekWord("groups") { return nil, p.errorf("only the phase 5 ROWS frame is supported") }
	if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }
	if (name == "count" || name == "sum") && frame == "none" && (len(partition) != 0 || len(order) != 0) { return nil, p.errorf("phase 5 %s window without explicit frame supports OVER () only", strings.ToUpper(name)) }
	desc := make([]byte, len(order)); nulls := make([]byte, len(order)); for i, term := range order { if term.Descending { desc[i] = '1' } else { desc[i] = '0' }; if term.NullsFirst != nil && *term.NullsFirst { nulls[i] = 'f' } else { nulls[i] = 'l' } }
	spec := fmt.Sprintf("p=%d;o=%d;d=%s;n=%s;f=%s", len(partition), len(order), string(desc), string(nulls), frame)
	span := ast.Span{Start: call.Span.Start, End: p.previous().Span.End}; args := []ast.Expr{&ast.LiteralExpr{Span: span, Kind: ast.LiteralString, Value: spec}}
	if name == "sum" { args = append(args, call.Args[0]) }
	args = append(args, partition...); for _, term := range order { args = append(args, term.Expr) }
	return phase4Call(synthetic, span, args...), nil
}

func (p *Parser) parseWindowExprList() ([]ast.Expr, error) {
	var out []ast.Expr
	for { e, err := p.parseExpr(0); if err != nil { return nil, err }; out = append(out, e); if !p.accept(lexer.Comma) { return out, nil } }
}

func (p *Parser) parseWindowOrderTerms() ([]ast.OrderTerm, error) {
	var out []ast.OrderTerm
	for {
		start := p.peek().Span.Start; e, err := p.parseExpr(0); if err != nil { return nil, err }; t := ast.OrderTerm{Expr: e}
		if p.accept(lexer.KeywordDesc) { t.Descending = true } else { p.accept(lexer.KeywordAsc) }
		if p.accept(lexer.KeywordNulls) { first := false; if p.accept(lexer.KeywordFirst) { first = true } else if !p.accept(lexer.KeywordLast) { return nil, p.errorf("expected FIRST or LAST") }; t.NullsFirst = &first }
		t.Span = ast.Span{Start: start, End: p.previous().Span.End}; out = append(out, t)
		if !p.accept(lexer.Comma) { return out, nil }
	}
}

func (p *Parser) parsePhase4Cast() (ast.Expr, error) {
	start := p.peek().Span.Start; p.pos++; if err := p.expect(lexer.LParen, "("); err != nil { return nil, err }; value, err := p.parseExpr(0); if err != nil { return nil, err }; if err := p.expect(lexer.KeywordAs, "AS"); err != nil { return nil, err }; castName, err := p.parsePhase4CastType(); if err != nil { return nil, err }; if err := p.expect(lexer.RParen, ")"); err != nil { return nil, err }; return phase4Call("__fssql_cast_"+castName, ast.Span{Start: start, End: p.previous().Span.End}, value), nil
}

func (p *Parser) parsePhase4CastType() (string, error) {
	id, err := p.parseIdentifier(); if err != nil { return "", err }; name := strings.ToLower(id.Name)
	switch name {
	case "boolean", "tinyint", "smallint", "integer", "bigint", "utinyint", "usmallint", "uinteger", "ubigint", "float", "double", "varchar", "blob", "date", "timestamp", "timestamp_ns", "timestamptz": return name, nil
	case "decimal", "numeric":
		if err := p.expect(lexer.LParen, "("); err != nil { return "", err }; wTok := p.peek(); if wTok.Kind != lexer.Number || strings.Contains(wTok.Raw, ".") { return "", p.errorf("expected DECIMAL precision") }; p.pos++; w, err := strconv.Atoi(wTok.Raw); if err != nil || w < 1 || w > 38 { return "", p.errorf("invalid DECIMAL precision") }; scale := 0
		if p.accept(lexer.Comma) { sTok := p.peek(); if sTok.Kind != lexer.Number || strings.Contains(sTok.Raw, ".") { return "", p.errorf("expected DECIMAL scale") }; p.pos++; scale, err = strconv.Atoi(sTok.Raw); if err != nil || scale < 0 || scale > w { return "", p.errorf("invalid DECIMAL scale") } }
		if err := p.expect(lexer.RParen, ")"); err != nil { return "", err }; return fmt.Sprintf("decimal_%d_%d", w, scale), nil
	default: return "", p.errorf("unsupported CAST type %s", id.Name)
	}
}

func (p *Parser) parsePhase4Case() (ast.Expr, error) {
	start := p.peek().Span.Start; p.pos++; var base ast.Expr
	if !p.peekWord("when") { var err error; base, err = p.parseExpr(0); if err != nil { return nil, err } }
	type arm struct{ cond, value ast.Expr }; var arms []arm
	for p.peekWord("when") { p.pos++; cond, err := p.parseExpr(0); if err != nil { return nil, err }; if !p.acceptWord("then") { return nil, p.errorf("expected THEN") }; value, err := p.parseExpr(0); if err != nil { return nil, err }; if base != nil { cond = &ast.BinaryExpr{Span: ast.Span{Start: base.NodeSpan().Start, End: cond.NodeSpan().End}, Left: base, Op: "=", Right: cond} }; arms = append(arms, arm{cond: cond, value: value}) }
	if len(arms) == 0 { return nil, p.errorf("CASE requires WHEN") }
	var otherwise ast.Expr; if p.acceptWord("else") { var err error; otherwise, err = p.parseExpr(0); if err != nil { return nil, err } } else { otherwise = &ast.LiteralExpr{Span: p.peek().Span, Kind: ast.LiteralNull, Value: "NULL"} }
	if !p.acceptWord("end") { return nil, p.errorf("expected END") }; end := p.previous().Span.End
	for i := len(arms)-1; i >= 0; i-- { otherwise = phase4Call("__fssql_case_when", ast.Span{Start: start, End: end}, arms[i].cond, arms[i].value, otherwise) }
	return otherwise, nil
}

func phase4Call(name string, span ast.Span, args ...ast.Expr) *ast.CallExpr { id := ast.Identifier{Span: span, Raw: name, Name: name}; return &ast.CallExpr{Span: span, Function: ast.QualifiedName{Span: span, Parts: []ast.Identifier{id}}, Args: args} }
func (p *Parser) peekWord(word string) bool { t := p.peek(); return t.Kind == lexer.Identifier && strings.EqualFold(t.Text, word) }
func (p *Parser) peekNWord(n int, word string) bool { if p.pos+n >= len(p.tokens) { return false }; t := p.tokens[p.pos+n]; return t.Kind == lexer.Identifier && strings.EqualFold(t.Text, word) }
func (p *Parser) acceptWord(word string) bool { if p.peekWord(word) { p.pos++; return true }; return false }
func (p *Parser) acceptPhase4Word(word string) bool { return p.acceptWord(word) }

func infixBinding(k lexer.Kind) (int, int, string, bool) {
	switch k {
	case lexer.KeywordOr: return 10, 11, "OR", true
	case lexer.KeywordAnd: return 20, 21, "AND", true
	case lexer.Eq: return 30, 31, "=", true
	case lexer.Neq: return 30, 31, "<>", true
	case lexer.Lt: return 30, 31, "<", true
	case lexer.Lte: return 30, 31, "<=", true
	case lexer.Gt: return 30, 31, ">", true
	case lexer.Gte: return 30, 31, ">=", true
	case lexer.Concat: return 35, 36, "||", true
	case lexer.Plus: return 40, 41, "+", true
	case lexer.Minus: return 40, 41, "-", true
	case lexer.Star: return 50, 51, "*", true
	case lexer.Slash: return 50, 51, "/", true
	case lexer.Percent: return 50, 51, "%", true
	default: return 0, 0, "", false
	}
}

func (p *Parser) parseQualifiedName() (ast.QualifiedName, error) { start := p.peek().Span.Start; id, err := p.parseIdentifier(); if err != nil { return ast.QualifiedName{}, err }; parts := []ast.Identifier{id}; for p.accept(lexer.Dot) { next, err := p.parseIdentifier(); if err != nil { return ast.QualifiedName{}, err }; parts = append(parts, next) }; return ast.QualifiedName{Span: ast.Span{Start: start, End: parts[len(parts)-1].Span.End}, Parts: parts}, nil }
func (p *Parser) parseIdentifier() (ast.Identifier, error) { t := p.peek(); if t.Kind != lexer.Identifier && t.Kind != lexer.QuotedIdentifier { return ast.Identifier{}, p.errorf("expected identifier") }; p.pos++; return ast.Identifier{Span: t.Span, Raw: t.Raw, Name: t.Text, Quoted: t.Kind == lexer.QuotedIdentifier}, nil }
func (p *Parser) accept(k lexer.Kind) bool { if p.peek().Kind == k { p.pos++; return true }; return false }
func (p *Parser) expect(k lexer.Kind, label string) error { if !p.accept(k) { return p.errorf("expected %s", label) }; return nil }
func (p *Parser) peek() lexer.Token { if p.pos >= len(p.tokens) { return p.tokens[len(p.tokens)-1] }; return p.tokens[p.pos] }
func (p *Parser) previous() lexer.Token { if p.pos == 0 { return p.tokens[0] }; return p.tokens[p.pos-1] }
func (p *Parser) errorf(format string, args ...any) error { t := p.peek(); return fmt.Errorf("sql parser %d:%d: %s", t.Span.Start.Line, t.Span.Start.Column, fmt.Sprintf(format, args...)) }
