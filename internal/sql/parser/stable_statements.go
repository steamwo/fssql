package parser

import (
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

// parseStatement 按冻结 grammar 做 ordered choice；Pratt 仍只负责 expression。
func (p *Parser) parseStatement() (ast.Statement, bool, error) {
	if s, ok, err := p.parseSelectStatement(); ok || err != nil {
		return s, ok, err
	}
	if s, ok, err := p.parseCreateStatement(); ok || err != nil {
		return s, ok, err
	}
	if s, ok, err := p.parseCopyStatement(); ok || err != nil {
		return s, ok, err
	}
	if s, ok, err := p.parseDescribeStatement(); ok || err != nil {
		return s, ok, err
	}
	return nil, false, nil
}

// parseCreateStatement 解析 AD-045 冻结的 CREATE VIEW / EXTERNAL TABLE / SECRET。
func (p *Parser) parseCreateStatement() (ast.Statement, bool, error) {
	mark := p.mark()
	start := p.peek().Span.Start
	if !p.acceptWord("create") {
		return nil, false, nil
	}
	if p.acceptWord("view") {
		s, err := p.parseCreateView(start)
		return s, true, err
	}
	if p.acceptWord("external") {
		if !p.acceptWord("table") {
			return nil, true, p.errorf("expected TABLE")
		}
		s, err := p.parseCreateExternalTable(start)
		return s, true, err
	}
	if p.acceptWord("secret") {
		s, err := p.parseCreateSecret(start)
		return s, true, err
	}
	p.restore(mark)
	return nil, false, nil
}

// parseCreateView 解析 CREATE VIEW <qualified_name> AS <select_statement>。
func (p *Parser) parseCreateView(start ast.Position) (*ast.CreateViewStmt, error) {
	name, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.KeywordAs, "AS"); err != nil {
		return nil, err
	}
	q, ok, err := p.parseSelectStatement()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, p.errorf("expected SELECT after AS")
	}
	return &ast.CreateViewStmt{Span: ast.Span{Start: start, End: q.Span.End}, Name: name, Query: q}, nil
}

// parseCreateExternalTable 解析项目特有 USING <stable_table_function>('source') 语法；函数合法性留给 Binder。
func (p *Parser) parseCreateExternalTable(start ast.Position) (*ast.CreateExternalTableStmt, error) {
	name, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if !p.accept(lexer.KeywordUsing) {
		return nil, p.errorf("expected USING")
	}
	fn, err := p.parseQualifiedName()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.LParen, "("); err != nil {
		return nil, err
	}
	t := p.peek()
	if t.Kind != lexer.String {
		return nil, p.errorf("external table source must be a string literal")
	}
	p.pos++
	source := &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}
	if err := p.expect(lexer.RParen, ")"); err != nil {
		return nil, err
	}
	return &ast.CreateExternalTableStmt{Span: ast.Span{Start: start, End: p.previous().Span.End}, Name: name, Function: fn, Source: source}, nil
}

// parseCreateSecret 解析受限 option list；必需/重复/ACL 规则在 Binder 验证。
func (p *Parser) parseCreateSecret(start ast.Position) (*ast.CreateSecretStmt, error) {
	name, err := p.parseIdentifier()
	if err != nil {
		return nil, err
	}
	if err := p.expect(lexer.LParen, "("); err != nil {
		return nil, err
	}
	s := &ast.CreateSecretStmt{Name: name}
	if p.accept(lexer.RParen) {
		return nil, p.errorf("CREATE SECRET requires options")
	}
	for {
		o, err := p.parseSecretOption()
		if err != nil {
			return nil, err
		}
		s.Options = append(s.Options, o)
		if !p.accept(lexer.Comma) {
			break
		}
	}
	if err := p.expect(lexer.RParen, ")"); err != nil {
		return nil, err
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return s, nil
}

// parseSecretOption 将 TYPE/ACL 字段与普通 credential key 分开表示，避免 parser 处理安全策略。
func (p *Parser) parseSecretOption() (ast.SecretOption, error) {
	start := p.peek().Span.Start
	name, err := p.parseIdentifier()
	if err != nil {
		return ast.SecretOption{}, err
	}
	key := strings.ToLower(name.Name)
	o := ast.SecretOption{Name: name, Kind: ast.SecretCredential}
	switch key {
	case "type":
		o.Kind = ast.SecretType
		v, err := p.parseIdentifier()
		if err != nil {
			return ast.SecretOption{}, p.errorf("TYPE requires identifier")
		}
		o.IdentifierValue = &v
	case "subject", "scheme", "host":
		t := p.peek()
		if t.Kind != lexer.String {
			return ast.SecretOption{}, p.errorf("%s requires string literal", strings.ToUpper(key))
		}
		p.pos++
		lit := &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}
		o.StringValue = lit
		if key == "subject" {
			o.Kind = ast.SecretSubject
		} else if key == "scheme" {
			o.Kind = ast.SecretScheme
		} else {
			o.Kind = ast.SecretHost
		}
	default:
		t := p.peek()
		if t.Kind != lexer.String {
			return ast.SecretOption{}, p.errorf("credential %s requires string literal", name.Name)
		}
		p.pos++
		o.StringValue = &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}
	}
	o.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return o, nil
}

// parseCopyStatement 解析 COPY relation/query TO destination 及通用 scalar option syntax。
func (p *Parser) parseCopyStatement() (ast.Statement, bool, error) {
	mark := p.mark()
	start := p.peek().Span.Start
	if !p.acceptWord("copy") {
		return nil, false, nil
	}
	s := &ast.CopyStmt{}
	if p.accept(lexer.LParen) {
		q, ok, err := p.parseSelectStatement()
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, true, p.errorf("COPY query source requires SELECT")
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, true, err
		}
		s.Query = q
	} else {
		name, err := p.parseQualifiedName()
		if err != nil {
			p.restore(mark)
			return nil, false, nil
		}
		s.Relation = &name
	}
	if !p.acceptWord("to") {
		return nil, true, p.errorf("COPY supports TO only")
	}
	t := p.peek()
	if t.Kind != lexer.String {
		return nil, true, p.errorf("COPY destination must be a string literal")
	}
	p.pos++
	s.Destination = &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}
	if p.accept(lexer.LParen) {
		if p.accept(lexer.RParen) {
			return nil, true, p.errorf("COPY option list must not be empty")
		}
		for {
			o, err := p.parseCopyOption()
			if err != nil {
				return nil, true, err
			}
			s.Options = append(s.Options, o)
			if !p.accept(lexer.Comma) {
				break
			}
		}
		if err := p.expect(lexer.RParen, ")"); err != nil {
			return nil, true, err
		}
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return s, true, nil
}

// parseCopyOption 只解析 name + optional scalar value；具体 option 名称/类型由 bounded Binder registry 决定。
func (p *Parser) parseCopyOption() (ast.CopyOption, error) {
	start := p.peek().Span.Start
	name, err := p.parseIdentifier()
	if err != nil {
		return ast.CopyOption{}, err
	}
	o := ast.CopyOption{Name: name}
	if p.peek().Kind != lexer.Comma && p.peek().Kind != lexer.RParen {
		v, err := p.parseCopyScalar()
		if err != nil {
			return ast.CopyOption{}, err
		}
		o.Value = v
	}
	o.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return o, nil
}

// parseCopyScalar 拒绝复合表达式，防止 option grammar 意外扩张为任意 SQL expression。
func (p *Parser) parseCopyScalar() (ast.Expr, error) {
	t := p.peek()
	switch t.Kind {
	case lexer.String:
		p.pos++
		return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}, nil
	case lexer.Number:
		p.pos++
		k := ast.LiteralInteger
		if strings.Contains(t.Raw, ".") {
			k = ast.LiteralDecimal
		}
		return &ast.LiteralExpr{Span: t.Span, Kind: k, Value: t.Raw}, nil
	case lexer.KeywordTrue, lexer.KeywordFalse:
		p.pos++
		return &ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralBool, Value: strings.ToUpper(t.Raw)}, nil
	case lexer.KeywordOn:
		// ON 是 JOIN keyword，但 DuckDB COPY boolean option 允许 HEADER ON；只在此局部把它解释为 option scalar。
		p.pos++
		id := ast.Identifier{Span: t.Span, Raw: t.Raw, Name: "on"}
		name := ast.QualifiedName{Span: t.Span, Parts: []ast.Identifier{id}}
		return &ast.NameExpr{Span: t.Span, Name: name}, nil
	case lexer.Identifier, lexer.QuotedIdentifier:
		name, err := p.parseQualifiedName()
		if err != nil {
			return nil, err
		}
		return &ast.NameExpr{Span: name.Span, Name: name}, nil
	default:
		return nil, p.errorf("COPY option value must be scalar")
	}
}

// parseDescribeStatement 解析 DESCRIBE relation 或 DESCRIBE SELECT/WITH query。
func (p *Parser) parseDescribeStatement() (ast.Statement, bool, error) {
	start := p.peek().Span.Start
	if !p.acceptWord("describe") {
		return nil, false, nil
	}
	s := &ast.DescribeStmt{}
	if p.peek().Kind == lexer.KeywordSelect || p.peek().Kind == lexer.KeywordWith {
		q, ok, err := p.parseSelectStatement()
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, true, p.errorf("expected query")
		}
		s.Query = q
	} else {
		name, err := p.parseQualifiedName()
		if err != nil {
			return nil, true, err
		}
		s.Relation = &name
	}
	s.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return s, true, nil
}

// acceptWord 消费 unquoted identifier 形式的非保留 statement keyword，避免 TYPE/VIEW 等破坏普通列名兼容性。
func (p *Parser) acceptWord(word string) bool {
	t := p.peek()
	if t.Kind == lexer.Identifier && strings.EqualFold(t.Text, word) {
		p.pos++
		return true
	}
	return false
}
