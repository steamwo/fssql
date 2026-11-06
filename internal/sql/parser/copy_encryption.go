package parser

import (
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
)

// Only struct fields with scalar values or a single from_base64 string call
// are accepted here. This does not open COPY options to runtime expressions.
func (p *Parser) parseCopyEncryption() (*ast.CopyEncryptionExpr, error) {
	start := p.peek().Span.Start
	if err := p.expect(lexer.LBrace, "{"); err != nil {
		return nil, err
	}
	out := &ast.CopyEncryptionExpr{}
	if !p.accept(lexer.RBrace) {
		for {
			name, err := p.parseIdentifier()
			if err != nil {
				return nil, err
			}
			if err := p.expect(lexer.Colon, ":"); err != nil {
				return nil, err
			}
			var value ast.Expr
			if p.peekWord("from_base64") {
				fn, err := p.parseQualifiedName()
				if err != nil {
					return nil, err
				}
				if err := p.expect(lexer.LParen, "("); err != nil {
					return nil, err
				}
				t := p.peek()
				if err := p.expect(lexer.String, "string literal"); err != nil {
					return nil, err
				}
				if err := p.expect(lexer.RParen, ")"); err != nil {
					return nil, err
				}
				value = &ast.CallExpr{Span: ast.Span{Start: fn.Span.Start, End: p.previous().Span.End}, Function: fn, Args: []ast.Expr{&ast.LiteralExpr{Span: t.Span, Kind: ast.LiteralString, Value: t.Text}}}
			} else {
				value, err = p.parseCopyScalar()
				if err != nil {
					return nil, err
				}
			}
			out.Fields = append(out.Fields, ast.CopyOption{Name: name, Value: value})
			if !p.accept(lexer.Comma) {
				break
			}
		}
		if err := p.expect(lexer.RBrace, "}"); err != nil {
			return nil, err
		}
	}
	out.Span = ast.Span{Start: start, End: p.previous().Span.End}
	return out, nil
}
