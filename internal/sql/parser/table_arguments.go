package parser

import (
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/lexer"
	"github.com/steamwo/fssql/internal/tablefunc"
)

// foldStaticTableArguments normalizes compatibility aliases before binding.
func foldStaticTableArguments(call *ast.CallExpr) error {
	if call == nil || len(call.Function.Parts) != 1 {
		return nil
	}
	name := strings.ToLower(call.Function.Parts[0].Name)
	if name == "read_csv_auto" {
		name = "read_csv"
		call.Function.Parts[0].Name = name
	}
	jsonAuto := name == "read_json_auto"
	if name != "read_csv" && !jsonAuto {
		return nil
	}
	if len(call.Args) == 0 {
		return fmt.Errorf("%s requires a source", name)
	}
	first, ok := call.Args[0].(*ast.LiteralExpr)
	if !ok || first.Kind != ast.LiteralString {
		return fmt.Errorf("%s source must be a string literal before named options", name)
	}
	opts := make(map[string]string, len(call.Args)-1)
	for _, raw := range call.Args[1:] {
		bin, ok := raw.(*ast.BinaryExpr)
		if !ok || bin.Op != "=" {
			return fmt.Errorf("read_csv options must use name=value")
		}
		name, ok := bin.Left.(*ast.NameExpr)
		if !ok || len(name.Name.Parts) != 1 {
			return fmt.Errorf("read_csv option name must be unqualified")
		}
		key := strings.ToLower(name.Name.Parts[0].Name)
		if _, exists := opts[key]; exists {
			return fmt.Errorf("read_csv option %s is repeated", key)
		}
		lit, ok := bin.Right.(*ast.LiteralExpr)
		if !ok {
			return fmt.Errorf("read_csv option %s must be a literal", key)
		}
		switch lit.Kind {
		case ast.LiteralString, ast.LiteralInteger, ast.LiteralDecimal:
			opts[key] = lit.Value
		case ast.LiteralBool:
			opts[key] = strings.ToLower(lit.Value)
		default:
			return fmt.Errorf("read_csv option %s cannot be NULL", key)
		}
	}
	if jsonAuto {
		format, has := opts["format"]
		delete(opts, "format")
		if len(opts) != 0 {
			return fmt.Errorf("read_json_auto: unsupported option")
		}
		name = "read_json"
		if has {
			if format != "newline_delimited" {
				return fmt.Errorf("read_json_auto: unsupported format %q", format)
			}
			name = "read_ndjson"
		}
		call.Function.Parts[0].Name = name
		call.Args = []ast.Expr{first}
		return nil
	}
	// SQL CSV 默认将空字段（包括引号空字段）识别为 NULL
	if _, specified := opts["allow_quoted_nulls"]; !specified {
		opts["allow_quoted_nulls"] = "true"
	}
	token := tablefunc.Encode(first.Value, opts)
	call.Args = []ast.Expr{&ast.LiteralExpr{Span: call.Span, Kind: ast.LiteralString, Value: token}}
	return nil
}

// parseTableArgument accepts columns={'name':'TYPE'} without introducing
// general struct expressions into scalar SQL. Both forms use the CSV schema parser.
func (p *Parser) parseTableArgument() (ast.Expr, error) {
	if p.pos+2 >= len(p.tokens) || p.peek().Kind != lexer.Identifier || !strings.EqualFold(p.peek().Text, "columns") || p.tokens[p.pos+1].Kind != lexer.Eq || p.tokens[p.pos+2].Kind != lexer.LBrace {
		return p.parseExpr(0)
	}
	start := p.peek().Span.Start
	name := p.peek()
	p.pos += 3
	var fields []string
	for {
		key := p.peek()
		if key.Kind != lexer.String && key.Kind != lexer.Identifier && key.Kind != lexer.QuotedIdentifier {
			return nil, p.errorf("columns requires a field name")
		}
		p.pos++
		if err := p.expect(lexer.Colon, ":"); err != nil {
			return nil, err
		}
		value := p.peek()
		if value.Kind != lexer.String {
			return nil, p.errorf("columns type must be a string")
		}
		p.pos++
		fields = append(fields, "\""+strings.ReplaceAll(key.Text, "\"", "\"\"")+"\" "+value.Text)
		if !p.accept(lexer.Comma) {
			break
		}
	}
	if err := p.expect(lexer.RBrace, "}"); err != nil {
		return nil, err
	}
	span := ast.Span{Start: start, End: p.previous().Span.End}
	id := ast.Identifier{Span: name.Span, Name: "columns", Raw: name.Raw}
	return &ast.BinaryExpr{Span: span, Op: "=", Left: &ast.NameExpr{Span: name.Span, Name: ast.QualifiedName{Parts: []ast.Identifier{id}}}, Right: &ast.LiteralExpr{Span: span, Kind: ast.LiteralString, Value: strings.Join(fields, ",")}}, nil
}
