package parser

import (
	"fmt"
"encoding/json"
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
	jsonReader:=jsonAuto||name=="read_json"||name=="read_ndjson"
	if name != "read_csv" && !jsonReader {
		return nil
	}
	if len(call.Args) == 0 {
		return fmt.Errorf("%s requires a source", name)
	}
 if value,ok:=staticString(call.Args[0]);ok{call.Args[0]=&ast.LiteralExpr{Span:call.Args[0].NodeSpan(),Kind:ast.LiteralString,Value:value}}
	first, ok := call.Args[0].(*ast.LiteralExpr)
	if !ok || first.Kind != ast.LiteralString {
		return nil // Column-dependent file paths remain a binder error, matching DuckDB.
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
  if key=="auto_type_candidates" {if list,ok:=bin.Right.(*ast.CallExpr);ok&&len(list.Function.Parts)==1&&list.Function.Parts[0].Name=="list_value"{names:=[]string{};for _,arg:=range list.Args{lit,ok:=unwrapParameter(arg).(*ast.LiteralExpr);if !ok||lit.Kind!=ast.LiteralString{return fmt.Errorf("auto_type_candidates requires constant type names")};names=append(names,lit.Value)};data,_:=json.Marshal(names);opts[key]=string(data);continue}}
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
		name = "read_json"
		if has {
			if format != "newline_delimited" {
				return fmt.Errorf("read_json_auto: unsupported format %q", format)
			}
			name = "read_ndjson"
		}
		call.Function.Parts[0].Name = name
	}
	if jsonReader {
		for key:=range opts { if key!="compression" { return fmt.Errorf("%s: unsupported option %s",name,key) } }
		if len(opts)==0 {call.Args=[]ast.Expr{first};return nil}
		token:=tablefunc.Encode(first.Value,opts)
		call.Args=[]ast.Expr{&ast.LiteralExpr{Span:call.Span,Kind:ast.LiteralString,Value:token}}
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
