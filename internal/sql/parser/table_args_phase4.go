package parser

import (
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/tablefunc"
)

// foldStaticTableArguments is intentionally limited to read_csv. Other table
// functions retain their frozen single-source arity and reject extra options.
func foldStaticTableArguments(call *ast.CallExpr) error {
	if call==nil||len(call.Function.Parts)!=1||!strings.EqualFold(call.Function.Parts[0].Name,"read_csv")||len(call.Args)<=1{return nil}
	first,ok:=call.Args[0].(*ast.LiteralExpr);if !ok||first.Kind!=ast.LiteralString{return fmt.Errorf("read_csv source must be a string literal before named options")}
	opts:=make(map[string]string,len(call.Args)-1)
	for _,raw:=range call.Args[1:]{bin,ok:=raw.(*ast.BinaryExpr);if !ok||bin.Op!="="{return fmt.Errorf("read_csv options must use name=value")};name,ok:=bin.Left.(*ast.NameExpr);if !ok||len(name.Name.Parts)!=1{return fmt.Errorf("read_csv option name must be unqualified")};key:=strings.ToLower(name.Name.Parts[0].Name);if _,exists:=opts[key];exists{return fmt.Errorf("read_csv option %s is repeated",key)};lit,ok:=bin.Right.(*ast.LiteralExpr);if !ok{return fmt.Errorf("read_csv option %s must be a literal",key)};switch lit.Kind{case ast.LiteralString,ast.LiteralInteger,ast.LiteralDecimal:opts[key]=lit.Value;case ast.LiteralBool:opts[key]=strings.ToLower(lit.Value);default:return fmt.Errorf("read_csv option %s cannot be NULL",key)}}
	token:=tablefunc.Encode(first.Value,opts);call.Args=[]ast.Expr{&ast.LiteralExpr{Span:call.Span,Kind:ast.LiteralString,Value:token}};return nil
}
