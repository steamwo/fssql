package parser

import (
	"github.com/steamwo/fssql/internal/sql/ast"
	"strings"
)

func staticString(e ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.ParameterExpr:
		if x.Value != nil {
			return staticString(x.Value)
		}
	case *ast.LiteralExpr:
		return x.Value, x.Kind == ast.LiteralString
	case *ast.BinaryExpr:
		if x.Op == "||" {
			a, ok := staticString(x.Left)
			if !ok {
				return "", false
			}
			b, ok := staticString(x.Right)
			return a + b, ok
		}
	case *ast.CallExpr:
		if len(x.Function.Parts) != 1 {
			return "", false
		}
		args := []string{}
		for _, a := range x.Args {
			v, ok := staticString(a)
			if !ok {
				return "", false
			}
			args = append(args, v)
		}
		switch strings.ToLower(x.Function.Parts[0].Name) {
		case "lower":
			if len(args) == 1 {
				return strings.ToLower(args[0]), true
			}
		case "upper":
			if len(args) == 1 {
				return strings.ToUpper(args[0]), true
			}
		case "__fssql_concat", "concat":
			if len(args) > 0 {
				return strings.Join(args, ""), true
			}
		case "replace":
			if len(args) == 3 {
				return strings.ReplaceAll(args[0], args[1], args[2]), true
			}
		}
	}
	return "", false
}
