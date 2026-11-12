package parser

import (
	"testing"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func TestTemplateBindDoesNotMutateParsedSyntax(t *testing.T) {
	template, err := Compile("WITH c AS (SELECT ? AS value) SELECT value FROM c WHERE value > ?")
	if err != nil {
		t.Fatal(err)
	}
	if template.NumInput() != 2 {
		t.Fatalf("inputs=%d", template.NumInput())
	}
	literal := func(value string) ast.Expr { return &ast.LiteralExpr{Kind: ast.LiteralInteger, Value: value} }
	first, err := template.Bind([]ast.Expr{literal("1"), literal("0")}, []string{"", ""})
	if err != nil {
		t.Fatal(err)
	}
	second, err := template.Bind([]ast.Expr{literal("2"), literal("1")}, []string{"", ""})
	if err != nil {
		t.Fatal(err)
	}
	parameter := func(stmt ast.Statement) *ast.ParameterExpr {
		return stmt.(*ast.SelectStmt).With.CTEs[0].Query.Projection[0].Expr.(*ast.ParameterExpr)
	}
	if parameter(first).Value.(*ast.LiteralExpr).Value != "1" || parameter(second).Value.(*ast.LiteralExpr).Value != "2" || parameter(template.statement).Value != nil {
		t.Fatal("binding mutated an earlier execution or the template")
	}
}

func TestTemplatePlaceholderValidation(t *testing.T) {
	for _, sql := range []string{"SELECT $", "SELECT $0", "SELECT $2", "SELECT ?, $1", "SELECT $name, $1", "SELECT ? FROM ?"} {
		if _, err := Compile(sql); err == nil {
			t.Errorf("accepted %s", sql)
		}
	}
	if _, err := Parse("SELECT ?"); err == nil {
		t.Fatal("native parse accepted an unbound parameter")
	}
	template, err := Compile("SELECT '$ignored ?', $name, $name -- ?\n")
	if err != nil || template.NumInput() != 1 {
		t.Fatalf("named placeholders: %v", err)
	}
}
