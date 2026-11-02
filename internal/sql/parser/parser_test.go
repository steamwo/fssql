package parser

import (
	"testing"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func mustSelect(t *testing.T, sql string) *ast.SelectStmt {
	t.Helper()
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	s, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("%T", stmt)
	}
	return s
}

func TestParseSelectStableClauses(t *testing.T) {
	s := mustSelect(t, "SELECT DISTINCT a + 2 * b AS x, count(*) n FROM schema.t AS t WHERE a >= 1 AND b IS NOT NULL GROUP BY a, b HAVING count(*) > 2 ORDER BY x DESC NULLS LAST LIMIT 10 OFFSET 3;")
	if !s.Distinct || len(s.Projection) != 2 || s.From == nil || s.Where == nil || len(s.GroupBy) != 2 || s.Having == nil || len(s.OrderBy) != 1 || s.Limit == nil || s.Offset == nil {
		t.Fatalf("unexpected AST: %+v", s)
	}
	bin, ok := s.Projection[0].Expr.(*ast.BinaryExpr)
	if !ok || bin.Op != "+" {
		t.Fatalf("projection=%T %+v", s.Projection[0].Expr, s.Projection[0].Expr)
	}
	right, ok := bin.Right.(*ast.BinaryExpr)
	if !ok || right.Op != "*" {
		t.Fatalf("precedence right=%T %+v", bin.Right, bin.Right)
	}
}

func TestParseNonRecursiveCTEAndJoins(t *testing.T) {
	s := mustSelect(t, "WITH x(v) AS (SELECT a FROM t WHERE a > 0) SELECT x.v FROM x LEFT JOIN read_parquet('a.parquet') p ON x.v = p.v")
	if s.With == nil || len(s.With.CTEs) != 1 {
		t.Fatalf("with=%+v", s.With)
	}
	j, ok := s.From.(*ast.JoinExpr)
	if !ok || j.Type != ast.JoinLeft || j.Condition == nil {
		t.Fatalf("from=%T %+v", s.From, s.From)
	}
	if _, ok := j.Right.(*ast.TableFunction); !ok {
		t.Fatalf("right=%T", j.Right)
	}
}

func TestParseSubqueryExpression(t *testing.T) {
	s := mustSelect(t, "SELECT (SELECT max(v) FROM t) AS m")
	if _, ok := s.Projection[0].Expr.(*ast.SubqueryExpr); !ok {
		t.Fatalf("expr=%T", s.Projection[0].Expr)
	}
}

func TestParseRejectsRecursiveAndMalformedJoin(t *testing.T) {
	for _, sql := range []string{"WITH RECURSIVE x AS (SELECT 1) SELECT * FROM x", "SELECT * FROM a LEFT JOIN b"} {
		if _, err := Parse(sql); err == nil {
			t.Fatalf("expected error for %q", sql)
		}
	}
}

func TestSourceSpan(t *testing.T) {
	s := mustSelect(t, "\nSELECT a FROM t")
	if s.Span.Start.Line != 2 || s.Span.Start.Column != 1 {
		t.Fatalf("span=%+v", s.Span)
	}
}
