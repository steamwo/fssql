package parser

import (
	"testing"

	"github.com/steamwo/fssql/internal/sql/ast"
)

func TestSetOperationPrecedenceAndSuffix(t *testing.T) {
	q := mustSelect(t, "SELECT 1 AS v UNION SELECT 2 INTERSECT ALL SELECT 3 EXCEPT SELECT 4 ORDER BY v LIMIT 2")
	if q.SetOperation != ast.ExceptDistinct || q.UnionLeft.SetOperation != ast.UnionDistinct || q.UnionLeft.UnionRight.SetOperation != ast.IntersectAll {
		t.Fatalf("incorrect set operation precedence: %+v", q)
	}
	if len(q.OrderBy) != 1 || q.Limit == nil || len(q.UnionRight.OrderBy) > 0 || q.UnionLeft.Limit != nil {
		t.Fatal("suffix was not bound to the full result")
	}
	q = mustSelect(t, "(SELECT 1 AS v UNION SELECT 2) INTERSECT SELECT 2")
	if q.SetOperation != ast.IntersectDistinct || q.UnionLeft.SetOperation != ast.UnionDistinct {
		t.Fatal("parenthesized grouping lost")
	}
}
