package binder

import (
	"context"
	"testing"
)

func TestLateralSiblingScopeDoesNotEscapeQuery(t *testing.T) {
	sql := "SELECT t.a,d.x FROM t CROSS JOIN LATERAL (SELECT t.a AS x) d"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	right := q.Source.Children[1]
	if !q.Source.Dependent || !right.Lateral {
		t.Fatal("missing LATERAL scope provenance")
	}
	ref := right.CTEQuery.Projection[0].Expression
	if ref.Kind != BoundOuterColumn || ref.Depth != 1 {
		t.Fatalf("sibling reference=%+v", ref)
	}
	if QueryCorrelated(q) {
		t.Fatal("sibling reference must not escape its owning query")
	}
}

func TestLateralGrandparentScopeRemainsCorrelated(t *testing.T) {
	sql := "SELECT (SELECT sum(d.x) FROM t i CROSS JOIN LATERAL (SELECT i.a+o.a AS x) d) FROM t o"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	inner := q.Projection[0].Expression.Subquery
	if !QueryCorrelated(inner) || QueryCorrelated(q) {
		t.Fatal("incorrect LATERAL grandparent scope")
	}
	count := 0
	_, err = RewriteOuterReferences(inner, func(e *BoundExpression, depth int) (*BoundExpression, error) {
		count++
		if depth != 1 || e.Depth != 2 {
			t.Fatalf("external reference depth=%d ref=%+v", depth, e)
		}
		return e, nil
	})
	if err != nil || count != 1 {
		t.Fatalf("external references=%d err=%v", count, err)
	}
}
