package binder

import (
	"context"
	"testing"
)

func TestQueryBinderScalarNonCorrelatedSubquery(t *testing.T) {
	for _, sql := range []string{
		"SELECT (SELECT max(a) FROM t) AS m",
		"SELECT 1 + (SELECT max(a) FROM t) AS v",
	} {
		q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(q.Output) != 1 {
			t.Fatalf("%s output=%+v", sql, q.Output)
		}
	}
}

func TestQueryBinderCorrelatedReferenceAndMultiColumnScalarSubquery(t *testing.T) {
	if BoundOuterColumn == BoundRuntimeConstant {
		t.Fatal("outer references must not share the runtime-constant expression kind")
	}
	sql := "SELECT a, (SELECT t.a) FROM t"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	inner := q.Projection[1].Expression.Subquery.Projection[0].Expression
	if inner.Kind != BoundOuterColumn || inner.Depth != 1 {
		t.Fatalf("outer binding=%+v", inner)
	}
	for _, sql := range []string{
		"SELECT (SELECT a, b FROM t) FROM t",
	} {
		if _, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql)); err == nil {
			t.Fatalf("expected scalar subquery rejection for %s", sql)
		}
	}
}

func TestQueryBinderAggregateGroupingLegality(t *testing.T) {
	for _, sql := range []string{
		"SELECT a, count(*) FROM t GROUP BY a",
		"SELECT a + 1 FROM t GROUP BY a",
		"SELECT count(*) FROM t HAVING count(*) > 0",
	} {
		if _, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql)); err != nil {
			t.Fatalf("valid aggregate query %s: %v", sql, err)
		}
	}
	for _, sql := range []string{
		"SELECT a, b, count(*) FROM t GROUP BY a",
		"SELECT a FROM t GROUP BY a + 1",
		"SELECT sum(max(a)) FROM t",
	} {
		if _, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql)); err == nil {
			t.Fatalf("expected aggregate legality rejection for %s", sql)
		}
	}
}

func TestQueryBinderOrderByProjectionAliasAndOrdinal(t *testing.T) {
	for _, tc := range []struct {
		sql   string
		index int
	}{
		{sql: "SELECT a AS x, b FROM t ORDER BY x", index: 0},
		{sql: "SELECT a, b FROM t ORDER BY 2", index: 1},
	} {
		q, err := testQueryBinder(tc.sql).BindSelect(context.Background(), mustQueryAST(t, tc.sql))
		if err != nil {
			t.Fatal(err)
		}
		if len(q.OrderBy) != 1 || !q.OrderBy[0].UsesOutput || q.OrderBy[0].OutputIndex != tc.index {
			t.Fatalf("order=%+v", q.OrderBy)
		}
	}
	if _, err := testQueryBinder("SELECT a FROM t ORDER BY 2").BindSelect(context.Background(), mustQueryAST(t, "SELECT a FROM t ORDER BY 2")); err == nil {
		t.Fatal("expected ORDER BY ordinal range error")
	}
}

func TestQueryBinderPreservesRelationNullability(t *testing.T) {
	sql := "SELECT b FROM t"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if len(q.Output) != 1 || !q.Output[0].Nullable || !q.Projection[0].Expression.Nullable {
		t.Fatalf("output=%+v expr=%+v", q.Output, q.Projection[0].Expression)
	}
}
