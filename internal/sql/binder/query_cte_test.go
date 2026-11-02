package binder

import (
	"context"
	"testing"
)

func TestQueryBinderRejectsSelfAndForwardCTEReferences(t *testing.T) {
	for _, sql := range []string{
		"WITH x AS (SELECT a FROM x) SELECT * FROM x",
		"WITH x AS (SELECT a FROM y), y AS (SELECT a FROM t) SELECT * FROM x",
	} {
		if _, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql)); err == nil {
			t.Fatalf("expected non-recursive CTE rejection for %s", sql)
		}
	}
}

func TestQueryBinderAllowsEarlierCTEReference(t *testing.T) {
	sql := "WITH x AS (SELECT a FROM t), y AS (SELECT a FROM x) SELECT * FROM y"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if q.Source == nil || q.Source.Kind != RelationCTE || len(q.Output) != 1 {
		t.Fatalf("query=%+v", q)
	}
}
