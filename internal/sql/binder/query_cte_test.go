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
	if q.Source == nil || q.Source.Kind != RelationCTE || len(q.Output) != 1 || q.Source.CTEQuery == nil {
		t.Fatalf("query=%+v", q)
	}
	if q.Source.CTEQuery.Source == nil || q.Source.CTEQuery.Source.Kind != RelationCTE || q.Source.CTEQuery.Source.CTEQuery == nil {
		t.Fatalf("nested CTE provenance was not retained: %+v", q.Source.CTEQuery)
	}
}

func TestQueryBinderCTEColumnAliasIsAppliedToProvenance(t *testing.T) {
	sql := "WITH x(renamed) AS (SELECT a FROM t) SELECT renamed FROM x"
	q, err := testQueryBinder(sql).BindSelect(context.Background(), mustQueryAST(t, sql))
	if err != nil {
		t.Fatal(err)
	}
	if q.Source == nil || q.Source.CTEQuery == nil || len(q.Source.CTEQuery.Output) != 1 {
		t.Fatalf("query=%+v", q)
	}
	if q.Source.Columns[0].Name != "renamed" || q.Source.CTEQuery.Output[0].Name != "renamed" || q.Source.CTEQuery.Projection[0].Output.Name != "renamed" {
		t.Fatalf("CTE alias metadata drift: relation=%+v query=%+v", q.Source.Columns, q.Source.CTEQuery.Output)
	}
}
