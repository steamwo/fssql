package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestExternalSortFactoryUsesPreallocatedTopNForFixedWidthInput(t *testing.T) {
	manager, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 64 << 10, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	factory.setTopNRows(100)
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "value", Type: types.LogicalType{ID: types.Double}},
	}
	keys := []planner.SortKey{
		{Expr: &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[1].Type, ColumnIndex: 1}, Descending: true},
		{Expr: &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}},
	}
	operator, err := factory.OpenSort(context.Background(), columns, keys, planner.CardinalityEstimate{Rows: 1_000_000, Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.TopN); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.TopN", operator)
	}
	operator.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortFactoryTopNFallsBackForVariableWidthInput(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 64 << 10, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	factory.setTopNRows(100)
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "payload", Type: types.LogicalType{ID: types.Varchar}},
	}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	operator, err := factory.OpenSort(context.Background(), columns, []planner.SortKey{{Expr: key}}, planner.CardinalityEstimate{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.ExternalSort); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.ExternalSort", operator)
	}
	operator.Release()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortFactoryTopNFallsBackWhenPreallocationExceedsBudget(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 4 << 10, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	factory.setTopNRows(10_000)
	columns := []planner.Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	operator, err := factory.OpenSort(context.Background(), columns, []planner.SortKey{{Expr: key}}, planner.CardinalityEstimate{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.ExternalSort); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.ExternalSort", operator)
	}
	operator.Release()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
