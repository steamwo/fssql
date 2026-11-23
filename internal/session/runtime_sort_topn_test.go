package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestExternalSortFactoryUsesPackedTopNForFixedWidthInput(t *testing.T) {
	manager, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 64 << 10, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "value", Type: types.LogicalType{ID: types.Double}},
	}
	keys := []planner.SortKey{
		{Expr: &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[1].Type, ColumnIndex: 1}, Descending: true},
		{Expr: &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}},
	}
	operator, err := factory.OpenTopNSort(context.Background(), columns, keys, planner.CardinalityEstimate{Rows: 1_000_000, Confidence: 1}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.PackedTopN); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.PackedTopN", operator)
	}
	operator.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestExternalSortFactoryUsesSpillableTopNForVariableWidthInput(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 64 << 10, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "payload", Type: types.LogicalType{ID: types.Varchar}},
	}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	operator, err := factory.OpenTopNSort(context.Background(), columns, []planner.SortKey{{Expr: key}}, planner.CardinalityEstimate{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.SpillableTopN); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.SpillableTopN", operator)
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
	columns := []planner.Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	operator, err := factory.OpenTopNSort(context.Background(), columns, []planner.SortKey{{Expr: key}}, planner.CardinalityEstimate{}, 10_000)
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

// TestExternalSortFactoryTopNUsesQueryBudgetForFixedWidth 验证固定宽度 TopN 按查询内存预算保留候选集，
// 不因较小的溢写预算而退回全排序；这是 OFFSET+LIMIT 不走 TopN 的直接原因。
func TestExternalSortFactoryTopNUsesQueryBudgetForFixedWidth(t *testing.T) {
	manager, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{
		MemoryBudget: 4 << 10, TopNMemoryBudget: 1 << 20, SpillBlockBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "value", Type: types.LogicalType{ID: types.BigInt}},
	}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	operator, err := factory.OpenTopNSort(context.Background(), columns, []planner.SortKey{{Expr: key}}, planner.CardinalityEstimate{}, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := operator.(*execsort.PackedTopN); !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.PackedTopN", operator)
	}
	operator.Release()
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
