package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func aggregateFactoryChunk(t *testing.T, qscope interface {
}, _ int) {
	// 占位签名禁止被使用；真实 chunk helper 见 makeAggregateFactoryChunk。
}

func makeAggregateFactoryChunk(t *testing.T, scope interface {
}, _ int, _ int) *vector.DataChunk {
	t.Helper()
	return nil
}

// TestExternalAggregateFactoryRequiresExplicitResources 验证 budget/radix/depth/spill 配置必须显式且可执行。
func TestExternalAggregateFactoryRequiresExplicitResources(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	defer func() {
		if !q.Closed() {
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}()
	for _, cfg := range []ExternalAggregateRuntimeConfig{
		{MemoryBudget: 0, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 0, MaxDepth: 8, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 9, MaxDepth: 4, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 8, MaxDepth: 8, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: 0},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: 1024, Compression: storagetemp.Compression(99)},
	} {
		if _, err := NewExternalAggregateFactory(q, buffers, cfg); err == nil {
			t.Fatalf("expected invalid config rejection: %+v", cfg)
		}
	}
}

// TestMapAggregateSpecStableFunctions 验证 FunctionCatalog ID 与 execution aggregate kind 的显式映射。
func TestMapAggregateSpecStableFunctions(t *testing.T) {
	columns := []planner.Column{{Name: "x", Type: types.LogicalType{ID: types.BigInt}}}
	arg := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	cases := []struct {
		id   function.ID
		args []*binder.BoundExpression
		want aggregate.Kind
	}{
		{function.CountID, nil, aggregate.CountStar},
		{function.CountID, []*binder.BoundExpression{arg}, aggregate.Count},
		{function.SumID, []*binder.BoundExpression{arg}, aggregate.Sum},
		{function.AvgID, []*binder.BoundExpression{arg}, aggregate.Avg},
		{function.MinID, []*binder.BoundExpression{arg}, aggregate.Min},
		{function.MaxID, []*binder.BoundExpression{arg}, aggregate.Max},
	}
	for _, tc := range cases {
		got, err := mapAggregateSpec(planner.AggregateSpec{FunctionID: uint16(tc.id), Arguments: tc.args}, columns)
		if err != nil {
			t.Fatalf("id=%d: %v", tc.id, err)
		}
		if got.Kind != tc.want {
			t.Fatalf("id=%d kind=%d want=%d", tc.id, got.Kind, tc.want)
		}
	}
}

// TestExternalAggregateFactoryRejectsUnmaterializedAndTypeDrift 验证 execution 不重新执行表达式，也不接受 Binder/execution 输出类型漂移。
func TestExternalAggregateFactoryRejectsUnmaterializedAndTypeDrift(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalAggregateFactory(q, buffers, ExternalAggregateRuntimeConfig{
		MemoryBudget: 4096, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{{Name: "x", Type: types.LogicalType{ID: types.BigInt}}}
	badGroup := &binder.BoundExpression{Kind: binder.BoundBinary, Type: columns[0].Type}
	if _, err := factory.OpenAggregate(context.Background(), columns, []*binder.BoundExpression{badGroup}, nil, planner.CardinalityEstimate{}); err == nil {
		t.Fatal("expected unmaterialized group rejection")
	}
	arg := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	badOutput := planner.AggregateSpec{
		FunctionID: uint16(function.SumID), Arguments: []*binder.BoundExpression{arg},
		Output: planner.Column{Name: "sum", Type: types.LogicalType{ID: types.BigInt}},
	}
	if _, err := factory.OpenAggregate(context.Background(), columns, nil, []planner.AggregateSpec{badOutput}, planner.CardinalityEstimate{}); err == nil {
		t.Fatal("expected aggregate output type drift rejection")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
