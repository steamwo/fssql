package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func joinFactoryConfig() ExternalJoinRuntimeConfig {
	return ExternalJoinRuntimeConfig{
		MemoryBudget: 32 << 10, RadixBits: 4, MaxDepth: 4, MinReductionPercent: 10,
		SpillBlockBytes: 2048, Compression: storagetemp.CompressionRLE,
	}
}

func joinFactorySchemas() ([]planner.Column, []planner.Column) {
	left := []planner.Column{{Name: "l_id", Type: types.LogicalType{ID: types.BigInt}}}
	right := []planner.Column{{Name: "r_id", Type: types.LogicalType{ID: types.BigInt}}}
	return left, right
}

func equiJoinPredicate(op string) *binder.BoundExpression {
	return &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: op,
		Children: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0},
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 1},
		},
	}
}

// TestExternalJoinFactoryRequiresExplicitResources 验证 production JOIN 不从测试参数或隐藏默认值创建资源策略。
func TestExternalJoinFactoryRequiresExplicitResources(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	defer func() {
		if !q.Closed() {
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}()
	cases := []ExternalJoinRuntimeConfig{
		{MemoryBudget: 1024, RadixBits: 4, MaxDepth: 4, MinReductionPercent: 10, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 0, MaxDepth: 4, MinReductionPercent: 10, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 9, MaxDepth: 4, MinReductionPercent: 10, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 16, MinReductionPercent: 10, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 4, MinReductionPercent: 0, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 4, MinReductionPercent: 10, SpillBlockBytes: 0},
		{MemoryBudget: 4096, RadixBits: 4, MaxDepth: 4, MinReductionPercent: 10, SpillBlockBytes: 1024, Compression: storagetemp.Compression(99)},
	}
	for _, config := range cases {
		if _, err := NewExternalJoinFactory(q, buffers, config); err == nil {
			t.Fatalf("expected invalid config rejection: %+v", config)
		}
	}
}

// TestExternalJoinFactoryMapsStableJoinTypes 验证全部七种 Stable JOIN 都映射到明确 runtime，且 preserved-side build 方向正确。
func TestExternalJoinFactoryMapsStableJoinTypes(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalJoinFactory(q, buffers, joinFactoryConfig())
	if err != nil {
		t.Fatal(err)
	}
	left, right := joinFactorySchemas()
	cases := []struct {
		kind planner.JoinType
		want pipeline.BinaryBuildSide
	}{
		{planner.JoinInner, pipeline.BinaryBuildRight},
		{planner.JoinLeft, pipeline.BinaryBuildRight},
		{planner.JoinRight, pipeline.BinaryBuildLeft},
		{planner.JoinFull, pipeline.BinaryBuildRight},
		{planner.JoinSemi, pipeline.BinaryBuildRight},
		{planner.JoinAnti, pipeline.BinaryBuildRight},
	}
	for _, tc := range cases {
		runtime, err := factory.OpenJoin(context.Background(), tc.kind, left, right, equiJoinPredicate("="), planner.CardinalityEstimate{}, planner.CardinalityEstimate{})
		if err != nil {
			t.Fatalf("join %d: %v", tc.kind, err)
		}
		if runtime.BuildSide() != tc.want || runtime.BuildSink() == nil {
			runtime.Close()
			t.Fatalf("join %d build side=%d want=%d", tc.kind, runtime.BuildSide(), tc.want)
		}
		runtime.Close()
	}
	cross, err := factory.OpenJoin(context.Background(), planner.JoinCross, left, right, nil, planner.CardinalityEstimate{}, planner.CardinalityEstimate{})
	if err != nil {
		t.Fatal(err)
	}
	if cross.BuildSide() != pipeline.BinaryBuildRight || cross.BuildSink() == nil {
		cross.Close()
		t.Fatal("cross join runtime mapping failed")
	}
	cross.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestExternalJoinFactoryRejectsResidualPredicate 验证 runtime 不会只执行 equi key 而丢掉未支持 residual ON 语义。
func TestExternalJoinFactoryRejectsResidualPredicate(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalJoinFactory(q, buffers, joinFactoryConfig())
	if err != nil {
		t.Fatal(err)
	}
	left, right := joinFactorySchemas()
	if _, err := factory.OpenJoin(context.Background(), planner.JoinInner, left, right, equiJoinPredicate(">"), planner.CardinalityEstimate{}, planner.CardinalityEstimate{}); err == nil {
		t.Fatal("expected non-equi/residual predicate rejection")
	}
	if _, err := factory.OpenJoin(context.Background(), planner.JoinCross, left, right, equiJoinPredicate("="), planner.CardinalityEstimate{}, planner.CardinalityEstimate{}); err == nil {
		t.Fatal("expected CROSS predicate rejection")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
