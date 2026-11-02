package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func makeAggregateFactoryChunk(t *testing.T, scope *memory.Scope, buffers *memory.BufferManager, start, count int) *vector.DataChunk {
	t.Helper()
	groups, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, count)
	if err != nil {
		t.Fatal(err)
	}
	values, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, count)
	if err != nil {
		groups.Release()
		t.Fatal(err)
	}
	for row := 0; row < count; row++ {
		groups.SetInt64(row, int64(start+row))
		values.SetInt64(row, 1)
	}
	chunk, err := vector.NewDataChunk(groups, values)
	if err != nil {
		groups.Release()
		values.Release()
		t.Fatal(err)
	}
	return chunk
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

// TestExternalAggregateFactoryForcedSpill 验证 factory 真正绑定 QueryContext TemporaryStorage，并在 forced spill 后无 temp/managed leak。
func TestExternalAggregateFactoryForcedSpill(t *testing.T) {
	manager, buffers, tempMemory, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalAggregateFactory(q, buffers, ExternalAggregateRuntimeConfig{
		// 8 KiB 仅用于测试中强制 spill，不是 production 默认值。
		MemoryBudget: 8 << 10, RadixBits: 4, MaxDepth: 8, SpillBlockBytes: 2048, Compression: storagetemp.CompressionRLE,
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "group", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "value", Type: types.LogicalType{ID: types.BigInt}},
	}
	group := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[0].Type, ColumnIndex: 0}
	arg := &binder.BoundExpression{Kind: binder.BoundColumn, Type: columns[1].Type, ColumnIndex: 1}
	spec := planner.AggregateSpec{
		FunctionID: uint16(function.SumID), Arguments: []*binder.BoundExpression{arg},
		Output: planner.Column{Name: "sum", Type: types.LogicalType{ID: types.HugeInt}, Nullable: true},
	}
	operator, err := factory.OpenAggregate(context.Background(), columns, []*binder.BoundExpression{group}, []planner.AggregateSpec{spec}, planner.CardinalityEstimate{Rows: ^uint64(0), Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	external, ok := operator.(*aggregate.ExternalAggregate)
	if !ok {
		operator.Release()
		t.Fatalf("operator=%T want *aggregate.ExternalAggregate", operator)
	}
	for start := 0; start < 4096; start += 256 {
		chunk := makeAggregateFactoryChunk(t, q.Memory, buffers, start, 256)
		if err := external.Consume(context.Background(), chunk); err != nil {
			chunk.Release()
			external.Release()
			t.Fatal(err)
		}
		chunk.Release()
	}
	rows := 0
	if err := external.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		for row := 0; row < chunk.Cardinality; row++ {
			if _, valid := chunk.Columns[0].Int64(row); !valid {
				return context.Canceled
			}
			sum, valid := chunk.Columns[1].Int128(row)
			if !valid || sum.Hi != 0 || sum.Lo != 1 {
				return context.DeadlineExceeded
			}
			rows++
		}
		chunk.Release()
		return nil
	}); err != nil {
		external.Release()
		t.Fatalf("emit/value verification: %v", err)
	}
	stats := external.Stats()
	if stats.SpillPasses == 0 || stats.SpillPages == 0 || stats.SpilledGroups == 0 {
		external.Release()
		t.Fatalf("expected forced external aggregate stats: %+v", stats)
	}
	if rows != 4096 {
		external.Release()
		t.Fatalf("aggregate rows=%d want=4096", rows)
	}
	external.Release()
	if q.Temp.Used() != 0 || q.Temp.OwnedFiles() != 0 || manager.Used() != 0 {
		t.Fatalf("post-aggregate leak temp=%d files=%d managed=%d", q.Temp.Used(), q.Temp.OwnedFiles(), manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if tempMemory.GlobalUsed() != 0 {
		t.Fatalf("temporary memory leak=%d", tempMemory.GlobalUsed())
	}
}
