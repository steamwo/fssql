package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	execsort "github.com/steamwo/fssql/internal/execution/sort"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// TestExternalSortFactoryRequiresExplicitResources 验证 production sort factory 不静默创建预算/临时存储默认值。
func TestExternalSortFactoryRequiresExplicitResources(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	defer func() {
		if !q.Closed() {
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}()
	for _, cfg := range []ExternalSortRuntimeConfig{
		{MemoryBudget: 0, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, SpillBlockBytes: 0},
		{MemoryBudget: 4096, SpillBlockBytes: 1024, Compression: storagetemp.Compression(99)},
	} {
		if _, err := NewExternalSortFactory(q, buffers, cfg); err == nil {
			t.Fatalf("expected invalid config rejection: %+v", cfg)
		}
	}
}

// TestExternalSortFactoryRejectsNonMaterializedKey 验证 runtime 不会重新执行 Planner 尚未 materialize 的 ORDER BY expression。
func TestExternalSortFactoryRejectsNonMaterializedKey(t *testing.T) {
	_, buffers, _, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{MemoryBudget: 4096, SpillBlockBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{{Name: "id", Type: types.LogicalType{ID: types.BigInt}}}
	key := planner.SortKey{Expr: &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "1"}}
	if _, err := factory.OpenSort(context.Background(), columns, []planner.SortKey{key}, planner.CardinalityEstimate{}); err == nil {
		t.Fatal("expected non-materialized sort key rejection")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestExternalSortFactoryForcedSpill 验证 factory 绑定 QueryContext TemporaryStorage，并在 multi-run merge 后无 temp/managed leak。
func TestExternalSortFactoryForcedSpill(t *testing.T) {
	manager, buffers, tempMemory, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalSortFactory(q, buffers, ExternalSortRuntimeConfig{
		// 4 KiB 仅用于测试中强制生成多个 runs，不是 production 默认值。
		MemoryBudget: 4 << 10, SpillBlockBytes: 1024, Compression: storagetemp.CompressionRLE,
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}},
	}
	key := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0}
	operator, err := factory.OpenSort(context.Background(), columns, []planner.SortKey{{Expr: key, Descending: true}}, planner.CardinalityEstimate{Rows: 4096, Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	external, ok := operator.(*execsort.ExternalSort)
	if !ok {
		operator.Release()
		t.Fatalf("operator=%T want *sort.ExternalSort", operator)
	}
	for start := 0; start < 4096; start += 256 {
		chunk := distinctFactoryChunk(t, q.Memory, buffers, start, 256)
		if err := external.Consume(context.Background(), chunk); err != nil {
			chunk.Release()
			external.Release()
			t.Fatal(err)
		}
		chunk.Release()
	}
	rows := 0
	var previous int64
	first := true
	if err := external.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		for row := 0; row < chunk.Cardinality; row++ {
			value, valid := chunk.Columns[0].Int64(row)
			if !valid {
				chunk.Release()
				return context.Canceled
			}
			if !first && previous < value {
				chunk.Release()
				return context.DeadlineExceeded
			}
			first = false
			previous = value
			rows++
		}
		chunk.Release()
		return nil
	}); err != nil {
		external.Release()
		t.Fatalf("emit/order verification: %v", err)
	}
	stats := external.Stats()
	if stats.RunsCreated < 2 || stats.MergePasses == 0 || stats.SpillPages == 0 {
		external.Release()
		t.Fatalf("expected forced external sort stats: %+v", stats)
	}
	if rows != 4096 {
		external.Release()
		t.Fatalf("sorted rows=%d want=4096", rows)
	}
	external.Release()
	if q.Temp.Used() != 0 || q.Temp.OwnedFiles() != 0 || manager.Used() != 0 {
		t.Fatalf("post-sort leak temp=%d files=%d managed=%d", q.Temp.Used(), q.Temp.OwnedFiles(), manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if tempMemory.GlobalUsed() != 0 {
		t.Fatalf("temporary memory leak=%d", tempMemory.GlobalUsed())
	}
}
