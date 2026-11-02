package session

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/distinct"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

func newDistinctFactoryQuery(t *testing.T) (*memory.Manager, *memory.BufferManager, *storagetemp.TemporaryMemoryManager, *query.Context) {
	t.Helper()
	manager := memory.NewManager(64 << 20)
	buffers := memory.NewBufferManager(nil)
	tempMemory := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{
		Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20,
	})
	tempManager, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), tempMemory)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := tempManager.NewQueryScope("distinct-factory-test")
	if err != nil {
		t.Fatal(err)
	}
	return manager, buffers, tempMemory, query.NewWithTemp(context.Background(), manager, tempScope)
}

func distinctFactoryChunk(t *testing.T, scope *memory.Scope, buffers *memory.BufferManager, start, count int) *vector.DataChunk {
	t.Helper()
	ids, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, count)
	if err != nil {
		t.Fatal(err)
	}
	names, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.Varchar}, count)
	if err != nil {
		ids.Release()
		t.Fatal(err)
	}
	for row := 0; row < count; row++ {
		value := start + row
		ids.SetInt64(row, int64(value))
		if err := names.SetString(row, fmt.Sprintf("unique-%08d", value)); err != nil {
			ids.Release()
			names.Release()
			t.Fatal(err)
		}
	}
	chunk, err := vector.NewDataChunk(ids, names)
	if err != nil {
		ids.Release()
		names.Release()
		t.Fatal(err)
	}
	return chunk
}

// TestExternalDistinctFactoryRequiresExplicitQueryResources 验证 production factory 不静默创建 temp/budget 默认值。
func TestExternalDistinctFactoryRequiresExplicitQueryResources(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	q := query.New(context.Background(), manager)
	buffers := memory.NewBufferManager(nil)
	valid := ExternalDistinctRuntimeConfig{MemoryBudget: 4096, RadixBits: 3, SpillBlockBytes: 1024, Compression: storagetemp.CompressionNone}
	if _, err := NewExternalDistinctFactory(q, buffers, valid); err == nil {
		t.Fatal("expected missing TemporaryStorage query scope rejection")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	manager, buffers, _, q = newDistinctFactoryQuery(t)
	defer func() {
		if !q.Closed() {
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}()
	for _, cfg := range []ExternalDistinctRuntimeConfig{
		{MemoryBudget: 0, RadixBits: 3, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 0, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 9, SpillBlockBytes: 1024},
		{MemoryBudget: 4096, RadixBits: 3, SpillBlockBytes: 0},
		{MemoryBudget: 4096, RadixBits: 3, SpillBlockBytes: 1024, Compression: storagetemp.Compression(99)},
	} {
		if _, err := NewExternalDistinctFactory(q, buffers, cfg); err == nil {
			t.Fatalf("expected invalid config rejection: %+v", cfg)
		}
	}
	_ = manager
}

// TestExternalDistinctFactoryForcedSpill 验证 session factory 真正绑定 QueryContext Temp/Memory，并在 forced spill 后无 temp/managed leak。
func TestExternalDistinctFactoryForcedSpill(t *testing.T) {
	manager, buffers, tempMemory, q := newDistinctFactoryQuery(t)
	factory, err := NewExternalDistinctFactory(q, buffers, ExternalDistinctRuntimeConfig{
		// 12 KiB 仅用于测试中强制触发 spill，不是 production 默认值。
		MemoryBudget: 12 << 10, RadixBits: 8, SpillBlockBytes: 2048, Compression: storagetemp.CompressionRLE,
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := []planner.Column{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}},
	}
	// 极大 estimate 用于证明 factory 不会按 optimizer cardinality 直接预分配巨大 hash state。
	operator, err := factory.OpenDistinct(context.Background(), columns, planner.CardinalityEstimate{Rows: math.MaxUint64, Confidence: 1})
	if err != nil {
		t.Fatal(err)
	}
	external, ok := operator.(*distinct.ExternalDistinct)
	if !ok {
		operator.Release()
		t.Fatalf("operator=%T want *distinct.ExternalDistinct", operator)
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
	if err := external.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		rows += chunk.Cardinality
		chunk.Release()
		return nil
	}); err != nil {
		external.Release()
		t.Fatal(err)
	}
	stats := external.Stats()
	if stats.SpillPasses == 0 || stats.SpillPages == 0 {
		external.Release()
		t.Fatalf("forced-spill stats=%+v", stats)
	}
	if rows != 4096 {
		external.Release()
		t.Fatalf("distinct rows=%d want=4096", rows)
	}
	external.Release()
	if q.Temp.Used() != 0 || q.Temp.OwnedFiles() != 0 {
		t.Fatalf("temp leak used=%d files=%d", q.Temp.Used(), q.Temp.OwnedFiles())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if tempMemory.GlobalUsed() != 0 {
		t.Fatalf("temporary memory leak=%d", tempMemory.GlobalUsed())
	}
}
