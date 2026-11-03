package sort

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
)

func TestTopNPreallocateFixedReservesStateBeforeConsume(t *testing.T) {
	h, clean := newSortHarness(t, 1<<20)
	defer clean()
	top, err := NewTopN(TopNConfig{
		InputTypes:       []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
		Keys:             []Key{{Column: 1, Desc: true}},
		N:                100,
		MemoryBudget:     16 << 10,
		Scope:            h.scope,
		Buffers:          h.bm,
		PreallocateFixed: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(top.preallocated) != 100 || top.indices == nil {
		t.Fatalf("preallocated rows=%d indices=%v", len(top.preallocated), top.indices != nil)
	}
	usedBefore := h.scope.Used()
	for base := 0; base < 1000; base += 125 {
		chunk := makeFixedTopNChunk(t, h, base, 125)
		if err := top.Consume(context.Background(), chunk); err != nil {
			chunk.Release()
			t.Fatal(err)
		}
		chunk.Release()
	}
	if h.scope.Used() != usedBefore {
		t.Fatalf("consume changed preallocated footprint: before=%d after=%d", usedBefore, h.scope.Used())
	}
	rows := 0
	if err := top.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		rows += chunk.Cardinality
		chunk.Release()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 100 {
		t.Fatalf("rows=%d", rows)
	}
	top.Release()
	if h.scope.Used() != 0 {
		t.Fatalf("managed leak=%d", h.scope.Used())
	}
}

// makeFixedTopNChunk uses only fixed-width vectors so PreallocateFixed cannot
// grow row state after construction.
func makeFixedTopNChunk(t testing.TB, h *sortHarness, base, count int) *vector.DataChunk {
	t.Helper()
	id, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.BigInt}, count)
	if err != nil {
		t.Fatal(err)
	}
	value, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.Double}, count)
	if err != nil {
		id.Release()
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		id.SetInt64(i, int64(base+i))
		value.SetFloat64(i, float64((base+i)%97))
	}
	chunk, err := vector.NewDataChunk(id, value)
	if err != nil {
		id.Release()
		value.Release()
		t.Fatal(err)
	}
	return chunk
}

func TestTopNPreallocateFixedRejectsVariableWidth(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	buffers := memory.NewBufferManager(nil)
	_, err := NewTopN(TopNConfig{
		InputTypes:       []types.LogicalType{{ID: types.BigInt}, {ID: types.Varchar}},
		Keys:             []Key{{Column: 0}},
		N:                10,
		MemoryBudget:     16 << 10,
		Scope:            scope,
		Buffers:          buffers,
		PreallocateFixed: true,
	})
	if err == nil {
		t.Fatal("expected variable-width preallocation rejection")
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func TestTopNPreallocateFixedFailsBeforeConsumeWhenBudgetTooSmall(t *testing.T) {
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	buffers := memory.NewBufferManager(nil)
	_, err := NewTopN(TopNConfig{
		InputTypes:       []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
		Keys:             []Key{{Column: 0}},
		N:                100,
		MemoryBudget:     1024,
		Scope:            scope,
		Buffers:          buffers,
		PreallocateFixed: true,
	})
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("err=%v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed leak=%d", scope.Used())
	}
}

func BenchmarkTopNPreallocatedFixed(b *testing.B) {
	for i := 0; i < b.N; i++ {
		h, clean := newSortHarness(b, 1<<20)
		top, err := NewTopN(TopNConfig{
			InputTypes:       []types.LogicalType{{ID: types.BigInt}, {ID: types.Double}},
			Keys:             []Key{{Column: 1, Desc: true}, {Column: 0}},
			N:                100,
			MemoryBudget:     64 << 10,
			Scope:            h.scope,
			Buffers:          h.bm,
			PreallocateFixed: true,
		})
		if err != nil {
			b.Fatal(err)
		}
		for base := 0; base < 2048; base += 256 {
			chunk := makeFixedTopNChunk(b, h, base, 256)
			if err := top.Consume(context.Background(), chunk); err != nil {
				chunk.Release()
				top.Release()
				clean()
				b.Fatal(err)
			}
			chunk.Release()
		}
		if err := top.Emit(context.Background(), func(chunk *vector.DataChunk) error {
			chunk.Release()
			return nil
		}); err != nil {
			top.Release()
			clean()
			b.Fatal(err)
		}
		top.Release()
		clean()
	}
}
