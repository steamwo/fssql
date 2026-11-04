package aggregate

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// Drive worker IDs in a fixed order to reproduce a valid pipeline schedule
// independently of goroutine scheduling. Every batch contains 64 distinct keys.
func consumeWorkerCountBatches(t *testing.T, h *extAggHarness, e *ExternalAggregate, workers, batches, groups int) (map[int64]int64, error) {
	t.Helper()
	want := make(map[int64]int64)
	for batch := 0; batch < batches; batch++ {
		v, err := vector.NewFlat(h.scope, h.bm, types.LogicalType{ID: types.BigInt}, 64)
		if err != nil {
			return want, err
		}
		for row := 0; row < 64; row++ {
			key := int64((batch*64 + row) % groups)
			v.SetInt64(row, key)
			want[key]++
		}
		chunk, err := vector.NewDataChunk(v)
		if err != nil {
			v.Release()
			return want, err
		}
		err = e.Consume(pipeline.WithWorkerID(context.Background(), batch%workers), chunk)
		chunk.Release()
		if err != nil {
			return want, err
		}
	}
	return want, nil
}

func newWorkerCountAggregate(t *testing.T, h *extAggHarness, budget int64) *ExternalAggregate {
	t.Helper()
	e, err := NewExternalAggregate(ExternalConfig{
		InputTypes: []types.LogicalType{{ID: types.BigInt}}, GroupColumns: []int{0},
		Specs: []Spec{{Kind: CountStar}}, MemoryBudget: budget, RadixBits: 4,
		MaxDepth: 8, SpillBlockBytes: 512, Scope: h.scope, Buffers: h.bm, Temp: h.temp,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func assertWorkerCounts(t *testing.T, e *ExternalAggregate, want map[int64]int64) {
	t.Helper()
	got := make(map[int64]int64)
	err := e.Emit(context.Background(), func(chunk *vector.DataChunk) error {
		defer chunk.Release()
		for row := 0; row < chunk.Cardinality; row++ {
			key, _ := chunk.Columns[0].Int64(row)
			count, _ := chunk.Columns[1].Int64(row)
			if _, exists := got[key]; exists {
				t.Errorf("duplicate output group %d", key)
			}
			got[key] = count
		}
		return nil
	})
	if err != nil {
		t.Fatalf("emit: %v (spilled=%v, managed=%d)", err, e.spilled, e.cfg.Scope.Used())
	}
	if len(got) != len(want) {
		t.Fatalf("groups=%d want=%d", len(got), len(want))
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("group %d count=%d want=%d", key, got[key], count)
		}
	}
}

func TestExternalAggregateWorkerSpillUnderHardLimit(t *testing.T) {
	for _, groups := range []int{97, 512, 2048} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("groups%d/workers%d", groups, workers), func(t *testing.T) {
				h, cleanup := newExtAggHarness(t, 64<<10, 64<<20)
				defer cleanup()
				e := newWorkerCountAggregate(t, h, 32<<10)
				defer e.Release()
				want, err := consumeWorkerCountBatches(t, h, e, workers, 32, groups)
				if err != nil {
					t.Fatal(err)
				}
				assertWorkerCounts(t, e, want)
				if groups == 2048 && e.Stats().SpillPasses == 0 {
					t.Fatal("expected bounded partial-state spill")
				}
				if h.mm.Peak() > 64<<10 {
					t.Fatalf("managed peak=%d exceeds hard limit", h.mm.Peak())
				}
				e.Release()
				if h.temp.Used() != 0 || h.temp.OwnedFiles() != 0 || h.mm.Used() != 0 {
					t.Fatalf("ownership leak: memory=%d temp=%d files=%d", h.mm.Used(), h.temp.Used(), h.temp.OwnedFiles())
				}
			})
		}
	}
}

func TestExternalAggregateEmitMergeFallsBackToSpill(t *testing.T) {
	h, cleanup := newExtAggHarness(t, 64<<10, 64<<20)
	defer cleanup()
	// Consume stays below the budget, but combining the disjoint worker keys
	// requires more capacity than the hard limit permits while sources are live.
	e := newWorkerCountAggregate(t, h, 64<<10)
	defer e.Release()
	want, err := consumeWorkerCountBatches(t, h, e, 4, 8, 512)
	if err != nil {
		t.Fatal(err)
	}
	if e.Stats().SpillPasses != 0 {
		t.Fatal("unexpected spill before final merge")
	}
	assertWorkerCounts(t, e, want)
	if e.Stats().SpillPasses == 0 {
		t.Fatal("expected merge preparation to fall back to spill")
	}
}

func TestExternalAggregateWorkerSpillFailureReleasesState(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "quota", true: "cancel"}[cancel], func(t *testing.T) {
			tempLimit := int64(64 << 20)
			if !cancel {
				tempLimit = 1
			}
			h, cleanup := newExtAggHarness(t, 64<<10, tempLimit)
			defer cleanup()
			e := newWorkerCountAggregate(t, h, 64<<10)
			defer e.Release()
			if _, err := consumeWorkerCountBatches(t, h, e, 4, 8, 512); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if cancel {
				var stop context.CancelFunc
				ctx, stop = context.WithCancel(ctx)
				stop()
			}
			var err error
			if cancel {
				// Exercise cancellation after entering the spill transition.
				e.shardGate.Lock()
				err = e.spillShardsLocked(ctx)
				e.shardGate.Unlock()
			} else {
				err = e.Emit(ctx, func(chunk *vector.DataChunk) error { chunk.Release(); return nil })
			}
			if err == nil || cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("spill error=%v", err)
			}
			if !cancel && !errors.Is(err, storagetemp.ErrQuotaExceeded) {
				t.Fatalf("spill error=%v want temp quota failure", err)
			}
			e.Release()
			if h.mm.Used() != 0 || h.temp.Used() != 0 || h.temp.OwnedFiles() != 0 {
				t.Fatalf("ownership leak: memory=%d temp=%d files=%d", h.mm.Used(), h.temp.Used(), h.temp.OwnedFiles())
			}
		})
	}
}
