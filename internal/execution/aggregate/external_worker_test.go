package aggregate

import (
	"context"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/blocking"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type aggregateChunkSource struct {
	mu     sync.Mutex
	chunks []*vector.DataChunk
	next   int
}

func (s *aggregateChunkSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.next >= len(s.chunks) {
		return nil, false, nil
	}
	chunk := s.chunks[s.next]
	s.chunks[s.next] = nil
	s.next++
	return chunk, true, nil
}

func makeAggregateChunks(t *testing.T, scope *memory.Scope, bm *memory.BufferManager, rows int) []*vector.DataChunk {
	t.Helper()
	var chunks []*vector.DataChunk
	for base := 0; base < rows; base += 256 {
		n := rows - base
		if n > 256 {
			n = 256
		}
		groups, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, n)
		if err != nil {
			t.Fatal(err)
		}
		values, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, n)
		if err != nil {
			groups.Release()
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			groups.SetInt64(i, int64((base+i)%97))
			values.SetInt64(i, int64(base+i+1))
		}
		chunk, err := vector.NewDataChunk(groups, values)
		if err != nil {
			groups.Release()
			values.Release()
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
	}
	return chunks
}

func recordAggregateChunk(out map[int64]extAggValue, chunk *vector.DataChunk) {
	for i := 0; i < chunk.Cardinality; i++ {
		group, _ := chunk.Columns[0].Int64(i)
		count, _ := chunk.Columns[1].Int64(i)
		sum, _ := chunk.Columns[2].Int128(i)
		avg, _ := chunk.Columns[3].Float64(i)
		min, _ := chunk.Columns[4].Int64(i)
		max, _ := chunk.Columns[5].Int64(i)
		out[group] = extAggValue{count: count, sum: sum, avg: avg, min: min, max: max}
	}
}

// TestExternalAggregateConcurrentWorkersMatchSerial drives Consume through the
// blocking bridge with four pipeline workers, covering worker-local shards,
// merge before emit, and partial-state spill when the budget is crossed mid-stream.
func TestExternalAggregateConcurrentWorkersMatchSerial(t *testing.T) {
	specs := []Spec{{Kind: CountStar}, {Kind: Sum, Input: 1}, {Kind: Avg, Input: 1}, {Kind: Min, Input: 1}, {Kind: Max, Input: 1}}
	types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	for _, tc := range []struct {
		name   string
		rows   int
		budget int64
		spill  bool
	}{
		{name: "resident-merge", rows: 2048, budget: 64 << 20},
		{name: "merge-spill", rows: 4096, budget: 8 << 10, spill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, cleanup := newExtAggHarness(t, 64<<20, 64<<20)
			defer cleanup()
			expectedAgg, err := NewHashAggregate(h.scope, h.bm, types2, []int{0}, specs, 128)
			if err != nil {
				t.Fatal(err)
			}
			consumeDataset(t, h.scope, h.bm, func(chunk *vector.DataChunk) error {
				return expectedAgg.Consume(context.Background(), chunk)
			}, tc.rows)
			expected := collectAgg(t, func(emit func(*vector.DataChunk) error) error {
				return expectedAgg.Emit(context.Background(), emit)
			})
			expectedAgg.Release()

			ext, err := NewExternalAggregate(ExternalConfig{
				InputTypes: types2, GroupColumns: []int{0}, Specs: specs, EstimatedGroups: 8,
				MemoryBudget: tc.budget, RadixBits: 4, MaxDepth: 4, SpillBlockBytes: 1024,
				Scope: h.scope, Buffers: h.bm, Temp: h.temp, Compression: storagetemp.CompressionRLE,
			})
			if err != nil {
				t.Fatal(err)
			}
			bridge, err := blocking.New(ext, 2)
			if err != nil {
				t.Fatal(err)
			}
			root := &pipeline.Pipeline{
				ID:     "concurrent-aggregate",
				Source: &aggregateChunkSource{chunks: makeAggregateChunks(t, h.scope, h.bm, tc.rows)},
				Sink:   bridge,
			}
			if err := pipeline.RunDAG(context.Background(), root, 4, nil); err != nil {
				bridge.Close()
				t.Fatal(err)
			}
			actual := map[int64]extAggValue{}
			for {
				chunk, ok, err := bridge.Next(context.Background())
				if err != nil {
					bridge.Close()
					t.Fatal(err)
				}
				if !ok {
					break
				}
				recordAggregateChunk(actual, chunk)
				chunk.Release()
			}
			stats := ext.Stats()
			if err := bridge.Close(); err != nil {
				t.Fatal(err)
			}
			if stats.PeakAggregateBytes == 0 {
				t.Fatalf("expected peak aggregate bytes: %+v", stats)
			}
			if tc.spill && stats.SpillPasses == 0 {
				t.Fatalf("expected concurrent merge to spill: %+v", stats)
			}
			if len(actual) != len(expected) {
				t.Fatalf("groups=%d want=%d", len(actual), len(expected))
			}
			for group, want := range expected {
				if got := actual[group]; got != want {
					t.Fatalf("group %d=%+v want %+v", group, got, want)
				}
			}
		})
	}
}
