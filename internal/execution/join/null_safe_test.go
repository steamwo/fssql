package join

import (
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

func TestNullSafeExternalJoinResidentAndSpill(t *testing.T) {
	for _, budget := range []int64{4 << 20, 4096} {
		for _, kind := range []Type{Inner, Left, Anti} {
			t.Run(fmt.Sprintf("%d/%d", budget, kind), func(t *testing.T) {
				manager, buffers, quotas, q := newStableExternalHarness(t)
				right := makeGeneratedChunks(t, q.Memory, buffers, 3000, 1, 0, nil)
				for _, chunk := range right {
					for row := 0; row < chunk.Cardinality; row++ {
						chunk.Columns[0].SetNull(row)
					}
				}
				left := makeIntChunk(t, q.Memory, buffers, []nullableInt{nullInt(), nullInt(), ni(1)}, []nullableInt{ni(10), ni(20), ni(30)})
				types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
				join, err := NewStableExternal(ExternalConfig{Type: kind, NullsEqual: true, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: budget, RadixBits: 4, MaxDepth: 3, MinReductionPercent: 10, SpillBlockBytes: 1024, Scope: q.Memory, Buffers: buffers, Temp: q.Temp})
				if err != nil {
					t.Fatal(err)
				}
				collector := &rowCollector{}
				build := &pipeline.Pipeline{ID: "null-safe-build", Source: &chunkSource{chunks: right}, Sink: join.BuildSink()}
				probe := &pipeline.Pipeline{ID: "null-safe-probe", Source: &chunkSource{chunks: []*vector.DataChunk{left}}, Sink: join.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
				if err := pipeline.RunDAG(q.Context(), probe, 4, q); err != nil {
					join.Close()
					_ = q.Close()
					t.Fatal(err)
				}
				want := 6000
				if kind == Left {
					want++
				}
				if kind == Anti {
					want = 1
				}
				if len(collector.rows) != want {
					t.Fatalf("rows=%d want=%d", len(collector.rows), want)
				}
				stats := join.Stats()
				if budget == 4096 && (stats.SpilledBuildPartitions == 0 || stats.TerminalSkewPartitions == 0) {
					t.Fatalf("expected spilled NULL skew: %+v", stats)
				}
				join.Close()
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
				if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
					t.Fatalf("memory=%d temp=%d", manager.Used(), quotas.GlobalUsed())
				}
			})
		}
	}
}
