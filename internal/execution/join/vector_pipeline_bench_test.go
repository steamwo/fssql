package join

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// BenchmarkJoinVectorPipeline times repeated probe batches against a frozen
// 1,000-row build. Fixtures, build, finalize and cleanup are outside the timer.
func BenchmarkJoinVectorPipeline(b *testing.B) {
	for _, external := range []bool{false, true} {
		for _, typ := range []Type{Inner, Left} {
			name := "memory/inner"
			if external {
				name = "stable/inner"
			}
			if typ == Left {
				name = name[:len(name)-5] + "left"
			}
			b.Run(name, func(b *testing.B) {
				m := memory.NewManager(256 << 20)
				storage, err := storagetemp.NewTemporaryStorageManager(b.TempDir(), storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20}))
				if err != nil {
					b.Fatal(err)
				}
				temp, err := storage.NewQueryScope("bench")
				if err != nil {
					b.Fatal(err)
				}
				q := query.NewWithTemp(context.Background(), m, temp)
				bm := memory.NewBufferManager(nil)
				ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
				var build, probe pipeline.Sink
				var closeJoin func()
				collector := &benchmarkJoinSink{}
				if external {
					j, err := NewStableExternal(ExternalConfig{Type: typ, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm, Temp: q.Temp, MemoryBudget: 256 << 20, RightEstimatedBytes: 16000, LeftEstimatedBytes: 1600000})
					if err != nil {
						b.Fatal(err)
					}
					build, probe, closeJoin = j.BuildSink(), j.ProbeSink(collector), j.Close
				} else {
					j, err := New(Config{Type: typ, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
					if err != nil {
						b.Fatal(err)
					}
					build, probe, closeJoin = j.BuildSink(), j.ProbeSink(collector), j.Close
				}
				right := makeGeneratedChunks(b, q.Memory, bm, 1000, 1000, 0, nil)[0]
				left := makeGeneratedChunks(b, q.Memory, bm, vector.DefaultVectorSize, 2000, 0, nil)[0]
				if err := build.Consume(q.Context(), right); err != nil {
					b.Fatal(err)
				}
				right.Release()
				if err := build.Finalize(q.Context()); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				// Populate optional query-local caches outside steady-state timing.
				if err := probe.Consume(q.Context(), left); err != nil {
					b.Fatal(err)
				}
				collector.rows = 0
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := probe.Consume(q.Context(), left); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(left.Cardinality), "rows/op")
				want := 1048 * b.N
				if typ == Left {
					want = left.Cardinality * b.N
				}
				if collector.rows != want {
					b.Fatalf("rows=%d want=%d", collector.rows, want)
				}
				left.Release()
				closeJoin()
				if err := q.Close(); err != nil {
					b.Fatal(err)
				}
			})
		}
	}
}
