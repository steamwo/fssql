package join

import (
	"context"
	"fmt"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type benchmarkJoinSink struct{ rows int }

func (s *benchmarkJoinSink) Consume(_ context.Context, c *vector.DataChunk) error {
	s.rows += c.Cardinality
	return nil
}
func (*benchmarkJoinSink) Finalize(context.Context) error { return nil }

// BenchmarkResidentProbeBatch 使用相同的预哈希输入和驻留表比较探测内核。
// 两条路径共用输出收集器；结果仅用于内核诊断，不代表端到端 SQL 耗时。
func BenchmarkResidentProbeBatch(b *testing.B) {
	for _, keyMod := range []int{1000, 113} {
		for _, payload := range []bool{false, true} {
			b.Run(fmt.Sprintf("keys%d/payload%v", keyMod, payload), func(b *testing.B) {
				manager := memory.NewManager(64 << 20)
				bm := memory.NewBufferManager(nil)
				quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 64 << 20, DefaultResourceGroup: 64 << 20, DefaultQuery: 64 << 20})
				storage, err := storagetemp.NewTemporaryStorageManager(b.TempDir(), quotas)
				if err != nil {
					b.Fatal(err)
				}
				temp, err := storage.NewQueryScope("bench")
				if err != nil {
					b.Fatal(err)
				}
				q := query.NewWithTemp(context.Background(), manager, temp)
				ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
				j, err := NewExternal(ExternalConfig{Type: Inner, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, MemoryBudget: 16 << 20, Scope: q.Memory, Buffers: bm, Temp: temp})
				if err != nil {
					b.Fatal(err)
				}
				build := makeGeneratedChunks(b, q.Memory, bm, 1000, keyMod, 0, nil)[0]
				probe := makeGeneratedChunks(b, q.Memory, bm, vector.DefaultVectorSize, keyMod, 10000, nil)[0]
				if err := j.BuildSink().Consume(q.Context(), build); err != nil {
					b.Fatal(err)
				}
				if err := j.BuildSink().Finalize(q.Context()); err != nil {
					b.Fatal(err)
				}
				build.Release()
				hashes := make([]uint64, probe.Cardinality)
				if err := exechash.HashChunk(probe, j.probeKeys, hashes); err != nil {
					b.Fatal(err)
				}
				comparer, err := exechash.NewChunkKeyComparer(probe, j.probeKeys)
				if err != nil {
					b.Fatal(err)
				}
				for _, batch := range []bool{false, true} {
					name := "scalar"
					if batch {
						name = "batch"
					}
					b.Run(name, func(b *testing.B) {
						sink := &benchmarkJoinSink{}
						b.ReportAllocs()
						b.ResetTimer()
						for iteration := 0; iteration < b.N; iteration++ {
							emitter := newBatchEmitter(Inner, q.Memory, bm, ts, ts, j.buildSide, j.buildLayout, sink)
							if !payload {
								emitter.required = []bool{false, false, false, false}
							}
							if batch {
								if err := j.probeBatch(q.Context(), probe, hashes, comparer, &emitter, nil); err != nil {
									b.Fatal(err)
								}
							} else {
								for row, hash := range hashes {
									part, _ := j.partitionFor(hash, 0)
									p := &j.partitions[part]
									if p.index == nil {
										continue
									}
									index, ok := p.index.Head(hash)
									for ok {
										_, encoded, valid := p.resident.record(index)
										if !valid {
											b.Fatal("invalid record")
										}
										equal, err := comparer.Equal(row, j.buildLayout, encoded, j.buildKeys, exechash.NullsUnequal)
										if err != nil {
											b.Fatal(err)
										}
										if equal {
											if err := emitter.appendPair(q.Context(), probe, row, encoded); err != nil {
												b.Fatal(err)
											}
										}
										index, ok = p.index.Next(index)
									}
								}
							}
							if err := emitter.flush(q.Context()); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						if sink.rows == 0 {
							b.Fatal("no matches")
						}
					})
				}
				probe.Release()
				j.Close()
				if err := q.Close(); err != nil {
					b.Fatal(err)
				}
				if manager.Used() != 0 || quotas.GlobalUsed() != 0 {
					b.Fatal("resource leak")
				}
			})
		}
	}
}
