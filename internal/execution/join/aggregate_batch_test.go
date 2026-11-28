package join

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/aggregate"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
)

type joinAggregateSink struct {
	*aggregate.HashAggregate
	batches      int
	materialized int
}

func (s *joinAggregateSink) Finalize(context.Context) error { return nil }
func (s *joinAggregateSink) Consume(ctx context.Context, c *vector.DataChunk) error {
	s.materialized++
	return s.HashAggregate.Consume(ctx, c)
}
func (s *joinAggregateSink) ConsumeJoinBatch(ctx context.Context, b pipeline.JoinBatch) (bool, error) {
	s.batches++
	return s.HashAggregate.ConsumeJoinBatch(ctx, b)
}

// Hide the optional batch interface to exercise the materialized reference.
type materializedJoinAggregate struct{ sink *joinAggregateSink }

func (s materializedJoinAggregate) Consume(ctx context.Context, c *vector.DataChunk) error {
	return s.sink.Consume(ctx, c)
}
func (s materializedJoinAggregate) Finalize(ctx context.Context) error { return s.sink.Finalize(ctx) }

func TestJoinGlobalAggregateMatchesMaterialized(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, typ := range []Type{Inner, Left, Right, Full, Semi, Anti} {
			for _, empty := range []bool{false, true} {
				t.Run(fmt.Sprintf("external%v/type%d/empty%v", external, typ, empty), func(t *testing.T) {
					var results [][]string
					for _, fused := range []bool{false, true} {
						_, bm, _, q := newStableExternalHarness(t)
						ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
						j := newVectorTestJoin(t, q, bm, external, typ, ts)
						input := append(append([]types.LogicalType(nil), ts...), ts...)
						specs := []aggregate.Spec{{Kind: aggregate.CountStar}, {Kind: aggregate.Count, Input: 1}, {Kind: aggregate.Sum, Input: 1}}
						if typ == Semi || typ == Anti {
							input = ts
						} else {
							specs = append(specs, aggregate.Spec{Kind: aggregate.Count, Input: 3}, aggregate.Spec{Kind: aggregate.Sum, Input: 3})
						}
						a, err := aggregate.NewHashAggregate(q.Memory, bm, input, nil, specs, 1)
						if err != nil {
							t.Fatal(err)
						}
						collector := &joinAggregateSink{HashAggregate: a}
						var downstream pipeline.Sink = collector
						if !fused {
							downstream = materializedJoinAggregate{collector}
						}
						probe := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1), ni(2), ni(9), nullInt()}, []nullableInt{ni(10), nullInt(), ni(90), ni(40)})
						build := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1), ni(1), ni(2), ni(8), nullInt()}, []nullableInt{ni(3), nullInt(), ni(5), ni(80), ni(99)})
						if !empty {
							if err := j.BuildSink().Consume(q.Context(), build); err != nil {
								t.Fatal(err)
							}
						}
						build.Release()
						if err := j.BuildSink().Finalize(q.Context()); err != nil {
							t.Fatal(err)
						}
						sink := j.ProbeSink(downstream)
						for i := 0; i < 2; i++ {
							if err := sink.Consume(q.Context(), probe); err != nil {
								t.Fatal(err)
							}
						}
						probe.Release()
						if err := sink.Finalize(q.Context()); err != nil {
							t.Fatal(err)
						}
						out, err := a.Finalize(q.Context())
						if err != nil {
							t.Fatal(err)
						}
						var row []string
						for _, col := range out.Columns {
							if !col.IsValid(0) {
								row = append(row, "NULL")
								continue
							}
							if col.Type.ID == types.HugeInt {
								v, _ := col.Int128(0)
								row = append(row, fmt.Sprint(v))
							} else {
								v, _ := col.Int64(0)
								row = append(row, fmt.Sprint(v))
							}
						}
						results = append(results, row)
						if fused && collector.materialized != 0 && !external {
							t.Fatal("in-memory join materialized fused output")
						}
						out.Release()
						a.Release()
						j.Close()
						if err := q.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if !reflect.DeepEqual(results[0], results[1]) {
						t.Fatalf("materialized=%v fused=%v", results[0], results[1])
					}
				})
			}
		}
	}
}

func BenchmarkJoinGlobalAggregate(b *testing.B) {
	for _, fused := range []bool{false, true} {
		b.Run(fmt.Sprintf("fused%v", fused), func(b *testing.B) {
			m := memory.NewManager(256 << 20)
			q := query.New(context.Background(), m)
			bm := memory.NewBufferManager(nil)
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j := newVectorTestJoin(b, q, bm, false, Inner, ts)
			if err := j.SetRequiredOutputColumns([]bool{false, false, false, true}); err != nil {
				b.Fatal(err)
			}
			a, err := aggregate.NewHashAggregate(q.Memory, bm, append(append([]types.LogicalType(nil), ts...), ts...), nil, []aggregate.Spec{{Kind: aggregate.CountStar}, {Kind: aggregate.Sum, Input: 3}}, 1)
			if err != nil {
				b.Fatal(err)
			}
			collector := &joinAggregateSink{HashAggregate: a}
			var downstream pipeline.Sink = collector
			if !fused {
				downstream = materializedJoinAggregate{collector}
			}
			build := makeGeneratedChunks(b, q.Memory, bm, 1000, 1000, 0, nil)[0]
			probe := makeGeneratedChunks(b, q.Memory, bm, vector.DefaultVectorSize, 1000, 0, nil)[0]
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				b.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				b.Fatal(err)
			}
			sink := j.ProbeSink(downstream)
			b.ReportAllocs()
			if err := sink.Consume(q.Context(), probe); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := sink.Consume(q.Context(), probe); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			probe.Release()
			if err := sink.Finalize(q.Context()); err != nil {
				b.Fatal(err)
			}
			out, err := a.Finalize(q.Context())
			if err != nil {
				b.Fatal(err)
			}
			count, ok := out.Columns[0].Int64(0)
			if !ok || count != int64(b.N+1)*vector.DefaultVectorSize {
				b.Fatalf("count=%d", count)
			}
			sum, ok := out.Columns[1].Int128(0)
			if !ok || sum != types.Int128FromInt64(int64(b.N+1)*1000128) {
				b.Fatalf("sum=%v", sum)
			}
			out.Release()
			a.Release()
			j.Close()
			if err := q.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
