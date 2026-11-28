package join

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
)

type vectorTestJoin interface {
	BuildSink() pipeline.Sink
	ProbeSink(pipeline.Sink) pipeline.Sink
	SetRequiredOutputColumns([]bool) error
	Close()
}

func newVectorTestJoin(t testing.TB, q *query.Context, bm *memory.BufferManager, external bool, typ Type, ts []types.LogicalType) vectorTestJoin {
	t.Helper()
	if external {
		j, err := NewStableExternal(ExternalConfig{Type: typ, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm, Temp: q.Temp, MemoryBudget: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	j, err := New(Config{Type: typ, LeftTypes: ts, RightTypes: ts, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestMemoryJoinDirectBigIntRepresentations(t *testing.T) {
	for _, unsigned := range []bool{false, true} {
		for _, representation := range []string{"flat", "dictionary", "constant"} {
			t.Run(fmt.Sprintf("unsigned%v/%s", unsigned, representation), func(t *testing.T) {
				_, bm, _, q := newStableExternalHarness(t)
				typ := types.LogicalType{ID: types.BigInt, Unsigned: unsigned}
				// Duplicate keys, both signed boundaries and NULLs must retain
				// exactly the same multiplicity as an independent nested loop.
				values := []uint64{0, 1 << 63, ^uint64(0), 1 << 63, 19, 0}
				build := denseTestChunk(t, q.Memory, bm, typ, values, map[int]bool{5: true}, "flat")
				probe := denseTestChunk(t, q.Memory, bm, typ, values, map[int]bool{5: true}, representation)
				want := denseReferenceRows(probe, build, false)
				j := newVectorTestJoin(t, q, bm, false, Inner, []types.LogicalType{typ, {ID: types.Varchar}})
				mask := []bool{false, true, false, true}
				if err := j.SetRequiredOutputColumns(mask); err != nil {
					t.Fatal(err)
				}
				collector := &batchJoinCollector{mask: mask}
				if err := j.BuildSink().Consume(q.Context(), build); err != nil {
					t.Fatal(err)
				}
				build.Release()
				if err := j.BuildSink().Finalize(q.Context()); err != nil {
					t.Fatal(err)
				}
				sink := j.ProbeSink(collector)
				if err := sink.Consume(q.Context(), probe); err != nil {
					t.Fatal(err)
				}
				probe.Release()
				if err := sink.Finalize(q.Context()); err != nil {
					t.Fatal(err)
				}
				sort.Strings(collector.rows)
				if !reflect.DeepEqual(collector.rows, want) {
					t.Fatalf("got=%v want=%v", collector.rows, want)
				}
				j.Close()
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestJoinReusesManagedOutputAndStringHeap(t *testing.T) {
	for _, external := range []bool{false, true} {
		for _, typ := range []Type{Inner, Left} {
			t.Run(fmt.Sprintf("external%v/type%d", external, typ), func(t *testing.T) {
				manager, _, _, q := newStableExternalHarness(t)
				allocations := 0
				bm := memory.NewBufferManager(func(n int) ([]byte, error) { allocations++; return make([]byte, n), nil })
				keyType := types.LogicalType{ID: types.BigInt}
				ts := []types.LogicalType{keyType, {ID: types.Varchar}}
				build := denseTestChunk(t, q.Memory, bm, keyType, []uint64{1, 1, 2}, nil, "flat")
				probe := denseTestChunk(t, q.Memory, bm, keyType, []uint64{1, 2, 9, 1, 0}, map[int]bool{4: true}, "flat")
				j := newVectorTestJoin(t, q, bm, external, typ, ts)
				mask := []bool{false, true, false, true}
				if err := j.SetRequiredOutputColumns(mask); err != nil {
					t.Fatal(err)
				}
				collector := &batchJoinCollector{mask: mask}
				if err := j.BuildSink().Consume(q.Context(), build); err != nil {
					t.Fatal(err)
				}
				build.Release()
				if err := j.BuildSink().Finalize(q.Context()); err != nil {
					t.Fatal(err)
				}
				sink := j.ProbeSink(collector)
				if err := sink.Consume(q.Context(), probe); err != nil {
					t.Fatal(err)
				}
				want := append([]string(nil), collector.rows...)
				beforeAlloc, beforeManaged := allocations, manager.Used()
				for i := 0; i < 50; i++ {
					probe.Cardinality = 1
					collector.rows = nil
					if err := sink.Consume(q.Context(), probe); err != nil {
						t.Fatal(err)
					}
					if len(collector.rows) != 2 {
						t.Fatalf("partial output rows=%d want=2", len(collector.rows))
					}
					probe.Cardinality = 5
					collector.rows = nil
					if err := sink.Consume(q.Context(), probe); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(collector.rows, want) {
						t.Fatalf("reused output changed: %v want %v", collector.rows, want)
					}
				}
				if allocations != beforeAlloc || manager.Used() != beforeManaged {
					t.Fatalf("output grew: allocations %d -> %d, managed %d -> %d", beforeAlloc, allocations, beforeManaged, manager.Used())
				}
				probe.Release()
				if err := sink.Finalize(q.Context()); err != nil {
					t.Fatal(err)
				}
				j.Close()
				if err := q.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type retainedJoinSink struct{ chunks []*vector.DataChunk }

func (s *retainedJoinSink) Consume(_ context.Context, c *vector.DataChunk) error {
	s.chunks = append(s.chunks, c)
	return nil
}
func (*retainedJoinSink) Finalize(context.Context) error { return nil }
func (*retainedJoinSink) TakesChunkOwnership() bool      { return true }

type movingJoinSink struct{ batches int }

func (*movingJoinSink) Consume(context.Context, *vector.DataChunk) error {
	return errors.New("join bypassed owned-input move path")
}
func (s *movingJoinSink) ConsumeOwned(_ context.Context, c *vector.DataChunk) error {
	s.batches++
	c.Release()
	return nil
}
func (*movingJoinSink) Finalize(context.Context) error { return nil }

func TestJoinPreservesOwnedInputMovePath(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprint(external), func(t *testing.T) {
			_, bm, _, q := newStableExternalHarness(t)
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j := newVectorTestJoin(t, q, bm, external, Inner, ts)
			build := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(1)})
			probe := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(1)})
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			collector := &movingJoinSink{}
			sink := j.ProbeSink(collector)
			for i := 0; i < 3; i++ {
				if err := sink.Consume(q.Context(), probe); err != nil {
					t.Fatal(err)
				}
			}
			probe.Release()
			if err := sink.Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			if collector.batches != 3 {
				t.Fatalf("owned batches=%d", collector.batches)
			}
			j.Close()
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJoinOutputOwnershipSurvivesReuseAndClose(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprint(external), func(t *testing.T) {
			_, bm, _, q := newStableExternalHarness(t)
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j := newVectorTestJoin(t, q, bm, external, Inner, ts)
			build := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(100)})
			probe := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(10)})
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			collector := &retainedJoinSink{}
			sink := j.ProbeSink(collector)
			for i := 0; i < 3; i++ {
				probe.Columns[1].SetInt64(0, int64(10+i))
				if err := sink.Consume(q.Context(), probe); err != nil {
					t.Fatal(err)
				}
			}
			probe.Release()
			if err := sink.Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			j.Close()
			for i, c := range collector.chunks {
				value, valid := c.Columns[1].Int64(0)
				if !valid || value != int64(10+i) {
					t.Fatalf("retained batch %d changed: %d valid=%v", i, value, valid)
				}
				c.Release()
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJoinOutputAllocationFailureReleasesScratch(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprint(external), func(t *testing.T) {
			_, _, _, q := newStableExternalHarness(t)
			armed := false
			failure := errors.New("output allocation failed")
			bm := memory.NewBufferManager(func(n int) ([]byte, error) {
				if armed {
					return nil, failure
				}
				return make([]byte, n), nil
			})
			ts := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
			j := newVectorTestJoin(t, q, bm, external, Inner, ts)
			build := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(1)})
			probe := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(1)})
			if err := j.BuildSink().Consume(q.Context(), build); err != nil {
				t.Fatal(err)
			}
			build.Release()
			if err := j.BuildSink().Finalize(q.Context()); err != nil {
				t.Fatal(err)
			}
			armed = true
			if err := j.ProbeSink(&countSink{}).Consume(q.Context(), probe); !errors.Is(err, failure) {
				t.Fatalf("err=%v want allocation failure", err)
			}
			probe.Release()
			j.Close()
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutputPoolClosePreservesActiveLease(t *testing.T) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	ts := []types.LogicalType{{ID: types.BigInt}}
	layout, err := exechash.NewRelocatableRowLayout(ts)
	if err != nil {
		t.Fatal(err)
	}
	var pool outputPool
	e, err := pool.emitter(scope, bm, Inner, ts, ts, RightSide, layout, nil, &countSink{})
	if err != nil {
		t.Fatal(err)
	}
	e.count = 1
	if err := e.ensure(); err != nil {
		t.Fatal(err)
	}
	e.cols[0].SetInt64(0, 42)
	pool.close()
	if value, ok := e.cols[0].Int64(0); !ok || value != 42 {
		t.Fatal("pool closed an active output")
	}
	pool.releaseEmitter(e)
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("managed leak=%d", m.Used())
	}
}
