package join

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
)

type nullableInt struct {
	valid bool
	value int64
}

func ni(v int64) nullableInt { return nullableInt{valid: true, value: v} }
func nullInt() nullableInt   { return nullableInt{} }

type chunkSource struct {
	mu     sync.Mutex
	chunks []*vector.DataChunk
	next   int
}

func (s *chunkSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if s.next >= len(s.chunks) {
		return nil, false, nil
	}
	c := s.chunks[s.next]
	s.next++
	return c, true, nil
}

type rowCollector struct {
	rows      []string
	finalized bool
}

func formatInt(v *vector.Vector, row int) string {
	x, ok := v.Int64(row)
	if !ok {
		return "N"
	}
	return fmt.Sprintf("%d", x)
}
func (c *rowCollector) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	for r := 0; r < chunk.Cardinality; r++ {
		parts := make([]string, len(chunk.Columns))
		for i, v := range chunk.Columns {
			parts[i] = formatInt(v, r)
		}
		row := ""
		for i, p := range parts {
			if i > 0 {
				row += ","
			}
			row += p
		}
		c.rows = append(c.rows, row)
	}
	return nil
}
func (c *rowCollector) Finalize(context.Context) error { c.finalized = true; return nil }

func makeIntChunk(t testing.TB, scope *memory.Scope, bm *memory.BufferManager, keys, vals []nullableInt) *vector.DataChunk {
	t.Helper()
	k, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, len(keys))
	if err != nil {
		t.Fatal(err)
	}
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, len(keys))
	if err != nil {
		k.Release()
		t.Fatal(err)
	}
	for i := range keys {
		if keys[i].valid {
			k.SetInt64(i, keys[i].value)
		} else {
			k.SetNull(i)
		}
		if vals[i].valid {
			v.SetInt64(i, vals[i].value)
		} else {
			v.SetNull(i)
		}
	}
	chunk, err := vector.NewDataChunk(k, v)
	if err != nil {
		k.Release()
		v.Release()
		t.Fatal(err)
	}
	return chunk
}

func runJoin(t *testing.T, jt Type) ([]string, int64) {
	t.Helper()
	m := memory.NewManager(8 << 20)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	leftTypes := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	rightTypes := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
	cfg := Config{Type: jt, LeftTypes: leftTypes, RightTypes: rightTypes, Scope: q.Memory, Buffers: bm}
	if jt != Cross {
		cfg.LeftKeys = []int{0}
		cfg.RightKeys = []int{0}
	}
	j, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	left := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1), ni(2), ni(2), ni(3), nullInt()}, []nullableInt{ni(10), ni(20), ni(21), ni(30), ni(40)})
	right := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(2), ni(2), ni(4), nullInt()}, []nullableInt{ni(200), ni(201), ni(400), ni(401)})
	collector := &rowCollector{}
	build := &pipeline.Pipeline{ID: "join-build", Source: &chunkSource{chunks: []*vector.DataChunk{right}}, Sink: j.BuildSink()}
	probe := &pipeline.Pipeline{ID: "join-probe", Source: &chunkSource{chunks: []*vector.DataChunk{left}}, Sink: j.ProbeSink(collector), Dependencies: []*pipeline.Pipeline{build}}
	if err := pipeline.RunDAG(q.Context(), probe, 1, q); err != nil {
		t.Fatal(err)
	}
	if !collector.finalized {
		t.Fatal("downstream not finalized")
	}
	if err := q.Close(); err != nil {
		t.Fatalf("query lifecycle leak: %v", err)
	}
	return collector.rows, m.Peak()
}

func sorted(rows []string) []string {
	out := append([]string(nil), rows...)
	sort.Strings(out)
	return out
}
func assertRows(t *testing.T, got, want []string) {
	t.Helper()
	g, w := sorted(got), sorted(want)
	if len(g) != len(w) {
		t.Fatalf("row count=%d want=%d\ngot=%v", len(g), len(w), g)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("row[%d]=%q want=%q\ngot=%v\nwant=%v", i, g[i], w[i], g, w)
		}
	}
}

func TestJoinTypeSuite(t *testing.T) {
	inner := []string{"2,20,2,200", "2,20,2,201", "2,21,2,200", "2,21,2,201"}
	leftUnmatched := []string{"1,10,N,N", "3,30,N,N", "N,40,N,N"}
	rightUnmatched := []string{"N,N,4,400", "N,N,N,401"}
	cases := []struct {
		name string
		typ  Type
		want []string
	}{
		{"inner", Inner, inner},
		{"left", Left, append(append([]string{}, inner...), leftUnmatched...)},
		{"right", Right, append(append([]string{}, inner...), rightUnmatched...)},
		{"full", Full, append(append(append([]string{}, inner...), leftUnmatched...), rightUnmatched...)},
		{"semi", Semi, []string{"2,20", "2,21"}},
		{"anti", Anti, []string{"1,10", "3,30", "N,40"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { got, _ := runJoin(t, tc.typ); assertRows(t, got, tc.want) })
	}
}

func TestCrossJoin(t *testing.T) {
	rows, _ := runJoin(t, Cross)
	if len(rows) != 20 {
		t.Fatalf("cross rows=%d want 20", len(rows))
	}
}

func TestBuildCancellationReleasesManagedState(t *testing.T) {
	m := memory.NewManager(1 << 20)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	j, err := New(Config{Type: Inner, LeftTypes: []types.LogicalType{{ID: types.BigInt}}, RightTypes: []types.LogicalType{{ID: types.BigInt}}, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
	if err != nil {
		t.Fatal(err)
	}
	chunk := makeIntChunk(t, q.Memory, bm, []nullableInt{ni(1)}, []nullableInt{ni(9)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = j.BuildSink().Consume(ctx, chunk)
	chunk.Release()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("leak after cancel: %v", err)
	}
}

func TestBuildAllocationFailureReleasesState(t *testing.T) {
	m := memory.NewManager(700)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	j, err := New(Config{Type: Inner, LeftTypes: []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}, RightTypes: []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]nullableInt, 12)
	vals := make([]nullableInt, 12)
	for i := range keys {
		keys[i] = ni(int64(i))
		vals[i] = ni(int64(i))
	}
	chunk := makeIntChunk(t, q.Memory, bm, keys, vals)
	sink := j.BuildSink()
	err = sink.Consume(context.Background(), chunk)
	chunk.Release()
	if err == nil {
		err = sink.Finalize(context.Background())
	}
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("error=%v want memory limit", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("leak after allocation failure: %v", err)
	}
}

func BenchmarkInnerHashJoin2048(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := memory.NewManager(16 << 20)
		q := query.New(context.Background(), m)
		bm := memory.NewBufferManager(nil)
		types2 := []types.LogicalType{{ID: types.BigInt}, {ID: types.BigInt}}
		j, err := New(Config{Type: Inner, LeftTypes: types2, RightTypes: types2, LeftKeys: []int{0}, RightKeys: []int{0}, Scope: q.Memory, Buffers: bm})
		if err != nil {
			b.Fatal(err)
		}
		keys := make([]nullableInt, vector.DefaultVectorSize)
		vals := make([]nullableInt, vector.DefaultVectorSize)
		for r := range keys {
			keys[r] = ni(int64(r))
			vals[r] = ni(int64(r))
		}
		left := makeIntChunk(b, q.Memory, bm, keys, vals)
		right := makeIntChunk(b, q.Memory, bm, keys, vals)
		collector := &countSink{}
		build := j.BuildSink()
		if err := build.Consume(context.Background(), right); err != nil {
			b.Fatal(err)
		}
		right.Release()
		if err := build.Finalize(context.Background()); err != nil {
			b.Fatal(err)
		}
		probe := j.ProbeSink(collector)
		if err := probe.Consume(context.Background(), left); err != nil {
			b.Fatal(err)
		}
		left.Release()
		if err := probe.Finalize(context.Background()); err != nil {
			b.Fatal(err)
		}
		if collector.rows != vector.DefaultVectorSize {
			b.Fatalf("rows=%d", collector.rows)
		}
		if err := q.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

type countSink struct{ rows int }

func (s *countSink) Consume(_ context.Context, c *vector.DataChunk) error {
	s.rows += c.Cardinality
	return nil
}
func (s *countSink) Finalize(context.Context) error { return nil }
