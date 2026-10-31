package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/query"
)

type intSource struct {
	mu        sync.Mutex
	next, end int
	scope     *memory.Scope
	bm        *memory.BufferManager
}

func (s *intSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= s.end {
		return nil, false, nil
	}
	n := s.end - s.next
	if n > vector.DefaultVectorSize {
		n = vector.DefaultVectorSize
	}
	v, err := vector.NewFlat(s.scope, s.bm, types.LogicalType{ID: types.BigInt}, n)
	if err != nil {
		return nil, false, err
	}
	for i := 0; i < n; i++ {
		v.SetInt64(i, int64(s.next+i))
	}
	s.next += n
	c, _ := vector.NewDataChunk(v)
	return c, true, nil
}

type addOne struct{}

func (addOne) Process(_ context.Context, c *vector.DataChunk) error {
	for i := 0; i < c.Cardinality; i++ {
		x, ok := c.Columns[0].Int64(i)
		if ok {
			c.Columns[0].SetInt64(i, x+1)
		}
	}
	return nil
}

type sumSink struct {
	mu   sync.Mutex
	sum  int64
	rows int
}

func (s *sumSink) Consume(_ context.Context, c *vector.DataChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < c.Cardinality; i++ {
		x, ok := c.Columns[0].Int64(i)
		if ok {
			s.sum += x
			s.rows++
		}
	}
	return nil
}
func (s *sumSink) Finalize(context.Context) error { return nil }
func runRows(t *testing.T, workers int) (int64, int) {
	m := memory.NewManager(1 << 20)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	src := &intSource{end: 10000, scope: q.Memory, bm: bm}
	sink := &sumSink{}
	p := &Pipeline{ID: "p", Source: src, Operators: []Operator{addOne{}}, Sink: sink}
	if err := p.Run(q.Context(), workers, q); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatalf("memory leak %d", m.Used())
	}
	return sink.sum, sink.rows
}
func TestSingleWorkerEqualsMultiWorker(t *testing.T) {
	a, ar := runRows(t, 1)
	b, br := runRows(t, 4)
	if a != b || ar != br {
		t.Fatalf("single=(%d,%d) multi=(%d,%d)", a, ar, b, br)
	}
}

type cancelSource struct {
	once  sync.Once
	scope *memory.Scope
	bm    *memory.BufferManager
}

func (s *cancelSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	var c *vector.DataChunk
	var err error
	made := false
	s.once.Do(func() {
		v, e := vector.NewFlat(s.scope, s.bm, types.LogicalType{ID: types.BigInt}, 1)
		if e != nil {
			err = e
			return
		}
		v.SetInt64(0, 1)
		c, _ = vector.NewDataChunk(v)
		made = true
	})
	if made {
		return c, true, err
	}
	<-ctx.Done()
	return nil, false, ctx.Err()
}

type cancelOp struct{ cancel context.CancelFunc }

func (o cancelOp) Process(context.Context, *vector.DataChunk) error {
	o.cancel()
	return context.Canceled
}
func TestPipelineCancelCleanup(t *testing.T) {
	m := memory.NewManager(1024)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	ctx, cancel := context.WithCancel(q.Context())
	p := &Pipeline{ID: "cancel", Source: &cancelSource{scope: q.Memory, bm: bm}, Operators: []Operator{cancelOp{cancel}}, Sink: &sumSink{}}
	err := p.Run(ctx, 1, q)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
	if q.ActiveTasks() != 0 {
		t.Fatalf("task leak %d", q.ActiveTasks())
	}
	if m.Used() != 0 {
		t.Fatalf("buffer/reservation leak %d", m.Used())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestDAGOrderAndCycle(t *testing.T) {
	m := memory.NewManager(1024)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	mk := func(id string) *Pipeline {
		return &Pipeline{ID: id, Source: &intSource{end: 1, scope: q.Memory, bm: bm}, Sink: &sumSink{}}
	}
	a := mk("a")
	b := mk("b")
	b.Dependencies = []*Pipeline{a}
	if err := RunDAG(q.Context(), b, 1, q); err != nil {
		t.Fatal(err)
	}
	c := mk("c")
	d := mk("d")
	c.Dependencies = []*Pipeline{d}
	d.Dependencies = []*Pipeline{c}
	ctx, cancel := context.WithTimeout(q.Context(), time.Second)
	defer cancel()
	if err := RunDAG(ctx, c, 1, q); err == nil {
		t.Fatal("expected cycle")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
