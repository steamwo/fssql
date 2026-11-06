package kernel

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
)

type Result struct {
	Rows        int64
	Sum         int64
	ManagedPeak int64
	ManagedEnd  int64
}

func Vector10M(rows int64) (Result, error) {
	if rows < 0 {
		return Result{}, fmt.Errorf("negative rows")
	}
	m := memory.NewManager(64 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	var sum int64
	var done int64
	for done < rows {
		n := int64(vector.DefaultVectorSize)
		if rows-done < n {
			n = rows - done
		}
		v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, int(n))
		if err != nil {
			return Result{}, err
		}
		for i := 0; i < int(n); i++ {
			v.SetInt64(i, done+int64(i))
		}
		for i := 0; i < int(n); i++ {
			x, _ := v.Int64(i)
			sum += x
		}
		done += n
		v.Release()
	}
	if err := scope.Close(); err != nil {
		return Result{}, err
	}
	return Result{Rows: rows, Sum: sum, ManagedPeak: m.Peak(), ManagedEnd: m.Used()}, nil
}

func MemoryAccounting() (Result, error) {
	m := memory.NewManager(1 << 20)
	scope := m.NewScope()
	bm := memory.NewBufferManager(nil)
	v, err := vector.NewFlat(scope, bm, types.LogicalType{ID: types.BigInt}, vector.DefaultVectorSize)
	if err != nil {
		return Result{}, err
	}
	peak := m.Peak()
	v.Release()
	if err := scope.Close(); err != nil {
		return Result{}, err
	}
	return Result{Rows: vector.DefaultVectorSize, ManagedPeak: peak, ManagedEnd: m.Used()}, nil
}

type demoSource struct {
	mu        sync.Mutex
	next, end int64
	scope     *memory.Scope
	bm        *memory.BufferManager
}

func (s *demoSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= s.end {
		return nil, false, nil
	}
	n := int64(vector.DefaultVectorSize)
	if s.end-s.next < n {
		n = s.end - s.next
	}
	v, err := vector.NewFlat(s.scope, s.bm, types.LogicalType{ID: types.BigInt}, int(n))
	if err != nil {
		return nil, false, err
	}
	for i := 0; i < int(n); i++ {
		v.SetInt64(i, s.next+int64(i))
	}
	s.next += n
	c, _ := vector.NewDataChunk(v)
	return c, true, nil
}

type demoSink struct {
	mu        sync.Mutex
	sum, rows int64
}

func (s *demoSink) Consume(_ context.Context, c *vector.DataChunk) error {
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
func (s *demoSink) Finalize(context.Context) error { return nil }
func PushPipeline(rows int64, workers int) (Result, error) {
	m := memory.NewManager(64 << 20)
	q := query.New(context.Background(), m)
	bm := memory.NewBufferManager(nil)
	sink := &demoSink{}
	p := &pipeline.Pipeline{ID: "kernel-demo", Source: &demoSource{end: rows, scope: q.Memory, bm: bm}, Sink: sink}
	if err := p.Run(q.Context(), workers, q); err != nil {
		return Result{}, err
	}
	if err := q.Close(); err != nil {
		return Result{}, err
	}
	return Result{Rows: sink.rows, Sum: sink.sum, ManagedPeak: m.Peak(), ManagedEnd: m.Used()}, nil
}
