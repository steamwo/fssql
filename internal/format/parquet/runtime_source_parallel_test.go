package parquet

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

type parallelProbeScanner struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	value   int64
	started chan<- struct{}
	release <-chan struct{}
	emitted bool
	closed  *atomic.Int64
}

func (s *parallelProbeScanner) Next(ctx context.Context) (*vector.DataChunk, error) {
	if s.emitted {
		return nil, io.EOF
	}
	s.emitted = true
	select {
	case s.started <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, s.value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, nil
}

func (s *parallelProbeScanner) Close() {
	if s.closed != nil {
		s.closed.Add(1)
	}
}

func TestRuntimeSourceRowGroupsExecuteOutsideOwnershipMutex(t *testing.T) {
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var closed atomic.Int64
	groups := []RowGroup{{NumRows: 1}, {NumRows: 2}, {NumRows: 3}, {NumRows: 4}}
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: groups,
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return &parallelProbeScanner{scope: scope, buffers: buffers, value: rg.NumRows, started: started, release: release, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "parallel.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		chunk *vector.DataChunk
		ok    bool
		err   error
	}
	results := make(chan result, 4)
	for i := 0; i < 4; i++ {
		go func() {
			chunk, ok, err := source.Next(context.Background())
			results <- result{chunk: chunk, ok: ok, err: err}
		}()
	}

	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			for j := 0; j < 4; j++ {
				res := <-results
				if res.chunk != nil {
					res.chunk.Release()
				}
			}
			t.Fatal("row-group scanner Next calls remained serialized")
		}
	}
	close(release)
	seen := map[int64]struct{}{}
	for i := 0; i < 4; i++ {
		res := <-results
		if res.err != nil || !res.ok || res.chunk == nil {
			t.Fatalf("result %d chunk=%v ok=%v err=%v", i, res.chunk, res.ok, res.err)
		}
		value, valid := res.chunk.Columns[0].Int64(0)
		res.chunk.Release()
		if !valid {
			t.Fatal("unexpected NULL")
		}
		seen[value] = struct{}{}
	}
	if len(seen) != 4 {
		t.Fatalf("seen=%v", seen)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 4 {
		t.Fatalf("closed=%d want=4", closed.Load())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeSourceCloseWaitsForInflightScanner(t *testing.T) {
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var closed atomic.Int64
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: []RowGroup{{NumRows: 1}},
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return &parallelProbeScanner{scope: scope, buffers: buffers, value: rg.NumRows, started: started, release: release, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "close-inflight.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	nextDone := make(chan error, 1)
	go func() {
		chunk, _, err := source.Next(context.Background())
		if chunk != nil {
			chunk.Release()
		}
		nextDone <- err
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- source.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before inflight scanner exited: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-nextDone; err == nil {
		t.Fatal("inflight Next should observe closed source")
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if closed.Load() != 1 {
		t.Fatalf("closed=%d want=1", closed.Load())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
}

type benchmarkRuntimeScanner struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	value   int64
	emitted bool
}

func (s *benchmarkRuntimeScanner) Next(context.Context) (*vector.DataChunk, error) {
	if s.emitted {
		return nil, io.EOF
	}
	s.emitted = true
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, s.value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, nil
}
func (s *benchmarkRuntimeScanner) Close() {}

func BenchmarkRuntimeSourceParallelRowGroups(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		manager := memory.NewManager(16 << 20)
		scope := manager.NewScope()
		buffers := memory.NewBufferManager(nil)
		groups := make([]RowGroup, 256)
		for j := range groups {
			groups[j].NumRows = int64(j + 1)
		}
		prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
			return runtimePreparedFile{rowGroups: groups, newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return &benchmarkRuntimeScanner{scope: scope, buffers: buffers, value: rg.NumRows}, nil
			}}, nil
		}
		source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(b, "bench-parallel.parquet")}, prepare)
		if err != nil {
			b.Fatal(err)
		}
		var wg sync.WaitGroup
		errCh := make(chan error, 4)
		for worker := 0; worker < 4; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					chunk, ok, err := source.Next(context.Background())
					if err != nil {
						errCh <- err
						return
					}
					if !ok {
						return
					}
					chunk.Release()
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := source.Close(); err != nil {
			b.Fatal(err)
		}
		if manager.Used() != 0 {
			b.Fatalf("managed leak=%d", manager.Used())
		}
		if err := scope.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
