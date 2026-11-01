package asyncio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var ErrClosed = errors.New("asyncio: manager closed")

type Request struct {
	Driver        vfs.Driver
	Range         vfs.RangeRequest
	Memory        *memory.Scope
	BufferManager *memory.BufferManager
}

type Result struct {
	Range vfs.RangeResult
	Err   error
}

type Future struct {
	result <-chan Result
}

func (f Future) Wait(ctx context.Context) (vfs.RangeResult, error) {
	select {
	case <-ctx.Done():
		return vfs.RangeResult{}, ctx.Err()
	case r := <-f.result:
		return r.Range, r.Err
	}
}

type job struct {
	ctx    context.Context
	req    Request
	result chan<- Result
}

type Manager struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan job
	wg     sync.WaitGroup
	mu     sync.RWMutex
	closed bool
	active atomic.Int64
}

func NewManager(workers, queue int) *Manager {
	if workers <= 0 {
		workers = 4
	}
	if queue <= 0 {
		queue = workers * 2
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{ctx: ctx, cancel: cancel, jobs: make(chan job, queue)}
	m.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go m.worker()
	}
	return m
}

func (m *Manager) Submit(ctx context.Context, req Request) Future {
	out := make(chan Result, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	if req.Driver == nil || req.Memory == nil || req.BufferManager == nil {
		out <- Result{Err: errors.New("asyncio: incomplete range request")}
		return Future{result: out}
	}
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		out <- Result{Err: ErrClosed}
		return Future{result: out}
	}
	select {
	case <-ctx.Done():
		m.mu.RUnlock()
		out <- Result{Err: ctx.Err()}
	case <-m.ctx.Done():
		m.mu.RUnlock()
		out <- Result{Err: ErrClosed}
	case m.jobs <- job{ctx: ctx, req: req, result: out}:
		m.mu.RUnlock()
	}
	return Future{result: out}
}

func (m *Manager) SubmitReadAhead(ctx context.Context, req Request) Future {
	req.Range.Tag = memory.TagReadAhead
	return m.Submit(ctx, req)
}

func (m *Manager) Active() int64 { return m.active.Load() }

func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.cancel()
	close(m.jobs)
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *Manager) worker() {
	defer m.wg.Done()
	for j := range m.jobs {
		select {
		case <-j.ctx.Done():
			j.result <- Result{Err: j.ctx.Err()}
			continue
		case <-m.ctx.Done():
			j.result <- Result{Err: ErrClosed}
			continue
		default:
		}
		ctx, cancel := context.WithCancel(j.ctx)
		stop := context.AfterFunc(m.ctx, cancel)
		m.active.Add(1)
		res, err := j.req.Driver.ReadRange(ctx, j.req.Range, j.req.Memory, j.req.BufferManager)
		m.active.Add(-1)
		stop()
		cancel()
		if err != nil && errors.Is(ctx.Err(), context.Canceled) && m.ctx.Err() != nil && j.ctx.Err() == nil {
			err = ErrClosed
		}
		j.result <- Result{Range: res, Err: err}
	}
}
