package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type Context struct {
	ctx    context.Context
	cancel context.CancelFunc
	Memory *memory.Scope
	Temp   *storagetemp.QueryScope
	active atomic.Int64
	closed atomic.Bool
	// sourceSnapshot 在第一次绑定后保持不可变，确保同一 Statement 的导出不会读取漂移的数据集。
	sourceSnapshot atomic.Pointer[SourceSnapshot]
	cleanupMu      sync.Mutex
	cleanups       []func() error
}

func New(parent context.Context, m *memory.Manager) *Context {
	return NewWithTemp(parent, m, nil)
}

func NewWithTemp(parent context.Context, m *memory.Manager, tempScope *storagetemp.QueryScope) *Context {
	ctx, cancel := context.WithCancel(parent)
	return &Context{ctx: ctx, cancel: cancel, Memory: m.NewScope(), Temp: tempScope}
}
func (q *Context) Context() context.Context { return q.ctx }
func (q *Context) Cancel()                  { q.cancel() }
func (q *Context) TaskStarted()             { q.active.Add(1) }
func (q *Context) TaskFinished()            { q.active.Add(-1) }
func (q *Context) ActiveTasks() int64       { return q.active.Load() }

// AddCleanup registers query-scoped ownership that must be released before the
// temporary scope and memory scope are closed. Callbacks run in reverse order.
func (q *Context) AddCleanup(cleanup func() error) error {
	if q == nil || cleanup == nil {
		return fmt.Errorf("query cleanup is nil")
	}
	q.cleanupMu.Lock()
	defer q.cleanupMu.Unlock()
	if q.closed.Load() {
		return fmt.Errorf("query is closed")
	}
	q.cleanups = append(q.cleanups, cleanup)
	return nil
}

func (q *Context) Close() error {
	if q == nil {
		return nil
	}
	q.cancel()
	if q.ActiveTasks() != 0 {
		return fmt.Errorf("query close with %d active tasks", q.ActiveTasks())
	}
	q.cleanupMu.Lock()
	if q.closed.Load() {
		q.cleanupMu.Unlock()
		return nil
	}
	q.closed.Store(true)
	cleanups := append([]func() error(nil), q.cleanups...)
	q.cleanups = nil
	q.cleanupMu.Unlock()

	var errs []error
	for i := len(cleanups) - 1; i >= 0; i-- {
		if err := cleanups[i](); err != nil {
			errs = append(errs, err)
		}
	}
	if q.Temp != nil {
		if err := q.Temp.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if q.Memory != nil {
		if err := q.Memory.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (q *Context) Closed() bool { return q == nil || q.closed.Load() }
