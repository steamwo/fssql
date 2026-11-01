package query

import (
	"context"
	"fmt"
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
func (q *Context) Close() error {
	q.cancel()
	if q.ActiveTasks() != 0 {
		return fmt.Errorf("query close with %d active tasks", q.ActiveTasks())
	}
	if q.Temp != nil {
		if err := q.Temp.Close(); err != nil {
			return err
		}
	}
	if err := q.Memory.Close(); err != nil {
		return err
	}
	q.closed.Store(true)
	return nil
}
func (q *Context) Closed() bool { return q.closed.Load() }
