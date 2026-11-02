package query

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/settings"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

type Context struct {
	ctx      context.Context
	cancel   context.CancelFunc
	Memory   *memory.Scope
	Temp     *storagetemp.QueryScope
	active   atomic.Int64
	closed   atomic.Bool
	settings settings.QuerySettings // settings 是创建 Query 时冻结的 AD-035 immutable QuerySettings。
	// sourceSnapshot 在第一次绑定后保持不可变，确保同一 Statement 的导出不会读取漂移的数据集。
	sourceSnapshot atomic.Pointer[SourceSnapshot]
}

func New(parent context.Context, m *memory.Manager) *Context {
	return NewWithTemp(parent, m, nil)
}

func NewWithTemp(parent context.Context, m *memory.Manager, tempScope *storagetemp.QueryScope) *Context {
	return NewConfigured(parent, m, tempScope, settings.QuerySettings{})
}

// NewConfigured 创建带 immutable QuerySettings 的 QueryContext；调用方负责提供独立于 transport request 的 Query lifetime parent。
func NewConfigured(parent context.Context, m *memory.Manager, tempScope *storagetemp.QueryScope, querySettings settings.QuerySettings) *Context {
	ctx, cancel := context.WithCancel(parent)
	return &Context{ctx: ctx, cancel: cancel, Memory: m.NewScope(), Temp: tempScope, settings: querySettings}
}

func (q *Context) Context() context.Context { return q.ctx }

// Settings 返回 Query 创建时冻结的配置快照；QuerySettings 不暴露内部可写 map。
func (q *Context) Settings() settings.QuerySettings { return q.settings }

func (q *Context) Cancel()            { q.cancel() }
func (q *Context) TaskStarted()       { q.active.Add(1) }
func (q *Context) TaskFinished()      { q.active.Add(-1) }
func (q *Context) ActiveTasks() int64 { return q.active.Load() }
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
