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
	SessionID string // Frozen by the statement engine before source binding.
	ctx    context.Context
	cancel context.CancelFunc
	Memory *memory.Scope
	Temp   *storagetemp.QueryScope
	active atomic.Int64
	// closed 表示清理已经开始并拒绝新工作，不代表所有资源均已释放。
	closed atomic.Bool
	// sourceSnapshot 在第一次绑定后保持不可变，确保同一 Statement 的导出不会读取漂移的数据集。
	sourceSnapshot atomic.Pointer[SourceSnapshot]
	cleanupMu      sync.Mutex
	cleanups       []func() error
	closeMu        sync.Mutex // 串行执行每次完整清理，并发 Close 必须等待当前尝试结束。
	cleanupDone    bool       // 仅当 callback、Temp 和 Memory 全部清理成功后置位。
	cleanupErr     error      // 保留历次失败，避免后续清理尝试吞掉首次错误。
	tempCleaned    bool       // 避免重试时重复关闭已成功的 Temp。
	memoryCleaned  bool       // 避免重试时重复关闭已成功的 Memory。
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

// AddCleanup 注册在 Temp 和 Memory 之前按逆序执行的查询资源清理。
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

// Close 同步重试尚未成功的清理；并发调用串行等待，全部成功后才完成关闭。
func (q *Context) Close() error {
	if q == nil {
		return nil
	}
	q.cancel()
	if q.ActiveTasks() != 0 {
		return fmt.Errorf("query close with %d active tasks", q.ActiveTasks())
	}

	q.closeMu.Lock()
	defer q.closeMu.Unlock()
	if q.cleanupDone {
		return nil
	}
	if q.ActiveTasks() != 0 {
		return fmt.Errorf("query close with %d active tasks", q.ActiveTasks())
	}

	// 与 AddCleanup 的注册互斥：先登记者纳入本次清理，后登记者被拒绝。
	q.cleanupMu.Lock()
	q.closed.Store(true)
	q.cleanupMu.Unlock()

	var errs []error
	for i := len(q.cleanups) - 1; i >= 0; i-- {
		cleanup := q.cleanups[i]
		if cleanup == nil {
			continue
		}
		if err := cleanup(); err != nil {
			errs = append(errs, err)
		} else {
			q.cleanups[i] = nil
		}
	}
	pending := q.cleanups[:0]
	for _, cleanup := range q.cleanups {
		if cleanup != nil {
			pending = append(pending, cleanup)
		}
	}
	q.cleanups = pending

	if !q.tempCleaned && q.Temp != nil {
		if err := q.Temp.Close(); err != nil {
			errs = append(errs, err)
		} else {
			q.tempCleaned = true
		}
	}
	if !q.memoryCleaned && q.Memory != nil {
		if err := q.Memory.Close(); err != nil {
			errs = append(errs, err)
		} else {
			q.memoryCleaned = true
		}
	}

	attemptErr := errors.Join(errs...)
	if attemptErr == nil {
		q.cleanupDone = true
		q.cleanupErr = nil
		return nil
	}
	q.cleanupErr = errors.Join(q.cleanupErr, attemptErr)
	return q.cleanupErr
}

// Closed 报告是否已开始拒绝新工作；活动任务导致的提前返回不改变该状态。
func (q *Context) Closed() bool { return q == nil || q.closed.Load() }
