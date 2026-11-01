package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	querypkg "github.com/steamwo/fssql/internal/query"
)

var (
	// ErrInvalidQueryTransition 表示 Query 状态转换不符合 Phase 5 冻结状态机。
	ErrInvalidQueryTransition = errors.New("server: invalid query state transition")
	// ErrQueryTerminal 表示调用方试图修改已经进入终态的 Query。
	ErrQueryTerminal = errors.New("server: query already terminal")
	// ErrInvalidQueryTerminal 表示 Finalize 收到了非终态目标。
	ErrInvalidQueryTerminal = errors.New("server: invalid query terminal state")
)

// QueryState 描述 Phase 5 冻结的 Query 生命周期状态。
type QueryState uint8

const (
	// QuerySubmitted 表示 Query 已进入 QueryRegistry，但尚未进入 admission queue。
	QuerySubmitted QueryState = iota
	// QueryQueued 表示 Query 正等待资源调度；ResourceGroup 将在后续任务实现。
	QueryQueued
	// QueryPlanning 表示 Query 正在 parser/binder/planner 阶段。
	QueryPlanning
	// QueryRunning 表示 Query pipeline 正在执行。
	QueryRunning
	// QueryFinishing 表示执行已停止，正在完成 result/resource 生命周期收尾。
	QueryFinishing
	// QuerySucceeded 表示 Query 已成功且生命周期资源已经释放。
	QuerySucceeded
	// QueryFailed 表示 Query 失败且生命周期资源已经释放。
	QueryFailed
	// QueryCanceled 表示取消请求已完成并且生命周期资源已经释放。
	QueryCanceled
)

// String 返回 Phase 5 文档规定的稳定状态名称。
func (s QueryState) String() string {
	switch s {
	case QuerySubmitted:
		return "SUBMITTED"
	case QueryQueued:
		return "QUEUED"
	case QueryPlanning:
		return "PLANNING"
	case QueryRunning:
		return "RUNNING"
	case QueryFinishing:
		return "FINISHING"
	case QuerySucceeded:
		return "SUCCEEDED"
	case QueryFailed:
		return "FAILED"
	case QueryCanceled:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

// Terminal 判断状态是否已经完成 Query 生命周期。
func (s QueryState) Terminal() bool {
	return s == QuerySucceeded || s == QueryFailed || s == QueryCanceled
}

// QuerySnapshot 是 QueryHandle 的不可变状态快照，供后续 Server API 读取。
type QuerySnapshot struct {
	// ID 是 QueryRegistry 内稳定的 Query ID。
	ID QueryID
	// State 是当前 Query 状态。
	State QueryState
	// CancelRequested 表示已经收到取消请求，但不等价于已经进入 CANCELED 终态。
	CancelRequested bool
	// Error 是终态错误；成功 Query 为空。
	Error error
	// CreatedAt 是 QueryHandle 创建时间。
	CreatedAt time.Time
	// UpdatedAt 是最后一次状态或取消请求更新时间。
	UpdatedAt time.Time
}

// QueryHandle 是 AD-037 中由 QueryRegistry 持有、独立于 HTTP 请求生命周期的 Query owner。
type QueryHandle struct {
	// id 是 registry 内稳定标识。
	id QueryID
	// query 是真正拥有 cancel/memory/temp/task lifecycle 的 QueryContext。
	query *querypkg.Context
	// mu 保护 state/cancelRequested/terminalErr/updatedAt；Finalize 持锁跨 QueryContext.Close，保证取消和终态发布原子化。
	mu sync.RWMutex
	// state 是当前 Phase 5 Query 状态。
	state QueryState
	// cancelRequested 区分“请求取消”和“资源已经释放后的 CANCELED 终态”。
	cancelRequested bool
	// terminalErr 保存 FAILED/CANCELED 的终态原因。
	terminalErr error
	// createdAt 是 handle 创建时间。
	createdAt time.Time
	// updatedAt 是最近状态变化时间。
	updatedAt time.Time
}

// NewQueryHandle 创建初始状态为 SUBMITTED 的 QueryHandle。
func NewQueryHandle(id QueryID, queryContext *querypkg.Context, now time.Time) (*QueryHandle, error) {
	if id == "" {
		return nil, errors.New("server: empty query id")
	}
	if queryContext == nil {
		return nil, errors.New("server: nil query context")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return &QueryHandle{id: id, query: queryContext, state: QuerySubmitted, createdAt: now, updatedAt: now}, nil
}

// ID 返回稳定 Query ID。
func (h *QueryHandle) ID() QueryID { return h.id }

// Context 返回 server-owned QueryContext；transport 不得用 HTTP request context 替换其 parent ownership。
func (h *QueryHandle) Context() *querypkg.Context { return h.query }

// Snapshot 返回不暴露内部可写状态的 Query 快照。
func (h *QueryHandle) Snapshot() QuerySnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return QuerySnapshot{
		ID: h.id, State: h.state, CancelRequested: h.cancelRequested,
		Error: h.terminalErr, CreatedAt: h.createdAt, UpdatedAt: h.updatedAt,
	}
}

// Transition 执行正常前向状态转换；终态必须通过 Finalize，以确保先完成 QueryContext 生命周期检查。
func (h *QueryHandle) Transition(next QueryState, now time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Terminal() {
		return ErrQueryTerminal
	}
	if next.Terminal() || !allowedForwardTransition(h.state, next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidQueryTransition, h.state.String(), next.String())
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.state = next
	h.updatedAt = now
	return nil
}

// RequestCancel 传播 Query cancellation，但不会提前把状态标记成 CANCELED。
// 只有 Finalize(QueryCanceled) 在生命周期资源归零后才能形成终态。
func (h *QueryHandle) RequestCancel(now time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Terminal() {
		return ErrQueryTerminal
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.cancelRequested = true
	h.updatedAt = now
	h.query.Cancel()
	return nil
}

// Finalize 先关闭 QueryContext，再原子发布 SUCCEEDED/FAILED/CANCELED 终态。
// QueryContext.Close 失败时状态保持非终态，防止 lifecycle leak 被伪装成已完成 Query。
func (h *QueryHandle) Finalize(target QueryState, terminalErr error, now time.Time) error {
	if !target.Terminal() {
		return fmt.Errorf("%w: %s", ErrInvalidQueryTerminal, target.String())
	}

	// 持有状态写锁跨 Close：RequestCancel/其他 Finalize 必须等待，避免 Context 已关闭但状态仍非终态的竞态窗口。
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state.Terminal() {
		return ErrQueryTerminal
	}
	if target == QuerySucceeded && h.cancelRequested {
		return fmt.Errorf("%w: canceled query cannot succeed", ErrInvalidQueryTransition)
	}
	if target == QueryFailed && terminalErr == nil {
		return errors.New("server: failed query requires terminal error")
	}
	if target == QueryCanceled && terminalErr == nil {
		terminalErr = context.Canceled
	}
	if target == QueryCanceled {
		h.query.Cancel()
	}

	// 终态发布前必须通过 QueryContext 的 active-task/temp/memory lifecycle 检查。
	if err := h.query.Close(); err != nil {
		return fmt.Errorf("server: query lifecycle close before %s: %w", target.String(), err)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	h.state = target
	h.terminalErr = terminalErr
	h.updatedAt = now
	return nil
}

// allowedForwardTransition 固定 Phase 5 文档中的正常状态链。
func allowedForwardTransition(current, next QueryState) bool {
	switch current {
	case QuerySubmitted:
		return next == QueryQueued
	case QueryQueued:
		return next == QueryPlanning
	case QueryPlanning:
		return next == QueryRunning
	case QueryRunning:
		return next == QueryFinishing
	default:
		return false
	}
}
