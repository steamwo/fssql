package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

var (
	// ErrQueryServiceCapacity 表示 server 已达到 terminal/query retention 硬上限，拒绝继续增长 registry。
	ErrQueryServiceCapacity = errors.New("server: query service retention capacity reached")
	// ErrQueryResultsClaimed 表示 ResultCursor 已经被另一个 consumer 领取。
	ErrQueryResultsClaimed = errors.New("server: query results already claimed")
	// ErrInvalidSubmitRequest 表示 query submit 参数非法。
	ErrInvalidSubmitRequest = errors.New("server: invalid submit request")
	// ErrUnboundedQueryConcurrency 表示 resource scheduler 允许无限 active Query，会违反无界 goroutine 约束。
	ErrUnboundedQueryConcurrency = errors.New("server: unbounded query concurrency")
)

// PreparedQuery 是 QueryEngine 完成 parser/binder/planner 后可执行的计划。
type PreparedQuery interface {
	// Run 执行已准备 Query，并把 native DataChunk 推入有界 ResultCursor。
	Run(*querypkg.Context, *ResultCursor) error
}

// QueryEngine 是 Server 对真实 SQL engine 的唯一执行依赖。
// Server 不复制 Parser/Binder/Planner；PLANNING 与 RUNNING 通过 Plan/Run 明确分层。
type QueryEngine interface {
	// Plan 在 QueryContext 下完成 parser/binder/planner 并返回可执行计划。
	Plan(*querypkg.Context, string) (PreparedQuery, error)
}

// QueryServiceConfig 固定 server query API 的控制面上限。
type QueryServiceConfig struct {
	// CursorCapacityChunks 是每个 Query ResultCursor 的固定 chunk 容量。
	CursorCapacityChunks int
	// MaxRetainedQueries 是 Registry+runtime map 的硬上限，防止客户端不清理历史 Query 时无界增长。
	MaxRetainedQueries int
	// MaxSQLBytes 是单次 SQL 文本允许的最大字节数，限制 Go heap 控制对象大小。
	MaxSQLBytes int
	// QueryContexts 创建 QueryContext 及其 query-scoped resource ownership；nil 保持旧 managed-memory-only 行为。
	QueryContexts QueryContextFactory
}

// SubmitRequest 是 transport-neutral query submit 请求。
type SubmitRequest struct {
	// SQL 是交给真实 QueryEngine.Plan 的 SQL 文本；Server 不解析 SQL。
	SQL string
	// ResourceGroup 是 AD-016 admission 使用的 ResourceGroup 名称。
	ResourceGroup string
	// Class 是 admission query class；零值按 Interactive 处理。
	Class resource.QueryClass
	// CPUSlots 是 admission 期间声明的 CPU slot 预算。
	CPUSlots int64
	// MemoryBytes 是 admission 期间声明的 managed-memory 预算。
	MemoryBytes int64
	// RemoteIOSlots 是 admission 期间声明的远程 I/O slot 预算。
	RemoteIOSlots int64
	// SpillIOSlots 是 admission 期间声明的 spill I/O slot 预算。
	SpillIOSlots int64
}

// SubmitResponse 返回已经通过 admission 并开始 PLANNING 的稳定 Query ID。
type SubmitResponse struct {
	// ID 是后续 Status/Cancel/Results 使用的 Query ID。
	ID QueryID
}

// queryRuntime 保存 QueryHandle 下的 ResultCursor 与 admission Lease ownership。
type queryRuntime struct {
	// handle 是 AD-037 QueryHandle。
	handle *QueryHandle
	// cursor 是该 Query 唯一的有界 ResultCursor。
	cursor *ResultCursor
	// mu 保护 lease/resultsClaimed/coordinatorErr。
	mu sync.Mutex
	// lease 是 Query 获得的 AD-016 admission ownership；只有成功终态后释放。
	lease *resource.Lease
	// resultsClaimed 防止多个客户端并发消费同一个 native result stream。
	resultsClaimed bool
	// coordinatorErr 保存 server coordinator 无法发布终态等内部错误，便于测试/运维诊断。
	coordinatorErr error
}

// QueryService 把 QueryRegistry、Resource Scheduler、QueryEngine 与 ResultCursor 组合成 transport-neutral Server API。
type QueryService struct {
	// parent 是 server lifetime context，而不是任何单个 HTTP/gRPC request context。
	parent context.Context
	// registry 持有 QueryHandle 生命周期。
	registry *QueryRegistry
	// resources 负责 AD-016 admission/accounting。
	resources *resource.Scheduler
	// engine 是真正 SQL parser/binder/planner/executor 的适配边界。
	engine QueryEngine
	// memory 是 engine 级 MemoryManager；每个 QueryContext 从中创建独立 Scope。
	memory *memory.Manager
	// config 固定 cursor/registry/SQL/query-resource 控制面硬上限。
	config QueryServiceConfig
	// submitMu 让 retention capacity check 与 Register 原子化，避免并发 submit 突破上限。
	submitMu sync.Mutex
	// runtimesMu 保护 QueryID→runtime map。
	runtimesMu sync.RWMutex
	// runtimes 与 registry 同步受 MaxRetainedQueries 硬上限约束。
	runtimes map[QueryID]*queryRuntime
}

// NewQueryService 创建 transport-neutral QueryService，并拒绝任何可产生无界 active query goroutine 的配置。
func NewQueryService(parent context.Context, registry *QueryRegistry, resources *resource.Scheduler, engine QueryEngine, manager *memory.Manager, config QueryServiceConfig) (*QueryService, error) {
	if parent == nil {
		return nil, errors.New("server: parent context required")
	}
	if registry == nil || resources == nil || engine == nil || manager == nil {
		return nil, errors.New("server: registry, resource scheduler, query engine and memory manager are required")
	}
	if config.CursorCapacityChunks <= 0 {
		config.CursorCapacityChunks = 2
	}
	if config.MaxRetainedQueries <= 0 {
		config.MaxRetainedQueries = 1024
	}
	if config.MaxSQLBytes <= 0 {
		config.MaxSQLBytes = 1 << 20
	}
	if config.QueryContexts == nil {
		config.QueryContexts = defaultQueryContextFactory{}
	}
	if !schedulerConcurrencyBounded(resources.Snapshot()) {
		return nil, ErrUnboundedQueryConcurrency
	}
	return &QueryService{
		parent: parent, registry: registry, resources: resources, engine: engine, memory: manager,
		config: config, runtimes: make(map[QueryID]*queryRuntime),
	}, nil
}

// Submit 注册 Query、进入 QUEUED admission，并在资源可用后启动独立于 request context 的 PLANNING/RUNNING。
func (s *QueryService) Submit(ctx context.Context, request SubmitRequest) (SubmitResponse, error) {
	if err := s.validateSubmit(request); err != nil {
		return SubmitResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return SubmitResponse{}, err
	}
	if request.Class == 0 {
		request.Class = resource.QueryClassInteractive
	}

	queryContext, err := s.config.QueryContexts.NewQueryContext(s.parent, s.memory, request.ResourceGroup)
	if err != nil {
		return SubmitResponse{}, fmt.Errorf("server: create query context: %w", err)
	}
	if queryContext == nil {
		return SubmitResponse{}, errors.New("server: query context factory returned nil context")
	}
	cursor, err := NewResultCursor(s.config.CursorCapacityChunks)
	if err != nil {
		_ = queryContext.Close()
		return SubmitResponse{}, err
	}

	// retention check 与 Register 必须在同一 submit 临界区，确保 registry/runtimes 永远有硬上限。
	s.submitMu.Lock()
	if s.registry.Count() >= s.config.MaxRetainedQueries {
		s.submitMu.Unlock()
		cursor.Close()
		_ = queryContext.Close()
		return SubmitResponse{}, ErrQueryServiceCapacity
	}
	handle, err := s.registry.Register(queryContext)
	if err != nil {
		s.submitMu.Unlock()
		cursor.Close()
		_ = queryContext.Close()
		return SubmitResponse{}, err
	}
	runtime := &queryRuntime{handle: handle, cursor: cursor}
	s.runtimesMu.Lock()
	s.runtimes[handle.ID()] = runtime
	s.runtimesMu.Unlock()
	s.submitMu.Unlock()

	if err := handle.Transition(QueryQueued, time.Time{}); err != nil {
		s.cleanupRejectedSubmit(runtime, err, false)
		return SubmitResponse{}, err
	}
	admission := resource.Request{
		QueryID: string(handle.ID()), Group: request.ResourceGroup, Class: request.Class,
		CPUSlots: request.CPUSlots, MemoryBytes: request.MemoryBytes,
		RemoteIOSlots: request.RemoteIOSlots, SpillIOSlots: request.SpillIOSlots,
	}
	lease, err := s.resources.Acquire(ctx, admission)
	if err != nil {
		s.cleanupRejectedSubmit(runtime, err, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		return SubmitResponse{}, err
	}
	runtime.mu.Lock()
	runtime.lease = lease
	runtime.mu.Unlock()
	if err := handle.Transition(QueryPlanning, time.Time{}); err != nil {
		runtime.releaseLease()
		s.cleanupRejectedSubmit(runtime, err, false)
		return SubmitResponse{}, err
	}

	// 每个 execution goroutine 都对应一个已获得 admission Lease 的 Query；构造器已证明 active concurrency 有硬上限。
	go s.run(runtime, request.SQL)
	return SubmitResponse{ID: handle.ID()}, nil
}

// Status 返回 QueryRegistry 中的不可变 Query 状态快照。
func (s *QueryService) Status(id QueryID) (QuerySnapshot, error) {
	handle, err := s.registry.Get(id)
	if err != nil {
		return QuerySnapshot{}, err
	}
	return handle.Snapshot(), nil
}

// ClaimResults 一次性领取 Query 的 ResultCursor；禁止多个 transport consumer 拆分同一结果流。
func (s *QueryService) ClaimResults(id QueryID) (*ResultCursor, error) {
	runtime, err := s.runtime(id)
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.resultsClaimed {
		return nil, ErrQueryResultsClaimed
	}
	runtime.resultsClaimed = true
	return runtime.cursor, nil
}

// Cancel 传播 QueryContext cancellation 并关闭 ResultCursor，使 blocked Push/Next 及时解除等待。
func (s *QueryService) Cancel(id QueryID) error {
	runtime, err := s.runtime(id)
	if err != nil {
		return err
	}
	if runtime.handle.Snapshot().State.Terminal() {
		return nil
	}
	if err := runtime.handle.RequestCancel(time.Time{}); err != nil && !errors.Is(err, ErrQueryTerminal) {
		return err
	}
	runtime.cursor.Close()
	return nil
}

// Forget 释放 terminal Query 的 registry/runtime retention ownership；非终态 Query 必须先 Cancel 并等待终态。
func (s *QueryService) Forget(id QueryID) error {
	runtime, err := s.runtime(id)
	if err != nil {
		return err
	}
	if !runtime.handle.Snapshot().State.Terminal() {
		return fmt.Errorf("%w: %s", ErrQueryNotTerminal, id)
	}
	runtime.cursor.Close()
	if err := s.registry.Remove(id); err != nil {
		return err
	}
	s.runtimesMu.Lock()
	delete(s.runtimes, id)
	s.runtimesMu.Unlock()
	return nil
}

// run 执行 PLANNING→RUNNING→FINISHING，并只在 ResultCursor ownership 已排空后发布终态。
func (s *QueryService) run(runtime *queryRuntime, sql string) {
	handle := runtime.handle
	queryContext := handle.Context()
	prepared, err := s.engine.Plan(queryContext, sql)
	if err != nil {
		_ = runtime.cursor.Finish(err)
		_ = runtime.cursor.WaitDrained(queryContext.Context())
		s.finishRuntime(runtime, QueryFailed, err)
		return
	}
	if prepared == nil {
		err = errors.New("server: query engine returned nil prepared query")
		_ = runtime.cursor.Finish(err)
		_ = runtime.cursor.WaitDrained(queryContext.Context())
		s.finishRuntime(runtime, QueryFailed, err)
		return
	}
	if err = handle.Transition(QueryRunning, time.Time{}); err != nil {
		runtime.cursor.Close()
		s.finishRuntime(runtime, QueryFailed, err)
		return
	}

	runErr := prepared.Run(queryContext, runtime.cursor)
	if errors.Is(runErr, ErrResultCursorClosed) || errors.Is(queryContext.Context().Err(), context.Canceled) {
		_ = handle.RequestCancel(time.Time{})
		runtime.cursor.Close()
		s.finishRuntime(runtime, QueryCanceled, context.Canceled)
		return
	}
	if err = handle.Transition(QueryFinishing, time.Time{}); err != nil {
		runtime.cursor.Close()
		s.finishRuntime(runtime, QueryFailed, err)
		return
	}
	if finishErr := runtime.cursor.Finish(runErr); finishErr != nil {
		if errors.Is(finishErr, ErrResultCursorClosed) {
			_ = handle.RequestCancel(time.Time{})
			s.finishRuntime(runtime, QueryCanceled, context.Canceled)
			return
		}
		s.finishRuntime(runtime, QueryFailed, finishErr)
		return
	}
	if drainErr := runtime.cursor.WaitDrained(queryContext.Context()); drainErr != nil {
		_ = handle.RequestCancel(time.Time{})
		runtime.cursor.Close()
		s.finishRuntime(runtime, QueryCanceled, context.Canceled)
		return
	}
	if runErr != nil {
		s.finishRuntime(runtime, QueryFailed, runErr)
		return
	}
	s.finishRuntime(runtime, QuerySucceeded, nil)
}

// finishRuntime 在发布 terminal state 前等待所有 consumer-held result chunk 完成 Managed Buffer 释放。
// Close/lifecycle 失败时保留 admission Lease，避免把 I-10 泄漏伪装成已释放资源。
func (s *QueryService) finishRuntime(runtime *queryRuntime, target QueryState, terminalErr error) {
	if err := runtime.cursor.WaitConsumerReleased(s.parent); err != nil {
		runtime.mu.Lock()
		runtime.coordinatorErr = fmt.Errorf("server: wait for consumer result ownership before %s: %w", target.String(), err)
		runtime.mu.Unlock()
		return
	}
	if err := runtime.handle.Finalize(target, terminalErr, time.Time{}); err != nil {
		runtime.mu.Lock()
		runtime.coordinatorErr = err
		runtime.mu.Unlock()
		return
	}
	runtime.releaseLease()
}

// cleanupRejectedSubmit 清理尚未返回 Query ID 的失败 submit，确保不会在 registry 留下不可访问 orphan。
func (s *QueryService) cleanupRejectedSubmit(runtime *queryRuntime, cause error, canceled bool) {
	runtime.cursor.Close()
	if canceled {
		_ = runtime.handle.RequestCancel(time.Time{})
		_ = runtime.handle.Finalize(QueryCanceled, cause, time.Time{})
	} else {
		_ = runtime.handle.Finalize(QueryFailed, cause, time.Time{})
	}
	_ = s.registry.Remove(runtime.handle.ID())
	s.runtimesMu.Lock()
	delete(s.runtimes, runtime.handle.ID())
	s.runtimesMu.Unlock()
}

// runtime 查找 Query 的 server runtime ownership。
func (s *QueryService) runtime(id QueryID) (*queryRuntime, error) {
	s.runtimesMu.RLock()
	runtime := s.runtimes[id]
	s.runtimesMu.RUnlock()
	if runtime == nil {
		return nil, fmt.Errorf("%w: %s", ErrQueryNotFound, id)
	}
	return runtime, nil
}

// releaseLease 幂等释放 Query admission ownership。
func (r *queryRuntime) releaseLease() {
	r.mu.Lock()
	lease := r.lease
	r.lease = nil
	r.mu.Unlock()
	if lease != nil {
		lease.Release()
	}
}

// validateSubmit 限制 SQL 控制对象大小并验证 admission 参数的非负性。
func (s *QueryService) validateSubmit(request SubmitRequest) error {
	if strings.TrimSpace(request.SQL) == "" || len(request.SQL) > s.config.MaxSQLBytes || request.ResourceGroup == "" {
		return fmt.Errorf("%w: sql/group", ErrInvalidSubmitRequest)
	}
	if request.Class != 0 && !request.Class.Valid() {
		return fmt.Errorf("%w: query class %d", ErrInvalidSubmitRequest, request.Class)
	}
	if request.CPUSlots < 0 || request.MemoryBytes < 0 || request.RemoteIOSlots < 0 || request.SpillIOSlots < 0 {
		return fmt.Errorf("%w: negative resource request", ErrInvalidSubmitRequest)
	}
	return nil
}

// schedulerConcurrencyBounded 证明 execution goroutine 数存在硬上限：全局并发有限，或所有 ResourceGroup 并发均有限。
func schedulerConcurrencyBounded(snapshot resource.Snapshot) bool {
	if snapshot.GlobalBudget.ConcurrentQueries != resource.Unlimited {
		return true
	}
	if len(snapshot.Groups) == 0 {
		return false
	}
	for _, group := range snapshot.Groups {
		if group.Config.Budget.ConcurrentQueries == resource.Unlimited {
			return false
		}
	}
	return true
}
