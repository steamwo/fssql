package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

// preparedQueryFunc 允许测试函数直接实现 PreparedQuery。
type preparedQueryFunc func(*querypkg.Context, *ResultCursor) error

// Run 实现 PreparedQuery。
func (f preparedQueryFunc) Run(queryContext *querypkg.Context, cursor *ResultCursor) error {
	return f(queryContext, cursor)
}

// fakeQueryEngine 允许测试精确控制 PLANNING 结果。
type fakeQueryEngine struct {
	// plan 是测试注入的 planner/executor factory。
	plan func(*querypkg.Context, string) (PreparedQuery, error)
}

// Plan 实现 QueryEngine。
func (e fakeQueryEngine) Plan(queryContext *querypkg.Context, sql string) (PreparedQuery, error) {
	return e.plan(queryContext, sql)
}

// newQueryServiceHarness 创建 finite admission 预算的 QueryService 测试环境。
func newQueryServiceHarness(t *testing.T, engine QueryEngine, config QueryServiceConfig) (*QueryService, *resource.Scheduler, *memory.Manager) {
	t.Helper()
	budget := resource.Budget{
		ConcurrentQueries: 2,
		CPUSlots:          4,
		MemoryBytes:       1 << 20,
		RemoteIOSlots:     4,
		SpillIOSlots:      4,
	}
	scheduler, err := resource.NewScheduler(budget, []resource.ResourceGroup{{Name: "default", Weight: 1, Budget: budget}})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 22)
	service, err := NewQueryService(context.Background(), NewQueryRegistry(), scheduler, engine, manager, config)
	if err != nil {
		t.Fatal(err)
	}
	return service, scheduler, manager
}

// defaultSubmitRequest 创建常用 admission 请求。
func defaultSubmitRequest(sql string) SubmitRequest {
	return SubmitRequest{
		SQL: sql, ResourceGroup: "default", Class: resource.QueryClassInteractive,
		CPUSlots: 1, MemoryBytes: 1 << 16, RemoteIOSlots: 1, SpillIOSlots: 1,
	}
}

// waitQueryState 等待 Query 达到指定状态；仅用于并发生命周期测试。
func waitQueryState(t *testing.T, service *QueryService, id QueryID, want QueryState) QuerySnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, err := service.Status(id)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State == want {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := service.Status(id)
	t.Fatalf("query %s state=%s, want %s", id, snapshot.State.String(), want.String())
	return QuerySnapshot{}
}

func TestQueryServiceSuccessWaitsForResultDrainBeforeTerminal(t *testing.T) {
	var queryScope *memory.Scope
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, sql string) (PreparedQuery, error) {
		if sql != "SELECT 1" {
			t.Fatalf("sql=%q", sql)
		}
		queryScope = queryContext.Memory()
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			chunk := testResultChunk(t, queryContext.Memory(), memory.NewBufferManager(nil), 1)
			return cursor.Push(queryContext.Context(), chunk)
		}), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1, MaxRetainedQueries: 8})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QueryFinishing)
	if scheduler.Snapshot().GlobalUsed.ConcurrentQueries != 1 {
		t.Fatalf("lease released before result drain: %+v", scheduler.Snapshot().GlobalUsed)
	}
	cursor, err := service.ClaimResults(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := chunkInt64(t, chunk); got != 1 {
		t.Fatalf("value=%d", got)
	}
	chunk.Release()
	if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal result err=%v, want EOF", err)
	}
	waitQueryState(t, service, response.ID, QuerySucceeded)
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("resource lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
	if queryScope == nil || queryScope.Used() != 0 {
		t.Fatalf("query memory leaked: scope=%v used=%d", queryScope != nil, queryScope.Used())
	}
}

func TestQueryServiceLifetimeDoesNotUseSubmitRequestContextAfterAdmission(t *testing.T) {
	started := make(chan struct{})
	allowFinish := make(chan struct{})
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, _ string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
			close(started)
			<-allowFinish
			return queryContext.Context().Err()
		}), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	requestCtx, cancel := context.WithCancel(context.Background())
	response, err := service.Submit(requestCtx, defaultSubmitRequest("SELECT long_running"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	select {
	case <-serviceRuntimeContext(t, service, response.ID).Done():
		t.Fatal("submit HTTP/request context canceled server-owned QueryContext")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowFinish)
	cursor, err := service.ClaimResults(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("result err=%v, want EOF", err)
	}
	waitQueryState(t, service, response.ID, QuerySucceeded)
}

func TestQueryServiceCancelUnblocksSlowProducerAndReleasesManagedChunks(t *testing.T) {
	var scope *memory.Scope
	secondPushStarted := make(chan struct{})
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, _ string) (PreparedQuery, error) {
		scope = queryContext.Memory()
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			bm := memory.NewBufferManager(nil)
			first := testResultChunk(t, queryContext.Memory(), bm, 1)
			if err := cursor.Push(queryContext.Context(), first); err != nil {
				first.Release()
				return err
			}
			second := testResultChunk(t, queryContext.Memory(), bm, 2)
			close(secondPushStarted)
			if err := cursor.Push(queryContext.Context(), second); err != nil {
				// Push 失败时 ownership 仍属于 producer。
				second.Release()
				return err
			}
			return nil
		}), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT blocked"))
	if err != nil {
		t.Fatal(err)
	}
	<-secondPushStarted
	if err := service.Cancel(response.ID); err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QueryCanceled)
	if scope == nil || scope.Used() != 0 {
		t.Fatalf("managed result bytes leaked after cancel: %d", scope.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked after cancel: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestQueryServicePlanFailurePublishesFailedTerminal(t *testing.T) {
	failure := errors.New("bind failed")
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, failure }}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT bad"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitQueryState(t, service, response.ID, QueryFailed)
	if !errors.Is(snapshot.Error, failure) {
		t.Fatalf("terminal error=%v, want plan failure", snapshot.Error)
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("lease leaked after plan failure: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestQueryServiceRetentionCapAndForget(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{MaxRetainedQueries: 1})
	first, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, first.ID, QuerySucceeded)
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 2")); !errors.Is(err, ErrQueryServiceCapacity) {
		t.Fatalf("second submit err=%v, want ErrQueryServiceCapacity", err)
	}
	if err := service.Forget(first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 2"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, second.ID, QuerySucceeded)
}

func TestQueryServiceClaimResultsIsSingleConsumer(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ClaimResults(response.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ClaimResults(response.ID); !errors.Is(err, ErrQueryResultsClaimed) {
		t.Fatalf("second claim err=%v, want ErrQueryResultsClaimed", err)
	}
}

func TestQueryServiceRejectsUnboundedActiveConcurrency(t *testing.T) {
	unlimited := resource.Budget{
		ConcurrentQueries: resource.Unlimited, CPUSlots: resource.Unlimited,
		MemoryBytes: resource.Unlimited, RemoteIOSlots: resource.Unlimited, SpillIOSlots: resource.Unlimited,
	}
	scheduler, err := resource.NewScheduler(unlimited, []resource.ResourceGroup{{Name: "default", Weight: 1, Budget: unlimited}})
	if err != nil {
		t.Fatal(err)
	}
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, nil }}
	_, err = NewQueryService(context.Background(), NewQueryRegistry(), scheduler, engine, memory.NewManager(1<<20), QueryServiceConfig{})
	if !errors.Is(err, ErrUnboundedQueryConcurrency) {
		t.Fatalf("err=%v, want ErrUnboundedQueryConcurrency", err)
	}
}

func TestQueryServiceConcurrentRetentionCheckCannotExceedHardLimit(t *testing.T) {
	block := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { <-block; return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{MaxRetainedQueries: 4})
	const submitters = 16
	var wg sync.WaitGroup
	wg.Add(submitters)
	errs := make(chan error, submitters)
	for i := 0; i < submitters; i++ {
		go func() {
			defer wg.Done()
			_, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT concurrent"))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
			continue
		}
		if !errors.Is(err, ErrQueryServiceCapacity) {
			t.Fatalf("unexpected submit err=%v", err)
		}
	}
	if success != 4 || service.registry.Count() != 4 {
		t.Fatalf("success=%d registry=%d, want 4", success, service.registry.Count())
	}
	close(block)
}

// serviceRuntimeContext 返回测试 QueryContext context，用于验证其 parent 不等于 submit request context。
func serviceRuntimeContext(t *testing.T, service *QueryService, id QueryID) context.Context {
	t.Helper()
	runtime, err := service.runtime(id)
	if err != nil {
		t.Fatal(err)
	}
	return runtime.handle.Context().Context()
}
