package server

import (
	"context"
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
		ConcurrentQueries: 4,
		CPUSlots:          8,
		MemoryBytes:       1 << 20,
		RemoteIOSlots:     8,
		SpillIOSlots:      8,
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

// serviceRuntimeContext 返回测试 QueryContext context，用于验证其 parent 不等于 submit request context。
func serviceRuntimeContext(t *testing.T, service *QueryService, id QueryID) context.Context {
	t.Helper()
	runtime, err := service.runtime(id)
	if err != nil {
		t.Fatal(err)
	}
	return runtime.handle.Context().Context()
}
