package server

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// TestQueryServiceContextFactoryFailureDoesNotRegister 验证 QueryContext 创建失败不会留下 registry/runtime orphan。
func TestQueryServiceContextFactoryFailureDoesNotRegister(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		t.Fatal("engine must not be called when QueryContext creation fails")
		return nil, nil
	}}
	factoryErr := errors.New("context factory failed")
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{
		QueryContexts: QueryContextFactoryFunc(func(context.Context, *memory.Manager, string) (*querypkg.Context, error) {
			return nil, factoryErr
		}),
	})
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1")); !errors.Is(err, factoryErr) {
		t.Fatalf("submit err=%v want=%v", err, factoryErr)
	}
	if service.registry.Count() != 0 || len(service.runtimes) != 0 {
		t.Fatalf("factory failure leaked registry/runtime: registry=%d runtimes=%d", service.registry.Count(), len(service.runtimes))
	}
}

// TestTemporaryQueryContextFactoryUsesResourceGroupAndClosesTemp 验证 Server query terminal 通过 QueryContext.Close 清理 temp scope/quota。
func TestTemporaryQueryContextFactoryUsesResourceGroupAndClosesTemp(t *testing.T) {
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{
		Global: 1 << 20, DefaultResourceGroup: 1 << 20, DefaultQuery: 1 << 20,
		ResourceGroups: map[string]int64{"default": 1 << 19},
	})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	engine := fakeQueryEngine{plan: func(q *querypkg.Context, _ string) (PreparedQuery, error) {
		if q.Temp == nil {
			t.Fatal("expected query-scoped temporary storage")
		}
		file, err := q.Temp.CreateFile()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte("spill-owned-by-query")); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error {
			// QueryContext.Close/TemporaryStorageManager scope owns cleanup after result drain.
			return nil
		}), nil
	}}
	service, _, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{
		QueryContexts: &TemporaryQueryContextFactory{Storage: storage},
	})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QuerySucceeded)
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("temporary bytes leaked after terminal query: %d", got)
	}
	if got := quotas.ResourceGroupUsed("default"); got != 0 {
		t.Fatalf("resource-group temporary bytes leaked after terminal query: %d", got)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leaked after terminal query: %d", manager.Used())
	}
	if err := service.Forget(response.ID); err != nil {
		t.Fatal(err)
	}
}
