package server

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// QueryContextFactory 是 Server 创建 QueryContext 的 transport-neutral 资源边界。
// HTTP/gRPC 不直接依赖 TemporaryStorageManager；production composition 可注入 query-scoped Temp ownership。
type QueryContextFactory interface {
	NewQueryContext(context.Context, *memory.Manager, string) (*querypkg.Context, error)
}

// QueryContextFactoryFunc 便于测试或嵌入式装配用函数实现 QueryContextFactory。
type QueryContextFactoryFunc func(context.Context, *memory.Manager, string) (*querypkg.Context, error)

// NewQueryContext 实现 QueryContextFactory。
func (f QueryContextFactoryFunc) NewQueryContext(parent context.Context, manager *memory.Manager, resourceGroup string) (*querypkg.Context, error) {
	if f == nil {
		return nil, fmt.Errorf("server: nil QueryContextFactoryFunc")
	}
	return f(parent, manager, resourceGroup)
}

// defaultQueryContextFactory 保持旧 QueryService 行为：仅创建 managed-memory QueryContext，不隐式发明 temp storage。
type defaultQueryContextFactory struct{}

func (defaultQueryContextFactory) NewQueryContext(parent context.Context, manager *memory.Manager, _ string) (*querypkg.Context, error) {
	if parent == nil || manager == nil {
		return nil, fmt.Errorf("server: QueryContext requires parent context and memory manager")
	}
	return querypkg.New(parent, manager), nil
}

// TemporaryQueryContextFactory 使用统一 TemporaryStorageManager 为每个 Query 创建独立 spill scope。
type TemporaryQueryContextFactory struct {
	Storage *storagetemp.TemporaryStorageManager // Storage 是 Engine 级唯一 TemporaryStorageManager。
}

// NewQueryContext 创建 query-scoped temp directory/quota，并把 ownership 交给 QueryContext.Close。
func (f *TemporaryQueryContextFactory) NewQueryContext(parent context.Context, manager *memory.Manager, resourceGroup string) (*querypkg.Context, error) {
	if f == nil || f.Storage == nil || parent == nil || manager == nil {
		return nil, fmt.Errorf("server: temporary QueryContext factory is uninitialized")
	}
	tempScope, err := f.Storage.NewQueryScope(resourceGroup)
	if err != nil {
		return nil, err
	}
	q := querypkg.NewWithTemp(parent, manager, tempScope)
	if q == nil {
		_ = tempScope.Close()
		return nil, fmt.Errorf("server: failed to create QueryContext")
	}
	return q, nil
}
