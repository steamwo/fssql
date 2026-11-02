package session

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/settings"
)

// QueryLifecycle 是 Session 对既有 QueryContext 生命周期的最小依赖；不得在 Session 中复制状态机。
type QueryLifecycle interface {
	Cancel()
	Close() error
}

// QueryFactory 将冻结的 QuerySettings 和 resource group 交给现有 QueryContext 装配层。
type QueryFactory interface {
	NewQuery(context.Context, settings.QuerySettings, string) (QueryLifecycle, error)
}

// Config 是 Session 的 engine/resource-group 固定依赖。
type Config struct {
	Catalog       catalog.Catalog    // Catalog 是 metadata transaction provider。
	Settings      *settings.Registry // Settings 负责 AD-035 层级解析。
	Engine        settings.Layer     // Engine 是 engine-level setting snapshot。
	ResourceGroup settings.Layer     // ResourceGroup 是 resource-group override snapshot。
	GroupName     string             // GroupName 是 resource scheduler/resource setting identity。
	Queries       QueryFactory       // Queries 必须装配现有 Query lifecycle。
}

// Session 保存可变 Session setting layer；每个 Statement 开始时都会冻结快照。
type Session struct {
	mu     sync.RWMutex   // mu 保护 session layer 与 closed。
	config Config         // config 在 Session 生命周期内不变。
	layer  settings.Layer // layer 是当前 session-level overrides。
	closed bool           // closed 防止关闭后创建新 Statement。
}

// New 创建 Session，并验证必需的 catalog/settings/query lifecycle 依赖。
func New(config Config) (*Session, error) {
	if config.Catalog == nil || config.Settings == nil || config.Queries == nil {
		return nil, fmt.Errorf("session requires catalog, settings registry, and query factory")
	}
	return &Session{config: config, layer: settings.NewLayer(nil)}, nil
}

// SetSetting 原子替换 Session setting snapshot，不修改已开始 Statement 的 QuerySettings。
func (s *Session) SetSetting(name string, value settings.Value) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("session is closed")
	}
	s.layer = s.layer.With(name, value)
	return nil
}

// BeginStatement 冻结 settings、开启独立 Catalog Transaction，并创建唯一 Query lifecycle。
func (s *Session) BeginStatement(ctx context.Context, statement settings.Layer) (*Statement, error) {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, fmt.Errorf("session is closed")
	}
	config := s.config
	layer := s.layer
	s.mu.RUnlock()

	querySettings, err := config.Settings.Resolve(config.Engine, config.ResourceGroup, layer, statement)
	if err != nil {
		return nil, err
	}
	catalogTx, err := config.Catalog.Begin(ctx)
	if err != nil {
		return nil, err
	}
	queryLifecycle, err := config.Queries.NewQuery(ctx, querySettings, config.GroupName)
	if err != nil {
		_ = catalogTx.Rollback(context.Background())
		return nil, err
	}
	return &Statement{catalog: catalogTx, query: queryLifecycle, settings: querySettings}, nil
}

// Close 阻止新 Statement；已存在 Statement 继续由各自 lifecycle ownership 收尾。
func (s *Session) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// Statement 持有单语句冻结 settings、Catalog Transaction 和唯一 Query lifecycle。
type Statement struct {
	mu        sync.Mutex             // mu 串行化 commit/close ownership transition。
	catalog   catalog.Transaction    // catalog 是与 SourceSnapshot/Export commit 分离的 metadata transaction。
	query     QueryLifecycle         // query 是既有 QueryContext lifecycle ownership。
	settings  settings.QuerySettings // settings 是 AD-035 最终不可变 QuerySettings。
	committed bool                   // committed 表示 DDL metadata transaction 已提交。
	closed    bool                   // closed 防止重复收尾。
}

// Settings 返回冻结 QuerySettings 值；内部 map 不暴露可写引用。
func (s *Statement) Settings() settings.QuerySettings { return s.settings }

// Catalog 返回 Statement 独占的 metadata transaction，供 Binder/DDL 使用。
func (s *Statement) Catalog() catalog.Transaction { return s.catalog }

// Query 返回唯一 Query lifecycle；调用方不得替换其 ownership。
func (s *Statement) Query() QueryLifecycle { return s.query }

// CommitCatalog 提交 DDL metadata；query lifecycle 仍由 Close 独立收尾。
func (s *Statement) CommitCatalog(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("statement is closed")
	}
	if s.committed {
		return nil
	}
	if err := s.catalog.Commit(ctx); err != nil {
		return err
	}
	s.committed = true
	return nil
}

// Close 先终止/释放 Query，再回滚未提交 Catalog transaction；任一路径失败都继续尝试另一清理路径。
func (s *Statement) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var queryErr, catalogErr error
	if s.query != nil {
		s.query.Cancel()
		queryErr = s.query.Close()
	}
	if !s.committed && s.catalog != nil {
		catalogErr = s.catalog.Rollback(context.Background())
	}
	s.closed = true
	s.query = nil
	s.catalog = nil
	return errors.Join(queryErr, catalogErr)
}
