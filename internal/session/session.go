package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/settings"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
)

// Session 保存 AD-035 的 Engine/ResourceGroup/Session 配置层与 Catalog 入口；它不拥有 Query runtime。
type Session struct {
	catalog       catalog.Catalog    // catalog 是 statement 级 Catalog Transaction 的来源。
	registry      *settings.Registry // registry 负责冻结 immutable QuerySettings。
	engine        settings.Layer     // engine 是引擎默认覆盖层快照。
	resourceGroup settings.Layer     // resourceGroup 是资源组覆盖层快照。
	session       settings.Layer     // session 是会话覆盖层快照。
}

// New 创建 Session 控制面对象；Session 本身不启动 goroutine 或分配 Query managed memory。
func New(c catalog.Catalog, registry *settings.Registry, engine, resourceGroup, sessionLayer settings.Layer) (*Session, error) {
	if c == nil || registry == nil {
		return nil, fmt.Errorf("session requires catalog and settings registry")
	}
	return &Session{catalog: c, registry: registry, engine: engine, resourceGroup: resourceGroup, session: sessionLayer}, nil
}

// BeginStatement 冻结 Statement 设置并开启独立 Catalog Transaction。
func (s *Session) BeginStatement(ctx context.Context, statement settings.Layer) (*Statement, error) {
	if s == nil || s.catalog == nil || s.registry == nil {
		return nil, fmt.Errorf("session is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resolved, err := s.registry.Resolve(s.engine, s.resourceGroup, s.session, statement)
	if err != nil {
		return nil, err
	}
	tx, err := s.catalog.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &Statement{settings: resolved, catalogTx: tx}, nil
}

// Statement 是单条 SQL 的 binding/planning 控制面 ownership。
type Statement struct {
	mu        sync.Mutex             // mu 保护 catalog transaction ownership 与 closed 状态。
	settings  settings.QuerySettings // settings 是创建 Statement 时冻结的 immutable QuerySettings。
	catalogTx catalog.Transaction    // catalogTx 与 SourceSnapshot/Query/Export transaction 相互独立。
	closed    bool                   // closed 防止 Statement 关闭后继续创建 QueryContext。
}

// Settings 返回 immutable QuerySettings 的值副本。
func (s *Statement) Settings() settings.QuerySettings {
	if s == nil {
		return settings.QuerySettings{}
	}
	return s.settings
}

// CatalogTransaction 返回当前 statement 的 Catalog Transaction；closed/commit 后返回错误。
func (s *Statement) CatalogTransaction() (catalog.Transaction, error) {
	if s == nil {
		return nil, fmt.Errorf("statement is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.catalogTx == nil {
		return nil, fmt.Errorf("statement catalog transaction is closed")
	}
	return s.catalogTx, nil
}

// NewQueryContext 将冻结设置复制到独立 QueryContext；Query ownership 随返回值移交给调用方。
func (s *Statement) NewQueryContext(parent context.Context, manager *memory.Manager, tempScope *storagetemp.QueryScope) (*query.Context, error) {
	if s == nil || manager == nil {
		return nil, fmt.Errorf("statement and memory manager are required")
	}
	if parent == nil {
		return nil, fmt.Errorf("query parent context is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("statement is closed")
	}
	return query.NewConfigured(parent, manager, tempScope, s.settings), nil
}

// CommitCatalog 提交 Catalog metadata transaction；不会关闭或取消已经移交的 QueryContext。
func (s *Statement) CommitCatalog(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("statement is nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("statement is closed")
	}
	if s.catalogTx == nil {
		return fmt.Errorf("statement catalog transaction is closed")
	}
	if err := s.catalogTx.Commit(ctx); err != nil {
		return err
	}
	s.catalogTx = nil
	return nil
}

// Close 释放 Statement 的 Catalog Transaction；即使 ctx 已取消也调用 Rollback 以确保 cleanup。
func (s *Statement) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var err error
	if s.catalogTx != nil {
		err = s.catalogTx.Rollback(ctx)
		s.catalogTx = nil
	}
	s.closed = true
	return err
}
