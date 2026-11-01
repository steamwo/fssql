package server

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	querypkg "github.com/steamwo/fssql/internal/query"
)

var (
	// ErrQueryNotFound 表示 QueryRegistry 中没有对应 Query。
	ErrQueryNotFound = errors.New("server: query not found")
	// ErrQueryExists 表示重复注册同一个 Query ID。
	ErrQueryExists = errors.New("server: query already exists")
	// ErrQueryNotTerminal 表示调用方试图移除仍在执行生命周期中的 Query。
	ErrQueryNotTerminal = errors.New("server: query is not terminal")
)

// QueryID 是 server QueryRegistry 的稳定查询标识。
type QueryID string

// IDGenerator 生成 Query ID，测试可注入确定性实现。
type IDGenerator func() (QueryID, error)

// QueryRegistry 是 AD-037 中独立于 HTTP 请求生命周期的 Query 所有权注册表。
type QueryRegistry struct {
	// mu 保护 handles map，QueryHandle 自身状态由其内部锁保护。
	mu sync.RWMutex
	// handles 保存当前仍由 server 持有 ownership 的 QueryHandle。
	handles map[QueryID]*QueryHandle
	// idGenerator 生成不可预测 Query ID。
	idGenerator IDGenerator
	// now 为测试提供确定性时间源。
	now func() time.Time
}

// NewQueryRegistry 创建空 QueryRegistry。
func NewQueryRegistry() *QueryRegistry {
	return NewQueryRegistryWith(generateQueryID, func() time.Time { return time.Now().UTC() })
}

// NewQueryRegistryWith 创建可注入 ID/时钟的 QueryRegistry；主要用于确定性测试。
func NewQueryRegistryWith(idGenerator IDGenerator, now func() time.Time) *QueryRegistry {
	if idGenerator == nil {
		idGenerator = generateQueryID
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &QueryRegistry{handles: make(map[QueryID]*QueryHandle), idGenerator: idGenerator, now: now}
}

// Register 为 QueryContext 创建 SUBMITTED QueryHandle 并取得 registry ownership。
func (r *QueryRegistry) Register(queryContext *querypkg.Context) (*QueryHandle, error) {
	if queryContext == nil {
		return nil, errors.New("server: nil query context")
	}
	id, err := r.idGenerator()
	if err != nil {
		return nil, fmt.Errorf("server: generate query id: %w", err)
	}
	return r.RegisterWithID(id, queryContext)
}

// RegisterWithID 使用显式 Query ID 注册；用于恢复/测试等需要稳定标识的内部路径。
func (r *QueryRegistry) RegisterWithID(id QueryID, queryContext *querypkg.Context) (*QueryHandle, error) {
	handle, err := NewQueryHandle(id, queryContext, r.now())
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handles[id]; exists {
		return nil, fmt.Errorf("%w: %s", ErrQueryExists, id)
	}
	r.handles[id] = handle
	return handle, nil
}

// Get 返回 QueryHandle；调用方获得的是 server-owned handle 引用，不转移 registry ownership。
func (r *QueryRegistry) Get(id QueryID) (*QueryHandle, error) {
	r.mu.RLock()
	handle := r.handles[id]
	r.mu.RUnlock()
	if handle == nil {
		return nil, fmt.Errorf("%w: %s", ErrQueryNotFound, id)
	}
	return handle, nil
}

// RequestCancel 向 QueryContext 传播取消请求，但不把 Query 提前标记为 CANCELED。
func (r *QueryRegistry) RequestCancel(id QueryID) error {
	handle, err := r.Get(id)
	if err != nil {
		return err
	}
	return handle.RequestCancel(r.now())
}

// Remove 只释放已经终态且 QueryContext 已关闭的 handle ownership。
func (r *QueryRegistry) Remove(id QueryID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	handle := r.handles[id]
	if handle == nil {
		return fmt.Errorf("%w: %s", ErrQueryNotFound, id)
	}
	snapshot := handle.Snapshot()
	if !snapshot.State.Terminal() || !handle.Context().Closed() {
		return fmt.Errorf("%w: %s state=%s", ErrQueryNotTerminal, id, snapshot.State.String())
	}
	delete(r.handles, id)
	return nil
}

// Count 返回 registry 当前持有的 QueryHandle 数量。
func (r *QueryRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handles)
}

// generateQueryID 生成 128-bit 随机 Query ID，避免外部 API 暴露可预测序号。
func generateQueryID() (QueryID, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return QueryID(hex.EncodeToString(raw[:])), nil
}
