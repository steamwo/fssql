package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/storage/remote"
)

var (
	// ErrPoolClosed 表示 FTP/FTPS session pool 已关闭。
	ErrPoolClosed = errors.New("ftp: session pool closed")
	// ErrConnection 表示可重试的 FTP/FTPS 连接级失败。
	ErrConnection = errors.New("ftp: connection failure")
)

// Features 描述 FTP endpoint 运行时探测到的协议能力。
type Features struct {
	// RestStream 表示服务器支持 REST STREAM，可用于精确 offset 读取。
	RestStream bool
	// MLSD 表示服务器支持机器可读目录列表。
	MLSD bool
}

// FileInfo 是 FTP 层与 VFS ObjectIdentity 之间的最小元数据边界。
type FileInfo struct {
	// Size 是远端对象字节数。
	Size int64
	// ModUnix 是 UTC 秒级修改时间；未知时为 0。
	ModUnix int64
	// IsDir 表示目录对象。
	IsDir bool
}

// DirEntry 是 FTP 目录列表项。
type DirEntry struct {
	// Name 是目录内名称，不包含父路径。
	Name string
	// Info 是列表项元数据。
	Info FileInfo
}

// StreamWriter 表示绑定单个 FTP data connection 的流式写事务。
type StreamWriter interface {
	// Write 写入数据；实现不得在返回后保留 caller slice。
	Write(context.Context, []byte) (int, error)
	// Close 完成 STOR 并等待服务器最终确认。
	Close(context.Context) error
	// Abort 中止未完成 STOR，并清理 data/control 状态。
	Abort(context.Context) error
}

// Session 是 FTP/FTPS 驱动使用的协议会话抽象。
type Session interface {
	// Features 返回登录后探测到的 endpoint 能力。
	Features(context.Context) (Features, error)
	// Stat 返回对象大小与修改时间。
	Stat(context.Context, string) (FileInfo, error)
	// List 返回目录机器可读列表。
	List(context.Context, string) ([]DirEntry, error)
	// ReadAt 从指定 offset 精确读取 dst 长度的数据，不得整文件 fallback。
	ReadAt(context.Context, string, []byte, int64) (int, error)
	// OpenWrite 打开 STOR 流式写事务。
	OpenWrite(context.Context, string) (StreamWriter, error)
	// Delete 删除远端 incomplete/staging 文件。
	Delete(context.Context, string) error
	// Close 关闭 control connection。
	Close() error
}

// SessionFactory 按 endpoint/credential/security profile 创建协议会话。
type SessionFactory interface {
	Dial(context.Context, remote.ClientKey) (Session, error)
}

// SessionFactoryFunc 允许函数直接作为 SessionFactory。
type SessionFactoryFunc func(context.Context, remote.ClientKey) (Session, error)

// Dial 实现 SessionFactory。
func (f SessionFactoryFunc) Dial(ctx context.Context, key remote.ClientKey) (Session, error) {
	return f(ctx, key)
}

// Pool 是 bounded FTP/FTPS session pool。
type Pool struct {
	// key 固定 endpoint、credential version 与 security profile。
	key remote.ClientKey
	// factory 创建新的协议 session。
	factory SessionFactory
	// max 限制单 endpoint 同时存在的 session 数。
	max int
	// mu 保护 idle/total/leased/closed。
	mu sync.Mutex
	// idle 保存可复用 session。
	idle []Session
	// total 是 pool 当前拥有的 session 总数。
	total int
	// leased 是当前被调用方持有的 session 数。
	leased int
	// closed 阻止新的 Acquire。
	closed bool
	// notify 在 pool 状态变化时唤醒等待者。
	notify chan struct{}
}

// NewPool 创建 bounded session pool。
func NewPool(key remote.ClientKey, max int, factory SessionFactory) (*Pool, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if max <= 0 {
		max = 4
	}
	if factory == nil {
		return nil, errors.New("ftp: nil session factory")
	}
	return &Pool{key: key, factory: factory, max: max, notify: make(chan struct{})}, nil
}

// signal 通知等待 Acquire 的 goroutine 重新检查 pool 状态；调用方必须持有 p.mu。
func (p *Pool) signal() {
	close(p.notify)
	p.notify = make(chan struct{})
}

// Acquire 获取一个 session；达到上限时阻塞但可被 context 取消。
func (p *Pool) Acquire(ctx context.Context) (Session, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if n := len(p.idle); n > 0 {
			session := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.leased++
			p.mu.Unlock()
			return session, nil
		}
		if p.total < p.max {
			p.total++
			p.leased++
			p.mu.Unlock()
			session, err := p.factory.Dial(ctx, p.key)
			if err != nil {
				p.mu.Lock()
				p.total--
				p.leased--
				p.signal()
				p.mu.Unlock()
				return nil, err
			}
			if session == nil {
				p.mu.Lock()
				p.total--
				p.leased--
				p.signal()
				p.mu.Unlock()
				return nil, errors.New("ftp: factory returned nil session")
			}
			return session, nil
		}
		ch := p.notify
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}

// Release 归还 session；poison=true 时关闭并丢弃连接。
func (p *Pool) Release(session Session, poison bool) {
	if session == nil {
		return
	}
	p.mu.Lock()
	if p.leased > 0 {
		p.leased--
	}
	if p.closed || poison {
		if p.total > 0 {
			p.total--
		}
		p.signal()
		p.mu.Unlock()
		_ = session.Close()
		return
	}
	p.idle = append(p.idle, session)
	p.signal()
	p.mu.Unlock()
}

// Close 关闭全部 idle session；leased session 会在归还时被关闭。
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.total -= len(idle)
	p.signal()
	p.mu.Unlock()
	var first error
	for _, session := range idle {
		if err := session.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Stats 返回当前 pool ownership 计数，用于 leak assertion。
func (p *Pool) Stats() (total, leased, idle int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total, p.leased, len(p.idle)
}

// Manager 按 RemoteClientManager 的 endpoint identity 管理多个 bounded pool。
type Manager struct {
	// mu 保护 pools。
	mu sync.Mutex
	// pools 以 endpoint+credential version+security profile 隔离连接复用。
	pools map[remote.ClientKey]*Pool
	// maxPools 限制全局 FTP endpoint pool 数量。
	maxPools int
	// maxSessions 限制单 endpoint session 数量。
	maxSessions int
	// factory 创建协议 session。
	factory SessionFactory
}

// NewManager 创建 FTP/FTPS pool manager。
func NewManager(maxPools, maxSessions int, factory SessionFactory) *Manager {
	if maxPools <= 0 {
		maxPools = 32
	}
	if maxSessions <= 0 {
		maxSessions = 4
	}
	return &Manager{pools: make(map[remote.ClientKey]*Pool), maxPools: maxPools, maxSessions: maxSessions, factory: factory}
}

// Pool 返回 endpoint-scoped pool；credential version 变化会自然生成新 pool。
func (m *Manager) Pool(key remote.ClientKey) (*Pool, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if pool := m.pools[key]; pool != nil {
		return pool, nil
	}
	if len(m.pools) >= m.maxPools {
		return nil, fmt.Errorf("ftp: pool limit reached")
	}
	pool, err := NewPool(key, m.maxSessions, m.factory)
	if err != nil {
		return nil, err
	}
	m.pools[key] = pool
	return pool, nil
}

// Close 关闭全部 endpoint pool。
func (m *Manager) Close() error {
	m.mu.Lock()
	pools := make([]*Pool, 0, len(m.pools))
	for _, pool := range m.pools {
		pools = append(pools, pool)
	}
	m.pools = make(map[remote.ClientKey]*Pool)
	m.mu.Unlock()
	var first error
	for _, pool := range pools {
		if err := pool.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// MaxSessions 返回单 endpoint session 上限，用于 capability 计算。
func (m *Manager) MaxSessions() int { return m.maxSessions }

// IsRetryable 判断错误是否应丢弃当前 session 并允许上层重试。
func IsRetryable(err error) bool {
	return errors.Is(err, ErrConnection) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}
