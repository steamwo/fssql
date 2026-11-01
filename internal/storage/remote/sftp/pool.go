package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/storage/remote"
)

var (
	ErrPoolClosed = errors.New("sftp: session pool closed")
	ErrHostKey    = errors.New("sftp: host-key verification failed")
	ErrConnection = errors.New("sftp: connection failure")
)

type FileInfo struct {
	Size    int64
	ModUnix int64
	IsDir   bool
}
type DirEntry struct {
	Name string
	Info FileInfo
}
type Session interface {
	Stat(context.Context, string) (FileInfo, error)
	List(context.Context, string) ([]DirEntry, error)
	ReadAt(context.Context, string, []byte, int64) (int, error)
	Close() error
}
type SessionFactory interface {
	Dial(context.Context, remote.ClientKey) (Session, error)
}
type SessionFactoryFunc func(context.Context, remote.ClientKey) (Session, error)

func (f SessionFactoryFunc) Dial(ctx context.Context, k remote.ClientKey) (Session, error) {
	return f(ctx, k)
}

type Pool struct {
	key     remote.ClientKey
	factory SessionFactory
	max     int
	mu      sync.Mutex
	idle    []Session
	total   int
	leased  int
	closed  bool
	notify  chan struct{}
}

func NewPool(key remote.ClientKey, max int, f SessionFactory) (*Pool, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if max <= 0 {
		max = 4
	}
	if f == nil {
		return nil, errors.New("sftp: nil session factory")
	}
	return &Pool{key: key, factory: f, max: max, notify: make(chan struct{})}, nil
}
func (p *Pool) signal() { close(p.notify); p.notify = make(chan struct{}) }
func (p *Pool) Acquire(ctx context.Context) (Session, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		n := len(p.idle)
		if n > 0 {
			s := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.leased++
			p.mu.Unlock()
			return s, nil
		}
		if p.total < p.max {
			p.total++
			p.leased++
			p.mu.Unlock()
			s, err := p.factory.Dial(ctx, p.key)
			if err != nil {
				p.mu.Lock()
				p.total--
				p.leased--
				p.signal()
				p.mu.Unlock()
				return nil, err
			}
			if s == nil {
				p.mu.Lock()
				p.total--
				p.leased--
				p.signal()
				p.mu.Unlock()
				return nil, errors.New("sftp: factory returned nil session")
			}
			return s, nil
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
func (p *Pool) Release(s Session, poison bool) {
	if s == nil {
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
		_ = s.Close()
		return
	}
	p.idle = append(p.idle, s)
	p.signal()
	p.mu.Unlock()
}
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
	for _, s := range idle {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
func (p *Pool) Stats() (total, leased, idle int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total, p.leased, len(p.idle)
}

type Manager struct {
	mu                    sync.Mutex
	pools                 map[remote.ClientKey]*Pool
	maxPools, maxSessions int
	factory               SessionFactory
}

func NewManager(maxPools, maxSessions int, f SessionFactory) *Manager {
	if maxPools <= 0 {
		maxPools = 32
	}
	if maxSessions <= 0 {
		maxSessions = 4
	}
	return &Manager{pools: map[remote.ClientKey]*Pool{}, maxPools: maxPools, maxSessions: maxSessions, factory: f}
}
func (m *Manager) Pool(key remote.ClientKey) (*Pool, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.pools[key]; p != nil {
		return p, nil
	}
	if len(m.pools) >= m.maxPools {
		return nil, fmt.Errorf("sftp: pool limit reached")
	}
	p, err := NewPool(key, m.maxSessions, m.factory)
	if err != nil {
		return nil, err
	}
	m.pools[key] = p
	return p, nil
}
func (m *Manager) Close() error {
	m.mu.Lock()
	ps := make([]*Pool, 0, len(m.pools))
	for _, p := range m.pools {
		ps = append(ps, p)
	}
	m.pools = map[remote.ClientKey]*Pool{}
	m.mu.Unlock()
	var first error
	for _, p := range ps {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func IsRetryable(err error) bool {
	return errors.Is(err, ErrConnection) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

func (m *Manager) MaxSessions() int { return m.maxSessions }
