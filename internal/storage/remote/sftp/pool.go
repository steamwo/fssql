package sftp

import (
	"context"
	"errors"
	"sync"
)

var ErrPoolClosed = errors.New("sftp: session pool closed")

type Session interface {
	Stat(context.Context, string) (RemoteInfo, error)
	List(context.Context, string, string, int) (RemotePage, error)
	ReadAt(context.Context, string, int64, []byte) (int, error)
	Close() error
}

type DialResult struct {
	Session       Session
	HostKeySHA256 string
}

type Dialer interface {
	Dial(context.Context, DialConfig) (DialResult, error)
}

type DialConfig struct {
	Endpoint          string
	CredentialScope   string
	CredentialVersion string
	SecurityProfile   string
}

type sessionPool struct {
	dialer Dialer
	cfg    DialConfig
	max    int
	slots  chan struct{}

	mu     sync.Mutex
	idle   []Session
	open   int
	leased int
	closed bool
}

func newSessionPool(max int, dialer Dialer, cfg DialConfig) *sessionPool {
	if max <= 0 {
		max = 8
	}
	return &sessionPool{dialer: dialer, cfg: cfg, max: max, slots: make(chan struct{}, max)}
}

type lease struct {
	pool     *sessionPool
	session  Session
	poisoned bool
	once     sync.Once
}

func (p *sessionPool) acquire(ctx context.Context) (*lease, string, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		<-p.slots
		return nil, "", ErrPoolClosed
	}
	if n := len(p.idle); n > 0 {
		s := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.leased++
		p.mu.Unlock()
		return &lease{pool: p, session: s}, "", nil
	}
	p.open++
	p.leased++
	p.mu.Unlock()

	result, err := p.dialer.Dial(ctx, p.cfg)
	if err != nil || result.Session == nil {
		p.mu.Lock()
		p.open--
		p.leased--
		p.mu.Unlock()
		<-p.slots
		if err == nil {
			err = errors.New("sftp: dialer returned nil session")
		}
		return nil, result.HostKeySHA256, err
	}
	return &lease{pool: p, session: result.Session}, result.HostKeySHA256, nil
}

func (l *lease) Poison() { l.poisoned = true }

func (l *lease) Release() {
	l.once.Do(func() {
		p := l.pool
		p.mu.Lock()
		p.leased--
		closed := p.closed
		if l.poisoned || closed {
			p.open--
		} else {
			p.idle = append(p.idle, l.session)
		}
		p.mu.Unlock()
		if l.poisoned || closed {
			_ = l.session.Close()
		}
		<-p.slots
	})
}

func (p *sessionPool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	idle := append([]Session(nil), p.idle...)
	p.open -= len(idle)
	p.idle = nil
	p.mu.Unlock()
	for _, s := range idle {
		_ = s.Close()
	}
}

func (p *sessionPool) stats() (open, leased, idle int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.open, p.leased, len(p.idle)
}
