package memory

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrLimitExceeded = errors.New("memory limit exceeded")
var ErrClosed = errors.New("memory scope closed")

type Tag string

const (
	TagVector     Tag = "Vector"
	TagStringHeap Tag = "StringHeap"
	TagHashTable  Tag = "HashTable"
	TagMetadata   Tag = "Metadata"
)

type Manager struct {
	limit int64
	used  atomic.Int64
	peak  atomic.Int64
}

func NewManager(limit int64) *Manager { return &Manager{limit: limit} }
func (m *Manager) Limit() int64       { return m.limit }
func (m *Manager) Used() int64        { return m.used.Load() }
func (m *Manager) Peak() int64        { return m.peak.Load() }
func (m *Manager) reserve(n int64) error {
	if n < 0 {
		return fmt.Errorf("negative reservation: %d", n)
	}
	for {
		cur := m.used.Load()
		if m.limit >= 0 && n > m.limit-cur {
			return ErrLimitExceeded
		}
		next := cur + n
		if m.used.CompareAndSwap(cur, next) {
			for p := m.peak.Load(); next > p && !m.peak.CompareAndSwap(p, next); p = m.peak.Load() {
			}
			return nil
		}
	}
}
func (m *Manager) release(n int64) { m.used.Add(-n) }

type Scope struct {
	manager *Manager
	mu      sync.Mutex
	closed  bool
	used    int64
	peak    int64
	byTag   map[Tag]int64
}

func (m *Manager) NewScope() *Scope      { return &Scope{manager: m, byTag: make(map[Tag]int64)} }
func (s *Scope) Used() int64             { s.mu.Lock(); defer s.mu.Unlock(); return s.used }
func (s *Scope) Peak() int64             { s.mu.Lock(); defer s.mu.Unlock(); return s.peak }
func (s *Scope) UsedByTag(tag Tag) int64 { s.mu.Lock(); defer s.mu.Unlock(); return s.byTag[tag] }
func (s *Scope) Reserve(tag Tag, n int64) (*Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if err := s.manager.reserve(n); err != nil {
		return nil, err
	}
	s.used += n
	s.peak = max(s.peak, s.used)
	s.byTag[tag] += n
	return &Reservation{scope: s, tag: tag, size: n}, nil
}
func (s *Scope) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.used != 0 {
		return fmt.Errorf("memory scope closed with %d reserved bytes", s.used)
	}
	s.closed = true
	return nil
}

type Reservation struct {
	mu       sync.Mutex
	scope    *Scope
	tag      Tag
	size     int64
	released bool
}

func (r *Reservation) Size() int64 { r.mu.Lock(); defer r.mu.Unlock(); return r.size }
func (r *Reservation) Resize(n int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return ErrClosed
	}
	delta := n - r.size
	if delta == 0 {
		return nil
	}
	s := r.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if delta > 0 {
		if err := s.manager.reserve(delta); err != nil {
			return err
		}
	} else {
		s.manager.release(-delta)
	}
	s.used += delta
	s.peak = max(s.peak, s.used)
	s.byTag[r.tag] += delta
	r.size = n
	return nil
}
func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	s := r.scope
	s.mu.Lock()
	s.used -= r.size
	s.byTag[r.tag] -= r.size
	s.manager.release(r.size)
	s.mu.Unlock()
	r.size = 0
	r.released = true
}

type Allocator func(int) ([]byte, error)

func DefaultAllocator(n int) ([]byte, error) { return make([]byte, n), nil }

type Buffer struct {
	mu          sync.Mutex
	reservation *Reservation
	data        []byte
	released    bool
}

func (b *Buffer) Bytes() []byte { b.mu.Lock(); defer b.mu.Unlock(); return b.data }
func (b *Buffer) Len() int      { b.mu.Lock(); defer b.mu.Unlock(); return len(b.data) }
func (b *Buffer) Resize(n int, allocator Allocator) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return ErrClosed
	}
	if allocator == nil {
		allocator = DefaultAllocator
	}
	oldLen := len(b.data)
	if n == oldLen {
		return nil
	}
	if err := b.reservation.Resize(int64(n)); err != nil {
		return err
	}
	data, err := allocator(n)
	if err != nil {
		_ = b.reservation.Resize(int64(oldLen))
		return err
	}
	copy(data, b.data)
	b.data = data
	return nil
}

func (b *Buffer) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return
	}
	b.data = nil
	b.reservation.Release()
	b.released = true
}

type BufferManager struct{ allocator Allocator }

func NewBufferManager(allocator Allocator) *BufferManager {
	if allocator == nil {
		allocator = DefaultAllocator
	}
	return &BufferManager{allocator: allocator}
}
func (bm *BufferManager) Allocate(scope *Scope, tag Tag, n int) (*Buffer, error) {
	res, err := scope.Reserve(tag, int64(n))
	if err != nil {
		return nil, err
	}
	data, err := bm.allocator(n)
	if err != nil {
		res.Release()
		return nil, err
	}
	if len(data) != n {
		res.Release()
		return nil, fmt.Errorf("allocator returned %d bytes, want %d", len(data), n)
	}
	return &Buffer{reservation: res, data: data}, nil
}

// MemoryManager is the canonical engine memory accounting component.
type MemoryManager = Manager

// BufferAllocator reserves managed bytes before producing backing storage.
type BufferAllocator = Allocator

// SpillCoordinator is a Phase 0 boundary only; spill algorithms are implemented in Phase 2.
type SpillCoordinator interface {
	Reclaim(targetBytes int64) (reclaimed int64, err error)
}
