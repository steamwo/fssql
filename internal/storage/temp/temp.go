package temp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	ErrQuotaExceeded = errors.New("temporary storage quota exceeded")
	ErrClosed        = errors.New("temporary storage scope closed")
)

type Limits struct {
	Global               int64
	DefaultResourceGroup int64
	DefaultQuery         int64
	ResourceGroups       map[string]int64
}

type quota struct {
	limit int64
	used  atomic.Int64
	peak  atomic.Int64
}

func newQuota(limit int64) *quota { return &quota{limit: limit} }
func (q *quota) Limit() int64     { return q.limit }
func (q *quota) Used() int64      { return q.used.Load() }
func (q *quota) Peak() int64      { return q.peak.Load() }

func (q *quota) reserve(n int64) error {
	if n < 0 {
		return fmt.Errorf("negative temporary storage reservation: %d", n)
	}
	for {
		cur := q.used.Load()
		if q.limit >= 0 && n > q.limit-cur {
			return ErrQuotaExceeded
		}
		next := cur + n
		if q.used.CompareAndSwap(cur, next) {
			for peak := q.peak.Load(); next > peak && !q.peak.CompareAndSwap(peak, next); peak = q.peak.Load() {
			}
			return nil
		}
	}
}

func (q *quota) release(n int64) { q.used.Add(-n) }

type TemporaryMemoryManager struct {
	global       *quota
	defaultGroup int64
	defaultQuery int64

	mu          sync.Mutex
	groupLimits map[string]int64
	groups      map[string]*quota
}

func NewTemporaryMemoryManager(limits Limits) *TemporaryMemoryManager {
	groupLimits := make(map[string]int64, len(limits.ResourceGroups))
	for name, limit := range limits.ResourceGroups {
		groupLimits[name] = limit
	}
	return &TemporaryMemoryManager{
		global:       newQuota(limits.Global),
		defaultGroup: limits.DefaultResourceGroup,
		defaultQuery: limits.DefaultQuery,
		groupLimits:  groupLimits,
		groups:       make(map[string]*quota),
	}
}

func (m *TemporaryMemoryManager) GlobalUsed() int64  { return m.global.Used() }
func (m *TemporaryMemoryManager) GlobalPeak() int64  { return m.global.Peak() }
func (m *TemporaryMemoryManager) GlobalLimit() int64 { return m.global.Limit() }

func (m *TemporaryMemoryManager) resourceGroup(name string) *quota {
	m.mu.Lock()
	defer m.mu.Unlock()
	if q, ok := m.groups[name]; ok {
		return q
	}
	limit := m.defaultGroup
	if configured, ok := m.groupLimits[name]; ok {
		limit = configured
	}
	q := newQuota(limit)
	m.groups[name] = q
	return q
}

func (m *TemporaryMemoryManager) ResourceGroupUsed(name string) int64 {
	return m.resourceGroup(name).Used()
}

func (m *TemporaryMemoryManager) NewQueryScope(resourceGroup string) *QuotaScope {
	return &QuotaScope{
		manager: m,
		group:   m.resourceGroup(resourceGroup),
		query:   newQuota(m.defaultQuery),
	}
}

type QuotaScope struct {
	manager *TemporaryMemoryManager
	group   *quota
	query   *quota
	mu      sync.Mutex
	closed  bool
}

func (s *QuotaScope) Used() int64 { return s.query.Used() }
func (s *QuotaScope) Peak() int64 { return s.query.Peak() }

func (s *QuotaScope) Reserve(n int64) (*Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if err := s.manager.global.reserve(n); err != nil {
		return nil, err
	}
	if err := s.group.reserve(n); err != nil {
		s.manager.global.release(n)
		return nil, err
	}
	if err := s.query.reserve(n); err != nil {
		s.group.release(n)
		s.manager.global.release(n)
		return nil, err
	}
	return &Reservation{scope: s, size: n}, nil
}

func (s *QuotaScope) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if used := s.query.Used(); used != 0 {
		return fmt.Errorf("temporary storage scope closed with %d reserved bytes", used)
	}
	s.closed = true
	return nil
}

type Reservation struct {
	mu       sync.Mutex
	scope    *QuotaScope
	size     int64
	released bool
}

func (r *Reservation) Size() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

func (r *Reservation) shrink(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released || n >= r.size {
		return
	}
	delta := r.size - n
	r.scope.query.release(delta)
	r.scope.group.release(delta)
	r.scope.manager.global.release(delta)
	r.size = n
}

func (r *Reservation) transfer() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return 0
	}
	n := r.size
	r.size = 0
	r.released = true
	return n
}

func (r *Reservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return
	}
	r.scope.query.release(r.size)
	r.scope.group.release(r.size)
	r.scope.manager.global.release(r.size)
	r.size = 0
	r.released = true
}

type FileHandle interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Close() error
	Name() string
}

type Backend interface {
	MkdirAll(path string, perm fs.FileMode) error
	MkdirTemp(dir, pattern string) (string, error)
	CreateTemp(dir, pattern string) (FileHandle, error)
	Remove(path string) error
	RemoveAll(path string) error
}

type osBackend struct{}

func (osBackend) MkdirAll(path string, perm fs.FileMode) error  { return os.MkdirAll(path, perm) }
func (osBackend) MkdirTemp(dir, pattern string) (string, error) { return os.MkdirTemp(dir, pattern) }
func (osBackend) CreateTemp(dir, pattern string) (FileHandle, error) {
	return os.CreateTemp(dir, pattern)
}
func (osBackend) Remove(path string) error    { return os.Remove(path) }
func (osBackend) RemoveAll(path string) error { return os.RemoveAll(path) }

type TemporaryStorageManager struct {
	root    string
	quotas  *TemporaryMemoryManager
	backend Backend
}

// Used 返回当前存活临时文件的精确配额用量，不依赖目录元数据刷新。
func (m *TemporaryStorageManager) Used() int64 { return m.quotas.GlobalUsed() }

// Peak 返回该 manager 生命周期内实际写入临时文件的精确配额峰值。
func (m *TemporaryStorageManager) Peak() int64 { return m.quotas.GlobalPeak() }

func NewTemporaryStorageManager(root string, quotas *TemporaryMemoryManager) (*TemporaryStorageManager, error) {
	return newTemporaryStorageManager(root, quotas, osBackend{})
}

func newTemporaryStorageManager(root string, quotas *TemporaryMemoryManager, backend Backend) (*TemporaryStorageManager, error) {
	if quotas == nil {
		return nil, errors.New("temporary memory manager is required")
	}
	if backend == nil {
		return nil, errors.New("temporary storage backend is required")
	}
	root = filepath.Clean(root)
	if err := backend.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create temporary storage root: %w", err)
	}
	return &TemporaryStorageManager{root: root, quotas: quotas, backend: backend}, nil
}

func (m *TemporaryStorageManager) NewQueryScope(resourceGroup string) (*QueryScope, error) {
	quotaScope := m.quotas.NewQueryScope(resourceGroup)
	dir, err := m.backend.MkdirTemp(m.root, "query-")
	if err != nil {
		_ = quotaScope.Close()
		return nil, fmt.Errorf("create query temporary directory: %w", err)
	}
	return &QueryScope{
		quota:   quotaScope,
		backend: m.backend,
		dir:     dir,
		files:   make(map[*File]struct{}),
	}, nil
}

type QueryScope struct {
	quota   *QuotaScope
	backend Backend
	dir     string

	mu      sync.Mutex
	closing bool
	closed  bool
	files   map[*File]struct{}
}

func (s *QueryScope) Dir() string { return s.dir }
func (s *QueryScope) Used() int64 { return s.quota.Used() }
func (s *QueryScope) Peak() int64 { return s.quota.Peak() }

func (s *QueryScope) OwnedFiles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.files)
}

func (s *QueryScope) CreateFile() (*File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.closing {
		return nil, ErrClosed
	}
	h, err := s.backend.CreateTemp(s.dir, "spill-")
	if err != nil {
		return nil, fmt.Errorf("create temporary file: %w", err)
	}
	f := &File{scope: s, handle: h}
	s.files[f] = struct{}{}
	return f, nil
}

func (s *QueryScope) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	files := make([]*File, 0, len(s.files))
	for f := range s.files {
		files = append(files, f)
	}
	s.mu.Unlock()

	var errs []error
	for _, f := range files {
		if err := f.Remove(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	if err := s.backend.RemoveAll(s.dir); err != nil {
		return fmt.Errorf("remove query temporary directory: %w", err)
	}
	if err := s.quota.Close(); err != nil {
		return err
	}

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

type File struct {
	scope  *QueryScope
	handle FileHandle

	mu      sync.Mutex
	bytes   int64
	closed  bool
	removed bool
}

func (f *File) Name() string { return f.handle.Name() }
func (f *File) Size() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bytes
}

func (f *File) Write(p []byte) (int, error) {
	return f.WriteContext(context.Background(), p)
}

func (f *File) WriteContext(ctx context.Context, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.removed {
		return 0, ErrClosed
	}

	reservation, err := f.scope.quota.Reserve(int64(len(p)))
	if err != nil {
		return 0, err
	}

	n, writeErr := f.handle.Write(p)
	if n < 0 || n > len(p) {
		reservation.Release()
		return 0, fmt.Errorf("temporary file backend returned invalid write count %d for %d bytes", n, len(p))
	}
	if n < len(p) {
		reservation.shrink(int64(n))
		f.bytes += reservation.transfer()
		if writeErr == nil {
			writeErr = io.ErrShortWrite
		}
		return n, writeErr
	}

	f.bytes += reservation.transfer()
	if err := ctx.Err(); err != nil && writeErr == nil {
		writeErr = err
	}
	return n, writeErr
}

func (f *File) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.removed {
		return 0, ErrClosed
	}
	return f.handle.ReadAt(p, off)
}

func (f *File) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.removed {
		return ErrClosed
	}
	return f.handle.Sync()
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.removed {
		return nil
	}
	if err := f.handle.Close(); err != nil {
		return err
	}
	f.closed = true
	return nil
}

func (f *File) Remove() error {
	f.mu.Lock()
	if f.removed {
		f.mu.Unlock()
		return nil
	}
	if !f.closed {
		if err := f.handle.Close(); err != nil {
			f.mu.Unlock()
			return fmt.Errorf("close temporary file: %w", err)
		}
		f.closed = true
	}
	name := f.handle.Name()
	bytes := f.bytes
	f.mu.Unlock()

	if err := f.scope.backend.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove temporary file: %w", err)
	}

	f.mu.Lock()
	if f.removed {
		f.mu.Unlock()
		return nil
	}
	f.removed = true
	f.bytes = 0
	f.mu.Unlock()

	if bytes > 0 {
		f.scope.quota.query.release(bytes)
		f.scope.quota.group.release(bytes)
		f.scope.quota.manager.global.release(bytes)
	}
	f.scope.mu.Lock()
	delete(f.scope.files, f)
	f.scope.mu.Unlock()
	return nil
}
