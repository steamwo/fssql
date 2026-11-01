package sftp

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// writeTestStore 在多个 fake session 之间共享远端对象状态，用于验证 poison 后的新 session 能清理 incomplete file。
type writeTestStore struct {
	mu        sync.Mutex
	files     map[string][]byte
	failWrite error
	removes   int
}

// writeTestSession 是同时满足 Session/WritableSession 的测试 session。
type writeTestSession struct {
	store  *writeTestStore
	closed bool
}

func (s *writeTestSession) Stat(_ context.Context, path string) (FileInfo, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	b, ok := s.store.files[path]
	if !ok {
		return FileInfo{}, errors.New("not found")
	}
	return FileInfo{Size: int64(len(b)), ModUnix: 1234}, nil
}
func (*writeTestSession) List(context.Context, string) ([]DirEntry, error) { return nil, nil }
func (s *writeTestSession) ReadAt(_ context.Context, path string, dst []byte, off int64) (int, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	b := s.store.files[path]
	if off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(dst, b[off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *writeTestSession) WriteAt(_ context.Context, path string, src []byte, off int64, truncate bool) (int, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if s.store.failWrite != nil {
		err := s.store.failWrite
		s.store.failWrite = nil
		return 0, err
	}
	if truncate {
		s.store.files[path] = nil
	}
	b := s.store.files[path]
	need := int(off) + len(src)
	if need > len(b) {
		next := make([]byte, need)
		copy(next, b)
		b = next
	}
	copy(b[int(off):], src)
	s.store.files[path] = b
	return len(src), nil
}
func (s *writeTestSession) Remove(_ context.Context, path string) error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	delete(s.store.files, path)
	s.store.removes++
	return nil
}
func (s *writeTestSession) Close() error { s.closed = true; return nil }

// newWriteTestDriver 创建 endpoint-scoped bounded pool driver。
func newWriteTestDriver(t *testing.T, store *writeTestStore, maxSessions int) (*Driver, *Manager, *Pool) {
	t.Helper()
	key := remote.ClientKey{Endpoint: "host:22", CredentialScope: "scope", CredentialVersion: "v1", SecurityProfile: "strict"}
	manager := NewManager(2, maxSessions, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) {
		return &writeTestSession{store: store}, nil
	}))
	d, err := NewDriver(key, manager, 2)
	if err != nil {
		t.Fatal(err)
	}
	p, err := manager.Pool(key)
	if err != nil {
		t.Fatal(err)
	}
	return d, manager, p
}

// TestSFTPStreamingWriteThroughVFSCapabilityGate 验证真实 capability、顺序 offset 与最终 identity。
func TestSFTPStreamingWriteThroughVFSCapabilityGate(t *testing.T) {
	store := &writeTestStore{files: map[string][]byte{}}
	d, manager, pool := newWriteTestDriver(t, store, 2)
	defer manager.Close()
	if !d.StaticCapabilities().Has(vfs.StreamingWrite) {
		t.Fatal("SFTP driver must advertise implemented StreamingWrite")
	}
	if d.StaticCapabilities().Has(vfs.ConditionalWrite) || d.StaticCapabilities().Has(vfs.AtomicRename) {
		t.Fatal("SFTP driver advertised unimplemented commit capabilities")
	}
	registry, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("sftp://host:22/out/data.csv")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	h, err := registry.OpenWrite(context.Background(), u, vfs.WriteOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := h.Write(context.Background(), []byte("abc")); err != nil || n != 3 {
		t.Fatalf("write1 n=%d err=%v", n, err)
	}
	if n, err := h.Write(context.Background(), []byte("def")); err != nil || n != 3 {
		t.Fatalf("write2 n=%d err=%v", n, err)
	}
	identity, err := h.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Size != 6 {
		t.Fatalf("final size=%d", identity.Size)
	}
	store.mu.Lock()
	got := string(store.files["/out/data.csv"])
	store.mu.Unlock()
	if got != "abcdef" {
		t.Fatalf("remote data=%q", got)
	}
	_, leased, _ := pool.Stats()
	if leased != 0 {
		t.Fatalf("session lease leak=%d", leased)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory changed unexpectedly=%d", scope.Used())
	}
}

// TestSFTPWriteFailureKeepsAbortPath 验证连接失败后 Close 不会封死 Abort，poison session 被丢弃且 incomplete file 被删除。
func TestSFTPWriteFailureKeepsAbortPath(t *testing.T) {
	store := &writeTestStore{files: map[string][]byte{"/out/data.csv": []byte("old")}, failWrite: ErrConnection}
	d, manager, pool := newWriteTestDriver(t, store, 1)
	defer manager.Close()
	u, _ := vfs.ParseURI("sftp://host:22/out/data.csv")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	h, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(context.Background(), []byte("new")); !errors.Is(err, ErrConnection) {
		t.Fatalf("expected connection error, got %v", err)
	}
	if _, err := h.Close(context.Background()); !errors.Is(err, ErrConnection) {
		t.Fatalf("Close must preserve write failure for Abort, got %v", err)
	}
	if err := h.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	_, exists := store.files["/out/data.csv"]
	removes := store.removes
	store.mu.Unlock()
	if exists || removes != 1 {
		t.Fatalf("incomplete file cleanup failed exists=%v removes=%d", exists, removes)
	}
	_, leased, _ := pool.Stats()
	if leased != 0 {
		t.Fatalf("session lease leak=%d", leased)
	}
}

// TestSFTPConditionalWriteIsRejected 验证未实现的条件写不会越过 VFS capability gate。
func TestSFTPConditionalWriteIsRejected(t *testing.T) {
	store := &writeTestStore{files: map[string][]byte{}}
	d, manager, _ := newWriteTestDriver(t, store, 1)
	defer manager.Close()
	registry, _ := vfs.NewRegistry(d)
	u, _ := vfs.ParseURI("sftp://host:22/out/data.csv")
	mm := memory.NewManager(1 << 20)
	_, err := registry.OpenWrite(context.Background(), u, vfs.WriteOptions{IfNoneMatch: true}, mm.NewScope(), memory.NewBufferManager(nil))
	if !errors.Is(err, vfs.ErrWriteUnsupported) {
		t.Fatalf("expected conditional write rejection, got %v", err)
	}
}
