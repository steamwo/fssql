package ftp

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

type fakeWriter struct {
	mu      sync.Mutex
	data    []byte
	closed  bool
	aborted bool
}

func (w *fakeWriter) Write(_ context.Context, data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.aborted {
		return 0, io.ErrClosedPipe
	}
	w.data = append(w.data, data...)
	return len(data), nil
}
func (w *fakeWriter) Close(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}
func (w *fakeWriter) Abort(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.aborted = true
	return nil
}

type fakeSession struct {
	mu       sync.Mutex
	features Features
	files    map[string][]byte
	closed   bool
}

func newFakeSession() *fakeSession {
	return &fakeSession{features: Features{RestStream: true, MLSD: true}, files: map[string][]byte{"/data.bin": []byte("abcdefghij")}}
}
func (s *fakeSession) Features(context.Context) (Features, error) { return s.features, nil }
func (s *fakeSession) Stat(_ context.Context, path string) (FileInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[path]
	if !ok {
		return FileInfo{}, errors.New("not found")
	}
	return FileInfo{Size: int64(len(data)), ModUnix: 1700000000}, nil
}
func (s *fakeSession) List(context.Context, string) ([]DirEntry, error) {
	return []DirEntry{{Name: "data.bin", Info: FileInfo{Size: 10, ModUnix: 1700000000}}}, nil
}
func (s *fakeSession) ReadAt(_ context.Context, path string, dst []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data := s.files[path]
	if off >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(dst, data[off:])
	if n < len(dst) {
		return n, io.EOF
	}
	return n, nil
}
func (s *fakeSession) OpenWrite(_ context.Context, path string) (StreamWriter, error) {
	writer := &capturingWriter{session: s, path: path}
	return writer, nil
}
func (s *fakeSession) Delete(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, path)
	return nil
}
func (s *fakeSession) Close() error { s.mu.Lock(); defer s.mu.Unlock(); s.closed = true; return nil }

type capturingWriter struct {
	session  *fakeSession
	path     string
	data     []byte
	terminal bool
}

func (w *capturingWriter) Write(_ context.Context, data []byte) (int, error) {
	if w.terminal {
		return 0, io.ErrClosedPipe
	}
	w.data = append(w.data, data...)
	return len(data), nil
}
func (w *capturingWriter) Close(context.Context) error {
	if w.terminal {
		return io.ErrClosedPipe
	}
	w.terminal = true
	w.session.mu.Lock()
	w.session.files[w.path] = append([]byte(nil), w.data...)
	w.session.mu.Unlock()
	return nil
}
func (w *capturingWriter) Abort(context.Context) error { w.terminal = true; return nil }

func testDriver(t *testing.T, session *fakeSession) *Driver {
	t.Helper()
	key := remote.ClientKey{Endpoint: "example.com:990", CredentialScope: "secret:warehouse", CredentialVersion: "1", SecurityProfile: "strict"}
	manager := NewManager(4, 2, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { return session, nil }))
	driver, err := NewDriver("ftps", key, manager, 2)
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

func TestDriverProbeAndRangeRead(t *testing.T) {
	session := newFakeSession()
	driver := testDriver(t, session)
	probe, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !probe.Supported.Has(vfs.RandomRead) || !probe.Supported.Has(vfs.ServerSideList) {
		t.Fatalf("probe=%#x", uint64(probe.Supported))
	}
	uri, _ := vfs.ParseURI("ftps://example.com:990/data.bin")
	manager := memory.NewManager(1024)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	result, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Offset: 2, Length: 4}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(result.Buffer.Bytes()); got != "cdef" {
		t.Fatalf("range=%q", got)
	}
	result.Buffer.Release()
	if scope.Used() != 0 {
		t.Fatalf("reserved=%d", scope.Used())
	}
}

func TestDriverRejectsRangeWhenRESTUnavailable(t *testing.T) {
	session := newFakeSession()
	session.features.RestStream = false
	driver := testDriver(t, session)
	uri, _ := vfs.ParseURI("ftps://example.com:990/data.bin")
	scope := memory.NewManager(1024).NewScope()
	_, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Length: 1}, scope, memory.NewBufferManager(nil))
	if !errors.Is(err, vfs.ErrRangeUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestDriverWriteLifecycleAndPoolLease(t *testing.T) {
	session := newFakeSession()
	driver := testDriver(t, session)
	uri, _ := vfs.ParseURI("ftps://example.com:990/out.bin")
	scope := memory.NewManager(1024).NewScope()
	handle, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	identity, err := handle.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Size != 5 {
		t.Fatalf("size=%d", identity.Size)
	}
	pool, _ := driver.pool()
	_, leased, _ := pool.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
}

func TestDriverAbortDeletesIncompleteAndReleasesLease(t *testing.T) {
	session := newFakeSession()
	driver := testDriver(t, session)
	uri, _ := vfs.ParseURI("ftps://example.com:990/out.bin")
	scope := memory.NewManager(1024).NewScope()
	handle, err := driver.OpenWrite(context.Background(), uri, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := handle.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Stat(context.Background(), "/out.bin"); err == nil {
		t.Fatal("incomplete file still exists")
	}
	pool, _ := driver.pool()
	_, leased, _ := pool.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
}

func TestPoolAcquireCancellationHasNoLeak(t *testing.T) {
	key := remote.ClientKey{Endpoint: "example.com:21", CredentialScope: "s", CredentialVersion: "1", SecurityProfile: "strict"}
	first := newFakeSession()
	pool, err := NewPool(key, 1, SessionFactoryFunc(func(context.Context, remote.ClientKey) (Session, error) { return first, nil }))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	pool.Release(lease, false)
	_, leased, _ := pool.Stats()
	if leased != 0 {
		t.Fatalf("leased=%d", leased)
	}
}
