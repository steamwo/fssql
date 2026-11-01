package temp

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestTemporaryMemoryManagerEnforcesHierarchy(t *testing.T) {
	m := NewTemporaryMemoryManager(Limits{
		Global:               12,
		DefaultResourceGroup: 8,
		DefaultQuery:         6,
		ResourceGroups:       map[string]int64{"small": 4},
	})

	q1 := m.NewQueryScope("default")
	r1, err := q1.Reserve(6)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 6 {
		t.Fatalf("global used=%d, want 6", got)
	}
	if got := m.ResourceGroupUsed("default"); got != 6 {
		t.Fatalf("group used=%d, want 6", got)
	}

	q2 := m.NewQueryScope("default")
	if _, err := q2.Reserve(3); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("resource-group quota error=%v, want %v", err, ErrQuotaExceeded)
	}
	if got := m.GlobalUsed(); got != 6 {
		t.Fatalf("failed reservation leaked global quota: %d", got)
	}

	r1.Release()
	if err := q1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}

	small := m.NewQueryScope("small")
	if _, err := small.Reserve(5); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("configured group quota error=%v, want %v", err, ErrQuotaExceeded)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("failed group reservation leaked global quota: %d", got)
	}
	if err := small.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryQuotaCloseDetectsLeak(t *testing.T) {
	m := NewTemporaryMemoryManager(Limits{Global: 64, DefaultResourceGroup: 64, DefaultQuery: 64})
	q := m.NewQueryScope("default")
	r, err := q.Reserve(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err == nil {
		t.Fatal("expected reservation leak")
	}
	r.Release()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak: %d", got)
	}
}

func TestTemporaryStorageWriteAndCleanup(t *testing.T) {
	root := t.TempDir()
	m := NewTemporaryMemoryManager(Limits{Global: 64, DefaultResourceGroup: 64, DefaultQuery: 64})
	storage, err := NewTemporaryStorageManager(root, m)
	if err != nil {
		t.Fatal(err)
	}
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	dir := q.Dir()
	f, err := q.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("abcdef")); err != nil || n != 6 {
		t.Fatalf("write=(%d,%v), want (6,nil)", n, err)
	}
	if got := q.Used(); got != 6 {
		t.Fatalf("query used=%d, want 6", got)
	}
	if got := q.OwnedFiles(); got != 1 {
		t.Fatalf("owned files=%d, want 1", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak: %d", got)
	}
	if got := q.OwnedFiles(); got != 0 {
		t.Fatalf("file ownership leak: %d", got)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("query temp dir still exists: %v", err)
	}
}

type faultFile struct {
	FileHandle
	mode string
}

func (f *faultFile) Write(p []byte) (int, error) {
	switch f.mode {
	case "disk-full":
		return 0, syscall.ENOSPC
	case "short":
		n := len(p) / 2
		if n == 0 && len(p) > 0 {
			n = 1
		}
		written, err := f.FileHandle.Write(p[:n])
		if err != nil {
			return written, err
		}
		return written, nil
	default:
		return f.FileHandle.Write(p)
	}
}

type faultBackend struct {
	osBackend
	createErr    error
	removeErr    error
	removeAllErr error
	writeMode    string
}

func (b faultBackend) CreateTemp(dir, pattern string) (FileHandle, error) {
	if b.createErr != nil {
		return nil, b.createErr
	}
	h, err := b.osBackend.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	return &faultFile{FileHandle: h, mode: b.writeMode}, nil
}
func (b faultBackend) Remove(path string) error {
	if b.removeErr != nil {
		return b.removeErr
	}
	return b.osBackend.Remove(path)
}
func (b faultBackend) RemoveAll(path string) error {
	if b.removeAllErr != nil {
		return b.removeAllErr
	}
	return b.osBackend.RemoveAll(path)
}

func newFaultStorage(t *testing.T, backend Backend) (*TemporaryMemoryManager, *TemporaryStorageManager) {
	t.Helper()
	m := NewTemporaryMemoryManager(Limits{Global: 64, DefaultResourceGroup: 64, DefaultQuery: 64})
	storage, err := newTemporaryStorageManager(t.TempDir(), m, backend)
	if err != nil {
		t.Fatal(err)
	}
	return m, storage
}

func TestCreatePermissionDeniedHasNoOwnershipLeak(t *testing.T) {
	m, storage := newFaultStorage(t, faultBackend{createErr: fs.ErrPermission})
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.CreateFile(); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("create error=%v, want permission denied", err)
	}
	if got := q.OwnedFiles(); got != 0 {
		t.Fatalf("owned files=%d, want 0", got)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak: %d", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiskFullRollsBackReservation(t *testing.T) {
	m, storage := newFaultStorage(t, faultBackend{writeMode: "disk-full"})
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.Write([]byte("abcdef")); n != 0 || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("write=(%d,%v), want (0,ENOSPC)", n, err)
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("query quota leak after disk full: %d", got)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak after disk full: %d", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShortWriteAccountsOnlyWrittenBytes(t *testing.T) {
	m, storage := newFaultStorage(t, faultBackend{writeMode: "short"})
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	n, err := f.Write([]byte("abcdef"))
	if n != 3 || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("write=(%d,%v), want (3,short write)", n, err)
	}
	if got := q.Used(); got != 3 {
		t.Fatalf("query used=%d, want 3", got)
	}
	if got := f.Size(); got != 3 {
		t.Fatalf("file size accounting=%d, want 3", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak after cleanup: %d", got)
	}
}

func TestCanceledWriteDoesNotReserveOrWrite(t *testing.T) {
	m, storage := newFaultStorage(t, faultBackend{})
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, err := f.WriteContext(ctx, []byte("abcdef")); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("write=(%d,%v), want (0,canceled)", n, err)
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("query quota leak after canceled write: %d", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak: %d", got)
	}
}

func TestCleanupFailureRetainsOwnershipForRetry(t *testing.T) {
	backend := &mutableFaultBackend{Backend: osBackend{}, removeErr: fs.ErrPermission}
	m, storage := newFaultStorage(t, backend)
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := q.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("close error=%v, want permission denied", err)
	}
	if got := q.Used(); got != 3 {
		t.Fatalf("quota should remain owned after failed remove, got %d", got)
	}
	if got := q.OwnedFiles(); got != 1 {
		t.Fatalf("file ownership should remain after failed remove, got %d", got)
	}
	if _, err := q.CreateFile(); !errors.Is(err, ErrClosed) {
		t.Fatalf("create during cleanup error=%v, want %v", err, ErrClosed)
	}
	backend.removeErr = nil
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := m.GlobalUsed(); got != 0 {
		t.Fatalf("global quota leak after retry: %d", got)
	}
}

type mutableFaultBackend struct {
	Backend
	removeErr error
}

func (b *mutableFaultBackend) Remove(path string) error {
	if b.removeErr != nil {
		return b.removeErr
	}
	return b.Backend.Remove(path)
}

func TestQueryScopeDirectoryIsUnderConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	m := NewTemporaryMemoryManager(Limits{Global: -1, DefaultResourceGroup: -1, DefaultQuery: -1})
	storage, err := NewTemporaryStorageManager(root, m)
	if err != nil {
		t.Fatal(err)
	}
	q, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(root, q.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if rel == ".." || filepath.IsAbs(rel) {
		t.Fatalf("query dir escaped root: %q", q.Dir())
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkTemporaryQuotaReserveRelease(b *testing.B) {
	m := NewTemporaryMemoryManager(Limits{Global: -1, DefaultResourceGroup: -1, DefaultQuery: -1})
	q := m.NewQueryScope("benchmark")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := q.Reserve(4096)
		if err != nil {
			b.Fatal(err)
		}
		r.Release()
	}
}
