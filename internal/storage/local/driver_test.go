package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func managed(t *testing.T, limit int64) (*memory.Scope, *memory.BufferManager) {
	t.Helper()
	manager := memory.NewManager(limit)
	return manager.NewScope(), memory.NewBufferManager(nil)
}

func mustURI(t *testing.T, path string) vfs.URI {
	t.Helper()
	u, err := URIFromPath(path)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestDriverStatListAndExactRange(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "数据 set"), 0o755); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(root, "数据 set", "a 1.csv")
	second := filepath.Join(root, "数据 set", "b.csv")
	if err := os.WriteFile(first, []byte("abcdef"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("ghijkl"), 0o600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURI(t, first)
	if got := u.String(); got == "" || got == first {
		t.Fatalf("URIFromPath did not produce escaped file URI: %q", got)
	}
	info, err := driver.Stat(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 6 {
		t.Fatalf("size=%d", info.Size)
	}

	rootURI, err := vfs.ParseURI("file:///")
	if err != nil {
		t.Fatal(err)
	}
	prefix := mustURI(t, filepath.Join(root, "数据 set")).ObjectKey()
	page1, err := driver.List(context.Background(), rootURI, vfs.ListOptions{Prefix: prefix, MaxKeys: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Entries) != 1 || !page1.Truncated || page1.NextContinuationToken == "" {
		t.Fatalf("page1=%+v", page1)
	}
	page2, err := driver.List(context.Background(), rootURI, vfs.ListOptions{Prefix: prefix, MaxKeys: 1, ContinuationToken: page1.NextContinuationToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 1 || page2.Truncated {
		t.Fatalf("page2=%+v", page2)
	}

	scope, bm := managed(t, 32)
	result, err := driver.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 1, Length: 3, Expected: &info.Identity}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(result.Buffer.Bytes()); got != "bcd" {
		t.Fatalf("range=%q", got)
	}
	result.Buffer.Release()
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDriverRejectsEscapeAndSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.csv")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Stat(context.Background(), mustURI(t, outsideFile)); err == nil {
		t.Fatal("expected root escape rejection")
	}
	link := filepath.Join(root, "outside-link")
	if err := os.Symlink(outsideFile, link); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	if _, err := driver.Stat(context.Background(), mustURI(t, link)); err == nil {
		t.Fatal("expected symlink escape rejection")
	}
}

func TestReadRangeDetectsShortReadAndManagedLimit(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "input.ndjson")
	if err := os.WriteFile(path, []byte("1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURI(t, path)

	scope, bm := managed(t, 8)
	_, err = driver.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 2, Length: 4}, scope, bm)
	if !errors.Is(err, vfs.ErrShortRead) {
		t.Fatalf("short read error=%v", err)
	}
	if used := scope.Used(); used != 0 {
		t.Fatalf("short read leaked %d managed bytes", used)
	}
	_, err = driver.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 0, Length: 16}, scope, bm)
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("limit error=%v", err)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicPublishPreservesOldTargetUntilSuccessfulClose(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "out.csv")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	u := mustURI(t, path)
	scope, bm := managed(t, 1024)

	h, err := driver.OpenAtomicPublishWrite(context.Background(), u, vfs.WriteOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(context.Background(), []byte("partial-new")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "old" {
		t.Fatalf("target changed before close: %q err=%v", got, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("close cancellation=%v", err)
	}
	if err := h.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "old" {
		t.Fatalf("target changed after abort: %q err=%v", got, err)
	}

	h, err = driver.OpenAtomicPublishWrite(context.Background(), u, vfs.WriteOptions{}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(context.Background(), []byte("new")); err != nil {
		t.Fatal(err)
	}
	identity, err := h.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if identity.Size != 3 {
		t.Fatalf("identity size=%d", identity.Size)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("published target=%q err=%v", got, err)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupportedConditionalWriteFailsBeforeTouchingTarget(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "out.csv")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	scope, bm := managed(t, 1024)
	_, err = driver.OpenAtomicPublishWrite(context.Background(), mustURI(t, path), vfs.WriteOptions{Expected: &vfs.ObjectIdentity{}}, scope, bm)
	if !errors.Is(err, vfs.ErrWriteUnsupported) {
		t.Fatalf("conditional error=%v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "old" {
		t.Fatalf("conditional open touched target: %q err=%v", got, err)
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicConditionalPublishRejectsCompetingWriter(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprint(streaming), func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "out.parquet")
			driver, err := NewDriver(root)
			if err != nil {
				t.Fatal(err)
			}
			scope, bm := managed(t, 1024)
			defer scope.Close()
			open := driver.OpenAtomicPublishWrite
			if streaming {
				open = driver.OpenWrite
			}
			first, err := open(context.Background(), mustURI(t, path), vfs.WriteOptions{IfNoneMatch: true}, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Abort(context.Background())
			second, err := open(context.Background(), mustURI(t, path), vfs.WriteOptions{IfNoneMatch: true}, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Abort(context.Background())
			if _, err = first.Write(context.Background(), []byte("winner")); err != nil {
				t.Fatal(err)
			}
			if _, err = second.Write(context.Background(), []byte("loser")); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("uncommitted target visible")
			}
			if _, err = first.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err = second.Close(context.Background()); !errors.Is(err, os.ErrExist) {
				t.Fatalf("expected commit-time conflict, got %v", err)
			}
			if err = second.Abort(context.Background()); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != "winner" {
				t.Fatal("winning output changed")
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 1 {
				t.Fatal("staging file leaked")
			}
		})
	}
}
