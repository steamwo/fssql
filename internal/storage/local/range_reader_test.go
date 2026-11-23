package local

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// TestRangeReaderRetainsSnapshotAndReadBoundaries 验证句柄复用不会放宽范围、身份、取消和内存边界。
func TestRangeReaderRetainsSnapshotAndReadBoundaries(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "input.bin")
	if err := os.WriteFile(path, []byte("abcdef"), 0600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	uri := mustURI(t, path)
	info, err := driver.Stat(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := driver.OpenRangeReader(context.Background(), uri, &info.Identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scope, bm := managed(t, 16)
	defer scope.Close()
	for offset := int64(0); offset < 4; offset++ {
		requestURI := uri
		if offset != 0 {
			requestURI.Raw = ""
		}
		result, err := reader.ReadRange(context.Background(), vfs.RangeRequest{URI: requestURI, Offset: offset, Length: 3, Expected: &info.Identity}, scope, bm)
		if err != nil {
			t.Fatal(err)
		}
		got := string(result.Buffer.Bytes())
		result.Buffer.Release()
		if got != "abcdef"[offset:offset+3] {
			t.Fatalf("offset=%d data=%q", offset, got)
		}
	}
	other := mustURI(t, filepath.Join(root, "other.bin"))
	if result, err := reader.ReadRange(context.Background(), vfs.RangeRequest{URI: other, Length: 1}, scope, bm); err == nil {
		result.Buffer.Release()
		t.Fatal("range reader accepted a different object")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		ctx context.Context  // ctx 覆盖正常与取消调用。
		req vfs.RangeRequest // req 覆盖短读、预算不足和快照不一致。
		err error            // err 是必须保留的错误类型。
	}{
		{context.Background(), vfs.RangeRequest{URI: uri, Offset: 5, Length: 3}, vfs.ErrShortRead},
		{context.Background(), vfs.RangeRequest{URI: uri, Length: 32}, memory.ErrLimitExceeded},
		{context.Background(), vfs.RangeRequest{URI: uri, Length: 1, Expected: &vfs.ObjectIdentity{ETag: "changed"}}, vfs.ErrObjectChanged},
		{canceled, vfs.RangeRequest{URI: uri, Length: 1}, context.Canceled},
	} {
		if _, err := reader.ReadRange(tc.ctx, tc.req, scope, bm); !errors.Is(err, tc.err) {
			t.Fatalf("read error=%v want %v", err, tc.err)
		}
		if scope.Used() != 0 {
			t.Fatalf("failed read retained %d bytes", scope.Used())
		}
	}
	if err := os.WriteFile(path, []byte("changed payload"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Length: 1}, scope, bm); !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("mutated snapshot error=%v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Length: 1}, scope, bm); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed reader error=%v", err)
	}
	if _, err := driver.OpenRangeReader(context.Background(), mustURI(t, filepath.Join(t.TempDir(), "outside")), nil); err == nil {
		t.Fatal("expected root escape rejection")
	}
}

// TestRangeReaderDetectsModificationDuringRead 验证读前检查后发生修改时，仍回收已分配的范围缓冲区。
func TestRangeReaderDetectsModificationDuringRead(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "input.bin")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	driver, err := NewDriver(root)
	if err != nil {
		t.Fatal(err)
	}
	uri := mustURI(t, path)
	reader, err := driver.OpenRangeReader(context.Background(), uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	scope, _ := managed(t, 1024)
	defer scope.Close()
	bm := memory.NewBufferManager(func(n int) ([]byte, error) {
		if err := os.WriteFile(path, []byte("modified data"), 0600); err != nil {
			return nil, err
		}
		return make([]byte, n), nil
	})
	if _, err := reader.ReadRange(context.Background(), vfs.RangeRequest{URI: uri, Length: 3}, scope, bm); !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("concurrent modification error=%v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("failed read retained %d bytes", scope.Used())
	}
}
