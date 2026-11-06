package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestLocalManifestWindowsRetriesTransientMoveErrors(t *testing.T) {
	attempts := 0
	err := retryLocalManifestMove(context.Background(), func() error {
		attempts++
		switch attempts {
		case 1:
			return &os.LinkError{Op: "move-manifest", Err: syscall.ERROR_ACCESS_DENIED}
		case 2:
			return &os.LinkError{Op: "move-manifest", Err: syscall.Errno(32)}
		default:
			return nil
		}
	})
	if err != nil || attempts != 3 {
		t.Fatalf("move attempts=%d err=%v", attempts, err)
	}
}

func TestLocalManifestWindowsDoesNotRetryOtherMoveErrors(t *testing.T) {
	attempts := 0
	want := &os.LinkError{Op: "move-manifest", Err: syscall.ERROR_FILE_NOT_FOUND}
	err := retryLocalManifestMove(context.Background(), func() error {
		attempts++
		return want
	})
	if err != want || attempts != 1 {
		t.Fatalf("move attempts=%d err=%v want=%v", attempts, err, want)
	}
}

func TestLocalManifestWindowsMoveRetryObservesCancellation(t *testing.T) {
	for _, beforeMove := range []bool{true, false} {
		ctx, cancel := context.WithCancel(context.Background())
		if beforeMove {
			cancel()
		}
		attempts := 0
		err := retryLocalManifestMove(ctx, func() error {
			attempts++
			cancel()
			return syscall.ERROR_ACCESS_DENIED
		})
		cancel()
		wantAttempts := 1
		if beforeMove {
			wantAttempts = 0
		}
		if !errors.Is(err, context.Canceled) || attempts != wantAttempts {
			t.Fatalf("beforeMove=%v attempts=%d err=%v", beforeMove, attempts, err)
		}
	}
}

// TestLocalManifestWindowsTransientSnapshotLock 用真实 Windows 句柄覆盖短暂占用后发布/删除成功。
func TestLocalManifestWindowsTransientSnapshotLock(t *testing.T) {
	for _, operation := range []string{"save", "delete"} {
		t.Run(operation, func(t *testing.T) {
			store, manifest := newManifestPlatformFixture(t)
			ctx := context.Background()
			if err := store.Save(ctx, manifest); err != nil {
				t.Fatal(err)
			}
			path, err := localManifestWindowsPath(store.path)
			if err != nil {
				t.Fatal(err)
			}
			handle, err := syscall.CreateFile(path, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if handle != syscall.InvalidHandle {
					_ = syscall.CloseHandle(handle)
				}
			}()
			if err := manifest.TransitionExport(ExportStateWriting); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				if operation == "save" {
					result <- store.Save(ctx, manifest)
				} else {
					result <- store.Delete(ctx)
				}
			}()
			select {
			case err := <-result:
				t.Fatalf("operation returned before releasing file lock: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if err := syscall.CloseHandle(handle); err != nil {
				t.Fatal(err)
			}
			handle = syscall.InvalidHandle
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			loaded, err := store.Load(ctx)
			if operation == "save" {
				if err != nil || loaded.ExportState != ExportStateWriting {
					t.Fatalf("snapshot after unlocking: manifest=%v err=%v", loaded, err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("deleted snapshot still exists: %v", err)
			}
			entries, err := os.ReadDir(filepath.Dir(store.path))
			wantEntries := 0
			if operation == "save" {
				wantEntries = 1
			}
			if err != nil || len(entries) != wantEntries {
				t.Fatalf("temporary files remain: entries=%v err=%v", entries, err)
			}
		})
	}
}

// TestLocalManifestWindowsLockedSnapshotSurvivesFailure 用真实文件共享冲突验证发布/删除失败保留旧状态。
func TestLocalManifestWindowsLockedSnapshotSurvivesFailure(t *testing.T) {
	for _, operation := range []string{"save", "delete"} {
		t.Run(operation, func(t *testing.T) {
			store, manifest := newManifestPlatformFixture(t)
			ctx := context.Background()
			if err := store.Save(ctx, manifest); err != nil {
				t.Fatal(err)
			}
			path, err := localManifestWindowsPath(store.path)
			if err != nil {
				t.Fatal(err)
			}
			// 允许读取但拒绝共享删除，触发 Windows 原生替换与重命名失败。
			handle, err := syscall.CreateFile(path, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer syscall.CloseHandle(handle)
			if operation == "save" {
				if err := manifest.TransitionExport(ExportStateWriting); err != nil {
					t.Fatal(err)
				}
				err = store.Save(ctx, manifest)
			} else {
				err = store.Delete(ctx)
			}
			// Windows 替换被占用的目标可返回 ACCESS_DENIED，移动被占用的源可返回共享冲突（32）。
			if !errors.Is(err, syscall.Errno(32)) && !errors.Is(err, syscall.ERROR_ACCESS_DENIED) {
				t.Fatalf("expected native file-lock failure, got %v", err)
			}
			loaded, err := store.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.ExportState != ExportStatePlanned {
				t.Fatalf("failed operation changed snapshot: %s", loaded.ExportState)
			}
			entries, err := os.ReadDir(filepath.Dir(store.path))
			if err != nil || len(entries) != 1 || entries[0].Name() != "manifest.json" {
				t.Fatalf("failed operation left files: entries=%v err=%v", entries, err)
			}
		})
	}
}

// TestLocalManifestWindowsLongPath 验证原生移动适配保留 Go 文件 API 的 Unicode 与长路径能力。
func TestLocalManifestWindowsLongPath(t *testing.T) {
	store, manifest := newManifestPlatformFixture(t)
	store.path = filepath.Join(filepath.Dir(store.path), strings.Repeat("a", 90), strings.Repeat("b", 90), "清单.json")
	if err := store.Save(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil || loaded.ExportState != ExportStatePlanned {
		t.Fatalf("long-path snapshot: manifest=%v err=%v", loaded, err)
	}
	if err := store.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestLocalManifestWindowsConcurrentDirectoryCreation 验证并发查询首次共享根目录时不会产生发布冲突。
func TestLocalManifestWindowsConcurrentDirectoryCreation(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared", "manifest")
	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			<-start
			results <- prepareLocalManifestDir(dir)
		}()
	}
	close(start)
	for worker := 0; worker < workers; worker++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	entries, err := os.ReadDir(filepath.Dir(dir))
	if err != nil || len(entries) != 1 || entries[0].Name() != "manifest" {
		t.Fatalf("unexpected published directories: entries=%v err=%v", entries, err)
	}
}
