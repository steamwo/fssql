package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

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
