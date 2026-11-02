package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

// newManifestPlatformFixture 创建独立的嵌套 Unicode 路径与受管内存，并校验最终资源归零。
func newManifestPlatformFixture(t *testing.T) (*LocalManifestStore, *Manifest) {
	t.Helper()
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	t.Cleanup(func() {
		if used := scope.Used(); used != 0 {
			t.Errorf("managed memory leak=%d", used)
		}
		if err := scope.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := NewLocalManifestStore(filepath.Join(t.TempDir(), "恢复状态", "job", "manifest.json"), 0, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest("job", "query", "source", "s3://bucket/out", "csv", "identity", "snapshot", []string{"part.csv"})
	if err != nil {
		t.Fatal(err)
	}
	return store, manifest
}

// TestLocalManifestStoreReplacesSnapshotAndDeletes 覆盖首次目录发布、重复替换及删除后不恢复旧状态。
func TestLocalManifestStoreReplacesSnapshotAndDeletes(t *testing.T) {
	store, manifest := newManifestPlatformFixture(t)
	ctx := context.Background()
	if err := store.Save(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part.csv", FileStateComplete, 3, "sum"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ExportState != ExportStateWriting || loaded.FileStates[0].State != FileStateComplete || loaded.FileStates[0].Checksum != "sum" {
		t.Fatalf("latest snapshot was not published: %+v", loaded)
	}
	if err := store.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(ctx); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted manifest loaded: %v", err)
	}
	if err := store.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(store.path))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files remain: entries=%v err=%v", entries, err)
	}
}

// TestLocalManifestStoreCanceledSavePreservesSnapshot 验证取消后的保存不发布新状态且不遗留临时文件。
func TestLocalManifestStoreCanceledSavePreservesSnapshot(t *testing.T) {
	store, manifest := newManifestPlatformFixture(t)
	if err := store.Save(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Save(ctx, manifest); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	loaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ExportState != ExportStatePlanned {
		t.Fatalf("canceled save replaced old snapshot: %s", loaded.ExportState)
	}
	entries, err := os.ReadDir(filepath.Dir(store.path))
	if err != nil || len(entries) != 1 || entries[0].Name() != "manifest.json" {
		t.Fatalf("unexpected files after cancellation: entries=%v err=%v", entries, err)
	}
}
