package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestLocalManifestStoreRoundTripAndCrashResume(t *testing.T) {
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	store, err := NewLocalManifestStore(filepath.Join(t.TempDir(), "job", "manifest.json"), 0, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest("job", "query", "source", "s3://bucket/out", "csv", "identity", "snapshot", []string{"part-a.csv", "part-b.csv"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewExplicitSegmentPlan("planner-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.BindSegmentPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-a.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-a.csv", FileStateComplete, 10, "sum-a"); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-b.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}

	// 丢弃旧进程内对象，只从 staging Manifest 重建恢复状态。
	manifest = nil
	restarted, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.PrepareResume(ResumeIdentity{
		QueryFingerprint:          "query",
		SourceSnapshotFingerprint: "source",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}); err != nil {
		t.Fatal(err)
	}
	a, _ := restarted.LookupFile("part-a.csv")
	b, _ := restarted.LookupFile("part-b.csv")
	if a.State != FileStateComplete || a.Size != 10 || a.Checksum != "sum-a" {
		t.Fatalf("completed file changed: %+v", a)
	}
	if b.State != FileStateFailed {
		t.Fatalf("stale incomplete file not marked redoable: %+v", b)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestLocalManifestStoreMemoryLimitFailsBeforePublish(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	manager := memory.NewManager(1024)
	scope := manager.NewScope()
	store, err := NewLocalManifestStore(path, 0, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest("job", "query", "source", "s3://bucket/out", "csv", "identity", "snapshot", []string{"part.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), manifest); !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest must not be published after reservation failure: %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("reservation leak=%d", scope.Used())
	}
}

func TestLocalManifestStoreRejectsCorruptionAndReleasesBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	store, err := NewLocalManifestStore(path, 0, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("expected corrupt manifest rejection")
	}
	if scope.Used() != 0 {
		t.Fatalf("read buffer leak=%d", scope.Used())
	}
}

func TestLocalManifestStoreDeleteIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	manager := memory.NewManager(1 << 20)
	store, err := NewLocalManifestStore(path, 0, manager.NewScope(), memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := NewManifest("job", "query", "source", "s3://bucket/out", "csv", "identity", "snapshot", []string{"part.csv"})
	if err := store.Save(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest still exists: %v", err)
	}
}
