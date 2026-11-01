package export

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestLoadAndPrepareResumePersistsRedoState(t *testing.T) {
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	store, err := NewLocalManifestStore(filepath.Join(t.TempDir(), "manifest.json"), 0, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := NewManifest("job", "query", "source", "s3://bucket/out", "csv", "identity", "snapshot", []string{"part-a.csv"})
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
	if err := store.Save(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}

	resumed, err := LoadAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "query",
		SourceSnapshotFingerprint: "source",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ExportState != ExportStateWriting || resumed.FileStates[0].State != FileStateFailed {
		t.Fatalf("resume state=%+v", resumed)
	}

	// 再次从磁盘加载，确认 redo 状态不是只存在于当前进程内。
	reloaded, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.ExportState != ExportStateWriting || reloaded.FileStates[0].State != FileStateFailed {
		t.Fatalf("resume state was not durable: %+v", reloaded)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
