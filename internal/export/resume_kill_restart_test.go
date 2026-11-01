package export

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func TestKillRestartResumePreservesCompleteAndRedoesIncomplete(t *testing.T) {
	ctx := context.Background()
	manager := memory.NewManager(32 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	store, err := NewLocalManifestStore(filepath.Join(t.TempDir(), "export", "manifest.json"), 0, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}

	manifest, err := NewManifest(
		"job-restart",
		"query-v1",
		"source-v1",
		"test://commit-host/staging/job/",
		"csv",
		"identity",
		"snapshot",
		nil,
	)
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
	if err := store.Save(ctx, manifest); err != nil {
		t.Fatal(err)
	}

	schema := []Column{
		{Name: "group_key", Type: types.LogicalType{ID: types.Varchar}},
		{Name: "id", Type: types.LogicalType{ID: types.Integer}},
	}
	chunkA, scopeA := newRoutingTestChunk(t, schema, []any{"a"}, []int32{1})
	chunkB, scopeB := newRoutingTestChunk(t, schema, []any{"b"}, []int32{2})
	defer func() {
		chunkA.Release()
		chunkB.Release()
		_ = scopeA.Close()
		_ = scopeB.Close()
	}()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	remoteFiles := &datasetFakeOpen{}
	firstAttempt, err := newDurableResumableDatasetWriter(manifest, router, remoteFiles.open, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstAttempt.WriteChunk(ctx, chunkA, "seg-a"); err != nil {
		t.Fatal(err)
	}

	// 模拟进程在第二个文件进入 WRITING 后硬退出：Manifest 已 durable，但当前进程对象全部丢失。
	if err := manifest.PlanFile("part-seg-b.csv"); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-seg-b.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	firstAttempt = nil
	manifest = nil

	resumed, err := LoadAndPrepareResume(ctx, store, ResumeIdentity{
		QueryFingerprint:          "query-v1",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	restartedWriter, err := newDurableResumableDatasetWriter(resumed, router, remoteFiles.open, store)
	if err != nil {
		t.Fatal(err)
	}
	// Export planner 会重放相同稳定 SegmentPlan；seg-a 必须命中 COMPLETE 并跳过，seg-b 必须整文件重做。
	if err := restartedWriter.WriteChunk(ctx, chunkA, "seg-a"); err != nil {
		t.Fatal(err)
	}
	if err := restartedWriter.WriteChunk(ctx, chunkB, "seg-b"); err != nil {
		t.Fatal(err)
	}
	if err := restartedWriter.Close(ctx); err != nil {
		t.Fatal(err)
	}

	wantOpened := []string{
		"/staging/job/part-seg-a.csv",
		"/staging/job/part-seg-b.csv",
	}
	if !reflect.DeepEqual(remoteFiles.opened, wantOpened) {
		t.Fatalf("completed file was rewritten or file missing: opened=%v want=%v", remoteFiles.opened, wantOpened)
	}
	if got := remoteFiles.writes["/staging/job/part-seg-a.csv"]; !reflect.DeepEqual(got, []int32{1}) {
		t.Fatalf("seg-a duplicate/missing rows=%v", got)
	}
	if got := remoteFiles.writes["/staging/job/part-seg-b.csv"]; !reflect.DeepEqual(got, []int32{2}) {
		t.Fatalf("seg-b duplicate/missing rows=%v", got)
	}
	if remoteFiles.active != 0 {
		t.Fatalf("writer leak=%d", remoteFiles.active)
	}

	commitDriver := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite),
		handle: &commitTestHandle{},
	}
	registry, err := vfs.NewRegistry(commitDriver)
	if err != nil {
		t.Fatal(err)
	}
	committer, err := NewDurableCommitter(
		resumed,
		registry,
		scope,
		buffers,
		CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	finalManifest, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if finalManifest.ExportState != ExportStateCommitted || !finalManifest.AllFilesComplete() || len(finalManifest.FileStates) != 2 {
		t.Fatalf("invalid committed dataset manifest=%+v", finalManifest)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
