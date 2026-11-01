package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

// newResumableWritingManifest 构造已绑定 EXPLICIT_V1 SegmentPlan 且进入 WRITING 的测试 Manifest。
func newResumableWritingManifest(t *testing.T) (*Manifest, SegmentPlan) {
	t.Helper()
	m, err := NewManifest("job", "query", "source", "s3://bucket/staging/job/", "csv", "identity", "snapshot", nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := NewExplicitSegmentPlan("planner-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.BindSegmentPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	return m, plan
}

func TestResumableDatasetWriterRequiresPlanForMultiFile(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	fake := &datasetFakeOpen{}
	if _, err := newResumableDatasetWriter(newWritingManifest(t), router, fake.open); !errors.Is(err, ErrSegmentPlanRequired) {
		t.Fatalf("expected segment plan requirement, got %v", err)
	}

	manifest, _ := newResumableWritingManifest(t)
	if _, err := newResumableDatasetWriter(manifest, router, fake.open); err != nil {
		t.Fatalf("stable plan should be accepted: %v", err)
	}
}

func TestResumableDatasetWriterSingleFileAllowsWholeFileRedo(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutSingleFile, SingleFilePath: "result.csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	fake := &datasetFakeOpen{}
	if _, err := newResumableDatasetWriter(newWritingManifest(t), router, fake.open); err != nil {
		t.Fatalf("single-file resume should not require a segment plan: %v", err)
	}
}

func TestResumeSkipsCompleteFileAndRedoesIncompleteFile(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	first, firstScope := newRoutingTestChunk(t, schema, nil, []int32{1})
	second, secondScope := newRoutingTestChunk(t, schema, nil, []int32{2})
	defer func() {
		first.Release()
		second.Release()
		_ = firstScope.Close()
		_ = secondScope.Close()
	}()

	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	manifest, plan := newResumableWritingManifest(t)
	if err := manifest.PlanFile("part-seg-a.csv"); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-seg-a.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-seg-a.csv", FileStateComplete, 100, "complete-a"); err != nil {
		t.Fatal(err)
	}
	if err := manifest.PlanFile("part-seg-b.csv"); err != nil {
		t.Fatal(err)
	}
	if err := manifest.TransitionFile("part-seg-b.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}

	// 模拟进程退出后只从持久化 Manifest 恢复，不复用旧 DatasetWriter 对象。
	encoded, err := manifest.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := UnmarshalManifest(encoded)
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

	fake := &datasetFakeOpen{}
	writer, err := newResumableDatasetWriter(restarted, router, fake.open)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), first, "seg-a"); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), second, "seg-b"); err != nil {
		t.Fatal(err)
	}
	if len(fake.opened) != 1 || fake.opened[0] != "/staging/job/part-seg-b.csv" {
		t.Fatalf("resume reopened wrong files: %v", fake.opened)
	}
	a, _ := restarted.LookupFile("part-seg-a.csv")
	b, _ := restarted.LookupFile("part-seg-b.csv")
	if a.State != FileStateComplete || a.Size != 100 || a.Checksum != "complete-a" {
		t.Fatalf("completed file changed: %+v", a)
	}
	if b.State != FileStateComplete {
		t.Fatalf("incomplete file was not redone: %+v", b)
	}
}
