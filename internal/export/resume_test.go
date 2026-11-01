package export

import (
	"errors"
	"testing"
)

// newResumeManifest 构造包含三个稳定目标文件的恢复测试 Manifest。
func newResumeManifest(t *testing.T) *Manifest {
	t.Helper()
	m, err := NewManifest("job", "query-v1", "source-v1", "s3://bucket/out", "csv", "gzip", "snapshot", []string{"part-a.csv", "part-b.csv", "part-c.csv"})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSegmentPlanBindRoundTrip(t *testing.T) {
	m := newResumeManifest(t)
	plan, err := NewExplicitSegmentPlan("planner-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.BindSegmentPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := m.BindSegmentPlan(plan); err != nil {
		t.Fatalf("idempotent bind: %v", err)
	}
	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SegmentPlan == nil || !decoded.SegmentPlan.Equal(plan) {
		t.Fatalf("segment plan lost: %+v", decoded.SegmentPlan)
	}
	other, _ := NewExplicitSegmentPlan("planner-v2")
	if err := decoded.BindSegmentPlan(other); !errors.Is(err, ErrSegmentPlanMismatch) {
		t.Fatalf("expected mismatch, got %v", err)
	}
}

func TestBindSegmentPlanRejectsLateFirstBinding(t *testing.T) {
	m := newResumeManifest(t)
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	plan, _ := NewExplicitSegmentPlan("planner-v1")
	if err := m.BindSegmentPlan(plan); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected late bind rejection, got %v", err)
	}
}

func TestPrepareResumePreservesCompleteAndRedoesIncomplete(t *testing.T) {
	m := newResumeManifest(t)
	plan, _ := NewExplicitSegmentPlan("planner-v1")
	if err := m.BindSegmentPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-a.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-a.csv", FileStateComplete, 10, "a"); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-b.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-c.csv", FileStateFailed, 0, ""); err != nil {
		t.Fatal(err)
	}

	if err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutMultiFile, SegmentPlan: &plan}); err != nil {
		t.Fatal(err)
	}
	if m.ExportState != ExportStateWriting {
		t.Fatalf("state=%s", m.ExportState)
	}
	a, _ := m.LookupFile("part-a.csv")
	b, _ := m.LookupFile("part-b.csv")
	c, _ := m.LookupFile("part-c.csv")
	if a.State != FileStateComplete || a.Size != 10 || a.Checksum != "a" {
		t.Fatalf("complete changed: %+v", a)
	}
	if b.State != FileStateFailed || c.State != FileStateFailed {
		t.Fatalf("incomplete states b=%+v c=%+v", b, c)
	}
}

func TestPrepareResumeRejectsIdentityDriftWithoutMutation(t *testing.T) {
	m := newResumeManifest(t)
	plan, _ := NewExplicitSegmentPlan("planner-v1")
	_ = m.BindSegmentPlan(plan)
	_ = m.TransitionExport(ExportStateWriting)
	_ = m.TransitionFile("part-a.csv", FileStateWriting, 0, "")
	other, _ := NewExplicitSegmentPlan("planner-v2")
	err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutMultiFile, SegmentPlan: &other})
	if !errors.Is(err, ErrSegmentPlanMismatch) {
		t.Fatalf("expected plan mismatch, got %v", err)
	}
	if m.ExportState != ExportStateWriting || m.FileStates[0].State != FileStateWriting {
		t.Fatalf("manifest mutated on mismatch: %+v", m)
	}
	err = m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v2", SourceSnapshotFingerprint: "source-v1", Layout: LayoutMultiFile, SegmentPlan: &plan})
	if !errors.Is(err, ErrResumeIdentityMismatch) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
}

func TestPrepareResumeRequiresPlanForMultiFile(t *testing.T) {
	m := newResumeManifest(t)
	if err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutMultiFile}); !errors.Is(err, ErrSegmentPlanRequired) {
		t.Fatalf("got %v", err)
	}
}

func TestPrepareResumeSingleFileWithoutPlan(t *testing.T) {
	m, err := NewManifest("job", "query-v1", "source-v1", "sftp://host/out", "csv", "identity", "snapshot", []string{"result.csv"})
	if err != nil {
		t.Fatal(err)
	}
	_ = m.TransitionExport(ExportStateWriting)
	_ = m.TransitionFile("result.csv", FileStateWriting, 0, "")
	if err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutSingleFile}); err != nil {
		t.Fatal(err)
	}
	if m.FileStates[0].State != FileStateFailed || m.ExportState != ExportStateWriting {
		t.Fatalf("resume state=%+v", m)
	}
}

func TestPrepareResumeRejectsCommitAmbiguity(t *testing.T) {
	m := newResumeManifest(t)
	for i := range m.FileStates {
		m.FileStates[i].State = FileStateComplete
	}
	m.ExportState = ExportStateCommitting
	if err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutSingleFile}); !errors.Is(err, ErrCommitRecoveryRequired) {
		t.Fatalf("got %v", err)
	}
	m.ExportState = ExportStateCommitted
	if err := m.PrepareResume(ResumeIdentity{QueryFingerprint: "query-v1", SourceSnapshotFingerprint: "source-v1", Layout: LayoutSingleFile}); !errors.Is(err, ErrResumeAlreadyCommitted) {
		t.Fatalf("got %v", err)
	}
}
