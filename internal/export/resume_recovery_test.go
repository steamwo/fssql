package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// resumeRecoveryStore 是 ADR-0004 ordering 测试使用的内存 durable store。
type resumeRecoveryStore struct {
	// manifest 保存最后一次 durable snapshot。
	manifest *Manifest
	// saves 记录 Load 之后发生的 Save 次数。
	saves int
	// saveErr 注入 PrepareResume 后的 durable failure。
	saveErr error
}

func (s *resumeRecoveryStore) Save(_ context.Context, manifest *Manifest) error {
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.manifest = cloneManifest(manifest)
	return nil
}
func (s *resumeRecoveryStore) Load(context.Context) (*Manifest, error) {
	return cloneManifest(s.manifest), nil
}
func (s *resumeRecoveryStore) Delete(context.Context) error { return nil }

// resumeRecoveryDriver 模拟具有或不具有 IncompleteWriteRecovery capability 的目标协议。
type resumeRecoveryDriver struct {
	// scheme/caps 定义 Registry 看到的协议与端点能力。
	scheme string
	caps   vfs.CapabilitySet
	// recoverCalls/report/recoverErr 控制 recovery 行为。
	recoverCalls int
	report       vfs.RecoveryReport
	recoverErr   error
	// beforeRecover 在 driver 被调用时验证 durable ordering。
	beforeRecover func() error
}

func (d *resumeRecoveryDriver) Scheme() string { return d.scheme }
func (d *resumeRecoveryDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *resumeRecoveryDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: d.scheme + "://endpoint", CredentialScope: "test", SecurityProfile: "strict"}, nil
}
func (*resumeRecoveryDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return vfs.ObjectInfo{}, errors.New("not used")
}
func (*resumeRecoveryDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("not used")
}
func (*resumeRecoveryDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, errors.New("not used")
}
func (*resumeRecoveryDriver) OpenWrite(context.Context, vfs.URI, vfs.WriteOptions, *memory.Scope, *memory.BufferManager) (vfs.WriteHandle, error) {
	return nil, errors.New("not used")
}
func (*resumeRecoveryDriver) Delete(context.Context, vfs.URI, *vfs.ObjectIdentity) error { return nil }
func (d *resumeRecoveryDriver) RecoverIncompleteWrites(context.Context, vfs.URI, *memory.Scope) (vfs.RecoveryReport, error) {
	d.recoverCalls++
	if d.beforeRecover != nil {
		if err := d.beforeRecover(); err != nil {
			return vfs.RecoveryReport{}, err
		}
	}
	if d.recoverErr != nil {
		return vfs.RecoveryReport{}, d.recoverErr
	}
	return d.report, nil
}

// newRecoveryResumeManifest 构造一个 COMPLETE + WRITING 的 resumable MultiFile 作业。
func newRecoveryResumeManifest(t *testing.T, scheme string) (*Manifest, SegmentPlan) {
	t.Helper()
	m, err := NewManifest("job", "query-v1", "source-v1", scheme+"://host/staging/job-1/", "csv", "identity", "snapshot", []string{"part-a.csv", "part-b.csv"})
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
	if err := m.TransitionFile("part-a.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-a.csv", FileStateComplete, 10, "a"); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-b.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	return m, plan
}

func TestLoadRecoverAndPrepareResumeRecoversBeforeDurableStateChange(t *testing.T) {
	manifest, plan := newRecoveryResumeManifest(t, "recover")
	store := &resumeRecoveryStore{manifest: cloneManifest(manifest)}
	driver := &resumeRecoveryDriver{
		scheme: "recover",
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.IncompleteWriteRecovery),
		report: vfs.RecoveryReport{Discovered: 2, Recovered: 2},
	}
	driver.beforeRecover = func() error {
		if store.saves != 0 {
			return errors.New("manifest saved before remote recovery")
		}
		entry, _ := store.manifest.LookupFile("part-b.csv")
		if entry.State != FileStateWriting {
			return errors.New("durable file state changed before remote recovery")
		}
		return nil
	}
	registry, _ := vfs.NewRegistry(driver)
	scope := memory.NewManager(1 << 20).NewScope()
	resumed, report, err := LoadRecoverAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "query-v1",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}, registry, scope)
	if err != nil {
		t.Fatal(err)
	}
	if driver.recoverCalls != 1 || report.Discovered != 2 || report.Recovered != 2 || store.saves != 1 {
		t.Fatalf("calls=%d report=%+v saves=%d", driver.recoverCalls, report, store.saves)
	}
	a, _ := resumed.LookupFile("part-a.csv")
	b, _ := resumed.LookupFile("part-b.csv")
	if a.State != FileStateComplete || b.State != FileStateFailed || resumed.ExportState != ExportStateWriting {
		t.Fatalf("resumed=%+v", resumed)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestLoadRecoverAndPrepareResumeStopsOnMismatchOrRecoveryFailure(t *testing.T) {
	manifest, plan := newRecoveryResumeManifest(t, "recover")
	store := &resumeRecoveryStore{manifest: cloneManifest(manifest)}
	driver := &resumeRecoveryDriver{scheme: "recover", caps: vfs.Capabilities(vfs.StreamingWrite, vfs.IncompleteWriteRecovery)}
	registry, _ := vfs.NewRegistry(driver)
	scope := memory.NewManager(1 << 20).NewScope()
	if _, _, err := LoadRecoverAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "changed",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}, registry, scope); !errors.Is(err, ErrResumeIdentityMismatch) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
	if driver.recoverCalls != 0 || store.saves != 0 {
		t.Fatalf("mismatch side effect recover=%d saves=%d", driver.recoverCalls, store.saves)
	}

	boom := errors.New("remote cleanup failed")
	driver.recoverErr = boom
	if _, _, err := LoadRecoverAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "query-v1",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}, registry, scope); !errors.Is(err, boom) {
		t.Fatalf("expected recovery failure, got %v", err)
	}
	entry, _ := store.manifest.LookupFile("part-b.csv")
	if entry.State != FileStateWriting || store.saves != 0 {
		t.Fatalf("recovery failure mutated durable state entry=%+v saves=%d", entry, store.saves)
	}
}

func TestLoadRecoverAndPrepareResumeSkipsCapabilityForProtocolWithoutHiddenOwnership(t *testing.T) {
	manifest, plan := newRecoveryResumeManifest(t, "stream")
	store := &resumeRecoveryStore{manifest: cloneManifest(manifest)}
	driver := &resumeRecoveryDriver{scheme: "stream", caps: vfs.Capabilities(vfs.StreamingWrite)}
	registry, _ := vfs.NewRegistry(driver)
	scope := memory.NewManager(1 << 20).NewScope()
	resumed, report, err := LoadRecoverAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "query-v1",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}, registry, scope)
	if err != nil {
		t.Fatal(err)
	}
	if driver.recoverCalls != 0 || report.Discovered != 0 || report.Recovered != 0 {
		t.Fatalf("unexpected recovery calls=%d report=%+v", driver.recoverCalls, report)
	}
	entry, _ := resumed.LookupFile("part-b.csv")
	if entry.State != FileStateFailed || store.saves != 1 {
		t.Fatalf("entry=%+v saves=%d", entry, store.saves)
	}
}

func TestLoadRecoverAndPrepareResumeCleansOwnershipBeforeCommitAmbiguityReturn(t *testing.T) {
	manifest, plan := newRecoveryResumeManifest(t, "recover")
	manifest.FileStates[1].State = FileStateComplete
	manifest.ExportState = ExportStateCommitting
	store := &resumeRecoveryStore{manifest: cloneManifest(manifest)}
	driver := &resumeRecoveryDriver{
		scheme: "recover",
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.IncompleteWriteRecovery),
		report: vfs.RecoveryReport{Discovered: 1, Recovered: 1},
	}
	registry, _ := vfs.NewRegistry(driver)
	scope := memory.NewManager(1 << 20).NewScope()
	_, report, err := LoadRecoverAndPrepareResume(context.Background(), store, ResumeIdentity{
		QueryFingerprint:          "query-v1",
		SourceSnapshotFingerprint: "source-v1",
		Layout:                    LayoutMultiFile,
		SegmentPlan:               &plan,
	}, registry, scope)
	if !errors.Is(err, ErrCommitRecoveryRequired) {
		t.Fatalf("expected commit recovery requirement, got %v", err)
	}
	if driver.recoverCalls != 1 || report.Recovered != 1 || store.saves != 0 {
		t.Fatalf("recovery=%d report=%+v saves=%d", driver.recoverCalls, report, store.saves)
	}
}
