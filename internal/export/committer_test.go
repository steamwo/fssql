package export

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// commitTestDriver 是 Committer 测试使用的最小可写 VFS driver，显式控制 ConditionalWrite capability 和故障点。
type commitTestDriver struct {
	// caps 是 endpoint 实际暴露给 Committer 的 capability。
	caps vfs.CapabilitySet
	// handle 是下一次 OpenWrite 返回的测试句柄。
	handle *commitTestHandle
	// opens 记录 final manifest 被打开次数，用于幂等/前置失败断言。
	opens int
	// lastOpts 保存 VFS 条件提交参数。
	lastOpts vfs.WriteOptions
}

func (*commitTestDriver) Scheme() string { return "test" }
func (d *commitTestDriver) StaticCapabilities() vfs.CapabilitySet { return d.caps }
func (d *commitTestDriver) Probe(context.Context) (vfs.ProbeResult, error) {
	return vfs.ProbeResult{Supported: d.caps, Endpoint: "commit-host", CredentialScope: "test", SecurityProfile: "strict"}, nil
}
func (*commitTestDriver) Stat(context.Context, vfs.URI) (vfs.ObjectInfo, error) {
	return vfs.ObjectInfo{}, errors.New("not used")
}
func (*commitTestDriver) List(context.Context, vfs.URI, vfs.ListOptions) (vfs.ListPage, error) {
	return vfs.ListPage{}, errors.New("not used")
}
func (*commitTestDriver) ReadRange(context.Context, vfs.RangeRequest, *memory.Scope, *memory.BufferManager) (vfs.RangeResult, error) {
	return vfs.RangeResult{}, errors.New("not used")
}
func (d *commitTestDriver) OpenWrite(_ context.Context, _ vfs.URI, opts vfs.WriteOptions, _ *memory.Scope, _ *memory.BufferManager) (vfs.WriteHandle, error) {
	d.opens++
	d.lastOpts = opts
	if d.handle == nil {
		d.handle = &commitTestHandle{}
	}
	return d.handle, nil
}
func (*commitTestDriver) Delete(context.Context, vfs.URI, *vfs.ObjectIdentity) error { return nil }

// commitTestHandle 记录 final manifest payload，并可注入 write/close failure。
type commitTestHandle struct {
	// data 保存 Committer 实际写入的 committed manifest JSON。
	data []byte
	// writeErr/closeErr 是对应 lifecycle 故障注入点。
	writeErr error
	closeErr error
	// shortFirst 令第一次 Write 合法 short write，验证 Committer 会继续写剩余 payload。
	shortFirst bool
	// writes/aborts 记录生命周期行为。
	writes int
	aborts int
}

func (h *commitTestHandle) Write(_ context.Context, data []byte) (int, error) {
	h.writes++
	if h.writeErr != nil {
		return 0, h.writeErr
	}
	if h.shortFirst && h.writes == 1 && len(data) > 1 {
		n := len(data) / 2
		h.data = append(h.data, data[:n]...)
		return n, nil
	}
	h.data = append(h.data, data...)
	return len(data), nil
}
func (h *commitTestHandle) Close(context.Context) (vfs.ObjectIdentity, error) {
	if h.closeErr != nil {
		return vfs.ObjectIdentity{}, h.closeErr
	}
	return vfs.ObjectIdentity{ETag: "final", Size: int64(len(h.data))}, nil
}
func (h *commitTestHandle) Abort(context.Context) error { h.aborts++; return nil }

// completeCommitManifest 构造一个满足 dataset commit 前置条件的 WRITING Manifest。
func completeCommitManifest(t *testing.T) *Manifest {
	t.Helper()
	m, err := NewManifest("job", "query", "source", "test://commit-host/staging/job/", "csv", "identity", "snapshot", []string{"part-0001.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-0001.csv", FileStateWriting, 0, ""); err != nil {
		t.Fatal(err)
	}
	if err := m.TransitionFile("part-0001.csv", FileStateComplete, 12, "sum"); err != nil {
		t.Fatal(err)
	}
	return m
}

// newCommitterForTest 创建带独立 MemoryScope 的 Committer 测试环境。
func newCommitterForTest(t *testing.T, manifest *Manifest, driver *commitTestDriver, memoryLimit int64) (*Committer, *memory.Scope) {
	t.Helper()
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(memoryLimit).NewScope()
	committer, err := NewCommitter(manifest, registry, scope, memory.NewBufferManager(nil), CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"})
	if err != nil {
		t.Fatal(err)
	}
	return committer, scope
}

func TestCommitterConditionallyCreatesCommittedManifest(t *testing.T) {
	manifest := completeCommitManifest(t)
	handle := &commitTestHandle{shortFirst: true}
	driver := &commitTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite), handle: handle}
	committer, scope := newCommitterForTest(t, manifest, driver, 1<<20)
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manifest.ExportState != ExportStateCommitted {
		t.Fatalf("state=%s", manifest.ExportState)
	}
	if !driver.lastOpts.IfNoneMatch || driver.lastOpts.Tag != memory.TagMetadata {
		t.Fatalf("write options=%+v", driver.lastOpts)
	}
	persisted, err := UnmarshalManifest(handle.data)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ExportState != ExportStateCommitted || !persisted.AllFilesComplete() {
		t.Fatalf("persisted=%+v", persisted)
	}
	if handle.writes < 2 {
		t.Fatalf("short write was not retried, writes=%d", handle.writes)
	}
	if scope.Used() != 0 {
		t.Fatalf("metadata reservation leak=%d", scope.Used())
	}
}

func TestCommitterRejectsIncompleteOrNonConditionalDestinationBeforeWriting(t *testing.T) {
	incomplete, err := NewManifest("job", "query", "source", "test://commit-host/staging/job/", "csv", "identity", "snapshot", []string{"part.csv"})
	if err != nil {
		t.Fatal(err)
	}
	if err := incomplete.TransitionExport(ExportStateWriting); err != nil {
		t.Fatal(err)
	}
	driver := &commitTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite)}
	committer, scope := newCommitterForTest(t, incomplete, driver, 1<<20)
	if err := committer.Commit(context.Background()); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected incomplete rejection, got %v", err)
	}
	if driver.opens != 0 || incomplete.ExportState != ExportStateWriting || scope.Used() != 0 {
		t.Fatalf("incomplete side effect opens=%d state=%s memory=%d", driver.opens, incomplete.ExportState, scope.Used())
	}

	complete := completeCommitManifest(t)
	committer, scope = newCommitterForTest(t, complete, driver, 1<<20)
	if err := committer.Commit(context.Background()); !errors.Is(err, vfs.ErrWriteUnsupported) {
		t.Fatalf("expected conditional capability rejection, got %v", err)
	}
	if driver.opens != 0 || complete.ExportState != ExportStateWriting || scope.Used() != 0 {
		t.Fatalf("capability side effect opens=%d state=%s memory=%d", driver.opens, complete.ExportState, scope.Used())
	}
}

func TestCommitterFailureAbortsAndReleasesManagedMetadata(t *testing.T) {
	boom := errors.New("close failed")
	manifest := completeCommitManifest(t)
	handle := &commitTestHandle{closeErr: boom}
	driver := &commitTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite), handle: handle}
	committer, scope := newCommitterForTest(t, manifest, driver, 1<<20)
	if err := committer.Commit(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected close failure, got %v", err)
	}
	if handle.aborts != 1 || manifest.ExportState != ExportStateFailed {
		t.Fatalf("aborts=%d state=%s", handle.aborts, manifest.ExportState)
	}
	if scope.Used() != 0 {
		t.Fatalf("metadata reservation leak=%d", scope.Used())
	}
}

func TestCommitterMemoryFailureOccursBeforeRemoteOpenAndCommittedIsIdempotent(t *testing.T) {
	manifest := completeCommitManifest(t)
	driver := &commitTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite)}
	committer, scope := newCommitterForTest(t, manifest, driver, 8)
	if err := committer.Commit(context.Background()); !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	if driver.opens != 0 || manifest.ExportState != ExportStateWriting || scope.Used() != 0 {
		t.Fatalf("memory failure side effect opens=%d state=%s memory=%d", driver.opens, manifest.ExportState, scope.Used())
	}

	manifest = completeCommitManifest(t)
	driver = &commitTestDriver{caps: vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite)}
	committer, scope = newCommitterForTest(t, manifest, driver, 1<<20)
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	opens := driver.opens
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.opens != opens || scope.Used() != 0 {
		t.Fatalf("idempotent commit reopened marker opens=%d/%d memory=%d", opens, driver.opens, scope.Used())
	}
}

// 保证 fake handle 的 short-write 测试不会被未使用的 io import 隐藏；生产代码必须返回 io.ErrShortWrite 处理零进展。
var _ = io.ErrShortWrite
