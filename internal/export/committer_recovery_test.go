package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// commitReconcileDriver 在 commitTestDriver 上补充可读取的既有 final manifest，用于模拟重启后的 412 reconciliation。
type commitReconcileDriver struct {
	*commitTestDriver
	// stored 是远端已经存在的 final manifest 内容。
	stored []byte
}

// Stat 返回既有 final manifest 的稳定 identity。
func (d *commitReconcileDriver) Stat(_ context.Context, uri vfs.URI) (vfs.ObjectInfo, error) {
	identity := vfs.ObjectIdentity{ETag: "existing", Size: int64(len(d.stored))}
	return vfs.ObjectInfo{URI: uri, Size: int64(len(d.stored)), Identity: identity}, nil
}

// ReadRange 通过 Managed Buffer 返回既有 final manifest，保持 reconciliation 也遵守 I-04。
func (d *commitReconcileDriver) ReadRange(_ context.Context, request vfs.RangeRequest, scope *memory.Scope, buffers *memory.BufferManager) (vfs.RangeResult, error) {
	if err := request.Validate(); err != nil {
		return vfs.RangeResult{}, err
	}
	if request.Offset != 0 || request.Length != int64(len(d.stored)) {
		return vfs.RangeResult{}, vfs.ErrShortRead
	}
	identity := vfs.ObjectIdentity{ETag: "existing", Size: int64(len(d.stored))}
	if request.Expected != nil && !request.Expected.Matches(identity) {
		return vfs.RangeResult{}, vfs.ErrObjectChanged
	}
	tag := request.Tag
	if tag == "" {
		tag = memory.TagMetadata
	}
	buffer, err := buffers.Allocate(scope, tag, len(d.stored))
	if err != nil {
		return vfs.RangeResult{}, err
	}
	copy(buffer.Bytes(), d.stored)
	return vfs.RangeResult{Buffer: buffer, Identity: identity}, nil
}

// committedManifestPayload 生成 Committer 应原子发布的最终 payload，用于模拟服务端已经成功但客户端只看到 412 的窗口。
func committedManifestPayload(t *testing.T, manifest *Manifest) []byte {
	t.Helper()
	candidate := *manifest
	candidate.FileStates = append([]FileEntry(nil), manifest.FileStates...)
	candidate.ExportState = ExportStateCommitted
	payload, err := candidate.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestCommitterReconcilesExistingMatchingFinalManifest(t *testing.T) {
	manifest := completeCommitManifest(t)
	expected := committedManifestPayload(t, manifest)
	base := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite, vfs.RandomRead),
		handle: &commitTestHandle{closeErr: vfs.ErrObjectChanged},
	}
	driver := &commitReconcileDriver{commitTestDriver: base, stored: expected}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(1 << 20).NewScope()
	committer, err := NewCommitter(manifest, registry, scope, memory.NewBufferManager(nil), CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"})
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manifest.ExportState != ExportStateCommitted || base.handle.aborts != 1 {
		t.Fatalf("state=%s aborts=%d", manifest.ExportState, base.handle.aborts)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestCommitterRejectsConflictingExistingFinalManifest(t *testing.T) {
	manifest := completeCommitManifest(t)
	base := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite, vfs.RandomRead),
		handle: &commitTestHandle{closeErr: vfs.ErrObjectChanged},
	}
	driver := &commitReconcileDriver{commitTestDriver: base, stored: []byte(`{"other":true}`)}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(1 << 20).NewScope()
	committer, err := NewCommitter(manifest, registry, scope, memory.NewBufferManager(nil), CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"})
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(context.Background()); !errors.Is(err, ErrCommitConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if manifest.ExportState != ExportStateCommitting {
		t.Fatalf("conflict must remain COMMITTING, got %s", manifest.ExportState)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestCommitterRetriesFromCommittingWithoutRewritingDataFiles(t *testing.T) {
	boom := errors.New("ambiguous close")
	manifest := completeCommitManifest(t)
	base := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite),
		handle: &commitTestHandle{closeErr: boom},
	}
	committer, scope := newCommitterForTest(t, manifest, base, 1<<20)
	if err := committer.Commit(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("first commit error=%v", err)
	}
	if manifest.ExportState != ExportStateCommitting {
		t.Fatalf("state=%s", manifest.ExportState)
	}
	base.handle = &commitTestHandle{}
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manifest.ExportState != ExportStateCommitted {
		t.Fatalf("state=%s", manifest.ExportState)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
