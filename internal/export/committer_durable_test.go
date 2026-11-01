package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// durableCommitTestStore 记录每次 durable Manifest snapshot，用于验证本地状态先于远端提交推进。
type durableCommitTestStore struct {
	// saves 保存每次成功 Save 的 ExportState 顺序。
	saves []ExportState
	// latest 是最近一次成功持久化的深复制 Manifest。
	latest *Manifest
	// fail 注入 staging Manifest 持久化失败。
	fail error
}

// Save 深复制 snapshot，避免测试把后续进程内 mutation 误当成 durable state。
func (s *durableCommitTestStore) Save(_ context.Context, manifest *Manifest) error {
	if s.fail != nil {
		return s.fail
	}
	s.saves = append(s.saves, manifest.ExportState)
	s.latest = cloneManifest(manifest)
	return nil
}

// Load 模拟进程重启后只从 durable staging state 恢复 Manifest。
func (s *durableCommitTestStore) Load(context.Context) (*Manifest, error) {
	if s.latest == nil {
		return nil, errors.New("test: empty durable manifest store")
	}
	return cloneManifest(s.latest), nil
}

// Delete 对测试内存 store 无副作用。
func (*durableCommitTestStore) Delete(context.Context) error { return nil }

// durableCommitCheckingDriver 在真正 OpenWrite 前断言 COMMITTING 已经 durable。
type durableCommitCheckingDriver struct {
	*commitTestDriver
	// store 是 Committer 使用的同一个 durable staging store。
	store *durableCommitTestStore
	// opens 记录实际 final manifest OpenWrite 次数。
	opens int
}

// OpenWrite 验证远端 dataset commit 之前已经持久化 COMMITTING。
func (d *durableCommitCheckingDriver) OpenWrite(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, buffers *memory.BufferManager) (vfs.WriteHandle, error) {
	d.opens++
	if d.store.latest == nil || d.store.latest.ExportState != ExportStateCommitting {
		return nil, errors.New("test: remote commit opened before COMMITTING was durable")
	}
	return d.commitTestDriver.OpenWrite(ctx, uri, opts, scope, buffers)
}

func TestDurableCommitterPersistsCommittingBeforeRemoteAndCommittedAfter(t *testing.T) {
	manifest := completeCommitManifest(t)
	store := &durableCommitTestStore{}
	base := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite),
		handle: &commitTestHandle{},
	}
	driver := &durableCommitCheckingDriver{commitTestDriver: base, store: store}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(1 << 20).NewScope()
	committer, err := NewDurableCommitter(
		manifest,
		registry,
		scope,
		memory.NewBufferManager(nil),
		CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if driver.opens != 1 {
		t.Fatalf("remote opens=%d", driver.opens)
	}
	if len(store.saves) != 2 || store.saves[0] != ExportStateCommitting || store.saves[1] != ExportStateCommitted {
		t.Fatalf("durable states=%v", store.saves)
	}
	if manifest.ExportState != ExportStateCommitted || store.latest == nil || store.latest.ExportState != ExportStateCommitted {
		t.Fatalf("memory/durable state mismatch memory=%s durable=%+v", manifest.ExportState, store.latest)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestDurableCommitterStoreFailurePreventsRemoteOpen(t *testing.T) {
	manifest := completeCommitManifest(t)
	boom := errors.New("test: manifest store failed")
	store := &durableCommitTestStore{fail: boom}
	base := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite),
		handle: &commitTestHandle{},
	}
	driver := &durableCommitCheckingDriver{commitTestDriver: base, store: store}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(1 << 20).NewScope()
	committer, err := NewDurableCommitter(
		manifest,
		registry,
		scope,
		memory.NewBufferManager(nil),
		CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected durable save failure, got %v", err)
	}
	if driver.opens != 0 || manifest.ExportState != ExportStateWriting {
		t.Fatalf("remote side effect after durable failure opens=%d state=%s", driver.opens, manifest.ExportState)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

func TestDurableCommitterPersistsAmbiguityAndRestartsFromStore(t *testing.T) {
	manifest := completeCommitManifest(t)
	store := &durableCommitTestStore{}
	lostAck := errors.New("test: final manifest close acknowledgement lost")
	driver := &commitTestDriver{
		caps:   vfs.Capabilities(vfs.StreamingWrite, vfs.ConditionalWrite),
		handle: &commitTestHandle{closeErr: lostAck},
	}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	scope := memory.NewManager(1 << 20).NewScope()
	committer, err := NewDurableCommitter(
		manifest,
		registry,
		scope,
		memory.NewBufferManager(nil),
		CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.Commit(context.Background()); !errors.Is(err, lostAck) {
		t.Fatalf("expected ambiguous close, got %v", err)
	}
	if manifest.ExportState != ExportStateCommitting || store.latest == nil || store.latest.ExportState != ExportStateCommitting {
		t.Fatalf("ambiguous commit was not durable memory=%s durable=%+v", manifest.ExportState, store.latest)
	}

	// 模拟硬重启：丢弃原 Manifest/Committer，只从 ManifestStore Load 后重试同一 final manifest。
	restarted, err := store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	driver.handle = &commitTestHandle{}
	restartedCommitter, err := NewDurableCommitter(
		restarted,
		registry,
		scope,
		memory.NewBufferManager(nil),
		CommitOptions{FinalManifestURI: "test://commit-host/final/job/_manifest.json"},
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedCommitter.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.ExportState != ExportStateCommitted || store.latest.ExportState != ExportStateCommitted {
		t.Fatalf("restart did not finish commit memory=%s durable=%s", restarted.ExportState, store.latest.ExportState)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
