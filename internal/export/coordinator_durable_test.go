package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/query"
)

// coordinatorDurableStore 是 Coordinator durable start 的最小状态存储测试实现。
type coordinatorDurableStore struct {
	// latest 保存最近一次成功写入的 Manifest snapshot。
	latest *Manifest
	// fail 注入 staging persistence failure。
	fail error
}

// Save 只在成功时替换 durable snapshot。
func (s *coordinatorDurableStore) Save(_ context.Context, manifest *Manifest) error {
	if s.fail != nil {
		return s.fail
	}
	s.latest = cloneManifest(manifest)
	return nil
}

// Load 返回最近一次 snapshot；本测试不使用空 store 的 Load。
func (s *coordinatorDurableStore) Load(context.Context) (*Manifest, error) {
	if s.latest == nil {
		return nil, errors.New("test: empty coordinator store")
	}
	return cloneManifest(s.latest), nil
}

// Delete 对内存 store 无副作用。
func (*coordinatorDurableStore) Delete(context.Context) error { return nil }

func TestCoordinatorStartDurablePersistsWritingAfterSnapshotValidation(t *testing.T) {
	q := query.New(context.Background(), memory.NewManager(1024))
	if err := q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: "snapshot-1"}); err != nil {
		t.Fatal(err)
	}
	manifest := newTestManifest(t)
	coordinator, err := NewCoordinator(q, manifest)
	if err != nil {
		t.Fatal(err)
	}
	store := &coordinatorDurableStore{}
	if err := coordinator.StartDurable(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	if manifest.ExportState != ExportStateWriting || store.latest == nil || store.latest.ExportState != ExportStateWriting {
		t.Fatalf("memory/durable state mismatch memory=%s durable=%+v", manifest.ExportState, store.latest)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCoordinatorStartDurableFailureLeavesManifestPlanned(t *testing.T) {
	q := query.New(context.Background(), memory.NewManager(1024))
	if err := q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: "snapshot-1"}); err != nil {
		t.Fatal(err)
	}
	manifest := newTestManifest(t)
	coordinator, err := NewCoordinator(q, manifest)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("test: staging manifest save failed")
	store := &coordinatorDurableStore{fail: boom}
	if err := coordinator.StartDurable(context.Background(), store); !errors.Is(err, boom) {
		t.Fatalf("expected store failure, got %v", err)
	}
	if manifest.ExportState != ExportStatePlanned || store.latest != nil {
		t.Fatalf("state advanced after persistence failure memory=%s durable=%+v", manifest.ExportState, store.latest)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
