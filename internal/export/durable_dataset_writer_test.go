package export

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

// durableDatasetTestStore 保存 DatasetWriter 最近一次 durable snapshot，并支持持久化故障注入。
type durableDatasetTestStore struct {
	// latest 是最近一次成功 Save 的深复制 Manifest。
	latest *Manifest
	// fail 注入 durable staging save failure。
	fail error
}

// Save 成功时发布完整 snapshot；失败时不改变原 durable state。
func (s *durableDatasetTestStore) Save(_ context.Context, manifest *Manifest) error {
	if s.fail != nil {
		return s.fail
	}
	s.latest = cloneManifest(manifest)
	return nil
}

// Load 返回最近一次 durable snapshot。
func (s *durableDatasetTestStore) Load(context.Context) (*Manifest, error) {
	if s.latest == nil {
		return nil, errors.New("test: empty durable dataset store")
	}
	return cloneManifest(s.latest), nil
}

// Delete 对内存测试 store 无副作用。
func (*durableDatasetTestStore) Delete(context.Context) error { return nil }

func TestDurableDatasetWriterPersistsCompletedMultiFile(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := newResumableWritingManifest(t)
	fake := &datasetFakeOpen{}
	store := &durableDatasetTestStore{}
	writer, err := newDurableResumableDatasetWriter(manifest, router, fake.open, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); err != nil {
		t.Fatal(err)
	}
	entry, ok := store.latest.LookupFile("part-seg1.csv")
	if !ok || entry.State != FileStateComplete || entry.Size != 11 || entry.Checksum != "sum" {
		t.Fatalf("durable completed file=%+v ok=%v", entry, ok)
	}
	if fake.active != 0 {
		t.Fatalf("active writer leak=%d", fake.active)
	}
}

func TestDurableDatasetWriterSaveFailureRollsBackAndBecomesTerminal(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := newResumableWritingManifest(t)
	fake := &datasetFakeOpen{}
	boom := errors.New("test: durable file-state save failed")
	store := &durableDatasetTestStore{fail: boom}
	writer, err := newDurableResumableDatasetWriter(manifest, router, fake.open, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); !errors.Is(err, boom) {
		t.Fatalf("expected durable save failure, got %v", err)
	}
	if len(manifest.FileStates) != 0 || fake.active != 0 {
		t.Fatalf("state not rolled back files=%+v active=%d", manifest.FileStates, fake.active)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); !errors.Is(err, ErrDurableDatasetWriterTerminal) {
		t.Fatalf("writer accepted data after durable failure: %v", err)
	}
}

func TestDurableDatasetWriterSingleFileSaveFailureAbortsOpenWriter(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutSingleFile, SingleFilePath: "result.csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	manifest := newWritingManifest(t)
	fake := &datasetFakeOpen{}
	store := &durableDatasetTestStore{fail: errors.New("test: durable save failed")}
	writer, err := newDurableResumableDatasetWriter(manifest, router, fake.open, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "ignored"); err == nil {
		t.Fatal("expected durable save failure")
	}
	if fake.active != 0 || fake.aborts != 1 {
		t.Fatalf("single writer not aborted active=%d aborts=%d", fake.active, fake.aborts)
	}
	if len(manifest.FileStates) != 0 {
		t.Fatalf("manifest was not rolled back: %+v", manifest.FileStates)
	}
}

func TestDurableDatasetWriterPersistsFailedFile(t *testing.T) {
	schema := []Column{{Name: "id", Type: types.LogicalType{ID: types.Integer}}}
	chunk, scope := newRoutingTestChunk(t, schema, nil, []int32{1})
	defer func() { chunk.Release(); _ = scope.Close() }()
	router, err := NewFileRouter(RoutingConfig{Mode: LayoutMultiFile, Extension: "csv"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := newResumableWritingManifest(t)
	fake := &datasetFakeOpen{failWrite: true}
	store := &durableDatasetTestStore{}
	writer, err := newDurableResumableDatasetWriter(manifest, router, fake.open, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteChunk(context.Background(), chunk, "seg1"); err == nil {
		t.Fatal("expected write failure")
	}
	entry, ok := store.latest.LookupFile("part-seg1.csv")
	if !ok || entry.State != FileStateFailed || fake.aborts != 1 || fake.active != 0 {
		t.Fatalf("durable failed file=%+v ok=%v aborts=%d active=%d", entry, ok, fake.aborts, fake.active)
	}
}
