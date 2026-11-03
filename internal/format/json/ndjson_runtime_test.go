package json

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// TestNewNDJSONRuntimeSourceReusesRealBindFingerprint 验证 runtime 复用真实 Bind/frozen ScanSet，并复用既有并发 NDJSONSource。
func TestNewNDJSONRuntimeSourceReusesRealBindFingerprint(t *testing.T) {
	data := fixture(128)
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(16 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewNDJSONBinder(registry, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Fingerprint() == "" {
		t.Fatal("empty NDJSON binding fingerprint")
	}
	if source, err := NewNDJSONRuntimeSource(context.Background(), NDJSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: "different-fingerprint",
	}); err == nil || source != nil {
		t.Fatalf("mismatched source=%v err=%v", source, err)
	}

	projection, err := NewProjectedPathSet("id")
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewNDJSONRuntimeSource(context.Background(), NDJSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(),
		Scan: NDJSONScanOptions{EnableParallel: true, TargetRangeBytes: 128, Projection: projection},
	})
	if err != nil {
		t.Fatal(err)
	}
	// 未绑定的路径候选仍被清空；runtime 只接受独立 Projection 字段中的绑定列索引。
	if !source.projection.Empty() {
		t.Fatal("runtime source accepted projection candidate without ColumnIndex remap contract")
	}
	ids := collect(t, source, 4)
	if len(ids) != 128 {
		t.Fatalf("ids=%d", len(ids))
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("id[%d]=%d", i, id)
		}
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestNewNDJSONRuntimeSourceRejectsSnapshotDrift 验证 bind 后对象变化会在 runtime source 构造阶段被 fingerprint gate 拒绝。
func TestNewNDJSONRuntimeSourceRejectsSnapshotDrift(t *testing.T) {
	driver := newMem(t, fixture(2), vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, _ := NewNDJSONBinder(registry, scope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	planned := binding.Fingerprint()
	driver.data = append(driver.data, []byte("{\"id\":99,\"name\":\"late\",\"tags\":[]}\n")...)
	if source, err := NewNDJSONRuntimeSource(context.Background(), NDJSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: planned,
	}); err == nil || source != nil {
		t.Fatalf("drift source=%v err=%v", source, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}
