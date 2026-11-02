package csv

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// TestNewRuntimeSourceReusesRealBindFingerprint 验证 runtime 使用真实 FileSetPlanner/Bind fingerprint，并复用现有并发 CSV Source。
func TestNewRuntimeSourceReusesRealBindFingerprint(t *testing.T) {
	data := []byte("1,true\n2,false\n")
	driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if binding.Fingerprint() == "" {
		t.Fatal("empty CSV binding fingerprint")
	}
	if source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: "different-fingerprint",
	}); err == nil || source != nil {
		t.Fatalf("mismatched source=%v err=%v", source, err)
	}

	source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(),
		Scan: ScanOptions{EnableParallel: true, TargetRangeBytes: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := collectIDs(t, source, 2)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids=%v", ids)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestNewRuntimeSourceRejectsSnapshotDrift 验证 Statement bind 后对象 size/identity 变化时，在读取任何结果前拒绝执行。
func TestNewRuntimeSourceRejectsSnapshotDrift(t *testing.T) {
	driver := newPhase3MemoryDriver(t, []byte("1,true\n"), vfs.Capabilities(vfs.RandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	planned := binding.Fingerprint()
	// 模拟 bind 后远端对象被替换；phase3MemoryDriver 的 Stat identity 会随 size 改变。
	driver.data = append(driver.data, []byte("2,false\n")...)
	if source, err := NewRuntimeSource(context.Background(), RuntimeSourceConfig{
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
