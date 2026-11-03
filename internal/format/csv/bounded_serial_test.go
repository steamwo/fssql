package csv

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type trackingCSVDriver struct {
	*phase3MemoryDriver
	mu       sync.Mutex
	reads    int
	maxRange int64
	total    int64
	failAt   int
}

func (d *trackingCSVDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	d.mu.Lock()
	d.reads++
	read := d.reads
	d.total += r.Length
	if r.Length > d.maxRange {
		d.maxRange = r.Length
	}
	failAt := d.failAt
	d.mu.Unlock()
	if failAt > 0 && read == failAt {
		return vfs.RangeResult{}, fmt.Errorf("injected csv range failure")
	}
	return d.phase3MemoryDriver.ReadRange(ctx, r, scope, bm)
}

func (d *trackingCSVDriver) reset(failAt int) {
	d.mu.Lock()
	d.reads = 0
	d.maxRange = 0
	d.total = 0
	d.failAt = failAt
	d.mu.Unlock()
}

func (d *trackingCSVDriver) stats() (reads int, maxRange int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads, d.maxRange
}

func (d *trackingCSVDriver) totalBytes() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.total
}

func TestSequentialCSVUsesBoundedRangeReads(t *testing.T) {
	const rows = 20000
	var fixture strings.Builder
	fixture.WriteString("id,note\n")
	payload := strings.Repeat("x", 128)
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&fixture, "%d,%s\n", i, payload)
	}
	data := []byte(fixture.String())
	base := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingCSVDriver{phase3MemoryDriver: base}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}

	bindManager := memory.NewManager(64 << 20)
	bindScope := bindManager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewSourceBinder(registry, bindScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true, MaxRows: 2048}})
	if err != nil {
		t.Fatal(err)
	}
	if bindScope.Used() != 0 {
		t.Fatalf("bind managed bytes=%d", bindScope.Used())
	}
	driver.reset(0)

	runManager := memory.NewManager(8 << 20)
	runScope := runManager.NewScope()
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	ids := collectIDs(t, source, 1)
	if len(ids) != rows {
		t.Fatalf("rows=%d want=%d", len(ids), rows)
	}
	reads, maxRange := driver.stats()
	if reads < 2 {
		t.Fatalf("range reads=%d, expected multiple bounded reads", reads)
	}
	if maxRange > defaultSerialRangeBytes {
		t.Fatalf("max serial range=%d want <=%d", maxRange, defaultSerialRangeBytes)
	}
	if maxRange >= int64(len(data)) {
		t.Fatalf("serial scan requested whole file: range=%d file=%d", maxRange, len(data))
	}
	// 串行窗口只应重复读取首个窗口未消费的尾部，不能按窗口大小逐批重复读取。
	if total := driver.totalBytes(); total > int64(len(data))+defaultSerialRangeBytes {
		t.Fatalf("serial scan read %d bytes for %d-byte file", total, len(data))
	}
	if runScope.Used() != 0 {
		t.Fatalf("runtime managed leak=%d", runScope.Used())
	}
	if err := runScope.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bindScope.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSequentialCSVStripsUTF8BOM(t *testing.T) {
	for _, header := range []bool{false, true} {
		t.Run(fmt.Sprintf("header=%v", header), func(t *testing.T) {
			body := "1,a\n2,b\n"
			if header {
				body = "id,note\n" + body
			}
			data := append([]byte{0xEF, 0xBB, 0xBF}, []byte(body)...)
			driver := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
			registry, err := vfs.NewRegistry(driver)
			if err != nil {
				t.Fatal(err)
			}
			buffers := memory.NewBufferManager(nil)
			bindManager := memory.NewManager(8 << 20)
			bindScope := bindManager.NewScope()
			binder, err := NewSourceBinder(registry, bindScope, buffers)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: header}})
			if err != nil {
				t.Fatal(err)
			}
			if header && binding.Schema().Fields[0].Name != "id" {
				t.Fatalf("header column name=%q", binding.Schema().Fields[0].Name)
			}
			runManager := memory.NewManager(8 << 20)
			runScope := runManager.NewScope()
			source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false}, runScope, buffers)
			if err != nil {
				t.Fatal(err)
			}
			ids := collectIDs(t, source, 1)
			if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
				t.Fatalf("ids=%v", ids)
			}
			if runScope.Used() != 0 {
				t.Fatalf("runtime managed leak=%d", runScope.Used())
			}
		})
	}
}

func TestSequentialCSVExpandsOnlyForOversizedRecord(t *testing.T) {
	const target = int64(4 << 10)
	data := []byte("id,note\n1,\"" + strings.Repeat("x", 96<<10) + "\"\n2,short\n")
	base := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingCSVDriver{phase3MemoryDriver: base}
	registry, _ := vfs.NewRegistry(driver)

	bindManager := memory.NewManager(2 << 20)
	bindScope := bindManager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, _ := NewSourceBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(0)

	runManager := memory.NewManager(2 << 20)
	runScope := runManager.NewScope()
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false, TargetRangeBytes: target}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	ids := collectIDs(t, source, 1)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids=%v", ids)
	}
	_, maxRange := driver.stats()
	if maxRange <= target {
		t.Fatalf("oversized record did not expand range: max=%d target=%d", maxRange, target)
	}
	if maxRange > int64(len(data)) {
		t.Fatalf("range=%d exceeds file=%d", maxRange, len(data))
	}
	if runScope.Used() != 0 {
		t.Fatalf("runtime managed leak=%d", runScope.Used())
	}
}

func TestSequentialCSVRangeFailureReleasesManagedMemory(t *testing.T) {
	data := makeParallelFixture(2000)
	base := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingCSVDriver{phase3MemoryDriver: base}
	registry, _ := vfs.NewRegistry(driver)
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(8 << 20)
	bindScope := bindManager.NewScope()
	binder, _ := NewSourceBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(2)

	runManager := memory.NewManager(2 << 20)
	runScope := runManager.NewScope()
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false, TargetRangeBytes: 4 << 10}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok || chunk == nil {
		t.Fatalf("first next chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	chunk.Release()
	if _, _, err := source.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "injected csv range failure") {
		t.Fatalf("second next err=%v", err)
	}
	if runScope.Used() != 0 {
		t.Fatalf("managed bytes after I/O failure=%d", runScope.Used())
	}
}

func TestSequentialCSVCancellationLeavesNoManagedState(t *testing.T) {
	data := makeParallelFixture(2000)
	base := newPhase3MemoryDriver(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(base)
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(8 << 20)
	bindScope := bindManager.NewScope()
	binder, _ := NewSourceBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), base.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		t.Fatal(err)
	}
	runManager := memory.NewManager(2 << 20)
	runScope := runManager.NewScope()
	source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false, TargetRangeBytes: 4 << 10}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	chunk, ok, err := source.Next(ctx)
	if err != nil || !ok || chunk == nil {
		t.Fatalf("first next chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	chunk.Release()
	cancel()
	if _, _, err := source.Next(ctx); err != context.Canceled {
		t.Fatalf("cancel err=%v", err)
	}
	if runScope.Used() != 0 {
		t.Fatalf("managed bytes after cancel=%d", runScope.Used())
	}
}

func BenchmarkSequentialCSVBoundedScan(b *testing.B) {
	data := makeParallelFixture(10000)
	u, err := vfs.ParseURI("mem://dataset/data.csv")
	if err != nil {
		b.Fatal(err)
	}
	driver := &phase3MemoryDriver{data: data, caps: vfs.Capabilities(vfs.RandomRead), uri: u}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		b.Fatal(err)
	}
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(64 << 20)
	bindScope := bindManager.NewScope()
	binder, err := NewSourceBinder(registry, bindScope, buffers)
	if err != nil {
		b.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), SourceBindOptions{Inference: InferenceOptions{Header: true}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager := memory.NewManager(16 << 20)
		scope := manager.NewScope()
		source, err := NewSource(context.Background(), registry, binding, ScanOptions{EnableParallel: false, TargetRangeBytes: 64 << 10}, scope, buffers)
		if err != nil {
			b.Fatal(err)
		}
		seen := 0
		for {
			chunk, ok, err := source.Next(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
			seen += chunk.Cardinality
			chunk.Release()
		}
		if seen != 10000 {
			b.Fatalf("rows=%d", seen)
		}
		if scope.Used() != 0 {
			b.Fatalf("managed leak=%d", scope.Used())
		}
	}
}
