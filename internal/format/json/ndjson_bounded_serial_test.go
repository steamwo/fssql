package json

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

type trackingNDJSONDriver struct {
	*memDriver
	mu       sync.Mutex
	reads    int
	maxRange int64
	failAt   int
}

func (d *trackingNDJSONDriver) ReadRange(ctx context.Context, r vfs.RangeRequest, scope *memory.Scope, bm *memory.BufferManager) (vfs.RangeResult, error) {
	d.mu.Lock()
	d.reads++
	read := d.reads
	if r.Length > d.maxRange {
		d.maxRange = r.Length
	}
	failAt := d.failAt
	d.mu.Unlock()
	if failAt > 0 && read == failAt {
		return vfs.RangeResult{}, fmt.Errorf("injected ndjson range failure")
	}
	return d.memDriver.ReadRange(ctx, r, scope, bm)
}

func (d *trackingNDJSONDriver) reset(failAt int) {
	d.mu.Lock()
	d.reads = 0
	d.maxRange = 0
	d.failAt = failAt
	d.mu.Unlock()
}

func (d *trackingNDJSONDriver) stats() (reads int, maxRange int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads, d.maxRange
}

func TestSequentialNDJSONUsesBoundedRangeReads(t *testing.T) {
	const rows = 20000
	var fixtureData strings.Builder
	payload := strings.Repeat("x", 128)
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&fixtureData, "{\"id\":%d,\"name\":\"%s\"}\n", i, payload)
	}
	data := []byte(fixtureData.String())
	base := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingNDJSONDriver{memDriver: base}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(64 << 20)
	bindScope := bindManager.NewScope()
	binder, err := NewNDJSONBinder(registry, bindScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if bindScope.Used() != 0 {
		t.Fatalf("bind managed bytes=%d", bindScope.Used())
	}
	driver.reset(0)

	runManager := memory.NewManager(16 << 20)
	runScope := runManager.NewScope()
	source, err := NewNDJSONSource(context.Background(), registry, binding, NDJSONScanOptions{EnableParallel: false}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	ids := collect(t, source, 1)
	if len(ids) != rows {
		t.Fatalf("rows=%d want=%d", len(ids), rows)
	}
	reads, maxRange := driver.stats()
	if reads < 2 {
		t.Fatalf("range reads=%d, expected multiple bounded reads", reads)
	}
	if maxRange > defaultNDJSONSerialRangeBytes {
		t.Fatalf("max serial range=%d want <=%d", maxRange, defaultNDJSONSerialRangeBytes)
	}
	if maxRange >= int64(len(data)) {
		t.Fatalf("serial scan requested whole file: range=%d file=%d", maxRange, len(data))
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

func TestSequentialNDJSONExpandsOnlyForOversizedLine(t *testing.T) {
	const target = int64(4 << 10)
	data := []byte("{\"id\":1,\"name\":\"" + strings.Repeat("x", 96<<10) + "\"}\n{\"id\":2,\"name\":\"short\"}\n")
	base := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingNDJSONDriver{memDriver: base}
	registry, _ := vfs.NewRegistry(driver)
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(2 << 20)
	bindScope := bindManager.NewScope()
	binder, _ := NewNDJSONBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(0)

	runManager := memory.NewManager(2 << 20)
	runScope := runManager.NewScope()
	source, err := NewNDJSONSource(context.Background(), registry, binding, NDJSONScanOptions{EnableParallel: false, TargetRangeBytes: target}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	ids := collect(t, source, 1)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids=%v", ids)
	}
	_, maxRange := driver.stats()
	if maxRange <= target {
		t.Fatalf("oversized line did not expand range: max=%d target=%d", maxRange, target)
	}
	if maxRange > int64(len(data)) {
		t.Fatalf("range=%d exceeds file=%d", maxRange, len(data))
	}
	if runScope.Used() != 0 {
		t.Fatalf("runtime managed leak=%d", runScope.Used())
	}
}

func TestSequentialNDJSONRangeFailureReleasesManagedMemory(t *testing.T) {
	data := fixture(2000)
	base := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	driver := &trackingNDJSONDriver{memDriver: base}
	registry, _ := vfs.NewRegistry(driver)
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(8 << 20)
	bindScope := bindManager.NewScope()
	binder, _ := NewNDJSONBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	driver.reset(2)

	runManager := memory.NewManager(4 << 20)
	runScope := runManager.NewScope()
	source, err := NewNDJSONSource(context.Background(), registry, binding, NDJSONScanOptions{EnableParallel: false, TargetRangeBytes: 4 << 10}, runScope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok || chunk == nil {
		t.Fatalf("first next chunk=%v ok=%v err=%v", chunk, ok, err)
	}
	chunk.Release()
	if _, _, err := source.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "injected ndjson range failure") {
		t.Fatalf("second next err=%v", err)
	}
	if runScope.Used() != 0 {
		t.Fatalf("managed bytes after I/O failure=%d", runScope.Used())
	}
}

func TestSequentialNDJSONCancellationLeavesNoManagedState(t *testing.T) {
	data := fixture(2000)
	base := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(base)
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(8 << 20)
	bindScope := bindManager.NewScope()
	binder, _ := NewNDJSONBinder(registry, bindScope, buffers)
	binding, err := binder.Bind(context.Background(), base.uri.String(), NDJSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runManager := memory.NewManager(4 << 20)
	runScope := runManager.NewScope()
	source, err := NewNDJSONSource(context.Background(), registry, binding, NDJSONScanOptions{EnableParallel: false, TargetRangeBytes: 4 << 10}, runScope, buffers)
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

func BenchmarkSequentialNDJSONBoundedScan(b *testing.B) {
	data := fixture(10000)
	u, err := vfs.ParseURI("memjson://dataset/data.ndjson")
	if err != nil {
		b.Fatal(err)
	}
	driver := &memDriver{data: data, caps: vfs.Capabilities(vfs.RandomRead), uri: u}
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		b.Fatal(err)
	}
	buffers := memory.NewBufferManager(nil)
	bindManager := memory.NewManager(64 << 20)
	bindScope := bindManager.NewScope()
	binder, err := NewNDJSONBinder(registry, bindScope, buffers)
	if err != nil {
		b.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), NDJSONBindOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager := memory.NewManager(32 << 20)
		scope := manager.NewScope()
		source, err := NewNDJSONSource(context.Background(), registry, binding, NDJSONScanOptions{EnableParallel: false, TargetRangeBytes: 64 << 10}, scope, buffers)
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
