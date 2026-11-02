package json

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// runtimeJSONArray 构造 top-level object array，供跨 range framing 测试使用。
func runtimeJSONArray(rows int) []byte {
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < rows; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":%d,"name":"row-%d"}`, i, i)
	}
	b.WriteByte(']')
	return []byte(b.String())
}

// collectJSONRuntime 用多个 Pipeline worker 形态并发调用 Next；source 自身必须串行 framing 且不重不漏。
func collectJSONRuntime(t *testing.T, source *JSONRuntimeSource, workers int) []int64 {
	t.Helper()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ids []int64
	var firstErr error
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				chunk, ok, err := source.Next(context.Background())
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				if !ok {
					return
				}
				local := make([]int64, 0, chunk.Cardinality)
				for row := 0; row < chunk.Cardinality; row++ {
					id, valid := chunk.Columns[0].Int64(row)
					if !valid {
						chunk.Release()
						mu.Lock()
						if firstErr == nil {
							firstErr = fmt.Errorf("unexpected NULL id")
						}
						mu.Unlock()
						return
					}
					local = append(local, id)
				}
				chunk.Release()
				mu.Lock()
				ids = append(ids, local...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestJSONRuntimeSourceArrayCrossesRanges 验证 object array 在极小 RandomRead range 下仍按 typed schema 正确输出。
func TestJSONRuntimeSourceArrayCrossesRanges(t *testing.T) {
	data := runtimeJSONArray(257)
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead, vfs.ParallelRandomRead))
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(16 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, err := NewJSONBinder(registry, scope, buffers)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewJSONRuntimeSource(context.Background(), JSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(), RangeBytes: 17,
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := collectJSONRuntime(t, source, 4)
	if len(ids) != 257 {
		t.Fatalf("ids=%d", len(ids))
	}
	for i, id := range ids {
		if id != int64(i) {
			t.Fatalf("id[%d]=%d", i, id)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestJSONRuntimeSourceWhitespaceValues 验证非 array 的多个 top-level object values 可以跨 range 串行 framing。
func TestJSONRuntimeSourceWhitespaceValues(t *testing.T) {
	data := []byte("  {\"id\":1,\"name\":\"a\"}\n {\"id\":2,\"name\":\"b\"}   {\"id\":3,\"name\":\"c\"}  ")
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, _ := NewJSONBinder(registry, scope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewJSONRuntimeSource(context.Background(), JSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(), RangeBytes: 9,
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := collectJSONRuntime(t, source, 2)
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("ids=%v", ids)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestJSONRuntimeSourceRejectsSnapshotDriftAndReleasesCarry 验证 snapshot drift 在读取前失败，提前 Close 会释放跨 range carry。
func TestJSONRuntimeSourceRejectsSnapshotDriftAndReleasesCarry(t *testing.T) {
	data := runtimeJSONArray(4)
	driver := newMem(t, data, vfs.Capabilities(vfs.RandomRead))
	registry, _ := vfs.NewRegistry(driver)
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	binder, _ := NewJSONBinder(registry, scope, buffers)
	binding, err := binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	planned := binding.Fingerprint()
	driver.data = append(driver.data, ' ')
	if source, err := NewJSONRuntimeSource(context.Background(), JSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: planned, RangeBytes: 8,
	}); err == nil || source != nil {
		t.Fatalf("drift source=%v err=%v", source, err)
	}
	// 恢复对象并重新 bind，随后只读首个 batch/部分 range 后提前关闭。
	driver.data = data
	binding, err = binder.Bind(context.Background(), driver.uri.String(), JSONBindOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := NewJSONRuntimeSource(context.Background(), JSONRuntimeSourceConfig{
		Binder: binder, Pattern: driver.uri.String(), ExpectedFingerprint: binding.Fingerprint(), RangeBytes: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	chunk.Release()
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFrameRuntimeJSONRejectsTrailingData 验证 array 关闭后任何非 whitespace 数据都显式失败。
func TestFrameRuntimeJSONRejectsTrailingData(t *testing.T) {
	state := jsonRuntimeFrameState{}
	if _, _, _, _, err := frameRuntimeJSON([]byte("[{\"id\":1}] garbage"), true, 10, &state); err == nil {
		t.Fatal("expected trailing-data rejection")
	}
}
