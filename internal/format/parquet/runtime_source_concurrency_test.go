package parquet

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/fileset"
)

// TestRuntimeSourceConcurrentNext 验证 Push Pipeline 多 worker 并发读取时每个 row group 只被消费一次。
func TestRuntimeSourceConcurrentNext(t *testing.T) {
	manager := memory.NewManager(8 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	closed := 0
	groups := make([]RowGroup, 500)
	for i := range groups {
		groups[i].NumRows = int64(i + 1)
	}
	prepare := func(context.Context, fileset.FileSnapshot) (runtimePreparedFile, error) {
		return runtimePreparedFile{
			rowGroups: groups,
			newScanner: func(rg RowGroup) (runtimeRowGroupScanner, error) {
				return &runtimeFakeScanner{scope: scope, buffers: buffers, value: rg.NumRows, closed: &closed}, nil
			},
		}, nil
	}
	source, err := newRuntimeSource([]fileset.FileSnapshot{runtimeTestFile(t, "concurrent.parquet")}, prepare)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int64]struct{}, len(groups))
	errCh := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				chunk, ok, err := source.Next(context.Background())
				if err != nil {
					errCh <- err
					return
				}
				if !ok {
					return
				}
				value, valid := chunk.Columns[0].Int64(0)
				chunk.Release()
				if !valid {
					errCh <- fmt.Errorf("unexpected NULL")
					return
				}
				mu.Lock()
				if _, exists := seen[value]; exists {
					mu.Unlock()
					errCh <- fmt.Errorf("duplicate row group %d", value)
					return
				}
				seen[value] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != len(groups) {
		t.Fatalf("seen=%d want=%d", len(seen), len(groups))
	}
	if closed != len(groups) {
		t.Fatalf("closed scanners=%d want=%d", closed, len(groups))
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
