package sqltransform

import (
	"context"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
)

// makeLimitChunk 创建共享 query scope 下的单列 BIGINT chunk。
func makeLimitChunk(t *testing.T, scope *memory.Scope, buffers *memory.BufferManager, start, count int) *vector.DataChunk {
	t.Helper()
	column, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, count)
	if err != nil {
		t.Fatal(err)
	}
	for row := 0; row < count; row++ {
		column.SetInt64(row, int64(start+row))
	}
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		t.Fatal(err)
	}
	return chunk
}

// TestLimitTransformSpansChunks 验证 OFFSET/LIMIT counters 跨 chunk 全局生效。
func TestLimitTransformSpansChunks(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	executor := expression.NewExecutor(scope, buffers)
	limit, err := NewLimit(executor, 3, 4, false)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for batch, values := range [][]int{{0, 1}, {2, 3, 4}, {5, 6, 7, 8}} {
		input := makeLimitChunk(t, scope, buffers, values[0], len(values))
		output, err := limit.Transform(context.Background(), input)
		input.Release()
		if err != nil {
			t.Fatalf("batch=%d: %v", batch, err)
		}
		for row := 0; row < output.Cardinality; row++ {
			value, ok := output.Columns[0].Int64(row)
			if !ok {
				output.Release()
				t.Fatal("unexpected NULL")
			}
			got = append(got, value)
		}
		output.Release()
	}
	want := []int64{3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("got=%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got=%v want=%v", got, want)
		}
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLimitTransformZeroAndOffsetOnly 区分 LIMIT 0 与没有 LIMIT 的 OFFSET-only。
func TestLimitTransformZeroAndOffsetOnly(t *testing.T) {
	for _, tc := range []struct {
		name      string
		offset    uint64
		limit     uint64
		unlimited bool
		want      int
	}{{"limit-zero", 0, 0, false, 0}, {"offset-only", 2, 0, true, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			manager := memory.NewManager(1 << 20)
			scope := manager.NewScope()
			buffers := memory.NewBufferManager(nil)
			executor := expression.NewExecutor(scope, buffers)
			operator, err := NewLimit(executor, tc.offset, tc.limit, tc.unlimited)
			if err != nil {
				t.Fatal(err)
			}
			input := makeLimitChunk(t, scope, buffers, 0, 5)
			output, err := operator.Transform(context.Background(), input)
			input.Release()
			if err != nil {
				t.Fatal(err)
			}
			if output.Cardinality != tc.want {
				output.Release()
				t.Fatalf("cardinality=%d want=%d", output.Cardinality, tc.want)
			}
			output.Release()
			if manager.Used() != 0 {
				t.Fatalf("managed leak=%d", manager.Used())
			}
			if err := scope.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLimitTransformOwnedReusesWholeBatch(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	limit, err := NewLimit(expression.NewExecutor(scope, bm), 0, 4, false)
	if err != nil {
		t.Fatal(err)
	}
	in := makeLimitChunk(t, scope, bm, 0, 4)
	defer in.Release()
	out, err := limit.TransformOwned(context.Background(), in)
	if out != nil && out != in {
		defer out.Release()
	}
	if err != nil || out != in || !limit.Finished() {
		t.Fatalf("owned batch reused=%t finished=%t err=%v", out == in, limit.Finished(), err)
	}
}

// TestLimitTransformConcurrentGlobalCount 验证多 Pipeline worker 共享 transform 时总输出不超过全局 LIMIT。
func TestLimitTransformConcurrentGlobalCount(t *testing.T) {
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	executor := expression.NewExecutor(scope, buffers)
	operator, err := NewLimit(executor, 5, 7, false)
	if err != nil {
		t.Fatal(err)
	}
	// 测试数据在启动 worker 前构造，避免在非 test goroutine 中调用 t.Fatal/FailNow。
	inputs := make([]*vector.DataChunk, 4)
	for worker := range inputs {
		inputs[worker] = makeLimitChunk(t, scope, buffers, worker*10, 10)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	total := 0
	for worker := 0; worker < 4; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			input := inputs[worker]
			output, transformErr := operator.Transform(context.Background(), input)
			input.Release()
			if transformErr != nil {
				t.Errorf("worker=%d: %v", worker, transformErr)
				return
			}
			mu.Lock()
			total += output.Cardinality
			mu.Unlock()
			output.Release()
		}()
	}
	wg.Wait()
	if total != 7 {
		t.Fatalf("total=%d want=7", total)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}
