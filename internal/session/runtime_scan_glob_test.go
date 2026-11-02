package session

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// globRuntimeFixture 创建 query-scoped managed memory 与可注入的 glob planner callback。
func globRuntimeFixture(t *testing.T, plan globPlanFunc) (*memory.Manager, *memory.Scope, ScanOpenAdapter) {
	t.Helper()
	manager := memory.NewManager(16 << 20)
	scope := manager.NewScope()
	adapter, err := newGlobScanOpenAdapter(plan, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	return manager, scope, adapter
}

func globRuntimeSpec(fingerprint string) planner.ScanSpec {
	return planner.ScanSpec{
		RelationID:  "tablefunc:glob:" + fingerprint,
		Function:    "glob",
		Source:      "memory://bucket/*.parquet",
		Fingerprint: stats.DatasetFingerprint(fingerprint),
	}
}

// TestGlobRuntimeSourceValidatesFrozenFingerprint 验证 execution 重新 Plan 后必须与 Statement frozen fingerprint 完全一致。
func TestGlobRuntimeSourceValidatesFrozenFingerprint(t *testing.T) {
	manager, scope, adapter := globRuntimeFixture(t, func(context.Context, string) ([]string, string, error) {
		return []string{"memory://bucket/a.parquet"}, "runtime-fingerprint", nil
	})
	if source, err := adapter.Open(context.Background(), globRuntimeSpec("bound-fingerprint")); err == nil || source != nil {
		t.Fatalf("source=%v err=%v", source, err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGlobRuntimeSourceBatchesLimitAndOwnsStrings 验证安全 limit candidate、vector batch 与 StringHeap ownership。
func TestGlobRuntimeSourceBatchesLimitAndOwnsStrings(t *testing.T) {
	uris := make([]string, 2100)
	for i := range uris {
		uris[i] = fmt.Sprintf("memory://bucket/file-%04d.parquet", i)
	}
	manager, scope, adapter := globRuntimeFixture(t, func(context.Context, string) ([]string, string, error) {
		return uris, "fp", nil
	})
	spec := globRuntimeSpec("fp")
	spec.Pushdown.Limit = 2050
	source, err := adapter.Open(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	// adapter 必须复制 URI slice，不能继续借用 planner callback 的 backing array。
	uris[0] = "memory://bucket/mutated.parquet"
	var got []string
	for {
		chunk, ok, err := source.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if chunk.Cardinality > 2048 {
			chunk.Release()
			t.Fatalf("batch cardinality=%d", chunk.Cardinality)
		}
		for row := 0; row < chunk.Cardinality; row++ {
			value, valid := chunk.Columns[0].String(row)
			if !valid {
				chunk.Release()
				t.Fatalf("row %d is NULL", row)
			}
			got = append(got, value)
		}
		chunk.Release()
	}
	if len(got) != 2050 {
		t.Fatalf("rows=%d", len(got))
	}
	if got[0] != "memory://bucket/file-0000.parquet" || got[len(got)-1] != "memory://bucket/file-2049.parquet" {
		t.Fatalf("unexpected URI bounds: first=%q last=%q", got[0], got[len(got)-1])
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGlobRuntimeSourceDeclinesFilterProjectionCandidates 验证未接受的 candidate 不会变成查询错误，source 仍输出完整 relation 交给 residual operator。
func TestGlobRuntimeSourceDeclinesFilterProjectionCandidates(t *testing.T) {
	calls := 0
	manager, scope, adapter := globRuntimeFixture(t, func(context.Context, string) ([]string, string, error) {
		calls++
		return []string{"memory://bucket/a.parquet"}, "fp", nil
	})
	spec := globRuntimeSpec("fp")
	spec.Pushdown.Filters = []*binder.BoundExpression{{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}, Literal: "true"}}
	// SELECT file,file 可产生重复 projection candidate；glob 当前不接受 projection，因此 source 仍输出完整单列 relation。
	spec.Pushdown.Projection = []int{0, 0}
	source, err := adapter.Open(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("planner calls=%d", calls)
	}
	chunk, ok, err := source.Next(context.Background())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if len(chunk.Columns) != 1 || chunk.Cardinality != 1 {
		chunk.Release()
		t.Fatalf("columns=%d cardinality=%d", len(chunk.Columns), chunk.Cardinality)
	}
	value, valid := chunk.Columns[0].String(0)
	chunk.Release()
	if !valid || value != "memory://bucket/a.parquet" {
		t.Fatalf("value=%q valid=%v", value, valid)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestGlobRuntimeSourceConcurrentNext 验证 Pipeline 多 worker 并发读取时游标不重复、不遗漏。
func TestGlobRuntimeSourceConcurrentNext(t *testing.T) {
	uris := make([]string, 5000)
	for i := range uris {
		uris[i] = fmt.Sprintf("memory://bucket/file-%04d.parquet", i)
	}
	manager, scope, adapter := globRuntimeFixture(t, func(context.Context, string) ([]string, string, error) {
		return uris, "fp", nil
	})
	source, err := adapter.Open(context.Background(), globRuntimeSpec("fp"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]struct{}, len(uris))
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
				mu.Lock()
				for row := 0; row < chunk.Cardinality; row++ {
					value, valid := chunk.Columns[0].String(row)
					if !valid {
						mu.Unlock()
						chunk.Release()
						errCh <- fmt.Errorf("NULL URI")
						return
					}
					if _, exists := seen[value]; exists {
						mu.Unlock()
						chunk.Release()
						errCh <- fmt.Errorf("duplicate URI %s", value)
						return
					}
					seen[value] = struct{}{}
				}
				mu.Unlock()
				chunk.Release()
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
	if len(seen) != len(uris) {
		t.Fatalf("seen=%d want=%d", len(seen), len(uris))
	}
	if manager.Used() != 0 {
		t.Fatalf("managed leak=%d", manager.Used())
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
}

var _ pipeline.Source = (*globSource)(nil)
