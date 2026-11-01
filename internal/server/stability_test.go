package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

const phase5StabilityIterations = 256

// stabilityResultChunk 创建一个完全由 Query MemoryScope 管理的单值结果块。
func stabilityResultChunk(scope *memory.Scope, buffers *memory.BufferManager, value int64) (*vector.DataChunk, error) {
	column, err := vector.NewFlat(scope, buffers, types.LogicalType{ID: types.BigInt}, 1)
	if err != nil {
		return nil, err
	}
	column.SetInt64(0, value)
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, err
	}
	return chunk, nil
}

// waitGoroutineStability 给已退出的 Query worker 一个调度/GC 窗口，并验证 goroutine 数没有持续增长。
func waitGoroutineStability(t *testing.T, baseline, tolerance int) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		if current := runtime.NumGoroutine(); current <= baseline+tolerance {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutine count did not return near baseline: baseline=%d current=%d tolerance=%d", baseline, runtime.NumGoroutine(), tolerance)
}

// assertStabilityOwnershipZero 验证一个完整 batch 后所有 Query/Resource ownership 都已释放。
func assertStabilityOwnershipZero(t *testing.T, service *QueryService, scheduler *resource.Scheduler, manager *memory.Manager) {
	t.Helper()
	if manager.Used() != 0 {
		t.Fatalf("managed memory leaked: used=%d peak=%d", manager.Used(), manager.Peak())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("resource lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
	if service.registry.Count() != 0 {
		t.Fatalf("query registry retention leaked: %d", service.registry.Count())
	}
	if runtimes := service.shutdownRuntimeIDs(); len(runtimes) != 0 {
		t.Fatalf("query runtime ownership leaked: %v", runtimes)
	}
}

// TestPhase5LongRunningStability 反复执行成功与取消 Query，验证长期运行后没有 Managed Memory、Lease、Registry、runtime 或 goroutine 持续泄漏。
func TestPhase5LongRunningStability(t *testing.T) {
	baselineGoroutines := runtime.NumGoroutine()
	buffers := memory.NewBufferManager(nil)
	engine := fakeQueryEngine{plan: func(_ *querypkg.Context, sql string) (PreparedQuery, error) {
		if strings.Contains(sql, "cancel") {
			return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
				<-queryContext.Context().Done()
				return queryContext.Context().Err()
			}), nil
		}
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			chunk, err := stabilityResultChunk(queryContext.Memory, buffers, 1)
			if err != nil {
				return err
			}
			if err := cursor.Push(queryContext.Context(), chunk); err != nil {
				chunk.Release()
				return err
			}
			return nil
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{
		CursorCapacityChunks: 1,
		MaxRetainedQueries:   16,
	})

	for iteration := 0; iteration < phase5StabilityIterations; iteration++ {
		if iteration%8 == 0 {
			response, err := service.Submit(context.Background(), defaultSubmitRequest(fmt.Sprintf("SELECT cancel_%d", iteration)))
			if err != nil {
				t.Fatalf("iteration %d cancel submit: %v", iteration, err)
			}
			waitQueryState(t, service, response.ID, QueryRunning)
			if err := service.Cancel(response.ID); err != nil {
				t.Fatalf("iteration %d cancel: %v", iteration, err)
			}
			waitQueryState(t, service, response.ID, QueryCanceled)
			if err := service.Forget(response.ID); err != nil {
				t.Fatalf("iteration %d cancel forget: %v", iteration, err)
			}
		} else {
			response, err := service.Submit(context.Background(), defaultSubmitRequest(fmt.Sprintf("SELECT success_%d", iteration)))
			if err != nil {
				t.Fatalf("iteration %d success submit: %v", iteration, err)
			}
			cursor, err := service.ClaimResults(response.ID)
			if err != nil {
				t.Fatalf("iteration %d claim results: %v", iteration, err)
			}
			chunk, err := cursor.Next(context.Background())
			if err != nil {
				t.Fatalf("iteration %d result: %v", iteration, err)
			}
			value, ok := chunk.Columns[0].Int64(0)
			chunk.Release()
			if !ok || value != 1 {
				t.Fatalf("iteration %d result value=%d valid=%v", iteration, value, ok)
			}
			if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
				t.Fatalf("iteration %d terminal result error=%v, want EOF", iteration, err)
			}
			waitQueryState(t, service, response.ID, QuerySucceeded)
			if err := service.Forget(response.ID); err != nil {
				t.Fatalf("iteration %d success forget: %v", iteration, err)
			}
		}

		// 每个固定 batch 都检查 ownership，避免只在测试末尾掩盖累积泄漏。
		if (iteration+1)%32 == 0 {
			assertStabilityOwnershipZero(t, service, scheduler, manager)
		}
	}

	assertStabilityOwnershipZero(t, service, scheduler, manager)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := service.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.CanceledQueries != 0 || report.ForgottenQueries != 0 || len(report.RemainingQueries) != 0 {
		t.Fatalf("unexpected final shutdown report: %+v", report)
	}
	assertStabilityOwnershipZero(t, service, scheduler, manager)
	waitGoroutineStability(t, baselineGoroutines, 4)
}
