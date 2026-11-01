package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

// newSingleSlotShutdownService 创建并发 Query=1 的 server，用于稳定构造 queued admission。
func newSingleSlotShutdownService(t *testing.T, engine QueryEngine) (*QueryService, *resource.Scheduler, *memory.Manager) {
	t.Helper()
	budget := resource.Budget{
		ConcurrentQueries: 1,
		CPUSlots:          1,
		MemoryBytes:       1 << 20,
		RemoteIOSlots:     1,
		SpillIOSlots:      1,
	}
	scheduler, err := resource.NewScheduler(budget, []resource.ResourceGroup{{Name: "default", Weight: 1, Budget: budget}})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewManager(1 << 22)
	service, err := NewQueryService(context.Background(), NewQueryRegistry(), scheduler, engine, manager, QueryServiceConfig{CursorCapacityChunks: 1, MaxRetainedQueries: 16})
	if err != nil {
		t.Fatal(err)
	}
	return service, scheduler, manager
}

// waitQueuedQueries 等待 ResourceGroup queue 达到指定长度。
func waitQueuedQueries(t *testing.T, scheduler *resource.Scheduler, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := scheduler.Snapshot().Groups["default"].QueuedQueries; got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued queries=%d, want %d", scheduler.Snapshot().Groups["default"].QueuedQueries, want)
}

func TestQueryServiceShutdownCancelsRunningQueryAndReleasesOwnership(t *testing.T) {
	started := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
			close(started)
			<-queryContext.Context().Done()
			return queryContext.Context().Err()
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT running"))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := service.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.CanceledQueries != 1 || report.ForgottenQueries != 1 || len(report.RemainingQueries) != 0 {
		t.Fatalf("shutdown report=%+v", report)
	}
	if !scheduler.Snapshot().Closed || scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("scheduler snapshot=%+v", scheduler.Snapshot())
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leaked: %d", manager.Used())
	}
	if service.registry.Count() != 0 || len(service.shutdownRuntimeIDs()) != 0 {
		t.Fatalf("query retention leaked: registry=%d runtimes=%v", service.registry.Count(), service.shutdownRuntimeIDs())
	}
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT after_shutdown")); !errors.Is(err, resource.ErrSchedulerClosed) {
		t.Fatalf("submit after shutdown error=%v, want scheduler closed", err)
	}
	if service.registry.Count() != 0 {
		t.Fatalf("rejected post-shutdown submit leaked registry entry: %d", service.registry.Count())
	}
	_ = response
}

func TestQueryServiceShutdownReleasesQueuedAdmissionWithoutCompetingTerminalOwner(t *testing.T) {
	started := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-queryContext.Context().Done()
			return queryContext.Context().Err()
		}), nil
	}}
	service, scheduler, manager := newSingleSlotShutdownService(t, engine)
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT active")); err != nil {
		t.Fatal(err)
	}
	<-started

	queuedErr := make(chan error, 1)
	go func() {
		_, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT queued"))
		queuedErr <- err
	}()
	waitQueuedQueries(t, scheduler, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := service.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-queuedErr:
		if !errors.Is(err, resource.ErrSchedulerClosed) {
			t.Fatalf("queued submit error=%v, want scheduler closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued submit did not wake during shutdown")
	}
	if report.CanceledQueries != 1 || len(report.RemainingQueries) != 0 {
		t.Fatalf("shutdown report=%+v", report)
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) || manager.Used() != 0 || service.registry.Count() != 0 {
		t.Fatalf("shutdown leak: scheduler=%+v memory=%d registry=%d", scheduler.Snapshot().GlobalUsed, manager.Used(), service.registry.Count())
	}
}

func TestQueryServiceShutdownUnblocksSlowResultProducer(t *testing.T) {
	secondPushStarted := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			bm := memory.NewBufferManager(nil)
			first := testResultChunk(t, queryContext.Memory, bm, 1)
			if err := cursor.Push(queryContext.Context(), first); err != nil {
				first.Release()
				return err
			}
			second := testResultChunk(t, queryContext.Memory, bm, 2)
			close(secondPushStarted)
			if err := cursor.Push(queryContext.Context(), second); err != nil {
				second.Release()
				return err
			}
			return nil
		}), nil
	}}
	service, scheduler, manager := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT slow_client")); err != nil {
		t.Fatal(err)
	}
	<-secondPushStarted

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := service.Shutdown(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.CanceledQueries != 1 || len(report.RemainingQueries) != 0 {
		t.Fatalf("shutdown report=%+v", report)
	}
	if manager.Used() != 0 {
		t.Fatalf("buffered result memory leaked: %d", manager.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestQueryServiceShutdownTimeoutReportsUncooperativeQuery(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error {
			close(started)
			<-release
			return nil
		}), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT ignores_cancel"))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	report, err := service.Shutdown(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error=%v, want deadline exceeded", err)
	}
	if len(report.RemainingQueries) != 1 || report.RemainingQueries[0] != response.ID {
		t.Fatalf("shutdown report=%+v", report)
	}
	if scheduler.Snapshot().GlobalUsed.ConcurrentQueries != 1 {
		t.Fatalf("uncooperative query lease was falsely released: %+v", scheduler.Snapshot().GlobalUsed)
	}

	close(release)
	waitQueryState(t, service, response.ID, QueryCanceled)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	finalReport, err := service.Shutdown(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if len(finalReport.RemainingQueries) != 0 || scheduler.Snapshot().GlobalUsed != (resource.Usage{}) || service.registry.Count() != 0 {
		t.Fatalf("final shutdown report=%+v scheduler=%+v registry=%d", finalReport, scheduler.Snapshot().GlobalUsed, service.registry.Count())
	}
}

func TestQueryServiceShutdownIsIdempotentWhenAlreadyDrained(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		report, err := service.Shutdown(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if report != (ShutdownReport{}) {
			t.Fatalf("idempotent shutdown report=%+v", report)
		}
	}
	if !scheduler.Snapshot().Closed {
		t.Fatal("scheduler not closed")
	}
}
