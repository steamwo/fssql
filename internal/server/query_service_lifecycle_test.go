package server

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

func TestQueryServiceSuccessWaitsForResultDrainBeforeTerminal(t *testing.T) {
	var queryScope *memory.Scope
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, sql string) (PreparedQuery, error) {
		if sql != "SELECT 1" {
			return nil, errors.New("unexpected sql")
		}
		queryScope = queryContext.Memory
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			chunk := testResultChunk(t, queryContext.Memory, memory.NewBufferManager(nil), 1)
			if err := cursor.Push(queryContext.Context(), chunk); err != nil {
				chunk.Release()
				return err
			}
			return nil
		}), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1, MaxRetainedQueries: 8})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QueryFinishing)
	if scheduler.Snapshot().GlobalUsed.ConcurrentQueries != 1 {
		t.Fatalf("lease released before result drain: %+v", scheduler.Snapshot().GlobalUsed)
	}
	cursor, err := service.ClaimResults(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := chunkInt64(t, chunk); got != 1 {
		t.Fatalf("value=%d", got)
	}

	// ring 已经排空，但 consumer 尚未释放 Managed DataChunk；此时绝不能越过 I-10 发布成功终态。
	if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal result err=%v, want EOF", err)
	}
	time.Sleep(20 * time.Millisecond)
	if snapshot, err := service.Status(response.ID); err != nil || snapshot.State != QueryFinishing {
		t.Fatalf("state before consumer release=%+v err=%v, want FINISHING", snapshot, err)
	}
	if queryScope == nil || queryScope.Used() == 0 {
		t.Fatalf("consumer-held managed result was not accounted: scope=%v", queryScope)
	}
	if scheduler.Snapshot().GlobalUsed.ConcurrentQueries != 1 {
		t.Fatalf("lease released while consumer still owns result: %+v", scheduler.Snapshot().GlobalUsed)
	}

	if err := cursor.ReleaseChunk(chunk); err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QuerySucceeded)
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("resource lease leaked: %+v", scheduler.Snapshot().GlobalUsed)
	}
	if queryScope.Used() != 0 {
		t.Fatalf("query memory leaked: %d", queryScope.Used())
	}
}

func TestQueryServiceCancelWaitsForConsumerResultRelease(t *testing.T) {
	var queryScope *memory.Scope
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, _ string) (PreparedQuery, error) {
		queryScope = queryContext.Memory
		return preparedQueryFunc(func(queryContext *querypkg.Context, cursor *ResultCursor) error {
			chunk := testResultChunk(t, queryContext.Memory, memory.NewBufferManager(nil), 7)
			if err := cursor.Push(queryContext.Context(), chunk); err != nil {
				chunk.Release()
				return err
			}
			<-queryContext.Context().Done()
			return queryContext.Context().Err()
		}), nil
	}}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT cancel_owned"))
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := service.ClaimResults(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Cancel(response.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	snapshot, err := service.Status(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State.Terminal() {
		t.Fatalf("query became terminal before consumer release: %+v", snapshot)
	}
	if scheduler.Snapshot().GlobalUsed.ConcurrentQueries != 1 || queryScope == nil || queryScope.Used() == 0 {
		t.Fatalf("ownership released early: usage=%+v memory=%v", scheduler.Snapshot().GlobalUsed, queryScope)
	}
	if err := cursor.ReleaseChunk(chunk); err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QueryCanceled)
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) || queryScope.Used() != 0 {
		t.Fatalf("cancel ownership leaked: usage=%+v memory=%d", scheduler.Snapshot().GlobalUsed, queryScope.Used())
	}
}

func TestQueryServiceLifetimeDoesNotUseSubmitRequestContextAfterAdmission(t *testing.T) {
	started := make(chan struct{})
	allowFinish := make(chan struct{})
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, _ string) (PreparedQuery, error) {
		return preparedQueryFunc(func(queryContext *querypkg.Context, _ *ResultCursor) error {
			close(started)
			<-allowFinish
			return queryContext.Context().Err()
		}), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	requestCtx, cancel := context.WithCancel(context.Background())
	response, err := service.Submit(requestCtx, defaultSubmitRequest("SELECT long_running"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	select {
	case <-serviceRuntimeContext(t, service, response.ID).Done():
		t.Fatal("submit request context canceled server-owned QueryContext")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowFinish)
	cursor, err := service.ClaimResults(response.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("result err=%v, want EOF", err)
	}
	waitQueryState(t, service, response.ID, QuerySucceeded)
}

func TestQueryServiceCancelUnblocksSlowProducerAndReleasesManagedChunks(t *testing.T) {
	var scope *memory.Scope
	secondPushStarted := make(chan struct{})
	engine := fakeQueryEngine{plan: func(queryContext *querypkg.Context, _ string) (PreparedQuery, error) {
		scope = queryContext.Memory
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
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{CursorCapacityChunks: 1})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT blocked"))
	if err != nil {
		t.Fatal(err)
	}
	<-secondPushStarted
	if err := service.Cancel(response.ID); err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, response.ID, QueryCanceled)
	if scope == nil {
		t.Fatal("query scope was not captured")
	}
	if scope.Used() != 0 {
		t.Fatalf("managed result bytes leaked after cancel: %d", scope.Used())
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("admission lease leaked after cancel: %+v", scheduler.Snapshot().GlobalUsed)
	}
}

func TestQueryServicePlanFailurePublishesFailedTerminal(t *testing.T) {
	failure := errors.New("bind failed")
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, failure }}
	service, scheduler, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT bad"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitQueryState(t, service, response.ID, QueryFailed)
	if !errors.Is(snapshot.Error, failure) {
		t.Fatalf("terminal error=%v, want plan failure", snapshot.Error)
	}
	if scheduler.Snapshot().GlobalUsed != (resource.Usage{}) {
		t.Fatalf("lease leaked after plan failure: %+v", scheduler.Snapshot().GlobalUsed)
	}
}
