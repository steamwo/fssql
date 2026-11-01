package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/resource"
)

func TestQueryServiceRetentionCapAndForget(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{MaxRetainedQueries: 1})
	first, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, first.ID, QuerySucceeded)
	if _, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 2")); !errors.Is(err, ErrQueryServiceCapacity) {
		t.Fatalf("second submit err=%v, want ErrQueryServiceCapacity", err)
	}
	if err := service.Forget(first.ID); err != nil {
		t.Fatal(err)
	}
	second, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 2"))
	if err != nil {
		t.Fatal(err)
	}
	waitQueryState(t, service, second.ID, QuerySucceeded)
}

func TestQueryServiceClaimResultsIsSingleConsumer(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{})
	response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ClaimResults(response.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ClaimResults(response.ID); !errors.Is(err, ErrQueryResultsClaimed) {
		t.Fatalf("second claim err=%v, want ErrQueryResultsClaimed", err)
	}
}

func TestQueryServiceRejectsUnboundedActiveConcurrency(t *testing.T) {
	unlimited := resource.Budget{
		ConcurrentQueries: resource.Unlimited,
		CPUSlots:          resource.Unlimited,
		MemoryBytes:       resource.Unlimited,
		RemoteIOSlots:     resource.Unlimited,
		SpillIOSlots:      resource.Unlimited,
	}
	scheduler, err := resource.NewScheduler(unlimited, []resource.ResourceGroup{{Name: "default", Weight: 1, Budget: unlimited}})
	if err != nil {
		t.Fatal(err)
	}
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, nil }}
	_, err = NewQueryService(context.Background(), NewQueryRegistry(), scheduler, engine, memory.NewManager(1<<20), QueryServiceConfig{})
	if !errors.Is(err, ErrUnboundedQueryConcurrency) {
		t.Fatalf("err=%v, want ErrUnboundedQueryConcurrency", err)
	}
}

func TestQueryServiceConcurrentRetentionCheckCannotExceedHardLimit(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error { return nil }), nil
	}}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{MaxRetainedQueries: 4})

	const submitters = 16
	type submitResult struct {
		response SubmitResponse
		err      error
	}
	results := make(chan submitResult, submitters)
	var wg sync.WaitGroup
	wg.Add(submitters)
	for i := 0; i < submitters; i++ {
		go func() {
			defer wg.Done()
			response, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT concurrent"))
			results <- submitResult{response: response, err: err}
		}()
	}
	wg.Wait()
	close(results)

	successIDs := make([]QueryID, 0, 4)
	for result := range results {
		if result.err == nil {
			successIDs = append(successIDs, result.response.ID)
			continue
		}
		if !errors.Is(result.err, ErrQueryServiceCapacity) {
			t.Fatalf("unexpected submit err=%v", result.err)
		}
	}
	if len(successIDs) != 4 || service.registry.Count() != 4 {
		t.Fatalf("success=%d registry=%d, want 4", len(successIDs), service.registry.Count())
	}
	for _, id := range successIDs {
		waitQueryState(t, service, id, QuerySucceeded)
		if err := service.Forget(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQueryServiceAdmissionCancellationLeavesNoOrphan(t *testing.T) {
	started := make(chan struct{})
	block := make(chan struct{})
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) {
		return preparedQueryFunc(func(*querypkg.Context, *ResultCursor) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-block
			return nil
		}), nil
	}}
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
	service, err := NewQueryService(context.Background(), NewQueryRegistry(), scheduler, engine, memory.NewManager(1<<22), QueryServiceConfig{MaxRetainedQueries: 8})
	if err != nil {
		t.Fatal(err)
	}

	first, err := service.Submit(context.Background(), defaultSubmitRequest("SELECT blocker"))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := service.Submit(ctx, defaultSubmitRequest("SELECT canceled_while_queued")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued submit err=%v, want DeadlineExceeded", err)
	}
	if service.registry.Count() != 1 {
		t.Fatalf("registry count=%d, want only first query", service.registry.Count())
	}
	if scheduler.Snapshot().Groups["default"].QueuedQueries != 0 {
		t.Fatalf("queued admission orphaned: %+v", scheduler.Snapshot())
	}

	close(block)
	cursor, err := service.ClaimResults(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(context.Background()); err == nil {
		t.Fatal("expected terminal EOF")
	}
	waitQueryState(t, service, first.ID, QuerySucceeded)
}

func TestQueryServiceRejectsInvalidSubmitControlValues(t *testing.T) {
	engine := fakeQueryEngine{plan: func(*querypkg.Context, string) (PreparedQuery, error) { return nil, nil }}
	service, _, _ := newQueryServiceHarness(t, engine, QueryServiceConfig{MaxSQLBytes: 8})
	cases := []SubmitRequest{
		{SQL: "", ResourceGroup: "default"},
		{SQL: "SELECT 123456789", ResourceGroup: "default"},
		{SQL: "SELECT 1", ResourceGroup: ""},
		{SQL: "SELECT 1", ResourceGroup: "default", Class: resource.QueryClass(99)},
		{SQL: "SELECT 1", ResourceGroup: "default", CPUSlots: -1},
	}
	for _, request := range cases {
		if _, err := service.Submit(context.Background(), request); !errors.Is(err, ErrInvalidSubmitRequest) {
			t.Fatalf("request=%+v err=%v, want ErrInvalidSubmitRequest", request, err)
		}
	}
}
