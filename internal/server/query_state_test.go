package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
)

// newTestQueryHandle 创建带独立 QueryContext 的确定性状态机测试对象。
func newTestQueryHandle(t *testing.T) *QueryHandle {
	t.Helper()
	manager := memory.NewManager(1 << 20)
	queryContext := querypkg.New(context.Background(), manager)
	handle, err := NewQueryHandle("q-test", queryContext, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	return handle
}

func TestQueryStateNames(t *testing.T) {
	cases := map[QueryState]string{
		QuerySubmitted: "SUBMITTED", QueryQueued: "QUEUED", QueryPlanning: "PLANNING",
		QueryRunning: "RUNNING", QueryFinishing: "FINISHING", QuerySucceeded: "SUCCEEDED",
		QueryFailed: "FAILED", QueryCanceled: "CANCELED",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Fatalf("state %d String()=%q, want %q", state, got, want)
		}
	}
}

func TestQueryHandleHappyPathRequiresFullForwardChain(t *testing.T) {
	handle := newTestQueryHandle(t)
	chain := []QueryState{QueryQueued, QueryPlanning, QueryRunning, QueryFinishing}
	for _, next := range chain {
		if err := handle.Transition(next, time.Time{}); err != nil {
			t.Fatalf("transition to %s: %v", next.String(), err)
		}
	}
	if err := handle.Finalize(QuerySucceeded, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	snapshot := handle.Snapshot()
	if snapshot.State != QuerySucceeded || !handle.Context().Closed() {
		t.Fatalf("snapshot=%+v closed=%v", snapshot, handle.Context().Closed())
	}
}

func TestQueryHandleRejectsSkippedAndDirectTerminalTransition(t *testing.T) {
	handle := newTestQueryHandle(t)
	if err := handle.Transition(QueryRunning, time.Time{}); !errors.Is(err, ErrInvalidQueryTransition) {
		t.Fatalf("skip err=%v, want ErrInvalidQueryTransition", err)
	}
	if err := handle.Transition(QuerySucceeded, time.Time{}); !errors.Is(err, ErrInvalidQueryTransition) {
		t.Fatalf("direct terminal err=%v, want ErrInvalidQueryTransition", err)
	}
	if err := handle.Finalize(QuerySucceeded, nil, time.Time{}); !errors.Is(err, ErrInvalidQueryTransition) {
		t.Fatalf("early success err=%v, want ErrInvalidQueryTransition", err)
	}
	if handle.Context().Closed() {
		t.Fatal("invalid transition must not close QueryContext")
	}
}

func TestRequestCancelDoesNotPublishTerminalBeforeCleanup(t *testing.T) {
	handle := newTestQueryHandle(t)
	if err := handle.Transition(QueryQueued, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := handle.RequestCancel(time.Time{}); err != nil {
		t.Fatal(err)
	}
	snapshot := handle.Snapshot()
	if snapshot.State != QueryQueued || !snapshot.CancelRequested || snapshot.State.Terminal() {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	select {
	case <-handle.Context().Context().Done():
	default:
		t.Fatal("QueryContext was not canceled")
	}
	if err := handle.Finalize(QueryCanceled, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	snapshot = handle.Snapshot()
	if snapshot.State != QueryCanceled || !errors.Is(snapshot.Error, context.Canceled) {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}

func TestFinalizeRefusesTerminalWhileActiveTaskExists(t *testing.T) {
	handle := newTestQueryHandle(t)
	handle.Context().TaskStarted()
	if err := handle.RequestCancel(time.Time{}); err != nil {
		t.Fatal(err)
	}
	err := handle.Finalize(QueryCanceled, nil, time.Time{})
	if err == nil {
		t.Fatal("expected lifecycle close failure")
	}
	if handle.Snapshot().State.Terminal() {
		t.Fatalf("terminal state published despite active task: %+v", handle.Snapshot())
	}
	if handle.Context().Closed() {
		t.Fatal("QueryContext marked closed despite active task")
	}
	handle.Context().TaskFinished()
	if err := handle.Finalize(QueryCanceled, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if !handle.Context().Closed() {
		t.Fatal("QueryContext should be closed after active task reaches zero")
	}
}

func TestFailedFinalizeRequiresErrorAndClosesContext(t *testing.T) {
	handle := newTestQueryHandle(t)
	if err := handle.Finalize(QueryFailed, nil, time.Time{}); err == nil {
		t.Fatal("expected failed query to require terminal error")
	}
	failure := errors.New("planner failed")
	if err := handle.Finalize(QueryFailed, failure, time.Time{}); err != nil {
		t.Fatal(err)
	}
	snapshot := handle.Snapshot()
	if snapshot.State != QueryFailed || !errors.Is(snapshot.Error, failure) || !handle.Context().Closed() {
		t.Fatalf("snapshot=%+v closed=%v", snapshot, handle.Context().Closed())
	}
}

func TestTerminalHandleRejectsFurtherMutation(t *testing.T) {
	handle := newTestQueryHandle(t)
	failure := errors.New("bind failed")
	if err := handle.Finalize(QueryFailed, failure, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := handle.RequestCancel(time.Time{}); !errors.Is(err, ErrQueryTerminal) {
		t.Fatalf("cancel err=%v, want ErrQueryTerminal", err)
	}
	if err := handle.Transition(QueryQueued, time.Time{}); !errors.Is(err, ErrQueryTerminal) {
		t.Fatalf("transition err=%v, want ErrQueryTerminal", err)
	}
	if err := handle.Finalize(QueryFailed, failure, time.Time{}); !errors.Is(err, ErrQueryTerminal) {
		t.Fatalf("finalize err=%v, want ErrQueryTerminal", err)
	}
}
