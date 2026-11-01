package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	querypkg "github.com/steamwo/fssql/internal/query"
)

// newRegistryQueryContext 创建 registry 测试使用的独立 QueryContext。
func newRegistryQueryContext() *querypkg.Context {
	return querypkg.New(context.Background(), memory.NewManager(1<<20))
}

func TestQueryRegistryRegisterGetAndRemoveTerminal(t *testing.T) {
	now := time.Unix(10, 0).UTC()
	registry := NewQueryRegistryWith(func() (QueryID, error) { return "q-1", nil }, func() time.Time { return now })
	handle, err := registry.Register(newRegistryQueryContext())
	if err != nil {
		t.Fatal(err)
	}
	if registry.Count() != 1 || handle.ID() != "q-1" {
		t.Fatalf("count=%d id=%s", registry.Count(), handle.ID())
	}
	got, err := registry.Get("q-1")
	if err != nil || got != handle {
		t.Fatalf("get=%p err=%v, want %p", got, err, handle)
	}
	if err := registry.Remove("q-1"); !errors.Is(err, ErrQueryNotTerminal) {
		t.Fatalf("remove nonterminal err=%v, want ErrQueryNotTerminal", err)
	}
	if err := handle.Finalize(QueryFailed, errors.New("parse failed"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove("q-1"); err != nil {
		t.Fatal(err)
	}
	if registry.Count() != 0 {
		t.Fatalf("count=%d, want 0", registry.Count())
	}
	if _, err := registry.Get("q-1"); !errors.Is(err, ErrQueryNotFound) {
		t.Fatalf("get removed err=%v, want ErrQueryNotFound", err)
	}
}

func TestQueryRegistryRejectsDuplicateID(t *testing.T) {
	registry := NewQueryRegistryWith(func() (QueryID, error) { return "same", nil }, nil)
	first := newRegistryQueryContext()
	if _, err := registry.Register(first); err != nil {
		t.Fatal(err)
	}
	second := newRegistryQueryContext()
	if _, err := registry.Register(second); !errors.Is(err, ErrQueryExists) {
		t.Fatalf("duplicate err=%v, want ErrQueryExists", err)
	}
	// Registry 未取得 second ownership，测试显式释放调用方仍拥有的 QueryContext。
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	handle, _ := registry.Get("same")
	if err := handle.Finalize(QueryFailed, errors.New("done"), time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove("same"); err != nil {
		t.Fatal(err)
	}
}

func TestQueryRegistryCancelKeepsHandleUntilFinalized(t *testing.T) {
	registry := NewQueryRegistryWith(func() (QueryID, error) { return "q-cancel", nil }, nil)
	handle, err := registry.Register(newRegistryQueryContext())
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Transition(QueryQueued, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RequestCancel("q-cancel"); err != nil {
		t.Fatal(err)
	}
	if registry.Count() != 1 || handle.Snapshot().State != QueryQueued {
		t.Fatalf("count=%d snapshot=%+v", registry.Count(), handle.Snapshot())
	}
	if err := registry.Remove("q-cancel"); !errors.Is(err, ErrQueryNotTerminal) {
		t.Fatalf("remove err=%v, want ErrQueryNotTerminal", err)
	}
	if err := handle.Finalize(QueryCanceled, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove("q-cancel"); err != nil {
		t.Fatal(err)
	}
}

func TestQueryRegistryConcurrentLookupAndCancel(t *testing.T) {
	registry := NewQueryRegistryWith(func() (QueryID, error) { return "q-race", nil }, nil)
	handle, err := registry.Register(newRegistryQueryContext())
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Transition(QueryQueued, time.Time{}); err != nil {
		t.Fatal(err)
	}

	const workers = 32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wg.Done()
			if index%2 == 0 {
				_, _ = registry.Get("q-race")
				return
			}
			_ = registry.RequestCancel("q-race")
		}(i)
	}
	wg.Wait()
	if !handle.Snapshot().CancelRequested {
		t.Fatal("cancel request was not recorded")
	}
	if err := handle.Finalize(QueryCanceled, nil, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove("q-race"); err != nil {
		t.Fatal(err)
	}
}

func TestGenerateQueryIDIsNonEmptyAndDistinct(t *testing.T) {
	first, err := generateQueryID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := generateQueryID()
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || second == "" || first == second {
		t.Fatalf("first=%q second=%q", first, second)
	}
	if len(first) != 32 || len(second) != 32 {
		t.Fatalf("unexpected ID lengths: %d %d", len(first), len(second))
	}
}
