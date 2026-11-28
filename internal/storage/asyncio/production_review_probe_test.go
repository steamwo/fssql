package asyncio

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
)

func TestProductionReviewCloseWithBlockedSubmit(t *testing.T) {
	d := &blockingDriver{start: make(chan struct{}, 1), release: make(chan struct{})}
	m := NewManager(1, 1)
	defer m.Close()
	scope := memory.NewManager(100).NewScope()
	defer scope.Close()
	bm := memory.NewBufferManager(nil)
	request := requestFor(d, scope, bm)
	first := m.Submit(context.Background(), request)
	<-d.start
	queued := m.Submit(context.Background(), request)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	submitted := make(chan Future, 1)
	go func() { submitted <- m.Submit(ctx, request) }()
	deadline := time.Now().Add(3 * time.Second)
	for m.mu.TryLock() {
		m.mu.Unlock()
		if time.Now().After(deadline) {
			cancel()
			m.Close()
			t.Fatal("submit did not acquire read lock")
		}
		runtime.Gosched()
	}
	done := make(chan struct{})
	go func() { m.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("cleanup remained blocked")
		}
		<-submitted
		t.Fatal("Close deadlocked behind a blocked Submit; canceling the submit context was required")
	}
	var blocked Future
	select {
	case blocked = <-submitted:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Submit")
	}
	for _, future := range []Future{first, queued, blocked, m.Submit(context.Background(), request)} {
		if _, err := future.Wait(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("shutdown result=%v, want ErrClosed", err)
		}
	}
	if m.Active() != 0 || scope.Used() != 0 {
		t.Fatalf("shutdown retained active=%d memory=%d", m.Active(), scope.Used())
	}
}
