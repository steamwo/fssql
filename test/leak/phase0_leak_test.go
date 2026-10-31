package leak

import (
	"context"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/execution/scheduler"
	"github.com/steamwo/fssql/internal/query"
	"runtime"
	"testing"
	"time"
)

func TestSchedulerNoGoroutineOrReservationLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	for iter := 0; iter < 50; iter++ {
		m := memory.NewManager(1024)
		q := query.New(context.Background(), m)
		tasks := make([]*scheduler.Task, 8)
		for i := range tasks {
			tasks[i] = scheduler.NewTask(func(context.Context) (scheduler.TaskState, error) { return scheduler.DONE, nil })
		}
		if err := scheduler.New(4).Run(q.Context(), tasks, q); err != nil {
			t.Fatal(err)
		}
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		if m.Used() != 0 {
			t.Fatalf("managed leak %d", m.Used())
		}
	}
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak before=%d after=%d", before, after)
	}
}
