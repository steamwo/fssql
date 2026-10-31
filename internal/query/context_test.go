package query

import (
	"context"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/execution/scheduler"
	"testing"
)

func TestQueryLifecycleQuiescent(t *testing.T) {
	m := memory.NewManager(1024)
	q := New(context.Background(), m)
	task := scheduler.NewTask(func(context.Context) (scheduler.TaskState, error) { return scheduler.DONE, nil })
	if err := scheduler.New(1).Run(q.Context(), []*scheduler.Task{task}, q); err != nil {
		t.Fatal(err)
	}
	if q.ActiveTasks() != 0 {
		t.Fatal("task leak")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if m.Used() != 0 {
		t.Fatal("memory leak")
	}
}
func TestQueryCloseDetectsReservationLeak(t *testing.T) {
	m := memory.NewManager(1024)
	q := New(context.Background(), m)
	r, err := q.Memory.Reserve(memory.TagVector, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err == nil {
		t.Fatal("expected leak")
	}
	r.Release()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
