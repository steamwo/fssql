package query

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/execution/scheduler"
	storagetemp "github.com/steamwo/fssql/internal/storage/temp"
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

func TestQueryCloseCleansTemporaryStorageOwnership(t *testing.T) {
	m := memory.NewManager(1024)
	quotas := storagetemp.NewTemporaryMemoryManager(storagetemp.Limits{Global: 1024, DefaultResourceGroup: 1024, DefaultQuery: 1024})
	storage, err := storagetemp.NewTemporaryStorageManager(t.TempDir(), quotas)
	if err != nil {
		t.Fatal(err)
	}
	tempScope, err := storage.NewQueryScope("default")
	if err != nil {
		t.Fatal(err)
	}
	f, err := tempScope.CreateFile()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("spill")); err != nil {
		t.Fatal(err)
	}

	q := NewWithTemp(context.Background(), m, tempScope)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if got := quotas.GlobalUsed(); got != 0 {
		t.Fatalf("temp quota leak: %d", got)
	}
	if got := tempScope.OwnedFiles(); got != 0 {
		t.Fatalf("temp file ownership leak: %d", got)
	}
}
