package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

type obs struct {
	active atomic.Int64
	peak   atomic.Int64
}

func (o *obs) TaskStarted() {
	n := o.active.Add(1)
	for p := o.peak.Load(); n > p && !o.peak.CompareAndSwap(p, n); p = o.peak.Load() {
	}
}
func (o *obs) TaskFinished() { o.active.Add(-1) }
func TestDeterministicParkedTransitions(t *testing.T) {
	var n int
	task := NewTask(func(context.Context) (TaskState, error) {
		n++
		if n == 1 {
			return PARKED, nil
		}
		return DONE, nil
	})
	if err := NewDeterministic().Run(context.Background(), []*Task{task}, nil); err != nil {
		t.Fatal(err)
	}
	if task.State() != DONE || n != 2 {
		t.Fatalf("state=%v n=%d", task.State(), n)
	}
}
func TestParallelBoundedWorkers(t *testing.T) {
	o := &obs{}
	gate := make(chan struct{})
	tasks := make([]*Task, 8)
	for i := range tasks {
		tasks[i] = NewTask(func(context.Context) (TaskState, error) { <-gate; return DONE, nil })
	}
	close(gate)
	if err := New(3).Run(context.Background(), tasks, o); err != nil {
		t.Fatal(err)
	}
	if o.active.Load() != 0 || o.peak.Load() > 3 {
		t.Fatalf("active=%d peak=%d", o.active.Load(), o.peak.Load())
	}
}
func TestTaskFailure(t *testing.T) {
	sentinel := errors.New("boom")
	task := NewTask(func(context.Context) (TaskState, error) { return FAILED, sentinel })
	if err := NewDeterministic().Run(context.Background(), []*Task{task}, nil); !errors.Is(err, sentinel) {
		t.Fatalf("%v", err)
	}
	if task.State() != FAILED {
		t.Fatal(task.State())
	}
}
func TestCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task := NewTask(func(context.Context) (TaskState, error) { return DONE, nil })
	if err := New(1).Run(ctx, []*Task{task}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("%v", err)
	}
}
func TestFailureCancelsSiblingTasks(t *testing.T) {
	started := make(chan struct{})
	sibling := NewTask(func(ctx context.Context) (TaskState, error) { close(started); <-ctx.Done(); return FAILED, ctx.Err() })
	sentinel := errors.New("first failure")
	failing := NewTask(func(context.Context) (TaskState, error) { <-started; return FAILED, sentinel })
	err := New(2).Run(context.Background(), []*Task{sibling, failing}, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v", err)
	}
	if sibling.State() != FAILED || failing.State() != FAILED {
		t.Fatalf("states %v %v", sibling.State(), failing.State())
	}
}
