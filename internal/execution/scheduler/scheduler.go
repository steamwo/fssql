package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type TaskState uint8

const (
	READY TaskState = iota
	RUNNING
	PARKED
	DONE
	FAILED
)

type TaskFunc func(context.Context) (TaskState, error)

type Task struct {
	mu    sync.RWMutex
	state TaskState
	fn    TaskFunc
	err   error
}

func NewTask(fn TaskFunc) *Task            { return &Task{state: READY, fn: fn} }
func (t *Task) State() TaskState           { t.mu.RLock(); defer t.mu.RUnlock(); return t.state }
func (t *Task) Err() error                 { t.mu.RLock(); defer t.mu.RUnlock(); return t.err }
func (t *Task) set(s TaskState, err error) { t.mu.Lock(); t.state = s; t.err = err; t.mu.Unlock() }

type Observer interface {
	TaskStarted()
	TaskFinished()
}
type nopObserver struct{}

func (nopObserver) TaskStarted()  {}
func (nopObserver) TaskFinished() {}

type Scheduler struct {
	workers       int
	deterministic bool
}

func New(workers int) *Scheduler {
	if workers < 1 {
		workers = 1
	}
	return &Scheduler{workers: workers}
}
func NewDeterministic() *Scheduler { return &Scheduler{workers: 1, deterministic: true} }

type taskResult struct {
	task  *Task
	state TaskState
	err   error
}

func execute(ctx context.Context, t *Task, o Observer) taskResult {
	if err := ctx.Err(); err != nil {
		t.set(FAILED, err)
		return taskResult{t, FAILED, err}
	}
	t.set(RUNNING, nil)
	o.TaskStarted()
	state, err := t.fn(ctx)
	o.TaskFinished()
	if err != nil {
		t.set(FAILED, err)
		return taskResult{t, FAILED, err}
	}
	if state != PARKED && state != DONE {
		err = fmt.Errorf("task returned invalid terminal/yield state %d", state)
		t.set(FAILED, err)
		return taskResult{t, FAILED, err}
	}
	t.set(state, nil)
	return taskResult{t, state, nil}
}
func (s *Scheduler) Run(ctx context.Context, tasks []*Task, obs Observer) error {
	if obs == nil {
		obs = nopObserver{}
	}
	if s.deterministic {
		return s.runDeterministic(ctx, tasks, obs)
	}
	return s.runParallel(ctx, tasks, obs)
}
func (s *Scheduler) runDeterministic(ctx context.Context, tasks []*Task, obs Observer) error {
	queue := append([]*Task(nil), tasks...)
	for len(queue) > 0 {
		t := queue[0]
		queue = queue[1:]
		r := execute(ctx, t, obs)
		if r.err != nil {
			return r.err
		}
		if r.state == PARKED {
			t.set(READY, nil)
			queue = append(queue, t)
		}
	}
	return nil
}
func (s *Scheduler) runParallel(ctx context.Context, tasks []*Task, obs Observer) error {
	if len(tasks) == 0 {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := s.workers
	if workers > len(tasks) {
		workers = len(tasks)
	}
	jobs := make(chan *Task, len(tasks))
	results := make(chan taskResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobs {
				results <- execute(runCtx, t, obs)
			}
		}()
	}
	for _, t := range tasks {
		jobs <- t
	}
	remaining := len(tasks)
	var firstErr error
	for remaining > 0 {
		r := <-results
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
				cancel()
			}
			remaining--
			continue
		}
		if r.state == PARKED {
			r.task.set(READY, nil)
			jobs <- r.task
		} else {
			remaining--
		}
	}
	close(jobs)
	wg.Wait()
	close(results)
	return firstErr
}

var ErrNotQuiescent = errors.New("scheduler not quiescent")
