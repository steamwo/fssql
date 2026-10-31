package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/scheduler"
)

type Source interface {
	Next(context.Context) (*vector.DataChunk, bool, error)
}
type Operator interface {
	Process(context.Context, *vector.DataChunk) error
}
type Sink interface {
	Consume(context.Context, *vector.DataChunk) error
	Finalize(context.Context) error
}

type Pipeline struct {
	ID           string
	Source       Source
	Operators    []Operator
	Sink         Sink
	Dependencies []*Pipeline
}

func (p *Pipeline) Run(ctx context.Context, workers int, obs scheduler.Observer) error {
	if p.Source == nil || p.Sink == nil {
		return errors.New("pipeline requires source and sink")
	}
	if workers < 1 {
		workers = 1
	}
	tasks := make([]*scheduler.Task, workers)
	for i := 0; i < workers; i++ {
		tasks[i] = scheduler.NewTask(func(ctx context.Context) (scheduler.TaskState, error) {
			for {
				chunk, ok, err := p.Source.Next(ctx)
				if err != nil {
					return scheduler.FAILED, err
				}
				if !ok {
					return scheduler.DONE, nil
				}
				owned := chunk
				for _, op := range p.Operators {
					if err := op.Process(ctx, chunk); err != nil {
						owned.Release()
						return scheduler.FAILED, err
					}
				}
				if err := p.Sink.Consume(ctx, chunk); err != nil {
					owned.Release()
					return scheduler.FAILED, err
				}
				owned.Release()
			}
		})
	}
	if err := scheduler.New(workers).Run(ctx, tasks, obs); err != nil {
		return err
	}
	return p.Sink.Finalize(ctx)
}

type dagState uint8

const (
	visiting dagState = iota + 1
	done
)

func RunDAG(ctx context.Context, root *Pipeline, workers int, obs scheduler.Observer) error {
	state := map[*Pipeline]dagState{}
	var mu sync.Mutex
	var visit func(*Pipeline) error
	visit = func(p *Pipeline) error {
		mu.Lock()
		s := state[p]
		if s == visiting {
			mu.Unlock()
			return fmt.Errorf("pipeline DAG cycle at %q", p.ID)
		}
		if s == done {
			mu.Unlock()
			return nil
		}
		state[p] = visiting
		mu.Unlock()
		for _, d := range p.Dependencies {
			if err := visit(d); err != nil {
				return err
			}
		}
		if err := p.Run(ctx, workers, obs); err != nil {
			return err
		}
		mu.Lock()
		state[p] = done
		mu.Unlock()
		return nil
	}
	return visit(root)
}
