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

// Operator 是旧的原地算子接口；实现只能修改当前 chunk，不能替换其 ownership。
type Operator interface {
	Process(context.Context, *vector.DataChunk) error
}

// TransformOperator 是可替换 DataChunk 的新算子接口。
// 成功返回 replacement 时，replacement 必须拥有独立 backing storage，不能借用 input Vector/Buffer；
// runner 会在 replacement 被接管后立即 Release input。返回 input 本身表示原地处理。
type TransformOperator interface {
	Transform(context.Context, *vector.DataChunk) (*vector.DataChunk, error)
}

type Sink interface {
	Consume(context.Context, *vector.DataChunk) error
	Finalize(context.Context) error
}

type Pipeline struct {
	ID           string
	Source       Source
	Operators    []Operator          // Operators 保留兼容旧原地 kernel；新 SQL transform 不应放入这里。
	Transforms   []TransformOperator // Transforms 按声明顺序执行，并由 runner 集中管理 replacement ownership。
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
					if err := op.Process(ctx, owned); err != nil {
						owned.Release()
						return scheduler.FAILED, err
					}
				}
				for _, op := range p.Transforms {
					next, err := op.Transform(ctx, owned)
					if err != nil {
						if next != nil && next != owned {
							next.Release()
						}
						owned.Release()
						return scheduler.FAILED, err
					}
					if next == nil {
						owned.Release()
						return scheduler.FAILED, errors.New("pipeline transform returned nil chunk")
					}
					if next != owned {
						owned.Release()
						owned = next
					}
				}
				if err := p.Sink.Consume(ctx, owned); err != nil {
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
