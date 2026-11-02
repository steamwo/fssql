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

// CloseableSource 是持有 reader/client/buffer 等 query-scoped ownership 的可选 Source 扩展。
// Pipeline 只会在所有 scheduler task 已退出后调用一次 Close，因此实现无需与 Next 并发关闭。
type CloseableSource interface {
	Source
	Close() error
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

// CloseableSink 是持有 blocking state/temp ownership 的可选 Sink 扩展。
// Pipeline 仅在执行失败或 Finalize 失败时调用 Close；成功 Finalize 后 Sink 可能继续作为下一 stage 的 Source，不能提前关闭。
type CloseableSink interface {
	Sink
	Close() error
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
	runErr := scheduler.New(workers).Run(ctx, tasks, obs)
	var sourceCloseErr error
	if source, ok := p.Source.(CloseableSource); ok {
		sourceCloseErr = source.Close()
	}
	if runErr != nil || sourceCloseErr != nil {
		// 失败路径必须释放持有 blocking/temp ownership 的 Sink；三条错误链都保留，不能互相遮蔽。
		return errors.Join(runErr, sourceCloseErr, closeSink(p.Sink))
	}
	finalizeErr := p.Sink.Finalize(ctx)
	if finalizeErr != nil {
		// Finalize 失败表示 stage 未成功移交 ownership；CloseableSink 必须立即回收其内部状态。
		return errors.Join(finalizeErr, closeSink(p.Sink))
	}
	// 成功 Finalize 后不能关闭 Sink：blocking bridge 可能在依赖 stage 完成后继续作为 root Source。
	return nil
}

// closeSink 只处理显式 CloseableSink；普通 Sink 没有额外 ownership，保持旧行为。
func closeSink(sink Sink) error {
	if closeable, ok := sink.(CloseableSink); ok {
		return closeable.Close()
	}
	return nil
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
