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

// OwnedTransform may move input columns only after all fallible preparation
// succeeds. On error input ownership remains unchanged. Only the runner's
// exclusively owned input uses this extension; borrowed TransformSink does not.
type OwnedTransform interface {
	TransformOperator
	TransformOwned(context.Context, *vector.DataChunk) (*vector.DataChunk, error)
}

// FinishingTransform reports normal early completion, rather than cancellation.
// Workers already inside Next finish their owned work before Source.Close.
type FinishingTransform interface{ Finished() bool }

type Sink interface {
	Consume(context.Context, *vector.DataChunk) error
	Finalize(context.Context) error
}

// OwnershipTakingSink 是最终 Native Chunk/ResultCursor 一类 Sink 的可选扩展。
// Consume 成功后若返回 true，chunk ownership 转移给 Sink，调用方不得再 Release；
// Consume 返回 error 时 ownership 永远没有转移，调用方仍负责 Release。
type OwnershipTakingSink interface {
	Sink
	TakesChunkOwnership() bool
}

// SinkTakesChunkOwnership 统一判断 Sink 的成功消费 ownership 语义，供 Pipeline 与直接 push 的 join emitter 共用。
func SinkTakesChunkOwnership(sink Sink) bool {
	owner, ok := sink.(OwnershipTakingSink)
	return ok && owner.TakesChunkOwnership()
}

// CloseableSink 是持有 blocking state/temp ownership 的可选 Sink 扩展。
// Pipeline 仅在执行失败或 Finalize 失败时调用 Close；成功 Finalize 后 Sink 可能继续作为下一 stage 的 Source，不能提前关闭。
type CloseableSink interface {
	Sink
	Close() error
}

type Pipeline struct {
	ID string
	// PreserveOrder keeps this stage's Source/transform/sink sequence ordered.
	// Dependency stages still use the requested worker count.
	PreserveOrder bool
	Source        Source
	Operators     []Operator          // Operators 保留兼容旧原地 kernel；新 SQL transform 不应放入这里。
	Transforms    []TransformOperator // Transforms 按声明顺序执行，并由 runner 集中管理 replacement ownership。
	Sink          Sink
	Dependencies  []*Pipeline
	// SkipDependencies reuses already materialized state. Dependencies must be
	// unstarted; their resources are closed before this stage runs normally.
	SkipDependencies func() bool
	// Abort 仅用于“dependency 失败导致本 Pipeline 根本未运行”时回收当前节点已创建的 ownership。
	// 成功运行、当前 Pipeline 自身失败、cycle 检测都不会通过此字段重复清理。
	Abort func() error
}

func (p *Pipeline) Run(ctx context.Context, workers int, obs scheduler.Observer) error {
	if p.Source == nil || p.Sink == nil {
		return errors.New("pipeline requires source and sink")
	}
	if workers < 1 || p.PreserveOrder {
		workers = 1
	}
	tasks := make([]*scheduler.Task, workers)
	for i := 0; i < workers; i++ {
		workerID := i
		tasks[i] = scheduler.NewTask(func(ctx context.Context) (scheduler.TaskState, error) {
			ctx = WithWorkerID(ctx, workerID)
			for {
				if err := ctx.Err(); err != nil {
					return scheduler.FAILED, err
				}
				for _, transform := range p.Transforms {
					if finishing, ok := transform.(FinishingTransform); ok && finishing.Finished() {
						return scheduler.DONE, nil
					}
				}
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
					var next *vector.DataChunk
					var err error
					if transfer, ok := op.(OwnedTransform); ok {
						next, err = transfer.TransformOwned(ctx, owned)
					} else {
						next, err = op.Transform(ctx, owned)
					}
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
				if !SinkTakesChunkOwnership(p.Sink) {
					owned.Release()
				}
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
		skip := p.SkipDependencies != nil && p.SkipDependencies()
		for _, d := range p.Dependencies {
			var err error
			if skip {
				err = AbortUnstarted(d)
			} else {
				err = visit(d)
			}
			if err != nil {
				// 当前 Pipeline 尚未运行；若它已经持有 join/runtime 等 ownership，必须在 dependency 错误返回前释放。
				var abortErr error
				if p.Abort != nil {
					abortErr = p.Abort()
				}
				return errors.Join(err, abortErr)
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
