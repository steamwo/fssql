package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
)

// TransformSink 把一组 TransformOperator 适配成 Sink，供 join 等“直接 push 到 downstream Sink”的算子复用。
// 输入 chunk ownership 仍属于上游调用者；replacement ownership 只有在 downstream 成功接管时才转移。
type TransformSink struct {
	Transforms []TransformOperator // Transforms 按 child-output -> parent-output 顺序执行。
	Downstream Sink                // Downstream 接收最终 transformed chunk。
}

func (s *TransformSink) Finished() bool {
	for _, transform := range s.Transforms {
		if finishing, ok := transform.(FinishingTransform); ok && finishing.Finished() {
			return true
		}
	}
	return SinkFinished(s.Downstream)
}

var _ Sink = (*TransformSink)(nil)
var _ CloseableSink = (*TransformSink)(nil)
var _ OwnershipTakingSink = (*TransformSink)(nil)
var _ OwnedInputSink = (*TransformSink)(nil)

// NewTransformSink 创建 streaming sink adapter；空 transform 仍允许，用于统一 Close/Finalize 透传。
func NewTransformSink(transforms []TransformOperator, downstream Sink) (*TransformSink, error) {
	if downstream == nil {
		return nil, fmt.Errorf("transform sink requires downstream")
	}
	for i, transform := range transforms {
		if transform == nil {
			return nil, fmt.Errorf("transform sink transform %d is nil", i)
		}
	}
	return &TransformSink{Transforms: append([]TransformOperator(nil), transforms...), Downstream: downstream}, nil
}

// TakesChunkOwnership 仅在最终 downstream 接管 chunk 时向上游传播 ownership transfer。
func (s *TransformSink) TakesChunkOwnership() bool {
	return s != nil && s.Downstream != nil && SinkTakesChunkOwnership(s.Downstream)
}

// Consume 执行 replacement-aware transform chain；原始 input 默认属于上游。
// downstream 成功声明 ownership transfer 时，最终 chunk（可能是 input，也可能是 replacement）不再由本 adapter Release。
func (s *TransformSink) Consume(ctx context.Context, input *vector.DataChunk) error {
	return s.consume(ctx, input, false)
}

// ConsumeOwned allows streaming transforms to move columns from this chunk.
func (s *TransformSink) ConsumeOwned(ctx context.Context, input *vector.DataChunk) error {
	return s.consume(ctx, input, true)
}

func (s *TransformSink) consume(ctx context.Context, current *vector.DataChunk, ownsCurrent bool) error {
	defer func() {
		if ownsCurrent && current != nil {
			current.Release()
		}
	}()
	if s == nil || s.Downstream == nil {
		return fmt.Errorf("transform sink is uninitialized")
	}
	if current == nil {
		return fmt.Errorf("transform sink cannot consume nil chunk")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, transform := range s.Transforms {
		var next *vector.DataChunk
		var err error
		if move, ok := transform.(OwnedTransform); ownsCurrent && ok {
			next, err = move.TransformOwned(ctx, current)
		} else {
			next, err = transform.Transform(ctx, current)
		}
		if err != nil {
			if next != nil && next != current {
				next.Release()
			}
			return err
		}
		if next == nil {
			return errors.New("transform sink transform returned nil chunk")
		}
		if next != current {
			if ownsCurrent {
				current.Release()
			}
			current = next
			ownsCurrent = true
		}
	}
	if ownsCurrent {
		ownsCurrent = false
		return ConsumeOwnedChunk(ctx, s.Downstream, current)
	}
	return s.Downstream.Consume(ctx, current)
}

// Finalize 把 stage 终态传给真实 downstream；transform 自身无额外 finalize state。
func (s *TransformSink) Finalize(ctx context.Context) error {
	if s == nil || s.Downstream == nil {
		return fmt.Errorf("transform sink is uninitialized")
	}
	return s.Downstream.Finalize(ctx)
}

// Close 仅在 abort 路径调用；若 downstream 持有 blocking/temp ownership，则继续向下释放。
func (s *TransformSink) Close() error {
	if s == nil || s.Downstream == nil {
		return nil
	}
	return closeSink(s.Downstream)
}
