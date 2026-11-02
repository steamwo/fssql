package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
)

// TransformSink 把一组 TransformOperator 适配成 Sink，供 join 等“直接 push 到 downstream Sink”的算子复用。
// 输入 chunk ownership 仍属于上游调用者；只有 transform 创建的 replacement 由本 adapter 负责释放。
type TransformSink struct {
	Transforms []TransformOperator // Transforms 按 child-output -> parent-output 顺序执行。
	Downstream Sink                // Downstream 接收最终 transformed chunk。
}

var _ Sink = (*TransformSink)(nil)
var _ CloseableSink = (*TransformSink)(nil)

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

// Consume 执行 replacement-aware transform chain；原始 input 永不由 adapter 释放，避免与 Pipeline runner 双重 ownership。
func (s *TransformSink) Consume(ctx context.Context, input *vector.DataChunk) error {
	if s == nil || s.Downstream == nil {
		return fmt.Errorf("transform sink is uninitialized")
	}
	if input == nil {
		return fmt.Errorf("transform sink cannot consume nil chunk")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owned := input
	ownsOwned := false
	for _, transform := range s.Transforms {
		next, err := transform.Transform(ctx, owned)
		if err != nil {
			if next != nil && next != owned {
				next.Release()
			}
			if ownsOwned {
				owned.Release()
			}
			return err
		}
		if next == nil {
			if ownsOwned {
				owned.Release()
			}
			return errors.New("transform sink transform returned nil chunk")
		}
		if next != owned {
			if ownsOwned {
				owned.Release()
			}
			owned = next
			ownsOwned = true
		}
	}
	err := s.Downstream.Consume(ctx, owned)
	if ownsOwned {
		owned.Release()
	}
	return err
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
