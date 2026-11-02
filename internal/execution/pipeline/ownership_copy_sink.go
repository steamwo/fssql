package pipeline

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
)

// OwnershipCopySink 把 borrowed-consume emitter（例如 join）与接管 chunk ownership 的最终 Sink 隔离。
// 输入仍归 emitter；本 adapter 用 Query MemoryScope 物化独立 DataChunk，再按 downstream ownership contract 释放或转移。
type OwnershipCopySink struct {
	Scope      *memory.Scope         // Scope 是当前 QueryContext managed memory scope。
	Buffers    *memory.BufferManager // Buffers 创建独立输出 vectors。
	Downstream Sink                  // Downstream 通常是 ResultCursor/Native Chunk queue。
}

var _ Sink = (*OwnershipCopySink)(nil)
var _ CloseableSink = (*OwnershipCopySink)(nil)

// NewOwnershipCopySink 创建 managed copy boundary；它本身故意不实现 OwnershipTakingSink，所以上游始终保留输入 ownership。
func NewOwnershipCopySink(scope *memory.Scope, buffers *memory.BufferManager, downstream Sink) (*OwnershipCopySink, error) {
	if scope == nil || buffers == nil || downstream == nil {
		return nil, fmt.Errorf("ownership copy sink requires managed scope, BufferManager and downstream")
	}
	return &OwnershipCopySink{Scope: scope, Buffers: buffers, Downstream: downstream}, nil
}

// Consume 深拷贝当前 DataChunk；成功交给 ownership-taking downstream 后 clone ownership 转移。
func (s *OwnershipCopySink) Consume(ctx context.Context, input *vector.DataChunk) error {
	if s == nil || s.Scope == nil || s.Buffers == nil || s.Downstream == nil {
		return fmt.Errorf("ownership copy sink is uninitialized")
	}
	if input == nil {
		return fmt.Errorf("ownership copy sink cannot consume nil chunk")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	columns := make([]*vector.Vector, len(input.Columns))
	for columnIndex, source := range input.Columns {
		if source == nil {
			for _, owned := range columns {
				if owned != nil {
					owned.Release()
				}
			}
			return fmt.Errorf("ownership copy sink input column %d is nil", columnIndex)
		}
		clone, err := vector.NewFlat(s.Scope, s.Buffers, source.Type, input.Cardinality)
		if err != nil {
			for _, owned := range columns {
				if owned != nil {
					owned.Release()
				}
			}
			return err
		}
		for row := 0; row < input.Cardinality; row++ {
			if err := vector.CopyValue(source, row, clone, row); err != nil {
				clone.Release()
				for _, owned := range columns {
					if owned != nil {
						owned.Release()
					}
				}
				return fmt.Errorf("ownership copy sink column %d row %d: %w", columnIndex, row, err)
			}
		}
		columns[columnIndex] = clone
	}
	chunk, err := vector.NewDataChunk(columns...)
	if err != nil {
		for _, owned := range columns {
			if owned != nil {
				owned.Release()
			}
		}
		return err
	}
	if err := s.Downstream.Consume(ctx, chunk); err != nil {
		chunk.Release()
		return err
	}
	if !SinkTakesChunkOwnership(s.Downstream) {
		chunk.Release()
	}
	return nil
}

// Finalize 直接透传 terminal result。
func (s *OwnershipCopySink) Finalize(ctx context.Context) error { return s.Downstream.Finalize(ctx) }

// Close 在 abort 路径继续释放 downstream state；已转移的 chunks 由 downstream 自身生命周期负责。
func (s *OwnershipCopySink) Close() error { return closeSink(s.Downstream) }
