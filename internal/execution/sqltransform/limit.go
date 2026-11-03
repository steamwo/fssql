package sqltransform

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

// Limit 是全局 streaming LIMIT/OFFSET transform。
// 多个 Pipeline worker 共享同一实例，因此 counters 必须受锁；未指定 LIMIT 时 unlimited=true。
type Limit struct {
	Executor  *expression.Executor // Executor 提供 managed materialization 依赖。
	Offset    uint64               // Offset 是全局需要跳过的前导行数。
	Limit     uint64               // Limit 是最多输出行数；unlimited=true 时忽略。
	Unlimited bool                 // Unlimited 表示 OFFSET-only query。

	mu      sync.Mutex // mu 保护全局 skipped/emitted counters 与 selection 决策。
	skipped uint64     // skipped 是已消费的 OFFSET 行数。
	emitted uint64     // emitted 是已输出的 LIMIT 行数。
}

var _ pipeline.TransformOperator = (*Limit)(nil)

// NewLimit 创建 LIMIT/OFFSET transform；输出仍使用 query managed memory。
func NewLimit(executor *expression.Executor, offset, limit uint64, unlimited bool) (*Limit, error) {
	if executor == nil || executor.Scope == nil || executor.Buffers == nil {
		return nil, fmt.Errorf("sql limit requires managed expression executor")
	}
	return &Limit{Executor: executor, Offset: offset, Limit: limit, Unlimited: unlimited}, nil
}

// Transform 对当前 chunk 应用全局 OFFSET/LIMIT，并物化独立 owned DataChunk。
// Pipeline checks Finished before claiming more source work.
func (l *Limit) Transform(ctx context.Context, in *vector.DataChunk) (*vector.DataChunk, error) {
	if l == nil || l.Executor == nil || in == nil {
		return nil, fmt.Errorf("sql limit requires initialized transform and input chunk")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	rows := make([]int, 0, in.Cardinality)
	for row := 0; row < in.Cardinality; row++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if l.skipped < l.Offset {
			l.skipped++
			continue
		}
		if !l.Unlimited && l.emitted >= l.Limit {
			continue
		}
		rows = append(rows, row)
		l.emitted++
	}
	return materializeSelection(ctx, l.Executor, in, rows)
}

func (l *Limit) Finished() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.Unlimited && l.emitted >= l.Limit
}
