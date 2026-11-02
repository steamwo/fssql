package lowering

import (
	"context"
	"sync"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
)

// oneRowSource 为 SELECT 无 FROM 提供一个 cardinality=1、零列的关系。
// 它不持有 payload buffer；后续 Project 仍通过 ExpressionExecutor 生成 managed Vector。
type oneRowSource struct {
	mu   sync.Mutex // mu 保证多 worker 下只发出一次逻辑行。
	done bool       // done 表示唯一一行已经交给 pipeline。
}

var _ pipeline.Source = (*oneRowSource)(nil)

// Next 只返回一次零列 DataChunk；DataChunk.Release 对零列安全且无 memory reservation。
func (s *oneRowSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return nil, false, nil
	}
	s.done = true
	return &vector.DataChunk{Cardinality: 1}, true, nil
}
