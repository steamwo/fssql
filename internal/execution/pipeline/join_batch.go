package pipeline

import (
	"context"

	"github.com/steamwo/fssql/internal/core/vector"
	exechash "github.com/steamwo/fssql/internal/execution/hash"
)

// JoinBatch borrows matches until ConsumeJoinBatch returns. A negative probe
// row or nil build row represents SQL NULL extension. Offsets preserve the
// logical left/right schema when the physical build side changes.
type JoinBatch struct {
	Probe                              *vector.DataChunk
	ProbeRows                          []int
	BuildRows                          [][]byte
	BuildLayout                        exechash.RowLayout
	ProbeStart, ProbeWidth, BuildStart int
}

// JoinBatchConsumer optionally consumes matches without joined output vectors.
// Returning false must leave state untouched, allowing ordinary materialization.
// Implementations must never retain borrowed row references.
type JoinBatchConsumer interface {
	ConsumeJoinBatch(context.Context, JoinBatch) (bool, error)
}
