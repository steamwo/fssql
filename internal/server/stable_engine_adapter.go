package server

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
)

// StableEngineAdapter 把 transport-neutral session.StableSQLEngine 适配为 AD-037 QueryService QueryEngine。
type StableEngineAdapter struct {
	Engine *session.StableSQLEngine // Engine 是 Parser→Push Pipeline 的唯一 production SELECT path。
}

var _ QueryEngine = (*StableEngineAdapter)(nil)

// Plan 在 QueryService 已创建的 QueryContext 上完成 planning；PreparedQuery 不持有 HTTP/request lifetime。
func (a *StableEngineAdapter) Plan(q *querypkg.Context, sql string) (PreparedQuery, error) {
	if a == nil || a.Engine == nil || q == nil {
		return nil, fmt.Errorf("server: Stable SQL engine adapter is uninitialized")
	}
	prepared, err := a.Engine.Prepare(q.Context(), q, sql)
	if err != nil {
		return nil, err
	}
	return &stablePreparedQuery{prepared: prepared}, nil
}

type stablePreparedQuery struct {
	prepared *session.PreparedSQLQuery // prepared 不持有 ResultCursor；每次 Run 绑定唯一 query result consumer。
}

func (p *stablePreparedQuery) Run(q *querypkg.Context, cursor *ResultCursor) error {
	if p == nil || p.prepared == nil || q == nil || cursor == nil {
		return fmt.Errorf("server: Stable prepared query requires query context and result cursor")
	}
	return p.prepared.Run(q, &resultCursorSink{cursor: cursor})
}

// resultCursorSink 将 final native DataChunk ownership 直接转移给 ResultCursor。
type resultCursorSink struct {
	cursor *ResultCursor // cursor 是唯一 consumer ownership queue。
}

var _ pipeline.Sink = (*resultCursorSink)(nil)
var _ pipeline.OwnershipTakingSink = (*resultCursorSink)(nil)

// TakesChunkOwnership 声明 Consume 成功后 ResultCursor 已接管 chunk；Pipeline 不得再次 Release。
func (*resultCursorSink) TakesChunkOwnership() bool { return true }

// Consume 复用 ResultCursor 的 bounded queued+inflight backpressure；Push 失败时 ownership 不转移。
func (s *resultCursorSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s == nil || s.cursor == nil {
		return fmt.Errorf("server: result cursor sink is uninitialized")
	}
	return s.cursor.Push(ctx, chunk)
}

// Finalize no-op：QueryService 在 PreparedQuery.Run 返回后统一调用 ResultCursor.Finish(runErr)，避免双 terminal ownership。
func (*resultCursorSink) Finalize(context.Context) error { return nil }
