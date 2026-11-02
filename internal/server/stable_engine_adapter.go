package server

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	querypkg "github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/session"
)

// StableEngineAdapter 把 transport-neutral session SQL engines 适配为 AD-037 QueryService QueryEngine。
// production 应配置 Statements；Engine 保留 SELECT-only 兼容 fallback，避免已有嵌入调用方立即破坏。
type StableEngineAdapter struct {
	Statements *session.StableStatementEngine // Statements 是 SELECT/control/DESCRIBE 的统一 Stable path。
	Engine     *session.StableSQLEngine       // Engine 是旧 SELECT-only fallback。
}

var _ QueryEngine = (*StableEngineAdapter)(nil)

// Plan 在 QueryService 已创建的 QueryContext 上完成 planning；PreparedQuery 不持有 HTTP/request lifetime。
func (a *StableEngineAdapter) Plan(q *querypkg.Context, sql string) (PreparedQuery, error) {
	if a == nil || q == nil {
		return nil, fmt.Errorf("server: Stable SQL engine adapter is uninitialized")
	}
	var prepared session.PreparedStatement
	var err error
	if a.Statements != nil {
		prepared, err = a.Statements.Prepare(q.Context(), q, sql)
	} else if a.Engine != nil {
		prepared, err = a.Engine.Prepare(q.Context(), q, sql)
	} else {
		return nil, fmt.Errorf("server: Stable SQL engine adapter has no statement/query engine")
	}
	if err != nil {
		return nil, err
	}
	return &stablePreparedQuery{prepared: prepared}, nil
}

type stablePreparedQuery struct {
	prepared session.PreparedStatement // prepared 不持有 ResultCursor；每次 Run 绑定唯一 query result consumer。
}

func (p *stablePreparedQuery) Run(q *querypkg.Context, cursor *ResultCursor) error {
	if p == nil || p.prepared == nil || q == nil || cursor == nil {
		return fmt.Errorf("server: Stable prepared query requires query context and result cursor")
	}
	output := p.prepared.Output()
	columns := make([]ResultColumn, len(output))
	for i, column := range output {
		columns[i] = ResultColumn{Name: column.Name, Type: cloneResultLogicalType(column.Type), Nullable: column.Nullable}
	}
	if err := cursor.SetColumns(columns); err != nil {
		return fmt.Errorf("server: publish prepared result schema: %w", err)
	}
	return p.prepared.Run(q, &resultCursorSink{cursor: cursor})
}

// resultCursorSink 将 final native DataChunk ownership 直接转移给 ResultCursor。
type resultCursorSink struct {
	cursor *ResultCursor // cursor 是唯一 consumer ownership queue。
}

var _ pipeline.Sink = (*resultCursorSink)(nil)
var _ pipeline.OwnershipTakingSink = (*resultCursorSink)(nil)

func (*resultCursorSink) TakesChunkOwnership() bool { return true }

func (s *resultCursorSink) Consume(ctx context.Context, chunk *vector.DataChunk) error {
	if s == nil || s.cursor == nil {
		return fmt.Errorf("server: result cursor sink is uninitialized")
	}
	return s.cursor.Push(ctx, chunk)
}

func (*resultCursorSink) Finalize(context.Context) error { return nil }
