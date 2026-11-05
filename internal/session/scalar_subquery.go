package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/optimizer"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type preparedScalarSubquery struct {
	id       int                   // id 对应 BoundRuntimeConstant.Literal。
	physical *planner.PhysicalPlan // physical 是独立、非相关 child query plan。
	typeID   types.LogicalType     // typeID 是 child 唯一输出列的 frozen logical type。
}

// rewriteScalarSubqueries 把 non-correlated scalar child query 改写成执行期 managed constant。
// 非相关子查询按依赖顺序在聚合之前物化；相关子查询交由 Planner 去相关。
func rewriteScalarSubqueries(ctx context.Context, queryPlanner *planner.QueryPlanner, opt *optimizer.Optimizer, query *binder.BoundQuery) (*binder.BoundQuery, []preparedScalarSubquery, error) {
	if query == nil || queryPlanner == nil || opt == nil {
		return nil, nil, fmt.Errorf("session: scalar subquery rewrite requires query, planner and optimizer")
	}
	rewriter := &scalarQueryRewriter{ctx: ctx, planner: queryPlanner, optimizer: opt, queries: make(map[*binder.BoundQuery]*binder.BoundQuery)}
	rewritten, err := rewriter.rewriteQuery(query)
	return rewritten, rewriter.prepared, err
}

func queryContainsScalarSubquery(query *binder.BoundQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Projection {
		if expressionContainsScalarSubquery(item.Expression) {
			return true
		}
	}
	if expressionContainsScalarSubquery(query.Where) || expressionContainsScalarSubquery(query.Having) {
		return true
	}
	for _, term := range query.OrderBy {
		if expressionContainsScalarSubquery(term.Expression) {
			return true
		}
	}
	return false
}

func expressionContainsScalarSubquery(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundSubquery {
		return true
	}
	for _, child := range expr.Children {
		if expressionContainsScalarSubquery(child) {
			return true
		}
	}
	return false
}

func queryHasAggregateExpression(query *binder.BoundQuery) bool {
	if query == nil {
		return false
	}
	for _, item := range query.Projection {
		if expressionHasAggregate(item.Expression) {
			return true
		}
	}
	if expressionHasAggregate(query.Where) || expressionHasAggregate(query.Having) {
		return true
	}
	for _, term := range query.OrderBy {
		if expressionHasAggregate(term.Expression) {
			return true
		}
	}
	return false
}

// expressionHasAggregate 只依据统一 FunctionCatalog kind 判断 aggregate，禁止在 session 层复制枚举数值语义。
func expressionHasAggregate(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundFunction && expr.FunctionKind == function.Aggregate {
		return true
	}
	for _, child := range expr.Children {
		if expressionHasAggregate(child) {
			return true
		}
	}
	return false
}

// scalarResultSink 把 child query 结果约束为一列、最多一行，并保存在 Query MemoryScope 的单值 Vector。
type scalarResultSink struct {
	scope   *memory.Scope
	buffers *memory.BufferManager
	typeID  types.LogicalType
	value   *vector.Vector
	rows    int
}

var _ pipeline.Sink = (*scalarResultSink)(nil)
var _ pipeline.CloseableSink = (*scalarResultSink)(nil)

func newScalarResultSink(scope *memory.Scope, buffers *memory.BufferManager, typeID types.LogicalType) (*scalarResultSink, error) {
	if scope == nil || buffers == nil || typeID.ID == types.Invalid {
		return nil, fmt.Errorf("session: scalar result sink requires managed scope, BufferManager and valid type")
	}
	return &scalarResultSink{scope: scope, buffers: buffers, typeID: cloneSourceType(typeID)}, nil
}

func (s *scalarResultSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	if chunk == nil || len(chunk.Columns) != 1 {
		return fmt.Errorf("session: scalar subquery result must contain exactly one column")
	}
	if s.rows+chunk.Cardinality > 1 {
		return fmt.Errorf("session: scalar subquery returned more than one row")
	}
	if chunk.Cardinality == 0 {
		return nil
	}
	value, err := vector.NewFlat(s.scope, s.buffers, s.typeID, 1)
	if err != nil {
		return err
	}
	if err := vector.CopyValue(chunk.Columns[0], 0, value, 0); err != nil {
		value.Release()
		return err
	}
	s.value = value
	s.rows = 1
	return nil
}

func (s *scalarResultSink) Finalize(context.Context) error {
	if s.rows != 0 {
		return nil
	}
	value, err := vector.NewFlat(s.scope, s.buffers, s.typeID, 1)
	if err != nil {
		return err
	}
	value.SetNull(0)
	s.value = value
	return nil
}

func (s *scalarResultSink) Close() error {
	if s.value != nil {
		s.value.Release()
		s.value = nil
	}
	return nil
}

func (s *scalarResultSink) take() (*vector.Vector, error) {
	if s == nil || s.value == nil {
		return nil, fmt.Errorf("session: scalar subquery did not finalize a value")
	}
	value := s.value
	s.value = nil
	return value, nil
}
