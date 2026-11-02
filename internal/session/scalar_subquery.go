package session

import (
	"context"
	"fmt"
	"strconv"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
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
// Stable v1 当前拒绝 nested subquery 以及 aggregate query 内 subquery，保持“limited”边界可验证。
func rewriteScalarSubqueries(ctx context.Context, queryPlanner *planner.QueryPlanner, opt *optimizer.Optimizer, query *binder.BoundQuery) (*binder.BoundQuery, []preparedScalarSubquery, error) {
	if query == nil || queryPlanner == nil || opt == nil {
		return nil, nil, fmt.Errorf("session: scalar subquery rewrite requires query, planner and optimizer")
	}
	if !queryContainsScalarSubquery(query) {
		return query, nil, nil
	}
	if len(query.GroupBy) != 0 || query.Having != nil || queryHasAggregateExpression(query) {
		return nil, nil, fmt.Errorf("session: scalar subquery inside aggregate/HAVING query is outside Stable v1 limited subquery support")
	}
	out := *query
	out.Projection = append([]binder.BoundSelectItem(nil), query.Projection...)
	out.OrderBy = append([]binder.BoundOrderTerm(nil), query.OrderBy...)
	prepared := make([]preparedScalarSubquery, 0, 2)

	rewrite := func(expr *binder.BoundExpression) (*binder.BoundExpression, error) {
		return nil, fmt.Errorf("session: internal scalar rewrite is uninitialized")
	}
	var rewriteExpr func(*binder.BoundExpression) (*binder.BoundExpression, error)
	rewriteExpr = func(expr *binder.BoundExpression) (*binder.BoundExpression, error) {
		if expr == nil {
			return nil, nil
		}
		if expr.Kind == binder.BoundSubquery {
			if expr.Subquery == nil || len(expr.Subquery.Output) != 1 {
				return nil, fmt.Errorf("session: scalar subquery must have one bound output")
			}
			if queryContainsScalarSubquery(expr.Subquery) {
				return nil, fmt.Errorf("session: nested scalar subquery is outside Stable v1 limited support")
			}
			logical, err := queryPlanner.PlanStable(ctx, expr.Subquery)
			if err != nil {
				return nil, fmt.Errorf("session: plan scalar subquery: %w", err)
			}
			physical, err := opt.Optimize(ctx, logical)
			if err != nil {
				return nil, fmt.Errorf("session: optimize scalar subquery: %w", err)
			}
			id := len(prepared)
			prepared = append(prepared, preparedScalarSubquery{id: id, physical: physical, typeID: cloneSourceType(expr.Type)})
			return &binder.BoundExpression{
				Kind:     binder.BoundRuntimeConstant,
				Span:     expr.Span,
				Type:     cloneSourceType(expr.Type),
				Nullable: true,
				Literal:  strconv.Itoa(id),
			}, nil
		}
		copyExpr := *expr
		copyExpr.Type = cloneSourceType(expr.Type)
		copyExpr.Children = make([]*binder.BoundExpression, len(expr.Children))
		for i, child := range expr.Children {
			rewritten, err := rewriteExpr(child)
			if err != nil {
				return nil, err
			}
			copyExpr.Children[i] = rewritten
		}
		return &copyExpr, nil
	}
	rewrite = rewriteExpr
	_ = rewrite // 保留具名闭包，便于后续扩展诊断而不引入包级状态。

	var err error
	for i := range out.Projection {
		out.Projection[i].Expression, err = rewriteExpr(query.Projection[i].Expression)
		if err != nil {
			return nil, nil, err
		}
	}
	out.Where, err = rewriteExpr(query.Where)
	if err != nil {
		return nil, nil, err
	}
	for i := range out.OrderBy {
		out.OrderBy[i].Expression, err = rewriteExpr(query.OrderBy[i].Expression)
		if err != nil {
			return nil, nil, err
		}
	}
	return &out, prepared, nil
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

func expressionHasAggregate(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundFunction && expr.FunctionKind == 1 { // function.Aggregate；避免 session helper 重新定义函数语义。
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
