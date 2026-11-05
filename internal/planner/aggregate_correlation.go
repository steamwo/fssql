package planner

import (
	"fmt"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Move aggregate arguments to their owning scope, also rebasing references
// captured by argument subqueries. Calls are cached because subquery identity
// forms part of aggregate provenance in the collector.
func (c *aggregateCollector) localizeOwnedAggregate(e *binder.BoundExpression) (*binder.BoundExpression, error) {
	if local := c.outerAggregates[e]; local != nil {
		return local, nil
	}
	steps := e.Depth
	query, err := binder.RewriteQueryExpressions(&binder.BoundQuery{Projection: []binder.BoundSelectItem{{Expression: e}}}, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		out := *ref
		if depth == 0 && ref.Kind == binder.BoundColumn {
			return nil, fmt.Errorf("outer aggregate contains a local scope")
		}
		if ref.Kind == binder.BoundOuterColumn {
			if ref.Depth < depth {
				return ref, nil
			}
			if ref.Depth < depth+steps {
				return nil, fmt.Errorf("outer aggregate argument subquery references a removed local scope")
			}
			out.Depth -= steps
			if out.Depth == 0 {
				out.Kind = binder.BoundColumn
			}
			return &out, nil
		}
		if ref.Kind == binder.BoundFunction && ref.FunctionKind == function.Aggregate {
			if ref.Depth < depth {
				return ref, nil
			}
			if ref.Depth < depth+steps {
				return nil, fmt.Errorf("outer aggregate argument contains a removed aggregate scope")
			}
			return ref, nil
		}
		return ref, nil
	})
	if err != nil {
		return nil, err
	}
	// This pass owns the first pass's copy; updating function depth before
	// recursion preserves rebasing inside argument subqueries too.
	query, err = binder.RewriteQueryExpressions(query, func(ref *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		if ref.Kind == binder.BoundFunction && ref.FunctionKind == function.Aggregate && ref.Depth >= depth+steps {
			ref.Depth -= steps
		}
		return ref, nil
	})
	if err != nil {
		return nil, err
	}
	local := query.Projection[0].Expression
	if c.outerAggregates == nil {
		c.outerAggregates = make(map[*binder.BoundExpression]*binder.BoundExpression)
	}
	c.outerAggregates[e] = local
	return local, nil
}

func (c *aggregateCollector) collectCorrelatedAggregates(query *binder.BoundQuery) error {
	_, err := binder.RewriteQueryExpressions(query, func(e *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		if e.Kind == binder.BoundFunction && e.FunctionKind == function.Aggregate && e.Depth == depth+1 {
			local, err := c.localizeOwnedAggregate(e)
			if err != nil {
				return nil, err
			}
			if err := c.collect(local); err != nil {
				return nil, err
			}
		}
		return e, nil
	})
	return err
}

func (c *aggregateCollector) rewriteCorrelatedAggregateQuery(query *binder.BoundQuery) (*binder.BoundQuery, error) {
	return binder.RewriteQueryExpressions(query, func(e *binder.BoundExpression, depth int) (*binder.BoundExpression, error) {
		index := -1
		if e.Kind == binder.BoundFunction && e.FunctionKind == function.Aggregate && e.Depth == depth+1 {
			local, err := c.localizeOwnedAggregate(e)
			if err != nil {
				return nil, err
			}
			i := c.aggregateIndex(local)
			if i < 0 {
				return nil, fmt.Errorf("outer aggregate output provenance missing")
			}
			index = len(c.groups) + i
		} else if e.Kind == binder.BoundOuterColumn && e.Depth == depth+1 {
			local := *e
			local.Kind, local.Depth = binder.BoundColumn, 0
			for i, group := range c.groups {
				if aggregateExpressionEqual(group, &local) {
					index = i
					break
				}
			}
		}
		if index >= 0 {
			return &binder.BoundExpression{Kind: binder.BoundOuterColumn, Depth: depth + 1, Type: e.Type, Nullable: e.Nullable, ColumnIndex: index}, nil
		}
		if e.Kind == binder.BoundOuterColumn && e.Depth == depth+1 {
			return nil, fmt.Errorf("query planner: ungrouped correlated column %d remained after aggregate binding", e.ColumnIndex)
		}
		return e, nil
	})
}
