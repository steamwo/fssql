package binder

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

func (b *QueryBinder) bindDerivedTable(ctx context.Context, t *ast.DerivedTable) (*BoundRelation, error) {
	child := *b
	child.allowNullProjection = true
	child.lateralScope = Scope{}
	frame := len(b.lateralScope.Columns) > 0
	if frame {
		child.outerScopes = append([]Scope{cloneScope(b.lateralScope)}, b.outerScopes...)
	}
	q, err := child.BindSelect(ctx, t.Query)
	if err != nil {
		return nil, err
	}
	q, dependent, err := normalizeSiblingQuery(q, frame, t.Span)
	if err != nil {
		return nil, err
	}
	if len(t.Columns) > len(q.Output) {
		return nil, b.errorAt(t.Span, "derived table column alias count exceeds output count")
	}
	for i, column := range q.Output {
		if column.Type.ID == types.Invalid {
			if err := resolveSubqueryNull(q, i, types.LogicalType{ID: types.Integer}, New(b.Functions, Scope{})); err != nil {
				return nil, err
			}
		}
	}
	columns := cloneRelationColumns(q.Output)
	for i, name := range t.Columns {
		columns[i].Name = name.Name
	}
	q = cloneCTEQuery(q, columns)
	return &BoundRelation{Kind: RelationCTE, ID: "derived:" + t.Alias.Name, Alias: t.Alias.Name, Columns: columns, Scope: makeScope(t.Alias.Name, columns), CTEQuery: q, Lateral: dependent}, nil
}

func relationHasLateralInput(r *BoundRelation) bool {
	if r == nil {
		return false
	}
	if r.Lateral {
		return true
	}
	if (r.Kind == RelationSample || r.Kind == RelationProject) && len(r.Children) == 1 {
		return relationHasLateralInput(r.Children[0])
	}
	return false
}

func markDependentJoin(r *BoundRelation, dependent bool) {
	if !dependent || r == nil {
		return
	}
	if r.Kind == RelationJoin {
		r.Dependent = true
		return
	}
	if len(r.Children) == 1 {
		markDependentJoin(r.Children[0], dependent)
	}
}

// A joined table after a comma is one right input (JOIN binds more tightly).
// Its original scopes stay visible while the whole input can capture prefixes.
func (b *QueryBinder) bindJoinedInput(ctx context.Context, j *ast.JoinExpr) (*BoundRelation, error) {
	child := *b
	child.lateralScope = Scope{}
	frame := len(b.lateralScope.Columns) > 0
	if frame {
		child.outerScopes = append([]Scope{cloneScope(b.lateralScope)}, b.outerScopes...)
	}
	input, err := child.bindJoin(ctx, j)
	if err != nil {
		return nil, err
	}
	q := &BoundQuery{Source: input, Output: cloneRelationColumns(input.Columns)}
	for i, column := range input.Columns {
		q.Projection = append(q.Projection, BoundSelectItem{Expression: boundRelationColumn(input, i, 0), Output: column})
	}
	q, dependent, err := normalizeSiblingQuery(q, frame, j.Span)
	if err != nil {
		return nil, err
	}
	return &BoundRelation{Kind: RelationCTE, ID: "joined-input", Columns: cloneRelationColumns(input.Columns), Scope: cloneScope(input.Scope), CTEQuery: q, Lateral: dependent}, nil
}

func normalizeSiblingQuery(q *BoundQuery, frame bool, span ast.Span) (*BoundQuery, bool, error) {
	var err error
	dependent := false
	if frame {
		_, err = RewriteOuterReferences(q, func(e *BoundExpression, depth int) (*BoundExpression, error) {
			if e.Depth == depth+1 {
				dependent = true
			}
			return e, nil
		})
		if err != nil {
			return nil, false, err
		}
		// A sibling aggregate cannot be bound as a local aggregate of the
		// lateral query (the same restriction is enforced by DuckDB).
		if ExpressionHasLocalAggregate(&BoundExpression{Kind: BoundSubquery, Subquery: q}) {
			return nil, false, fmt.Errorf("binder %d:%d: LATERAL join cannot contain aggregates referring to the left input", span.Start.Line, span.Start.Column)
		}
		if !dependent {
			q, err = RewriteOuterReferences(q, func(e *BoundExpression, depth int) (*BoundExpression, error) {
				if e.Depth > depth+1 {
					out := *e
					out.Depth--
					return &out, nil
				}
				return e, nil
			})
			if err != nil {
				return nil, false, err
			}
			q, err = RewriteQueryExpressions(q, func(e *BoundExpression, depth int) (*BoundExpression, error) {
				if e.Kind == BoundFunction && e.FunctionKind == function.Aggregate && e.Depth > depth+1 {
					e.Depth--
				}
				return e, nil
			})
			if err != nil {
				return nil, false, err
			}
		}
	}
	return q, dependent, nil
}
