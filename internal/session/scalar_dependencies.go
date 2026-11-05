package session

import (
	"fmt"
	"strconv"

	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// Preparation is bottom-up, but EXISTS can subsequently prune a dependency.
// Materialize only constants reachable from the final physical query tree.
func (p *PreparedSQLQuery) requiredScalarConstants() ([]bool, error) {
	required := make([]bool, len(p.subqueries))
	var plan func(*planner.PhysicalPlan) error
	var expression func(*binder.BoundExpression) error
	expression = func(e *binder.BoundExpression) error {
		if e == nil {
			return nil
		}
		if e.Kind == binder.BoundRuntimeConstant {
			id, err := strconv.Atoi(e.Literal)
			if err != nil || id < 0 || id >= len(required) {
				return fmt.Errorf("session: invalid scalar constant dependency %q", e.Literal)
			}
			if !required[id] {
				required[id] = true
				if err := plan(p.subqueries[id].physical); err != nil {
					return err
				}
			}
		}
		for _, child := range e.Children {
			if err := expression(child); err != nil {
				return err
			}
		}
		return nil
	}
	plan = func(node *planner.PhysicalPlan) error {
		if node == nil {
			return fmt.Errorf("session: nil scalar dependency plan")
		}
		if err := expression(node.Predicate); err != nil {
			return err
		}
		for _, e := range node.Projection {
			if err := expression(e); err != nil {
				return err
			}
		}
		for _, e := range node.GroupBy {
			if err := expression(e); err != nil {
				return err
			}
		}
		for _, spec := range node.Aggregates {
			for _, e := range spec.Arguments {
				if err := expression(e); err != nil {
					return err
				}
			}
		}
		for _, key := range node.Sort {
			if err := expression(key.Expr); err != nil {
				return err
			}
		}
		if node.Scan != nil {
			for _, e := range node.Scan.Pushdown.Filters {
				if err := expression(e); err != nil {
					return err
				}
			}
		}
		for _, child := range node.Children {
			if err := plan(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := plan(p.physical); err != nil {
		return nil, err
	}
	return required, nil
}
