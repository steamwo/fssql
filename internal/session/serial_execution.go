package session

import (
	"strings"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// requiresSerialExecution identifies the bounded query operations whose
// result depends on stable source/chunk order. Stateful windows consume the
// external-sort stream in order, while REPEATABLE sampling must not let worker
// scheduling change the RNG event sequence.
func requiresSerialExecution(plan *planner.PhysicalPlan) bool {
	if plan == nil {
		return false
	}
	if plan.Kind == planner.PhysicalProject {
		for _, expr := range plan.Projection {
			if expr != nil && expr.Kind == binder.BoundFunction && (function.IsWindowID(expr.FunctionID) || expr.FunctionID == function.DistinctValueID) {
				return true
			}
		}
	}
	if plan.Kind == planner.PhysicalSort {
		if raw, ok := planner.IsSampleSort(plan.Sort); ok {
			parts := strings.Split(raw, ":")
			if len(parts) == 5 && parts[3] == "s" {
				return true
			}
		}
	}
	for _, child := range plan.Children {
		if requiresSerialExecution(child) {
			return true
		}
	}
	return false
}
