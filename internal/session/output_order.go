package session

import "github.com/steamwo/fssql/internal/planner"

// physicalOutputOrdered proves ordering only along the final streaming spine.
// A sort inside a join or aggregate does not order that operator's output.
func physicalOutputOrdered(plan *planner.PhysicalPlan) bool {
	if plan == nil {
		return false
	}
	switch plan.Kind {
	case planner.PhysicalSort:
		if _, ok := planner.IsSampleSort(plan.Sort); ok {
			return false
		}
		if _, ok := planner.IsCTESort(plan.Sort); ok {
			return false
		}
		if _, ok := planner.IsGlobalWindowSort(plan.Sort); ok {
			return false
		}
		return len(plan.Sort) > 0
	case planner.PhysicalProject, planner.PhysicalFilter, planner.PhysicalLimit:
		return len(plan.Children) == 1 && physicalOutputOrdered(plan.Children[0])
	default:
		return false
	}
}
