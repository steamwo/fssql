package session

import (
	"math"

	"github.com/steamwo/fssql/internal/planner"
)

// configureTopNSort specializes exactly one root Sort+Limit into the bounded
// Top-N implementation. The residual PhysicalLimit remains in the pipeline, so
// OFFSET/LIMIT semantics stay unchanged even though the blocking sort retains
// only the best offset+limit rows.
func configureTopNSort(factory *ExternalSortFactory, plan *planner.PhysicalPlan) {
	if factory == nil {
		return
	}
	rows, ok := topNRowsForPhysicalRoot(plan)
	if ok {
		factory.setTopNRows(rows)
	}
}

func topNRowsForPhysicalRoot(plan *planner.PhysicalPlan) (uint64, bool) {
	if plan == nil || plan.Kind != planner.PhysicalLimit || plan.Limit == planner.UnlimitedRows || plan.Limit == 0 || len(plan.Children) != 1 {
		return 0, false
	}
	if countPhysicalKind(plan, planner.PhysicalSort) != 1 {
		return 0, false
	}
	sortPlan := plan.Children[0]
	if sortPlan == nil || sortPlan.Kind != planner.PhysicalSort || len(sortPlan.Children) != 1 || len(sortPlan.Sort) == 0 {
		return 0, false
	}
	if plan.Offset > math.MaxUint64-plan.Limit {
		return 0, false
	}
	rows := plan.Offset + plan.Limit
	if rows == 0 {
		return 0, false
	}
	return rows, true
}

func countPhysicalKind(plan *planner.PhysicalPlan, kind planner.PhysicalKind) int {
	if plan == nil {
		return 0
	}
	count := 0
	if plan.Kind == kind {
		count++
	}
	for _, child := range plan.Children {
		count += countPhysicalKind(child, kind)
	}
	return count
}
