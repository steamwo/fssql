package lowering

import "github.com/steamwo/fssql/internal/planner"

// Internal sorted stages need the same ordering guarantee as a final sorted
// result, especially when a subquery LIMIT/OFFSET follows the sort.
func sortStageOrdered(keys []planner.SortKey) bool {
	if len(keys) == 0 {
		return false
	}
	if _, ok := planner.IsPhase5SampleSort(keys); ok {
		return false
	}
	if _, ok := planner.IsPhase5CTESort(keys); ok {
		return false
	}
	if _, ok := planner.IsPhase5GlobalWindowSort(keys); ok {
		return false
	}
	return true
}
