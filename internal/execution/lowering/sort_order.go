package lowering

import "github.com/steamwo/fssql/internal/planner"

// Internal sorted stages need the same ordering guarantee as a final sorted
// result, especially when a subquery LIMIT/OFFSET follows the sort.
func sortStageOrdered(keys []planner.SortKey) bool {
	if len(keys) == 0 {
		return false
	}
	if _, ok := planner.IsSampleSort(keys); ok {
		return false
	}
	if _, ok := planner.IsCTESort(keys); ok {
		return false
	}
	if _, ok := planner.IsGlobalWindowSort(keys); ok {
		return false
	}
	return true
}
