package session

import (
	"testing"

	"github.com/steamwo/fssql/internal/planner"
)

func TestPhysicalOutputOrderFollowsOnlyFinalStreamingSpine(t *testing.T) {
	sorted := &planner.PhysicalPlan{Kind: planner.PhysicalSort, Sort: []planner.SortKey{{}}}
	for _, kind := range []planner.PhysicalKind{planner.PhysicalProject, planner.PhysicalFilter, planner.PhysicalLimit} {
		if !physicalOutputOrdered(&planner.PhysicalPlan{Kind: kind, Children: []*planner.PhysicalPlan{sorted}}) {
			t.Fatalf("kind %v lost final sort order", kind)
		}
	}
	for _, kind := range []planner.PhysicalKind{planner.PhysicalHashAggregate, planner.PhysicalHashJoin, planner.PhysicalDistinct, planner.PhysicalScan} {
		if physicalOutputOrdered(&planner.PhysicalPlan{Kind: kind, Children: []*planner.PhysicalPlan{sorted}}) {
			t.Fatalf("kind %v incorrectly propagated child order", kind)
		}
	}
	if physicalOutputOrdered(nil) || physicalOutputOrdered(&planner.PhysicalPlan{Kind: planner.PhysicalSort}) {
		t.Fatal("missing sort keys must not establish order")
	}
}
