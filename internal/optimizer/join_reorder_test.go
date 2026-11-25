package optimizer

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

func TestOptimizeJoinOrderUsesConnectedFilteredInput(t *testing.T) {
	fact, factStats := testScan("fact", 67000)
	carriers, carrierStats := testScan("carriers", 6000)
	airports, airportStats := testScan("airports", 8000)
	fact.Output = []planner.Column{joinTestColumn("carrier"), joinTestColumn("origin"), joinTestColumn("zone")}
	carriers.Output = []planner.Column{joinTestColumn("id")}
	airports.Output = []planner.Column{joinTestColumn("id"), joinTestColumn("zone")}
	first := joinTestPlan(carriers, fact, joinTestComparison("=", joinTestRef(0), joinTestRef(1)))
	airportsON := joinAnd(joinTestComparison("=", joinTestRef(2), joinTestRef(4)), joinTestComparison("=", joinTestRef(3), joinTestRef(5)))
	original := joinTestPlan(first, airports, airportsON)
	o := &Optimizer{Statistics: fakeStats{factStats.Fingerprint: factStats, carrierStats.Fingerprint: carrierStats, airportStats.Fingerprint: airportStats}}

	withoutFilter, err := o.Optimize(context.Background(), original)
	if err != nil {
		t.Fatal(err)
	}
	if firstBuild := withoutFilter.Children[0].Children[0].Children[1]; firstBuild.Scan == nil || firstBuild.Scan.RelationID != "carriers" {
		t.Fatal("expected the smaller connected carrier input first")
	}

	literal := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: joinTestRef(0).Type, Literal: "1"}
	where := &planner.LogicalPlan{Kind: planner.LogicalFilter, Output: original.Output,
		Children: []*planner.LogicalPlan{original}, Predicate: joinTestComparison("=", joinTestRef(5), literal)}
	physical, err := o.Optimize(context.Background(), where)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Kind != planner.PhysicalProject || physical.Projection[0].ColumnIndex != 5 {
		t.Fatal("expected projection restoring carrier as the first column")
	}
	join := physical.Children[0].Children[0]
	build := join.Children[1]
	if build.Kind != planner.PhysicalFilter || build.Children[0].Scan.RelationID != "airports" || build.Predicate.Children[0].ColumnIndex != 1 {
		t.Fatal("expected airport filter below the first join with a local column index")
	}
	keys, err := planner.ExtractHashJoinKeys(join.Predicate, len(fact.Output), len(airports.Output))
	if err != nil || len(keys.Left) != 2 || keys.Left[0] != 1 || keys.Left[1] != 2 || keys.Right[0] != 0 || keys.Right[1] != 1 {
		t.Fatalf("remapped hash keys = %+v, err = %v", keys, err)
	}
	if len(build.Children[0].Scan.Pushdown.Filters) != 1 || original.Children[0] != first || airportsON.Children[0].Children[0].ColumnIndex != 2 {
		t.Fatal("expected scan pushdown and an untouched input plan")
	}
}

func TestOptimizeJoinOrderPreservesDisconnectedAndOuterJoins(t *testing.T) {
	a, as := testScan("a", 1)
	b, bs := testScan("b", 100)
	c, cs := testScan("c", 10)
	for _, scan := range []*planner.LogicalPlan{a, b, c} {
		scan.Output = []planner.Column{joinTestColumn("id")}
	}
	o := &Optimizer{Statistics: fakeStats{as.Fingerprint: as, bs.Fingerprint: bs, cs.Fingerprint: cs}}
	eq := joinTestComparison("=", joinTestRef(0), joinTestRef(1))
	first := joinTestPlan(a, b, eq)
	root := joinTestPlan(first, c, nil)
	physical, err := o.Optimize(context.Background(), root)
	if err != nil || physical.Kind != planner.PhysicalCrossJoin || physical.Children[0].Children[0].Scan.RelationID != "a" {
		t.Fatalf("disconnected cross join changed: %v", err)
	}
	root = joinTestPlan(first, c, joinTestComparison("=", joinTestRef(1), joinTestRef(2)))
	first.Join = planner.JoinLeft
	physical, err = o.Optimize(context.Background(), root)
	if err != nil || physical.Children[0].Join != planner.JoinLeft || physical.Children[0].Children[0].Scan.RelationID != "a" {
		t.Fatalf("outer join changed: %v", err)
	}
}

func joinTestColumn(name string) planner.Column {
	return planner.Column{Name: name, Type: types.LogicalType{ID: types.BigInt}}
}

func joinTestRef(index int) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundColumn, Type: joinTestColumn("").Type, ColumnIndex: index}
}

func joinTestComparison(op string, left, right *binder.BoundExpression) *binder.BoundExpression {
	return &binder.BoundExpression{Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: op,
		Children: []*binder.BoundExpression{left, right}}
}

func joinTestPlan(left, right *planner.LogicalPlan, predicate *binder.BoundExpression) *planner.LogicalPlan {
	kind := planner.JoinInner
	if predicate == nil {
		kind = planner.JoinCross
	}
	return &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: kind, Children: []*planner.LogicalPlan{left, right},
		Output: append(append([]planner.Column(nil), left.Output...), right.Output...), Predicate: predicate}
}
