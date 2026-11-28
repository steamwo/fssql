package optimizer

import (
	"context"
	"reflect"
	"slices"
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

func TestOptimizeJoinOrderPreservesNestedJoinKeys(t *testing.T) {
	eq := func(left, right int) *binder.BoundExpression {
		return joinTestComparison("=", joinTestRef(left), joinTestRef(right))
	}
	for _, tc := range []struct {
		name   string
		inputs int
		build  func([]*planner.LogicalPlan) *planner.LogicalPlan
		keys   []string
	}{
		{
			name: "right_nested_composite_keys", inputs: 3,
			build: func(scans []*planner.LogicalPlan) *planner.LogicalPlan {
				right := joinTestPlan(scans[1], scans[2], joinAnd(eq(1, 2), eq(3, 0)))
				return joinTestPlan(scans[0], right, eq(0, 2))
			},
			keys: []string{"a_key=b_key", "b_extra=c_key", "b_key=c_extra"},
		},
		{
			name: "bushy", inputs: 4,
			build: func(scans []*planner.LogicalPlan) *planner.LogicalPlan {
				left := joinTestPlan(scans[0], scans[1], eq(1, 2))
				right := joinTestPlan(scans[2], scans[3], eq(3, 0))
				return joinTestPlan(left, right, eq(3, 5))
			},
			keys: []string{"a_extra=b_key", "b_extra=c_extra", "c_key=d_extra"},
		},
		{
			name: "deeply_right_nested", inputs: 4,
			build: func(scans []*planner.LogicalPlan) *planner.LogicalPlan {
				right := joinTestPlan(scans[2], scans[3], eq(1, 2))
				right = joinTestPlan(scans[1], right, eq(0, 2))
				return joinTestPlan(scans[0], right, eq(1, 2))
			},
			keys: []string{"a_extra=b_key", "b_key=c_key", "c_extra=d_key"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statistics := fakeStats{}
			var scans []*planner.LogicalPlan
			var wantColumns []string
			for i, name := range []string{"a", "b", "c", "d"}[:tc.inputs] {
				scan, estimate := testScan(name, []uint64{1000, 100, 10, 1}[i])
				scan.Output = []planner.Column{joinTestColumn(name + "_key"), joinTestColumn(name + "_extra")}
				scans = append(scans, scan)
				statistics[estimate.Fingerprint] = estimate
				wantColumns = append(wantColumns, name+"_key", name+"_extra")
			}
			original := tc.build(scans)
			if err := original.Validate(); err != nil {
				t.Fatal(err)
			}
			rightPredicate := original.Children[1].Predicate
			wantPredicate := remapJoinExpr(rightPredicate, func(index int) int { return index })
			physical, err := (&Optimizer{Statistics: statistics}).Optimize(context.Background(), original)
			if err != nil {
				t.Fatal(err)
			}

			var gotKeys []string
			var columns func(*planner.PhysicalPlan) []string
			columns = func(plan *planner.PhysicalPlan) []string {
				switch plan.Kind {
				case planner.PhysicalScan:
					return []string{plan.Output[0].Name, plan.Output[1].Name}
				case planner.PhysicalProject:
					input := columns(plan.Children[0])
					out := make([]string, len(plan.Projection))
					for i, expr := range plan.Projection {
						out[i] = input[expr.ColumnIndex]
					}
					return out
				case planner.PhysicalHashJoin:
					left, right := columns(plan.Children[0]), columns(plan.Children[1])
					keys, err := planner.ExtractHashJoinKeys(plan.Predicate, len(left), len(right))
					if err != nil {
						t.Fatal(err)
					}
					for i, index := range keys.Left {
						a, b := left[index], right[keys.Right[i]]
						if a > b {
							a, b = b, a
						}
						gotKeys = append(gotKeys, a+"="+b)
					}
					return append(left, right...)
				default:
					t.Fatalf("unexpected physical kind %v", plan.Kind)
					return nil
				}
			}
			if got := columns(physical); !slices.Equal(got, wantColumns) {
				t.Fatalf("output columns=%v, want %v", got, wantColumns)
			}
			slices.Sort(gotKeys)
			if !slices.Equal(gotKeys, tc.keys) {
				t.Fatalf("join keys=%v, want %v", gotKeys, tc.keys)
			}
			if !reflect.DeepEqual(rightPredicate, wantPredicate) {
				t.Fatal("optimizer mutated the original nested join predicate")
			}
		})
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
