package optimizer

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

type fakeStats map[stats.DatasetFingerprint]stats.RelationStatistics

func (f fakeStats) Lookup(_ context.Context, fp stats.DatasetFingerprint) (stats.RelationStatistics, bool, error) {
	s, ok := f[fp]
	return s, ok, nil
}

func testScan(id string, rows uint64) (*planner.LogicalPlan, stats.RelationStatistics) {
	fp := stats.DatasetFingerprint("fp-" + id)
	p := &planner.LogicalPlan{Kind: planner.LogicalScan, Scan: &planner.ScanSpec{RelationID: id, Function: "read_parquet", Fingerprint: fp}}
	return p, stats.RelationStatistics{Source: stats.StorageNative, Confidence: 1, Fingerprint: fp, Rows: rows}
}

func TestOptimizeMarksFilterCandidateButKeepsResidual(t *testing.T) {
	s, st := testScan("a", 1000)
	pred := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}
	root := &planner.LogicalPlan{Kind: planner.LogicalFilter, Children: []*planner.LogicalPlan{s}, Predicate: pred}
	o := &Optimizer{Statistics: fakeStats{s.Scan.Fingerprint: st}}
	physical, err := o.Optimize(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Kind != planner.PhysicalFilter || physical.Children[0].Kind != planner.PhysicalScan {
		t.Fatalf("physical=%+v", physical)
	}
	if len(physical.Children[0].Scan.Pushdown.Filters) != 1 {
		t.Fatal("filter candidate missing")
	}
	if s.Scan.Pushdown.Filters != nil {
		t.Fatal("optimizer mutated input logical plan")
	}
	if physical.Estimate.Rows != 100 || physical.Estimate.Confidence >= 1 {
		t.Fatalf("estimate=%+v", physical.Estimate)
	}
}

func TestProjectionAndLimitPushdownCandidatesRemainResidual(t *testing.T) {
	s, st := testScan("a", 100)
	col := &binder.BoundExpression{Kind: binder.BoundColumn, ColumnIndex: 2, Type: types.LogicalType{ID: types.BigInt}}
	project := &planner.LogicalPlan{Kind: planner.LogicalProject, Children: []*planner.LogicalPlan{s}, Projection: []*binder.BoundExpression{col}}
	o := &Optimizer{Statistics: fakeStats{s.Scan.Fingerprint: st}}
	p, err := o.Optimize(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Children[0].Scan.Pushdown.Projection) != 1 || p.Children[0].Scan.Pushdown.Projection[0] != 2 {
		t.Fatal("projection candidate missing")
	}

	limit := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{s}, Limit: 5}
	p, err = o.Optimize(context.Background(), limit)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != planner.PhysicalLimit || p.Children[0].Scan.Pushdown.Limit != 5 {
		t.Fatalf("limit=%+v", p)
	}
}

func TestJoinOrderCandidateUsesStatsWithoutRewritingBoundTree(t *testing.T) {
	a, as := testScan("a", 1000)
	b, bs := testScan("b", 10)
	c, cs := testScan("c", 100)
	pred := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}
	ab := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinInner, Predicate: pred, Children: []*planner.LogicalPlan{a, b}}
	root := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinInner, Predicate: pred, Children: []*planner.LogicalPlan{ab, c}}
	o := &Optimizer{Statistics: fakeStats{a.Scan.Fingerprint: as, b.Scan.Fingerprint: bs, c.Scan.Fingerprint: cs}}
	order, err := o.JoinOrder(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"b", "c", "a"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v", order)
		}
	}
	if root.Children[0] != ab || ab.Children[0] != a {
		t.Fatal("join order helper rewrote bound tree")
	}
}

func TestOuterJoinIsNotReorderable(t *testing.T) {
	a, _ := testScan("a", 1)
	b, _ := testScan("b", 1)
	pred := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}
	root := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinLeft, Predicate: pred, Children: []*planner.LogicalPlan{a, b}}
	if _, err := (&Optimizer{}).JoinOrder(context.Background(), root); err == nil {
		t.Fatal("outer join must not reorder")
	}
}

func TestPhysicalLoweringUsesHashAndCrossJoinKinds(t *testing.T) {
	a, as := testScan("a", 10)
	b, bs := testScan("b", 20)
	pred := &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Boolean}}
	o := &Optimizer{Statistics: fakeStats{a.Scan.Fingerprint: as, b.Scan.Fingerprint: bs}}
	inner := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinInner, Predicate: pred, Children: []*planner.LogicalPlan{a, b}}
	p, err := o.Optimize(context.Background(), inner)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != planner.PhysicalHashJoin || p.Estimate.Rows != 20 {
		t.Fatalf("join=%+v", p)
	}
	cross := &planner.LogicalPlan{Kind: planner.LogicalJoin, Join: planner.JoinCross, Children: []*planner.LogicalPlan{a, b}}
	p, err = o.Optimize(context.Background(), cross)
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != planner.PhysicalCrossJoin || p.Estimate.Rows != 200 {
		t.Fatalf("cross=%+v", p)
	}
}

func TestMismatchedFingerprintDoesNotBecomeTrustedEstimate(t *testing.T) {
	s, _ := testScan("a", 10)
	o := &Optimizer{Statistics: fakeStats{s.Scan.Fingerprint: {Source: stats.StorageNative, Confidence: 1, Fingerprint: "other", Rows: 999}}}
	p, err := o.Optimize(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if p.Estimate.Rows != 0 || p.Estimate.Confidence != 0 {
		t.Fatalf("estimate=%+v", p.Estimate)
	}
}
