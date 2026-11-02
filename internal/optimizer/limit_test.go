package optimizer

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/planner"
)

// TestOptimizeLimitZeroKeepsExactZero 验证 LIMIT 0 不会被旧的“0 表示未设置”语义吞掉。
func TestOptimizeLimitZeroKeepsExactZero(t *testing.T) {
	scan, stats := testScan("limit-zero", 100)
	logical := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{scan}, Limit: 0}
	physical, err := (&Optimizer{Statistics: fakeStats{scan.Scan.Fingerprint: stats}}).Optimize(context.Background(), logical)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Kind != planner.PhysicalLimit || physical.Limit != 0 || physical.Estimate.Rows != 0 || physical.Estimate.Confidence != 1 {
		t.Fatalf("physical=%+v", physical)
	}
	if physical.Children[0].Scan.Pushdown.Limit != 0 {
		t.Fatalf("LIMIT 0 must remain residual; pushdown=%d", physical.Children[0].Scan.Pushdown.Limit)
	}
}

// TestOptimizeOffsetOnlyDoesNotTreatUnlimitedSentinelAsRows 验证 OFFSET-only 的 UnlimitedRows 不会污染 cardinality 或 scan pushdown。
func TestOptimizeOffsetOnlyDoesNotTreatUnlimitedSentinelAsRows(t *testing.T) {
	scan, stats := testScan("offset", 100)
	logical := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{scan}, Limit: planner.UnlimitedRows, Offset: 7}
	physical, err := (&Optimizer{Statistics: fakeStats{scan.Scan.Fingerprint: stats}}).Optimize(context.Background(), logical)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Estimate.Rows != 93 {
		t.Fatalf("estimate=%+v", physical.Estimate)
	}
	if physical.Children[0].Scan.Pushdown.Limit != 0 {
		t.Fatalf("offset-only sentinel leaked into scan pushdown: %d", physical.Children[0].Scan.Pushdown.Limit)
	}
}

// TestOptimizeOffsetOnlyUnknownCardinalityStaysUnknown 验证没有可信统计时 OFFSET-only 不伪造 MaxUint64 cardinality。
func TestOptimizeOffsetOnlyUnknownCardinalityStaysUnknown(t *testing.T) {
	scan, _ := testScan("unknown-offset", 0)
	logical := &planner.LogicalPlan{Kind: planner.LogicalLimit, Children: []*planner.LogicalPlan{scan}, Limit: planner.UnlimitedRows, Offset: 7}
	physical, err := (&Optimizer{}).Optimize(context.Background(), logical)
	if err != nil {
		t.Fatal(err)
	}
	if physical.Estimate.Rows != 0 || physical.Estimate.Confidence != 0 {
		t.Fatalf("estimate=%+v", physical.Estimate)
	}
}
