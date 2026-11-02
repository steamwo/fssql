package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestQueryPlannerTableFunctionFilterProject 验证直接 Stable reader 可以形成 Scan -> Filter -> Project logical chain，并保留 frozen source identity。
func TestQueryPlannerTableFunctionFilterProject(t *testing.T) {
	columns := []binder.RelationColumn{
		{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false},
		{Name: "name", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
	}
	source := &binder.BoundRelation{
		Kind: binder.RelationTableFunction, ID: "tablefunc:read_csv:abc", Alias: "r",
		Columns: columns, FunctionID: function.ReadCSVID, Source: "s3://bucket/data.csv", Fingerprint: stats.DatasetFingerprint("abc"),
	}
	where := &binder.BoundExpression{
		Kind: binder.BoundBinary, Type: types.LogicalType{ID: types.Boolean}, Op: ">",
		Children: []*binder.BoundExpression{
			{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.BigInt}, ColumnIndex: 0},
			{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.BigInt}, Literal: "10"},
		},
	}
	projection := &binder.BoundExpression{Kind: binder.BoundColumn, Type: types.LogicalType{ID: types.Varchar}, Nullable: true, ColumnIndex: 1}
	query := &binder.BoundQuery{
		Source: source,
		Where:  where,
		Projection: []binder.BoundSelectItem{{Expression: projection, Output: columns[1]}},
		Output:     []binder.RelationColumn{columns[1]},
	}

	plan, err := NewQueryPlanner(nil, nil).Plan(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Kind != LogicalProject || len(plan.Children) != 1 || plan.Projection[0] != projection {
		t.Fatalf("project plan=%+v", plan)
	}
	filter := plan.Children[0]
	if filter.Kind != LogicalFilter || filter.Predicate != where || len(filter.Children) != 1 {
		t.Fatalf("filter plan=%+v", filter)
	}
	scan := filter.Children[0]
	if scan.Kind != LogicalScan || scan.Scan == nil {
		t.Fatalf("scan plan=%+v", scan)
	}
	if scan.Scan.Function != "read_csv" || scan.Scan.Source != source.Source || scan.Scan.Fingerprint != source.Fingerprint || scan.Scan.RelationID != source.ID {
		t.Fatalf("scan spec=%+v", scan.Scan)
	}
	if len(scan.Output) != 2 || scan.Output[0].Name != "id" || scan.Output[1].Name != "name" {
		t.Fatalf("scan output=%+v", scan.Output)
	}
}

// TestPlannerJoinTypeMapsAllStableKinds 防止依赖 ast/planner 两个 enum 的偶然数值相等。
func TestPlannerJoinTypeMapsAllStableKinds(t *testing.T) {
	cases := []struct {
		input ast.JoinType
		want  JoinType
	}{
		{ast.JoinInner, JoinInner},
		{ast.JoinLeft, JoinLeft},
		{ast.JoinRight, JoinRight},
		{ast.JoinFull, JoinFull},
		{ast.JoinCross, JoinCross},
		{ast.JoinSemi, JoinSemi},
		{ast.JoinAnti, JoinAnti},
	}
	for _, tc := range cases {
		got, err := plannerJoinType(tc.input)
		if err != nil {
			t.Fatalf("join=%d: %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("join=%d got=%d want=%d", tc.input, got, tc.want)
		}
	}
}

type countingRelationPlanResolver struct {
	calls int
	plan  *LogicalPlan
	err   error
}

func (r *countingRelationPlanResolver) PlanBoundRelation(context.Context, *binder.BoundRelation) (*LogicalPlan, error) {
	r.calls++
	return r.plan, r.err
}

// TestQueryPlannerPreflightFailsBeforeRelationResolver 验证尚未 lowering 的 SQL 能力不会先触发 Catalog/view provenance resolution。
func TestQueryPlannerPreflightFailsBeforeRelationResolver(t *testing.T) {
	column := binder.RelationColumn{Name: "id", Type: types.LogicalType{ID: types.BigInt}}
	catalogSource := &binder.BoundRelation{Kind: binder.RelationCatalog, ID: "view:main.v", Columns: []binder.RelationColumn{column}}
	baseProjection := []binder.BoundSelectItem{{Expression: &binder.BoundExpression{Kind: binder.BoundColumn, Type: column.Type, ColumnIndex: 0}, Output: column}}
	cases := []struct {
		name  string
		mutate func(*binder.BoundQuery)
	}{
		{"distinct", func(q *binder.BoundQuery) { q.Distinct = true }},
		{"order", func(q *binder.BoundQuery) { q.OrderBy = []binder.BoundOrderTerm{{Expression: baseProjection[0].Expression}} }},
		{"limit", func(q *binder.BoundQuery) { q.Limit = &binder.BoundExpression{Kind: binder.BoundLiteral, Type: types.LogicalType{ID: types.Integer}, Literal: "0"} }},
		{"aggregate", func(q *binder.BoundQuery) { q.Projection[0].Expression = &binder.BoundExpression{Kind: binder.BoundFunction, Type: types.LogicalType{ID: types.BigInt}, FunctionKind: function.Aggregate, FunctionID: function.CountID} }},
		{"subquery", func(q *binder.BoundQuery) { q.Where = &binder.BoundExpression{Kind: binder.BoundSubquery, Type: types.LogicalType{ID: types.Boolean}} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &countingRelationPlanResolver{}
			query := &binder.BoundQuery{Source: catalogSource, Projection: append([]binder.BoundSelectItem(nil), baseProjection...), Output: []binder.RelationColumn{column}}
			tc.mutate(query)
			if _, err := NewQueryPlanner(nil, resolver).Plan(context.Background(), query); err == nil {
				t.Fatal("expected unsupported foundation error")
			}
			if resolver.calls != 0 {
				t.Fatalf("resolver calls=%d want=0", resolver.calls)
			}
		})
	}
}

// TestQueryPlannerRelationResolverSchemaGate 验证 Catalog/CTE provenance resolver 不能返回与 Binder 可见 schema 不同的 executable plan。
func TestQueryPlannerRelationResolverSchemaGate(t *testing.T) {
	boundColumn := binder.RelationColumn{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}
	relation := &binder.BoundRelation{Kind: binder.RelationCatalog, ID: "external:main.t", Columns: []binder.RelationColumn{boundColumn}}
	projection := &binder.BoundExpression{Kind: binder.BoundColumn, Type: boundColumn.Type, ColumnIndex: 0}
	query := &binder.BoundQuery{Source: relation, Projection: []binder.BoundSelectItem{{Expression: projection, Output: boundColumn}}, Output: []binder.RelationColumn{boundColumn}}
	resolver := &countingRelationPlanResolver{plan: &LogicalPlan{
		Kind: LogicalScan,
		Output: []Column{{Name: "wrong", Type: boundColumn.Type}},
		Scan: &ScanSpec{RelationID: "x", Function: "read_csv", Source: "mem://x", Fingerprint: "fp"},
	}}
	_, err := NewQueryPlanner(nil, resolver).Plan(context.Background(), query)
	if err == nil || !strings.Contains(err.Error(), "schema does not match") {
		t.Fatalf("err=%v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls=%d want=1", resolver.calls)
	}
}

// TestQueryPlannerNestedTypeMetadataIsCopied 验证 Planner schema 不共享 Binder 的 LIST child pointer/slices。
func TestQueryPlannerNestedTypeMetadataIsCopied(t *testing.T) {
	listType := types.NewList(types.LogicalType{ID: types.Varchar})
	column := binder.RelationColumn{Name: "tags", Type: listType, Nullable: true}
	relation := &binder.BoundRelation{
		Kind: binder.RelationTableFunction, ID: "tablefunc:read_json:fp", Columns: []binder.RelationColumn{column},
		FunctionID: function.ReadJSONID, Source: "mem://data.json", Fingerprint: "fp",
	}
	projection := &binder.BoundExpression{Kind: binder.BoundColumn, Type: listType, Nullable: true, ColumnIndex: 0}
	query := &binder.BoundQuery{Source: relation, Projection: []binder.BoundSelectItem{{Expression: projection, Output: column}}, Output: []binder.RelationColumn{column}}
	plan, err := NewQueryPlanner(nil, nil).Plan(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	scanType := plan.Children[0].Output[0].Type
	if scanType.Child == nil || column.Type.Child == nil || scanType.Child == column.Type.Child {
		t.Fatal("planner LIST child metadata was not deep-copied")
	}
}
