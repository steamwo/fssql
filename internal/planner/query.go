package planner

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// RelationPlanResolver 负责 Planner 自身无法从 BoundRelation 恢复的可执行 relation provenance。
// Catalog view/external-table 的具体 provenance 由上层 composition 保存；CTE provenance 直接由 Binder IR 保存。
type RelationPlanResolver interface {
	PlanBoundRelation(context.Context, *binder.BoundRelation) (*LogicalPlan, error)
}

// QueryPlanner 将 Binder 的强类型 BoundQuery 转为 generic LogicalPlan。
// 它不创建 runtime reader/operator，也不依赖具体格式、VFS 或 Catalog 实现。
type QueryPlanner struct {
	Functions *function.Catalog    // Functions 是统一 FunctionCatalog，用于 table-function FunctionID 校验。
	Relations RelationPlanResolver // Relations 处理 Catalog relation 等需要额外 executable provenance 的 relation。
}

// NewQueryPlanner 创建 QueryPlanner；nil FunctionCatalog 使用 Stable builtin catalog。
func NewQueryPlanner(functions *function.Catalog, relations RelationPlanResolver) *QueryPlanner {
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	return &QueryPlanner{Functions: functions, Relations: relations}
}

// Plan 构造当前已可执行子集的 LogicalPlan。
// Aggregate/HAVING、DISTINCT、ORDER BY、LIMIT/OFFSET 与 scalar subquery 在各自 lowering 单元完成前显式拒绝，禁止生成半可执行计划。
func (p *QueryPlanner) Plan(ctx context.Context, query *binder.BoundQuery) (*LogicalPlan, error) {
	if p == nil || p.Functions == nil {
		return nil, fmt.Errorf("query planner is uninitialized")
	}
	if query == nil {
		return nil, fmt.Errorf("query planner: nil BoundQuery")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateFoundationQuery(query); err != nil {
		return nil, err
	}
	if query.Source == nil {
		return nil, fmt.Errorf("query planner: SELECT without FROM requires one-row source lowering")
	}

	root, err := p.planRelation(ctx, query.Source)
	if err != nil {
		return nil, err
	}
	if query.Where != nil {
		var predicate *binder.BoundExpression
		root, predicate, err = p.lowerSubqueryExpression(ctx, root, query.Where)
		if err != nil {
			return nil, err
		}
		root = &LogicalPlan{
			Kind:      LogicalFilter,
			Children:  []*LogicalPlan{root},
			Output:    clonePlannerColumns(root.Output),
			Predicate: predicate,
		}
	}

	projection := make([]*binder.BoundExpression, len(query.Projection))
	for i, item := range query.Projection {
		if item.Expression == nil {
			return nil, fmt.Errorf("query planner: projection %d is nil", i)
		}
		root, projection[i], err = p.lowerSubqueryExpression(ctx, root, item.Expression)
		if err != nil {
			return nil, err
		}
	}
	root = &LogicalPlan{
		Kind:       LogicalProject,
		Children:   []*LogicalPlan{root},
		Output:     relationColumnsToPlanner(query.Output),
		Projection: projection,
	}
	if err := root.Validate(); err != nil {
		return nil, err
	}
	return root, nil
}

// validateFoundationQuery 在访问 relation resolver 前拒绝尚未有完整 execution lowering 的语义。
// 这样 Catalog/view resolver 不会在最终必然失败的计划上产生额外解析或 I/O 副作用。
func validateFoundationQuery(query *binder.BoundQuery) error {
	if len(query.GroupBy) != 0 || query.Having != nil || queryHasAggregate(query) {
		return fmt.Errorf("query planner: aggregate/HAVING lowering is not implemented")
	}
	if query.Distinct {
		return fmt.Errorf("query planner: DISTINCT lowering is not implemented")
	}
	if len(query.OrderBy) != 0 {
		return fmt.Errorf("query planner: ORDER BY lowering is not implemented")
	}
	if query.Limit != nil || query.Offset != nil {
		return fmt.Errorf("query planner: LIMIT/OFFSET lowering is not implemented")
	}
	if queryHasSubquery(query) {
		return fmt.Errorf("query planner: scalar subquery lowering is not implemented")
	}
	return nil
}

// planRelation 把 Binder relation tree 映射为 scan/join logical tree；不打开 runtime source。
func (p *QueryPlanner) planRelation(ctx context.Context, relation *binder.BoundRelation) (*LogicalPlan, error) {
	if relation == nil {
		return nil, fmt.Errorf("query planner: nil relation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch relation.Kind {
	case binder.RelationApply:
		return &LogicalPlan{Kind:LogicalApply,Children:[]*LogicalPlan{{Kind:LogicalOneRow}},Output:relationColumnsToPlanner(relation.Columns),Projection:relation.Projection,Scan:&ScanSpec{Function:relation.Source}},nil
	case binder.RelationProject:
		if len(relation.Children) != 1 {
			return nil, fmt.Errorf("query planner: projected relation requires one input")
		}
		child, err := p.planRelation(ctx, relation.Children[0])
		if err != nil {
			return nil, err
		}
		return &LogicalPlan{Kind: LogicalProject, Children: []*LogicalPlan{child}, Output: relationColumnsToPlanner(relation.Columns), Projection: relation.Projection}, nil
	case binder.RelationUnion:
		return p.planUnionRelation(ctx, relation)
	case binder.RelationSetOperation:
		if len(relation.Children)!=2 { return nil,fmt.Errorf("set operation requires two inputs") }
		left,err:=p.planRelation(ctx,relation.Children[0]);if err!=nil{return nil,err}
		right,err:=p.planRelation(ctx,relation.Children[1]);if err!=nil{return nil,err}
		return planSetOperation(relation.SetOperation,left,right)
	case binder.RelationSample:
		if len(relation.Children) != 1 {
			return nil, fmt.Errorf("query planner: sample requires one input")
		}
		input, err := p.planRelation(ctx, relation.Children[0])
		if err != nil {
			return nil, err
		}
		return &LogicalPlan{Kind: LogicalSort, Children: []*LogicalPlan{input}, Output: clonePlannerColumns(input.Output), Sort: []SortKey{{Expr: internalMarker(function.SampleMarkerID, relation.ID)}}}, nil
	case binder.RelationTableFunction:
		return p.planTableFunction(relation)
	case binder.RelationJoin:
		return p.planJoin(ctx, relation)
	case binder.RelationCTE:
		if relation.CTEQuery == nil {
			return nil, fmt.Errorf("query planner: CTE relation %s has no bound query provenance", relation.ID)
		}
		planned, err := p.PlanStable(ctx, relation.CTEQuery)
		if err != nil {
			return nil, fmt.Errorf("query planner: plan CTE %s: %w", relation.ID, err)
		}
		if planned == nil {
			return nil, fmt.Errorf("query planner: CTE %s returned nil plan", relation.ID)
		}
		if err := planned.Validate(); err != nil {
			return nil, fmt.Errorf("query planner: CTE %s returned invalid plan: %w", relation.ID, err)
		}
		expected := relationColumnsToPlanner(relation.Columns)
		if !plannerColumnsEqual(planned.Output, expected) {
			return nil, fmt.Errorf("query planner: CTE %s executable schema does not match Binder schema", relation.ID)
		}
		if relation.Materialize {
			planned = &LogicalPlan{Kind: LogicalSort, Children: []*LogicalPlan{planned}, Output: clonePlannerColumns(planned.Output), Sort: []SortKey{{Expr: internalMarker(function.CTEMarkerID, relation.ID)}}}
		}
		return planned, nil
	case binder.RelationCatalog:
		if p.Relations == nil {
			return nil, fmt.Errorf("query planner: relation %s requires executable provenance resolver", relation.ID)
		}
		planned, err := p.Relations.PlanBoundRelation(ctx, relation)
		if err != nil {
			return nil, err
		}
		if planned == nil {
			return nil, fmt.Errorf("query planner: relation resolver returned nil plan for %s", relation.ID)
		}
		if err := planned.Validate(); err != nil {
			return nil, fmt.Errorf("query planner: relation %s returned invalid plan: %w", relation.ID, err)
		}
		expected := relationColumnsToPlanner(relation.Columns)
		if !plannerColumnsEqual(planned.Output, expected) {
			return nil, fmt.Errorf("query planner: relation %s executable schema does not match Binder schema", relation.ID)
		}
		return planned, nil
	default:
		return nil, fmt.Errorf("query planner: unsupported relation kind %d", relation.Kind)
	}
}

// planTableFunction 由稳定 FunctionID 恢复 scan function name，并保留 Binder 冻结的 source fingerprint。
func (p *QueryPlanner) planTableFunction(relation *binder.BoundRelation) (*LogicalPlan, error) {
	definition, ok := p.Functions.LookupID(relation.FunctionID)
	if !ok || definition.Kind != function.Table {
		return nil, fmt.Errorf("query planner: relation %s references unknown/non-table function id %d", relation.ID, relation.FunctionID)
	}
	if relation.ID == "" || relation.Fingerprint == "" {
		return nil, fmt.Errorf("query planner: table function %s has incomplete source identity", definition.Name)
	}
	source := relation.Source
	if source == "" {
		// Fixed-schema zero-argument registered table functions have no user
		// source argument. Their stable relation identity is the runtime token.
		source = relation.ID
	}
	return &LogicalPlan{
		Kind:   LogicalScan,
		Output: relationColumnsToPlanner(relation.Columns),
		Scan: &ScanSpec{
			RelationID:  relation.ID,
			Function:    definition.Name,
			Source:      source,
			Fingerprint: relation.Fingerprint,
		},
	}, nil
}

// planJoin 递归保留 Binder 的 join 语义与已经按最终输出布局绑定的 predicate ColumnIndex。
func (p *QueryPlanner) planJoin(ctx context.Context, relation *binder.BoundRelation) (*LogicalPlan, error) {
	if len(relation.Children) != 2 {
		return nil, fmt.Errorf("query planner: join relation requires two children")
	}
	general := relation.Dependent || joinConditionHasSubquery(relation.Condition)
	if !general && relation.Condition != nil {
		_, err := ExtractHashJoinKeys(relation.Condition, len(relation.Children[0].Columns), len(relation.Children[1].Columns))
		general = err != nil
	}
	if general {
		return p.planDomainJoin(ctx, relation, &LogicalPlan{Kind: LogicalOneRow}, nil)
	}
	left, err := p.planRelation(ctx, relation.Children[0])
	if err != nil {
		return nil, err
	}
	right, err := p.planRelation(ctx, relation.Children[1])
	if err != nil {
		return nil, err
	}
	join, err := plannerJoinType(relation.Join)
	if err != nil {
		return nil, err
	}
	if join != JoinCross && relation.Condition == nil {
		return nil, fmt.Errorf("query planner: non-cross join requires bound condition")
	}
	return &LogicalPlan{
		Kind:      LogicalJoin,
		Children:  []*LogicalPlan{left, right},
		Output:    relationColumnsToPlanner(relation.Columns),
		Predicate: relation.Condition,
		Join:      join,
	}, nil
}

func joinConditionHasSubquery(e *binder.BoundExpression) bool {
	if e == nil {
		return false
	}
	if e.Kind == binder.BoundSubquery || e.Kind == binder.BoundRuntimeConstant {
		return true
	}
	for _, c := range e.Children {
		if joinConditionHasSubquery(c) {
			return true
		}
	}
	return false
}

// plannerJoinType 显式映射 Own AST Stable join enum，禁止依赖两个 package 的枚举数值偶然一致。
func plannerJoinType(join ast.JoinType) (JoinType, error) {
	switch join {
	case ast.JoinInner:
		return JoinInner, nil
	case ast.JoinLeft:
		return JoinLeft, nil
	case ast.JoinRight:
		return JoinRight, nil
	case ast.JoinFull:
		return JoinFull, nil
	case ast.JoinCross:
		return JoinCross, nil
	case ast.JoinSemi:
		return JoinSemi, nil
	case ast.JoinAnti:
		return JoinAnti, nil
	default:
		return 0, fmt.Errorf("query planner: unknown join type %d", join)
	}
}

// relationColumnsToPlanner 深拷贝 Binder schema，避免 Planner IR 与 Binder metadata 共享 LIST/STRUCT slice/pointer。
func relationColumnsToPlanner(columns []binder.RelationColumn) []Column {
	out := make([]Column, len(columns))
	for i, column := range columns {
		out[i] = Column{Name: column.Name, Type: cloneQueryPlannerType(column.Type), Nullable: column.Nullable}
	}
	return out
}

// clonePlannerColumns 深拷贝 planner column metadata。
func clonePlannerColumns(columns []Column) []Column {
	out := make([]Column, len(columns))
	for i, column := range columns {
		out[i] = column
		out[i].Type = cloneQueryPlannerType(column.Type)
	}
	return out
}

// cloneQueryPlannerType 递归复制 LIST/STRUCT LogicalType metadata。
func cloneQueryPlannerType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneQueryPlannerType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneQueryPlannerType(field.Type)}
		}
	}
	return out
}

// plannerColumnsEqual 验证 provenance resolver 的 executable plan schema 与 Binder 已暴露 schema 完全一致。
func plannerColumnsEqual(left, right []Column) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].Nullable != right[i].Nullable || !queryPlannerTypeEqual(left[i].Type, right[i].Type) {
			return false
		}
	}
	return true
}

// queryPlannerTypeEqual 比较完整 LogicalType identity，包括 Phase 2 的无符号、BLOB 和时间语义。
func queryPlannerTypeEqual(left, right types.LogicalType) bool {
	return left.Same(right)
}

// queryHasAggregate 检查 projection/WHERE 中仍需要 aggregate physical lowering 的函数节点。
func queryHasAggregate(query *binder.BoundQuery) bool {
	for _, item := range query.Projection {
		if containsBoundAggregate(item.Expression) {
			return true
		}
	}
	if containsBoundAggregate(query.Where) || containsBoundAggregate(query.Having) {
		return true
	}
	for _, term := range query.OrderBy {
		if containsBoundAggregate(term.Expression) {
			return true
		}
	}
	return false
}

// containsBoundAggregate 递归识别 FunctionCatalog aggregate 调用。
func containsBoundAggregate(expression *binder.BoundExpression) bool {
	return binder.ExpressionHasLocalAggregate(expression)
}

// queryHasSubquery 检查当前 foundation 尚未 lowering 的 scalar subquery。
func queryHasSubquery(query *binder.BoundQuery) bool {
	for _, item := range query.Projection {
		if containsUnplannedSubquery(item.Expression) {
			return true
		}
	}
	if containsUnplannedSubquery(query.Where) || containsUnplannedSubquery(query.Having) {
		return true
	}
	for _, term := range query.OrderBy {
		if containsUnplannedSubquery(term.Expression) {
			return true
		}
	}
	return false
}

// containsBoundSubquery 递归识别 BoundSubquery；子查询必须由专门 plan/lifecycle lowering 处理。
func containsBoundSubquery(expression *binder.BoundExpression) bool {
	if expression == nil {
		return false
	}
	if expression.Kind == binder.BoundSubquery {
		return true
	}
	for _, child := range expression.Children {
		if containsBoundSubquery(child) {
			return true
		}
	}
	return false
}

func containsUnplannedSubquery(expr *binder.BoundExpression) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == binder.BoundSubquery {
		return !plannedSubquery(expr)
	}
	for _, child := range expr.Children {
		if containsUnplannedSubquery(child) {
			return true
		}
	}
	return false
}
