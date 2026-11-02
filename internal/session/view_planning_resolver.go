package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// ViewPlanningResolver 在 PlanningSourceResolver 之上增加 CREATE VIEW 的递归 parse/bind/plan 能力。
// 它仍是 statement-scoped orchestration；Planner 自身不依赖 parser 或 Catalog。
type ViewPlanningResolver struct {
	base *PlanningSourceResolver // base 保存 Catalog relation executable provenance。

	mu     sync.Mutex          // mu 保护 activeViews；递归进入前只短暂持锁。
	active map[string]struct{} // active 记录当前递归 planning stack 中的 view relation ID。
}

var _ binder.RelationResolver = (*ViewPlanningResolver)(nil)
var _ binder.TableFunctionResolver = (*ViewPlanningResolver)(nil)
var _ planner.RelationPlanResolver = (*ViewPlanningResolver)(nil)

// NewViewPlanningResolver 创建 statement-scoped view planning wrapper。
func NewViewPlanningResolver(base *PlanningSourceResolver) (*ViewPlanningResolver, error) {
	if base == nil || base.base == nil || base.base.functions == nil {
		return nil, fmt.Errorf("session: view planning resolver requires initialized PlanningSourceResolver")
	}
	return &ViewPlanningResolver{base: base, active: make(map[string]struct{})}, nil
}

// ResolveRelation 直接委托 PlanningSourceResolver，继续记录 Catalog external/view provenance。
func (r *ViewPlanningResolver) ResolveRelation(ctx context.Context, name ast.QualifiedName) (binder.ResolvedRelation, error) {
	if r == nil || r.base == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: view planning resolver is uninitialized")
	}
	return r.base.ResolveRelation(ctx, name)
}

// BindTableFunction 直接委托同一个真实 TableFunction.Bind path。
func (r *ViewPlanningResolver) BindTableFunction(ctx context.Context, definition function.Definition, args []binder.TableFunctionArgument) (binder.ResolvedRelation, error) {
	if r == nil || r.base == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: view planning resolver is uninitialized")
	}
	return r.base.BindTableFunction(ctx, definition, args)
}

// PlanBoundRelation 对 external table 继续复用 base；对 view 重新 parse/bind/plan Catalog 保存的 query SQL。
func (r *ViewPlanningResolver) PlanBoundRelation(ctx context.Context, relation *binder.BoundRelation) (*planner.LogicalPlan, error) {
	if r == nil || r.base == nil || relation == nil {
		return nil, fmt.Errorf("session: view planning requires initialized resolver and relation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	provenance, ok := r.base.lookupProvenance(relation.ID)
	if !ok {
		return nil, fmt.Errorf("session: no executable provenance recorded for relation %s", relation.ID)
	}
	if provenance.kind != planningView {
		return r.base.PlanBoundRelation(ctx, relation)
	}
	if provenance.viewSQL == "" {
		return nil, fmt.Errorf("session: view relation %s has empty SQL provenance", relation.ID)
	}
	if err := r.enterView(relation.ID); err != nil {
		return nil, err
	}
	defer r.leaveView(relation.ID)

	statement, err := parser.Parse(provenance.viewSQL)
	if err != nil {
		return nil, fmt.Errorf("session: parse view %s: %w", relation.ID, err)
	}
	selectStatement, ok := statement.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("session: view %s SQL is not SELECT", relation.ID)
	}
	functions := r.base.base.functions
	bound, err := binder.NewQueryBinder(functions, r, r, provenance.viewSQL).BindSelect(ctx, selectStatement)
	if err != nil {
		return nil, fmt.Errorf("session: bind view %s: %w", relation.ID, err)
	}
	// View 与 direct SELECT 必须经过同一个 Stable output-modifier/aggregate/CTE planner；禁止保留旧 foundation 子集。
	logical, err := planner.NewQueryPlanner(functions, r).PlanStable(ctx, bound)
	if err != nil {
		return nil, fmt.Errorf("session: plan view %s: %w", relation.ID, err)
	}
	if !viewPlanningSchemaEqual(logical.Output, relation.Columns) {
		return nil, fmt.Errorf("session: view %s current executable schema differs from frozen Catalog schema", relation.ID)
	}
	return logical, nil
}

// enterView 检测当前 planning stack 中的递归 view；同一 resolver 可安全用于多层 non-recursive view。
func (r *ViewPlanningResolver) enterView(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.active[id]; exists {
		return fmt.Errorf("session: recursive view reference detected for %s", id)
	}
	if len(r.active) >= maxPlanningRelationProvenance {
		return fmt.Errorf("session: view planning depth exceeds %d", maxPlanningRelationProvenance)
	}
	r.active[id] = struct{}{}
	return nil
}

// leaveView 释放当前递归 stack ownership；错误路径也必须执行。
func (r *ViewPlanningResolver) leaveView(id string) {
	r.mu.Lock()
	delete(r.active, id)
	r.mu.Unlock()
}

// viewPlanningSchemaEqual 比较重新绑定后的 executable output 与 CREATE VIEW 时冻结 schema。
func viewPlanningSchemaEqual(planned []planner.Column, frozen []binder.RelationColumn) bool {
	if len(planned) != len(frozen) {
		return false
	}
	for i := range planned {
		if planned[i].Name != frozen[i].Name || planned[i].Nullable != frozen[i].Nullable || !sourceTypeEqual(planned[i].Type, frozen[i].Type) {
			return false
		}
	}
	return true
}