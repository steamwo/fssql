package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxPlanningRelationProvenance = 256

type planningRelationKind uint8

const (
	planningExternalTable planningRelationKind = iota + 1
	planningView
)

// planningRelationProvenance 保存当前 Statement 解析 Catalog relation 时丢失于通用 Binder ABI 的可执行来源。
// 它只存在于 statement-scoped composition，不进入 Catalog 持久化、public API 或 Vector runtime。
type planningRelationProvenance struct {
	kind     planningRelationKind // kind 区分 external table 与 view。
	function string               // function 仅 external table 使用，为 FunctionCatalog Stable 名称。
	source   string               // source 仅 external table 使用，为 Catalog 保存的 canonical source spec。
	viewSQL  string               // viewSQL 仅 view 使用，为 Catalog 保存的 query SQL。
}

// PlanningSourceResolver 同时桥接 Binder relation resolution 与 Planner executable provenance。
// SourceResolver 仍是 schema/Bind 唯一路径；本类型只记录同一次解析对应的执行来源。
type PlanningSourceResolver struct {
	base *SourceResolver // base 复用现有 Catalog + real TableFunction.Bind schema path。

	mu         sync.Mutex                            // mu 保护 statement-scoped provenance map。
	provenance map[string]planningRelationProvenance // provenance 以返回给 Binder 的唯一 relation ID 为键。
}

var _ binder.RelationResolver = (*PlanningSourceResolver)(nil)
var _ binder.TableFunctionResolver = (*PlanningSourceResolver)(nil)
var _ planner.RelationPlanResolver = (*PlanningSourceResolver)(nil)

// NewPlanningSourceResolver 创建 bounded statement-scoped provenance wrapper。
func NewPlanningSourceResolver(base *SourceResolver) (*PlanningSourceResolver, error) {
	if base == nil || base.catalog == nil || base.functions == nil {
		return nil, fmt.Errorf("session: planning source resolver requires initialized SourceResolver")
	}
	return &PlanningSourceResolver{base: base, provenance: make(map[string]planningRelationProvenance)}, nil
}

// ResolveRelation 复用 SourceResolver 的真实 schema/fingerprint，并记录对应 Catalog executable provenance。
func (r *PlanningSourceResolver) ResolveRelation(ctx context.Context, name ast.QualifiedName) (binder.ResolvedRelation, error) {
	if r == nil || r.base == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: planning source resolver is uninitialized")
	}
	object, err := catalogObjectName(name)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	entry, ok, err := r.base.catalog.Get(ctx, object)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	if !ok {
		return binder.ResolvedRelation{}, fmt.Errorf("session: relation %s.%s not found", object.Schema, object.Name)
	}
	resolved, err := r.base.ResolveRelation(ctx, name)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}

	var provenance planningRelationProvenance
	switch entry.Kind {
	case catalog.ObjectExternalTable:
		if entry.ExternalTable == nil {
			return binder.ResolvedRelation{}, fmt.Errorf("session: invalid external-table metadata for %s.%s", object.Schema, object.Name)
		}
		provenance = planningRelationProvenance{
			kind:     planningExternalTable,
			function: entry.ExternalTable.TableFunction,
			source:   entry.ExternalTable.Source,
		}
		// Catalog object identity 是 query relation identity 的一部分，避免两个对象指向同一 snapshot 时 sidecar 冲突。
		resolved.ID = fmt.Sprintf("external:%s.%s:%s", object.Schema, object.Name, resolved.ID)
	case catalog.ObjectView:
		if entry.View == nil {
			return binder.ResolvedRelation{}, fmt.Errorf("session: invalid view metadata for %s.%s", object.Schema, object.Name)
		}
		provenance = planningRelationProvenance{kind: planningView, viewSQL: entry.View.SQL}
	default:
		return binder.ResolvedRelation{}, fmt.Errorf("session: unsupported Catalog object kind %d", entry.Kind)
	}
	if err := r.recordProvenance(resolved.ID, provenance); err != nil {
		return binder.ResolvedRelation{}, err
	}
	return resolved, nil
}

// BindTableFunction 直接委托 base，确保 direct table function 与 Catalog external table 共享同一真实 Bind path。
func (r *PlanningSourceResolver) BindTableFunction(ctx context.Context, definition function.Definition, args []binder.TableFunctionArgument) (binder.ResolvedRelation, error) {
	if r == nil || r.base == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: planning source resolver is uninitialized")
	}
	return r.base.BindTableFunction(ctx, definition, args)
}

// PlanBoundRelation 把已记录 external-table provenance 恢复为 generic LogicalScan。
// View provenance 已保存，但在递归 parse/bind/plan 单元完成前显式拒绝，禁止把 view 当作物理 table scan。
func (r *PlanningSourceResolver) PlanBoundRelation(ctx context.Context, relation *binder.BoundRelation) (*planner.LogicalPlan, error) {
	if r == nil || r.base == nil || relation == nil {
		return nil, fmt.Errorf("session: planning relation requires initialized resolver and relation")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if relation.Kind != binder.RelationCatalog {
		return nil, fmt.Errorf("session: relation kind %d is not Catalog provenance", relation.Kind)
	}
	provenance, ok := r.lookupProvenance(relation.ID)
	if !ok {
		return nil, fmt.Errorf("session: no executable provenance recorded for relation %s", relation.ID)
	}
	switch provenance.kind {
	case planningExternalTable:
		definition, found := r.base.functions.Lookup(provenance.function)
		if !found || definition.Kind != function.Table {
			return nil, fmt.Errorf("session: external relation %s references unknown/non-table function %s", relation.ID, provenance.function)
		}
		if provenance.source == "" || relation.Fingerprint == "" {
			return nil, fmt.Errorf("session: external relation %s has incomplete source identity", relation.ID)
		}
		return &planner.LogicalPlan{
			Kind:   planner.LogicalScan,
			Output: planningColumns(relation.Columns),
			Scan: &planner.ScanSpec{
				RelationID:  relation.ID,
				Function:    definition.Name,
				Source:      provenance.source,
				Fingerprint: relation.Fingerprint,
			},
		}, nil
	case planningView:
		return nil, fmt.Errorf("session: view relation %s requires recursive view planning", relation.ID)
	default:
		return nil, fmt.Errorf("session: unknown planning provenance kind %d", provenance.kind)
	}
}

// recordProvenance 以固定上限保存当前 Statement 的 relation provenance。
func (r *PlanningSourceResolver) recordProvenance(id string, provenance planningRelationProvenance) error {
	if id == "" {
		return fmt.Errorf("session: planning provenance requires relation identity")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.provenance[id]; ok {
		if existing != provenance {
			return fmt.Errorf("session: relation identity collision for %s", id)
		}
		return nil
	}
	if len(r.provenance) >= maxPlanningRelationProvenance {
		return fmt.Errorf("session: planning relation provenance limit exceeded: %d", maxPlanningRelationProvenance)
	}
	r.provenance[id] = provenance
	return nil
}

// lookupProvenance 返回 immutable-by-value provenance snapshot。
func (r *PlanningSourceResolver) lookupProvenance(id string) (planningRelationProvenance, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.provenance[id]
	return value, ok
}

// planningColumns 将 Binder-visible schema 深拷贝为 Planner schema。
func planningColumns(columns []binder.RelationColumn) []planner.Column {
	out := make([]planner.Column, len(columns))
	for i, column := range columns {
		out[i] = planner.Column{Name: column.Name, Type: cloneSourceType(column.Type), Nullable: column.Nullable}
	}
	return out
}
