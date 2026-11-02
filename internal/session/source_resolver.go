package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxTableBindAdapters = 32

// TableBindFunc 是 Engine composition root 注入的真实 TableFunction.Bind adapter。
// callback 必须完成 FileSetPlanner/Schema Resolution，并返回 immutable fingerprint；不得仅靠文件扩展名伪造 schema。
type TableBindFunc func(context.Context, string) (binder.ResolvedRelation, error)

// TableBindAdapter 将稳定 FunctionID 与真实格式 Bind callback 关联。
type TableBindAdapter struct {
	FunctionID function.ID   // FunctionID 必须对应 FunctionCatalog 中的 Table function。
	Bind       TableBindFunc // Bind 是该函数唯一实际 schema-resolution callback。
}

// SourceResolver 同时实现 Binder 的 Catalog relation 与 TableFunction resolver contracts。
// 它只位于 statement/control path，不创建 runtime reader/operator。
type SourceResolver struct {
	catalog   catalog.Transaction            // catalog 是当前 Statement 独占的 Catalog Transaction。
	functions *function.Catalog              // functions 是统一 FunctionCatalog。
	adapters  map[function.ID]TableBindFunc // adapters 在构造后不可变。
}

// NewSourceResolver 冻结一个 statement-scoped source resolver。
func NewSourceResolver(tx catalog.Transaction, functions *function.Catalog, adapters []TableBindAdapter) (*SourceResolver, error) {
	if tx == nil {
		return nil, fmt.Errorf("session: source resolver requires Catalog Transaction")
	}
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	if len(adapters) > maxTableBindAdapters {
		return nil, fmt.Errorf("session: too many table bind adapters: %d > %d", len(adapters), maxTableBindAdapters)
	}
	bound := make(map[function.ID]TableBindFunc, len(adapters))
	for _, adapter := range adapters {
		if adapter.FunctionID == function.InvalidID || adapter.Bind == nil {
			return nil, fmt.Errorf("session: invalid table bind adapter")
		}
		if _, exists := bound[adapter.FunctionID]; exists {
			return nil, fmt.Errorf("session: duplicate table bind adapter for function id %d", adapter.FunctionID)
		}
		def, ok := functions.LookupID(adapter.FunctionID)
		if !ok || def.Kind != function.Table {
			return nil, fmt.Errorf("session: table bind adapter references unknown/non-table function id %d", adapter.FunctionID)
		}
		bound[adapter.FunctionID] = adapter.Bind
	}
	return &SourceResolver{catalog: tx, functions: functions, adapters: bound}, nil
}

// ResolveRelation 解析 Catalog view/external-table schema。
// External table 每个 Statement 都重新走真实 TableFunction.Bind，以获得当前 immutable source fingerprint，并验证 schema 未漂移。
func (r *SourceResolver) ResolveRelation(ctx context.Context, name ast.QualifiedName) (binder.ResolvedRelation, error) {
	if r == nil || r.catalog == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: source resolver is uninitialized")
	}
	object, err := catalogObjectName(name)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	entry, ok, err := r.catalog.Get(ctx, object)
	if err != nil {
		return binder.ResolvedRelation{}, err
	}
	if !ok {
		return binder.ResolvedRelation{}, fmt.Errorf("session: relation %s.%s not found", object.Schema, object.Name)
	}
	switch entry.Kind {
	case catalog.ObjectView:
		if entry.View == nil {
			return binder.ResolvedRelation{}, fmt.Errorf("session: invalid view metadata for %s.%s", object.Schema, object.Name)
		}
		return binder.ResolvedRelation{
			ID:      "view:" + object.Schema + "." + object.Name,
			Columns: binderColumns(entry.View.Columns),
		}, nil
	case catalog.ObjectExternalTable:
		if entry.ExternalTable == nil {
			return binder.ResolvedRelation{}, fmt.Errorf("session: invalid external-table metadata for %s.%s", object.Schema, object.Name)
		}
		def, ok := r.functions.Lookup(entry.ExternalTable.TableFunction)
		if !ok || def.Kind != function.Table {
			return binder.ResolvedRelation{}, fmt.Errorf("session: external table %s.%s references unknown table function %s", object.Schema, object.Name, entry.ExternalTable.TableFunction)
		}
		resolved, err := r.BindTableFunction(ctx, def, []binder.TableFunctionArgument{{Type: types.LogicalType{ID: types.Varchar}, Value: entry.ExternalTable.Source}})
		if err != nil {
			return binder.ResolvedRelation{}, err
		}
		stored := binderColumns(entry.ExternalTable.Columns)
		if !relationSchemaEqual(stored, resolved.Columns) {
			return binder.ResolvedRelation{}, fmt.Errorf("session: external table %s.%s source schema drifted from Catalog binding", object.Schema, object.Name)
		}
		resolved.Columns = stored
		return resolved, nil
	default:
		return binder.ResolvedRelation{}, fmt.Errorf("session: unsupported Catalog object kind %d", entry.Kind)
	}
}

// BindTableFunction 调用已注册真实 Bind adapter；arity/type 已由 FunctionCatalog/QueryBinder 预验证，这里仍做边界防御。
func (r *SourceResolver) BindTableFunction(ctx context.Context, def function.Definition, args []binder.TableFunctionArgument) (binder.ResolvedRelation, error) {
	if r == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: source resolver is nil")
	}
	if def.Kind != function.Table {
		return binder.ResolvedRelation{}, fmt.Errorf("session: function %s is not a table function", def.Name)
	}
	if len(args) != 1 || args[0].Type.ID != types.Varchar || strings.TrimSpace(args[0].Value) == "" {
		return binder.ResolvedRelation{}, fmt.Errorf("session: Stable table function %s requires one non-empty VARCHAR source", def.Name)
	}
	bind := r.adapters[def.ID]
	if bind == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: no real Bind adapter registered for table function %s", def.Name)
	}
	resolved, err := bind(ctx, args[0].Value)
	if err != nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: bind table function %s: %w", def.Name, err)
	}
	if resolved.ID == "" || len(resolved.Columns) == 0 {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s Bind returned no identity/schema", def.Name)
	}
	if def.ID != function.GlobID && resolved.Fingerprint == stats.DatasetFingerprint("") {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s Bind returned empty dataset fingerprint", def.Name)
	}
	return cloneResolvedRelation(resolved), nil
}

// catalogObjectName 将 Stable [schema.]object 绑定到 v1 Catalog；ATTACH/catalog namespace 不在 Stable v1。
func catalogObjectName(name ast.QualifiedName) (catalog.ObjectName, error) {
	if len(name.Parts) == 0 || len(name.Parts) > 2 {
		return catalog.ObjectName{}, fmt.Errorf("session: relation name must be [schema.]object")
	}
	if len(name.Parts) == 1 {
		return catalog.ObjectName{Schema: "main", Name: name.Parts[0].Name}, nil
	}
	return catalog.ObjectName{Schema: name.Parts[0].Name, Name: name.Parts[1].Name}, nil
}

// binderColumns 将 Catalog schema 深拷贝为 Binder relation schema。
func binderColumns(columns []catalog.ColumnDefinition) []binder.RelationColumn {
	out := make([]binder.RelationColumn, len(columns))
	for i, column := range columns {
		out[i] = binder.RelationColumn{Name: column.Name, Type: cloneSourceType(column.Type), Nullable: column.Nullable}
	}
	return out
}

// relationSchemaEqual 比较 ExternalTable 创建时 schema 与当前真实 Bind schema，禁止静默 schema drift。
func relationSchemaEqual(a, b []binder.RelationColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Nullable != b[i].Nullable || !sourceTypeEqual(a[i].Type, b[i].Type) {
			return false
		}
	}
	return true
}

// cloneResolvedRelation 复制复杂 schema metadata，避免 adapter 返回可变 slice 被 Binder 持有。
func cloneResolvedRelation(in binder.ResolvedRelation) binder.ResolvedRelation {
	out := in
	out.Columns = make([]binder.RelationColumn, len(in.Columns))
	for i, column := range in.Columns {
		out.Columns[i] = column
		out.Columns[i].Type = cloneSourceType(column.Type)
	}
	return out
}

// cloneSourceType 深拷贝 LIST/STRUCT LogicalType metadata。
func cloneSourceType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneSourceType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]types.StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneSourceType(field.Type)}
		}
	}
	return out
}

// sourceTypeEqual 递归比较 LogicalType identity，避免 interface/deep-reflect 把可变 metadata 当 ABI。
func sourceTypeEqual(a, b types.LogicalType) bool {
	if a.ID != b.ID || a.Decimal != b.Decimal {
		return false
	}
	if (a.Child == nil) != (b.Child == nil) {
		return false
	}
	if a.Child != nil && !sourceTypeEqual(*a.Child, *b.Child) {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i].Name != b.Fields[i].Name || !sourceTypeEqual(a.Fields[i].Type, b.Fields[i].Type) {
			return false
		}
	}
	return true
}
