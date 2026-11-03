package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/catalog"
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
	FunctionID function.ID
	Bind       TableBindFunc
}

// SourceResolver 同时实现 Binder 的 Catalog relation 与 TableFunction resolver contracts。
// 它只位于 statement/control path，不创建 runtime reader/operator。
type SourceResolver struct {
	catalog   catalog.Transaction
	functions *function.Catalog
	adapters  map[function.ID]TableBindFunc
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
		return binder.ResolvedRelation{ID: "view:" + object.Schema + "." + object.Name, Columns: binderColumns(entry.View.Columns)}, nil
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

// BindTableFunction calls the registered real Bind adapter. Built-in file table
// functions keep the one-VARCHAR source contract; public registered functions
// are fixed-schema zero-argument functions and bind without opening row streams.
func (r *SourceResolver) BindTableFunction(ctx context.Context, def function.Definition, args []binder.TableFunctionArgument) (binder.ResolvedRelation, error) {
	if r == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: source resolver is nil")
	}
	if def.Kind != function.Table {
		return binder.ResolvedRelation{}, fmt.Errorf("session: function %s is not a table function", def.Name)
	}
	bind := r.adapters[def.ID]
	if bind == nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: no real Bind adapter registered for table function %s", def.Name)
	}
	var source string
	switch {
	case def.MinArgs == 0 && def.MaxArgs == 0 && len(args) == 0:
		// The legacy adapter signature carries a source string. Registered
		// zero-arg functions have none, so pass the immutable function name as
		// an internal bind token; their adapter never treats it as user data.
		source = def.Name
	case len(args) == 1 && args[0].Type.ID == types.Varchar && strings.TrimSpace(args[0].Value) != "":
		source = args[0].Value
	default:
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s received invalid bound arguments", def.Name)
	}
	resolved, err := bind(ctx, source)
	if err != nil {
		return binder.ResolvedRelation{}, fmt.Errorf("session: bind table function %s: %w", def.Name, err)
	}
	if resolved.ID == "" || len(resolved.Columns) == 0 {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s Bind returned no identity/schema", def.Name)
	}
	if resolved.Fingerprint == "" {
		return binder.ResolvedRelation{}, fmt.Errorf("session: table function %s Bind returned empty dataset fingerprint", def.Name)
	}
	return cloneResolvedRelation(resolved), nil
}

func catalogObjectName(name ast.QualifiedName) (catalog.ObjectName, error) {
	if len(name.Parts) == 0 || len(name.Parts) > 2 {
		return catalog.ObjectName{}, fmt.Errorf("session: relation name must be [schema.]object")
	}
	if len(name.Parts) == 1 {
		return catalog.ObjectName{Schema: "main", Name: name.Parts[0].Name}, nil
	}
	return catalog.ObjectName{Schema: name.Parts[0].Name, Name: name.Parts[1].Name}, nil
}

func binderColumns(columns []catalog.ColumnDefinition) []binder.RelationColumn {
	out := make([]binder.RelationColumn, len(columns))
	for i, column := range columns {
		out[i] = binder.RelationColumn{Name: column.Name, Type: cloneSourceType(column.Type), Nullable: column.Nullable}
	}
	return out
}

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

func cloneResolvedRelation(in binder.ResolvedRelation) binder.ResolvedRelation {
	out := in
	out.Columns = make([]binder.RelationColumn, len(in.Columns))
	for i, column := range in.Columns {
		out.Columns[i] = column
		out.Columns[i].Type = cloneSourceType(column.Type)
	}
	return out
}

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

func sourceTypeEqual(a, b types.LogicalType) bool { return a.Same(b) }
