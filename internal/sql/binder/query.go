package binder

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

// RelationColumn 描述 Query Binder 已解析 relation 的一列。
type RelationColumn struct {
	Name     string            // Name 是 relation-visible 列名。
	Type     types.LogicalType // Type 是真实 Bind/Catalog schema 的 logical type。
	Nullable bool              // Nullable 表示运行时允许 NULL。
}

// ResolvedRelation 是外部 relation resolver 返回的不可变 schema 身份。
type ResolvedRelation struct {
	ID          string                   // ID 是 planner 使用的稳定 relation identity。
	Columns     []RelationColumn         // Columns 是实际 Catalog/Bind schema。
	Fingerprint stats.DatasetFingerprint // Fingerprint 在文件来源时绑定真实数据集快照。
}

// RelationResolver 解析 Catalog relation/view；SQL Binder 不依赖具体 Catalog 实现。
type RelationResolver interface {
	ResolveRelation(context.Context, ast.QualifiedName) (ResolvedRelation, error)
}

// TableFunctionArgument 是传给真实 TableFunction.Bind adapter 的已验证常量参数。
type TableFunctionArgument struct {
	Type  types.LogicalType // Type 是参数 logical type。
	Value string            // Value 是解码后的 literal 值。
}

// TableFunctionResolver 是 FunctionCatalog 与真实格式 Bind/SchemaResolver 之间的窄边界。
type TableFunctionResolver interface {
	BindTableFunction(context.Context, function.Definition, []TableFunctionArgument) (ResolvedRelation, error)
}

// BoundRelationKind 定义 Query Binder 输出的 relation tree 类别。
type BoundRelationKind uint8

const (
	RelationCatalog BoundRelationKind = iota
	RelationTableFunction
	RelationCTE
	RelationJoin
)

// BoundRelation 是 Planner 之前的 relation binding；不包含 runtime reader/operator。
type BoundRelation struct {
	Kind          BoundRelationKind       // Kind 是 relation 来源类别。
	ID            string                  // ID 是 relation identity。
	Alias         string                  // Alias 是列限定符。
	Columns       []RelationColumn        // Columns 是该 relation 输出 schema。
	Scope         Scope                   // Scope 保存表达式 Binder 使用的列序号/限定符。
	Children      []*BoundRelation        // Children 仅 Join 使用。
	Join          ast.JoinType            // Join 仅 RelationJoin 使用。
	Condition     *BoundExpression        // Condition 是已绑定 ON 条件。
	FunctionID    function.ID             // FunctionID 仅 table function 使用。
	Source        string                  // Source 仅当前 Stable reader table function 使用。
	Fingerprint   stats.DatasetFingerprint // Fingerprint 绑定真实 source snapshot。
}

// BoundSelectItem 保存 SELECT projection 的强类型结果。
type BoundSelectItem struct {
	Expression *BoundExpression // Expression 是 bound projection。
	Output     RelationColumn   // Output 是 projection 输出列定义。
}

// BoundOrderTerm 保存 ORDER BY 的已绑定表达式。
type BoundOrderTerm struct {
	Expression *BoundExpression // Expression 是排序表达式。
	Descending bool             // Descending 表示 DESC。
	NullsFirst *bool            // NullsFirst 为 nil 时保留 SQL 默认。
}

// BoundQuery 是 Parser 与 Planner 之间的 Query Binder IR。
type BoundQuery struct {
	Source     *BoundRelation    // Source 是可选 FROM relation tree。
	Projection []BoundSelectItem // Projection 已展开 SELECT *。
	Output     []RelationColumn  // Output 是 query-visible schema。
	Where      *BoundExpression  // Where 是布尔 predicate。
	GroupBy    []*BoundExpression // GroupBy 是强类型 grouping expressions。
	Having     *BoundExpression  // Having 是布尔 predicate。
	OrderBy    []BoundOrderTerm  // OrderBy 保存排序语义。
	Limit      *BoundExpression  // Limit 是可选整数表达式。
	Offset     *BoundExpression  // Offset 是可选整数表达式。
	Distinct   bool              // Distinct 对应 SELECT DISTINCT。
}

// QueryBinder 绑定 SELECT/CTE/relation/table-function schema，不创建 Logical/Physical operator。
type QueryBinder struct {
	Functions      *function.Catalog    // Functions 是统一 FunctionCatalog。
	Relations      RelationResolver     // Relations 解析 Catalog relation/view。
	TableFunctions TableFunctionResolver // TableFunctions 调用真实 TableFunction.Bind schema path。
	SourceSQL      string               // SourceSQL 仅用于无 alias projection 的稳定输出标签。
	ctes           []boundCTE           // ctes 仅在当前 statement Query Binder 内可见。
}

type boundCTE struct {
	Name   ast.Identifier // Name 保留 quoted identifier 语义。
	Output []RelationColumn // Output 是已绑定 CTE schema。
}

// NewQueryBinder 创建 query binder；nil FunctionCatalog 使用 v1 builtin catalog。
func NewQueryBinder(functions *function.Catalog, relations RelationResolver, tableFunctions TableFunctionResolver, sourceSQL string) *QueryBinder {
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	return &QueryBinder{Functions: functions, Relations: relations, TableFunctions: tableFunctions, SourceSQL: sourceSQL}
}

// BindSelect 绑定 non-recursive CTE、FROM、projection 和 Stable SELECT clauses。
func (b *QueryBinder) BindSelect(ctx context.Context, stmt *ast.SelectStmt) (*BoundQuery, error) {
	if b == nil || stmt == nil {
		return nil, fmt.Errorf("query binder: nil binder/select")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	local := *b
	local.ctes = append([]boundCTE(nil), b.ctes...)
	if stmt.With != nil {
		for _, cte := range stmt.With.CTEs {
			if local.findCTE(cte.Name) != nil {
				return nil, local.errorAt(cte.Span, "duplicate CTE "+cte.Name.Name)
			}
			// 当前 CTE 在自身绑定完成前不加入 scope，因此 recursive/self-reference 不会被解析为 CTE。
			q, err := local.BindSelect(ctx, cte.Query)
			if err != nil {
				return nil, err
			}
			out := cloneRelationColumns(q.Output)
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(out) {
					return nil, local.errorAt(cte.Span, fmt.Sprintf("CTE column count %d does not match query output %d", len(cte.Columns), len(out)))
				}
				for i := range out {
					out[i].Name = cte.Columns[i].Name
				}
			}
			local.ctes = append(local.ctes, boundCTE{Name: cte.Name, Output: out})
		}
	}

	var source *BoundRelation
	var scope Scope
	if stmt.From != nil {
		var err error
		source, err = local.bindTableExpr(ctx, stmt.From)
		if err != nil {
			return nil, err
		}
		scope = source.Scope
	}
	exprs := New(local.Functions, scope)
	out := &BoundQuery{Source: source, Distinct: stmt.Distinct}

	for i, item := range stmt.Projection {
		if item.Star {
			if len(scope.Columns) == 0 {
				return nil, local.errorAt(item.Span, "SELECT * requires a FROM relation")
			}
			for _, col := range scope.Columns {
				e := &BoundExpression{Kind: BoundColumn, Span: item.Span, Type: col.Type, Nullable: true, ColumnIndex: col.Index}
				o := RelationColumn{Name: col.Name, Type: col.Type, Nullable: true}
				out.Projection = append(out.Projection, BoundSelectItem{Expression: e, Output: o})
				out.Output = append(out.Output, o)
			}
			continue
		}
		e, err := exprs.BindExpression(item.Expr)
		if err != nil {
			return nil, err
		}
		if e.SQLNull {
			return nil, local.errorAt(item.Span, "bare NULL projection requires a resolved logical type")
		}
		name := local.projectionName(item, i)
		o := RelationColumn{Name: name, Type: e.Type, Nullable: e.Nullable}
		out.Projection = append(out.Projection, BoundSelectItem{Expression: e, Output: o})
		out.Output = append(out.Output, o)
	}

	if stmt.Where != nil {
		p, err := exprs.BindExpression(stmt.Where)
		if err != nil {
			return nil, err
		}
		if containsAggregate(p) {
			return nil, local.errorAt(stmt.Where.NodeSpan(), "aggregate is not allowed in WHERE")
		}
		p, err = exprs.coerce(p, types.LogicalType{ID: types.Boolean})
		if err != nil {
			return nil, local.errorAt(stmt.Where.NodeSpan(), "WHERE requires BOOLEAN")
		}
		out.Where = p
	}
	for _, g := range stmt.GroupBy {
		e, err := exprs.BindExpression(g)
		if err != nil {
			return nil, err
		}
		if containsAggregate(e) {
			return nil, local.errorAt(g.NodeSpan(), "aggregate is not allowed in GROUP BY")
		}
		out.GroupBy = append(out.GroupBy, e)
	}
	if stmt.Having != nil {
		e, err := exprs.BindExpression(stmt.Having)
		if err != nil {
			return nil, err
		}
		e, err = exprs.coerce(e, types.LogicalType{ID: types.Boolean})
		if err != nil {
			return nil, local.errorAt(stmt.Having.NodeSpan(), "HAVING requires BOOLEAN")
		}
		out.Having = e
	}
	for _, term := range stmt.OrderBy {
		e, err := exprs.BindExpression(term.Expr)
		if err != nil {
			return nil, err
		}
		out.OrderBy = append(out.OrderBy, BoundOrderTerm{Expression: e, Descending: term.Descending, NullsFirst: term.NullsFirst})
	}
	if stmt.Limit != nil {
		e, err := local.bindRowCountExpression(exprs, stmt.Limit, "LIMIT")
		if err != nil {
			return nil, err
		}
		out.Limit = e
	}
	if stmt.Offset != nil {
		e, err := local.bindRowCountExpression(exprs, stmt.Offset, "OFFSET")
		if err != nil {
			return nil, err
		}
		out.Offset = e
	}
	return out, nil
}

// bindTableExpr 将 FROM relation tree 绑定为 schema/scope；真实 I/O reader 仍未创建。
func (b *QueryBinder) bindTableExpr(ctx context.Context, table ast.TableExpr) (*BoundRelation, error) {
	switch t := table.(type) {
	case *ast.TableName:
		return b.bindTableName(ctx, t)
	case *ast.TableFunction:
		return b.bindTableFunction(ctx, t)
	case *ast.JoinExpr:
		return b.bindJoin(ctx, t)
	default:
		return nil, fmt.Errorf("query binder: unsupported table expression %T", table)
	}
}

// bindTableName 优先解析 statement-local CTE，否则调用 Catalog relation resolver。
func (b *QueryBinder) bindTableName(ctx context.Context, t *ast.TableName) (*BoundRelation, error) {
	alias := relationAlias(t.Name, t.Alias)
	if len(t.Name.Parts) == 1 {
		if cte := b.findCTE(t.Name.Parts[0]); cte != nil {
			cols := cloneRelationColumns(cte.Output)
			return &BoundRelation{Kind: RelationCTE, ID: "cte:" + cte.Name.Name, Alias: alias, Columns: cols, Scope: makeScope(alias, cols)}, nil
		}
	}
	if b.Relations == nil {
		return nil, b.errorAt(t.Span, "relation resolver is not configured")
	}
	r, err := b.Relations.ResolveRelation(ctx, t.Name)
	if err != nil {
		return nil, b.errorAt(t.Span, "resolve relation: "+err.Error())
	}
	if r.ID == "" || len(r.Columns) == 0 {
		return nil, b.errorAt(t.Span, "resolved relation has no identity/schema")
	}
	cols := cloneRelationColumns(r.Columns)
	return &BoundRelation{Kind: RelationCatalog, ID: r.ID, Alias: alias, Columns: cols, Scope: makeScope(alias, cols), Fingerprint: r.Fingerprint}, nil
}

// bindTableFunction 先由 FunctionCatalog 校验 table-kind/arity，再调用真实 schema Bind adapter。
func (b *QueryBinder) bindTableFunction(ctx context.Context, t *ast.TableFunction) (*BoundRelation, error) {
	if t.Call == nil || len(t.Call.Function.Parts) != 1 {
		return nil, b.errorAt(t.Span, "table function must use an unqualified Stable name")
	}
	name := t.Call.Function.Parts[0].Name
	def, ok := b.Functions.Lookup(name)
	if !ok || def.Kind != function.Table {
		return nil, b.errorAt(t.Span, "unknown table function "+name)
	}
	args := make([]TableFunctionArgument, 0, len(t.Call.Args))
	bindArgs := make([]function.ArgumentType, 0, len(t.Call.Args))
	for _, raw := range t.Call.Args {
		lit, ok := raw.(*ast.LiteralExpr)
		if !ok || lit.Kind != ast.LiteralString {
			return nil, b.errorAt(raw.NodeSpan(), "Stable table function arguments must be string literals")
		}
		typ := types.LogicalType{ID: types.Varchar}
		args = append(args, TableFunctionArgument{Type: typ, Value: lit.Value})
		bindArgs = append(bindArgs, function.ArgumentType{Type: typ})
	}
	if _, _, err := b.Functions.BindCall(name, function.Table, function.BindInput{Arguments: bindArgs}); err != nil {
		return nil, b.errorAt(t.Span, err.Error())
	}
	if b.TableFunctions == nil {
		return nil, b.errorAt(t.Span, "table-function resolver is not configured")
	}
	r, err := b.TableFunctions.BindTableFunction(ctx, def, args)
	if err != nil {
		return nil, b.errorAt(t.Span, "bind table function: "+err.Error())
	}
	if r.ID == "" || len(r.Columns) == 0 {
		return nil, b.errorAt(t.Span, "table-function Bind returned no identity/schema")
	}
	alias := name
	if t.Alias != nil {
		alias = t.Alias.Name
	}
	cols := cloneRelationColumns(r.Columns)
	source := ""
	if len(args) == 1 {
		source = args[0].Value
	}
	return &BoundRelation{Kind: RelationTableFunction, ID: r.ID, Alias: alias, Columns: cols, Scope: makeScope(alias, cols), FunctionID: def.ID, Source: source, Fingerprint: r.Fingerprint}, nil
}

// bindJoin 绑定左右 relation 与 ON predicate；USING 的列合并语义留给独立可验证子任务，当前显式拒绝而非错误降级。
func (b *QueryBinder) bindJoin(ctx context.Context, j *ast.JoinExpr) (*BoundRelation, error) {
	if len(j.Using) != 0 {
		return nil, b.errorAt(j.Span, "JOIN USING binding is not implemented; use ON in current remediation")
	}
	left, err := b.bindTableExpr(ctx, j.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.bindTableExpr(ctx, j.Right)
	if err != nil {
		return nil, err
	}
	input := combineScopes(left.Scope, right.Scope)
	var condition *BoundExpression
	if j.Type != ast.JoinCross {
		if j.Condition == nil {
			return nil, b.errorAt(j.Span, "non-CROSS JOIN requires ON predicate")
		}
		exprs := New(b.Functions, input)
		condition, err = exprs.BindExpression(j.Condition)
		if err != nil {
			return nil, err
		}
		if containsAggregate(condition) {
			return nil, b.errorAt(j.Condition.NodeSpan(), "aggregate is not allowed in JOIN condition")
		}
		condition, err = exprs.coerce(condition, types.LogicalType{ID: types.Boolean})
		if err != nil {
			return nil, b.errorAt(j.Condition.NodeSpan(), "JOIN condition requires BOOLEAN")
		}
	}
	cols := append(cloneRelationColumns(left.Columns), cloneRelationColumns(right.Columns)...)
	if j.Type == ast.JoinSemi || j.Type == ast.JoinAnti {
		cols = cloneRelationColumns(left.Columns)
		input = cloneScope(left.Scope)
	} else {
		applyOuterNullability(j.Type, input.Columns, len(left.Scope.Columns))
		for i := range cols {
			if i < len(input.Columns) {
				cols[i].Nullable = input.Columns[i].Nullable
			}
		}
	}
	return &BoundRelation{Kind: RelationJoin, ID: "join", Columns: cols, Scope: input, Children: []*BoundRelation{left, right}, Join: j.Type, Condition: condition}, nil
}

// bindRowCountExpression 要求 LIMIT/OFFSET 是无列/无 aggregate 的整数表达式。
func (b *QueryBinder) bindRowCountExpression(exprs *Binder, raw ast.Expr, label string) (*BoundExpression, error) {
	e, err := exprs.BindExpression(raw)
	if err != nil {
		return nil, err
	}
	if containsColumn(e) || containsAggregate(e) {
		return nil, b.errorAt(raw.NodeSpan(), label+" must not depend on relation columns or aggregates")
	}
	switch e.Type.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt:
		return e, nil
	default:
		return nil, b.errorAt(raw.NodeSpan(), label+" requires an integer expression")
	}
}

func (b *QueryBinder) findCTE(name ast.Identifier) *boundCTE {
	for i := len(b.ctes) - 1; i >= 0; i-- {
		if matchesIdentifier(b.ctes[i].Name.Name, name) {
			return &b.ctes[i]
		}
	}
	return nil
}

func relationAlias(name ast.QualifiedName, alias *ast.Identifier) string {
	if alias != nil {
		return alias.Name
	}
	if len(name.Parts) == 0 {
		return ""
	}
	return name.Parts[len(name.Parts)-1].Name
}

func makeScope(alias string, cols []RelationColumn) Scope {
	s := Scope{Columns: make([]ColumnBinding, len(cols))}
	for i, c := range cols {
		s.Columns[i] = ColumnBinding{Table: alias, Name: c.Name, Type: c.Type, Index: i}
	}
	return s
}

func combineScopes(left, right Scope) Scope {
	out := Scope{Columns: make([]ColumnBinding, 0, len(left.Columns)+len(right.Columns))}
	for _, c := range left.Columns {
		c.Index = len(out.Columns)
		out.Columns = append(out.Columns, c)
	}
	for _, c := range right.Columns {
		c.Index = len(out.Columns)
		out.Columns = append(out.Columns, c)
	}
	return out
}

func cloneScope(in Scope) Scope {
	return Scope{Columns: append([]ColumnBinding(nil), in.Columns...)}
}

func applyOuterNullability(join ast.JoinType, cols []ColumnBinding, leftCount int) {
	for i := range cols {
		if (join == ast.JoinLeft && i >= leftCount) || (join == ast.JoinRight && i < leftCount) || join == ast.JoinFull {
			// ColumnBinding 目前没有 nullable 字段；BoundExpression/输出 schema 在 join 层保守标记可空。
		}
	}
}

func cloneRelationColumns(in []RelationColumn) []RelationColumn {
	return append([]RelationColumn(nil), in...)
}

func containsAggregate(e *BoundExpression) bool {
	if e == nil {
		return false
	}
	if e.Kind == BoundFunction && e.FunctionKind == function.Aggregate {
		return true
	}
	for _, child := range e.Children {
		if containsAggregate(child) {
			return true
		}
	}
	return false
}

func containsColumn(e *BoundExpression) bool {
	if e == nil {
		return false
	}
	if e.Kind == BoundColumn {
		return true
	}
	for _, child := range e.Children {
		if containsColumn(child) {
			return true
		}
	}
	return false
}

func (b *QueryBinder) projectionName(item ast.SelectItem, ordinal int) string {
	if item.Alias != nil {
		return item.Alias.Name
	}
	switch e := item.Expr.(type) {
	case *ast.NameExpr:
		if len(e.Name.Parts) != 0 {
			return e.Name.Parts[len(e.Name.Parts)-1].Name
		}
	case *ast.CallExpr:
		if len(e.Function.Parts) != 0 {
			return e.Function.Parts[len(e.Function.Parts)-1].Name
		}
	}
	span := item.Expr.NodeSpan()
	if span.Start.Offset >= 0 && span.End.Offset > span.Start.Offset && span.End.Offset <= len(b.SourceSQL) {
		if label := strings.TrimSpace(b.SourceSQL[span.Start.Offset:span.End.Offset]); label != "" {
			return label
		}
	}
	return fmt.Sprintf("expr_%d", ordinal+1)
}

func (b *QueryBinder) errorAt(span ast.Span, message string) error {
	return fmt.Errorf("query binder %d:%d: %s", span.Start.Line, span.Start.Column, message)
}
