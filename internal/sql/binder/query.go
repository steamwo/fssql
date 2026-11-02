package binder

import (
	"context"
	"fmt"
	"strconv"
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
	Kind        BoundRelationKind        // Kind 是 relation 来源类别。
	ID          string                   // ID 是 relation identity。
	Alias       string                   // Alias 是列限定符。
	Columns     []RelationColumn         // Columns 是该 relation 输出 schema。
	Scope       Scope                    // Scope 保存表达式 Binder 使用的列序号/限定符。
	Children    []*BoundRelation         // Children 仅 Join 使用。
	Join        ast.JoinType             // Join 仅 RelationJoin 使用。
	Condition   *BoundExpression         // Condition 是已绑定 ON 条件。
	FunctionID  function.ID              // FunctionID 仅 table function 使用。
	Source      string                   // Source 仅当前 Stable reader table function 使用。
	Fingerprint stats.DatasetFingerprint // Fingerprint 绑定真实 source snapshot。
}

// BoundSelectItem 保存 SELECT projection 的强类型结果。
type BoundSelectItem struct {
	Expression *BoundExpression // Expression 是 bound projection。
	Output     RelationColumn   // Output 是 projection 输出列定义。
}

// BoundOrderTerm 保存 ORDER BY 的已绑定表达式。
type BoundOrderTerm struct {
	Expression  *BoundExpression // Expression 是排序表达式。
	Descending  bool             // Descending 表示 DESC。
	NullsFirst  *bool            // NullsFirst 为 nil 时保留 SQL 默认。
	UsesOutput  bool             // UsesOutput 表示 ORDER BY 绑定到 projection alias/ordinal。
	OutputIndex int              // OutputIndex 仅 UsesOutput=true 时有效。
}

// BoundQuery 是 Parser 与 Planner 之间的 Query Binder IR。
type BoundQuery struct {
	Source     *BoundRelation     // Source 是可选 FROM relation tree。
	Projection []BoundSelectItem  // Projection 已展开 SELECT *。
	Output     []RelationColumn   // Output 是 query-visible schema。
	Where      *BoundExpression   // Where 是布尔 predicate。
	GroupBy    []*BoundExpression // GroupBy 是强类型 grouping expressions。
	Having     *BoundExpression   // Having 是布尔 predicate。
	OrderBy    []BoundOrderTerm   // OrderBy 保存排序语义。
	Limit      *BoundExpression   // Limit 是可选整数表达式。
	Offset     *BoundExpression   // Offset 是可选整数表达式。
	Distinct   bool               // Distinct 对应 SELECT DISTINCT。
}

// QueryBinder 绑定 SELECT/CTE/relation/table-function schema，不创建 Logical/Physical operator。
type QueryBinder struct {
	Functions      *function.Catalog     // Functions 是统一 FunctionCatalog。
	Relations      RelationResolver      // Relations 解析 Catalog relation/view。
	TableFunctions TableFunctionResolver // TableFunctions 调用真实 TableFunction.Bind schema path。
	SourceSQL      string                // SourceSQL 仅用于无 alias projection 的稳定输出标签。
	ctes           []boundCTE            // ctes 仅在当前 statement Query Binder 内可见。
	blockedCTEs    []ast.Identifier      // blockedCTEs 阻止 self/forward CTE 被错误回退到 Catalog relation。
}

type boundCTE struct {
	Name   ast.Identifier   // Name 保留 quoted identifier 语义。
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
	local.blockedCTEs = append([]ast.Identifier(nil), b.blockedCTEs...)
	if stmt.With != nil {
		names := make([]ast.Identifier, len(stmt.With.CTEs))
		for i, cte := range stmt.With.CTEs {
			names[i] = cte.Name
			for j := 0; j < i; j++ {
				if sameIdentifier(names[j], cte.Name) {
					return nil, local.errorAt(cte.Span, "duplicate CTE "+cte.Name.Name)
				}
			}
		}
		for i, cte := range stmt.With.CTEs {
			child := local
			child.blockedCTEs = append(append([]ast.Identifier(nil), local.blockedCTEs...), names[i:]...)
			q, err := child.BindSelect(ctx, cte.Query)
			if err != nil {
				return nil, err
			}
			out := cloneRelationColumns(q.Output)
			if len(cte.Columns) > 0 {
				if len(cte.Columns) != len(out) {
					return nil, local.errorAt(cte.Span, fmt.Sprintf("CTE column count %d does not match query output %d", len(cte.Columns), len(out)))
				}
				for j := range out {
					out[j].Name = cte.Columns[j].Name
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
	exprs := local.newExpressionBinder(ctx, scope)
	out := &BoundQuery{Source: source, Distinct: stmt.Distinct}

	for i, item := range stmt.Projection {
		if item.Star {
			if source == nil || len(scope.Columns) == 0 {
				return nil, local.errorAt(item.Span, "SELECT * requires a FROM relation")
			}
			for j, col := range scope.Columns {
				schemaCol := source.Columns[j]
				e := &BoundExpression{Kind: BoundColumn, Span: item.Span, Type: cloneLogicalType(col.Type), Nullable: schemaCol.Nullable, ColumnIndex: col.Index}
				o := RelationColumn{Name: schemaCol.Name, Type: cloneLogicalType(schemaCol.Type), Nullable: schemaCol.Nullable}
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
		o := RelationColumn{Name: name, Type: cloneLogicalType(e.Type), Nullable: e.Nullable}
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
		if containsSubquery(e) {
			return nil, local.errorAt(g.NodeSpan(), "scalar subquery is not allowed in GROUP BY in Stable v1")
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
		bound, err := local.bindOrderTerm(exprs, out.Output, term)
		if err != nil {
			return nil, err
		}
		out.OrderBy = append(out.OrderBy, bound)
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
	if err := local.validateAggregateSemantics(out); err != nil {
		return nil, err
	}
	return out, nil
}

// newExpressionBinder 为当前 query scope 创建 expression Binder，并注入不继承 outer Scope 的 scalar-subquery Binder。
func (b *QueryBinder) newExpressionBinder(ctx context.Context, scope Scope) *Binder {
	exprs := New(b.Functions, scope)
	exprs.Subqueries = func(stmt *ast.SelectStmt) (*BoundQuery, error) {
		child := *b
		// QueryBinder 自身不保存 outer relation Scope；递归 BindSelect 只看到 subquery 自己的 FROM/CTE，因此 correlation 会解析失败。
		return child.BindSelect(ctx, stmt)
	}
	return exprs
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
		if b.isBlockedCTE(t.Name.Parts[0]) {
			return nil, b.errorAt(t.Span, "recursive or forward CTE reference is not Stable v1: "+t.Name.Parts[0].Name)
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
		exprs := b.newExpressionBinder(ctx, input)
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
		leftCount := len(left.Columns)
		for i := range cols {
			if (j.Type == ast.JoinLeft && i >= leftCount) || (j.Type == ast.JoinRight && i < leftCount) || j.Type == ast.JoinFull {
				cols[i].Nullable = true
			}
		}
		input = makeJoinOutputScope(left, right, cols)
	}
	return &BoundRelation{Kind: RelationJoin, ID: "join", Columns: cols, Scope: input, Children: []*BoundRelation{left, right}, Join: j.Type, Condition: condition}, nil
}

// makeJoinOutputScope 保留左右 qualifier，同时把 outer-join nullability 写入 expression Scope。
func makeJoinOutputScope(left, right *BoundRelation, cols []RelationColumn) Scope {
	out := Scope{Columns: make([]ColumnBinding, 0, len(cols))}
	for i, c := range left.Scope.Columns {
		schema := cols[i]
		c.Index = len(out.Columns)
		c.Type = cloneLogicalType(schema.Type)
		c.Nullable = schema.Nullable
		out.Columns = append(out.Columns, c)
	}
	for i, c := range right.Scope.Columns {
		idx := len(left.Columns) + i
		if idx >= len(cols) {
			break
		}
		schema := cols[idx]
		c.Index = len(out.Columns)
		c.Type = cloneLogicalType(schema.Type)
		c.Nullable = schema.Nullable
		out.Columns = append(out.Columns, c)
	}
	return out
}

// bindOrderTerm 优先按 projection ordinal/alias 绑定，再回退到 input relation expression。
func (b *QueryBinder) bindOrderTerm(exprs *Binder, output []RelationColumn, term ast.OrderTerm) (BoundOrderTerm, error) {
	base := BoundOrderTerm{Descending: term.Descending, NullsFirst: term.NullsFirst}
	if lit, ok := term.Expr.(*ast.LiteralExpr); ok && lit.Kind == ast.LiteralInteger {
		ordinal, err := strconv.Atoi(lit.Value)
		if err != nil || ordinal <= 0 || ordinal > len(output) {
			return BoundOrderTerm{}, b.errorAt(term.Span, "ORDER BY position is out of range")
		}
		idx := ordinal - 1
		col := output[idx]
		base.UsesOutput = true
		base.OutputIndex = idx
		base.Expression = &BoundExpression{Kind: BoundColumn, Span: term.Expr.NodeSpan(), Type: cloneLogicalType(col.Type), Nullable: col.Nullable, ColumnIndex: idx}
		return base, nil
	}
	if name, ok := term.Expr.(*ast.NameExpr); ok && len(name.Name.Parts) == 1 {
		matches := -1
		for i, col := range output {
			if matchesIdentifier(col.Name, name.Name.Parts[0]) {
				if matches >= 0 {
					return BoundOrderTerm{}, b.errorAt(term.Span, "ORDER BY output name is ambiguous: "+name.Name.Parts[0].Name)
				}
				matches = i
			}
		}
		if matches >= 0 {
			col := output[matches]
			base.UsesOutput = true
			base.OutputIndex = matches
			base.Expression = &BoundExpression{Kind: BoundColumn, Span: term.Expr.NodeSpan(), Type: cloneLogicalType(col.Type), Nullable: col.Nullable, ColumnIndex: matches}
			return base, nil
		}
	}
	e, err := exprs.BindExpression(term.Expr)
	if err != nil {
		return BoundOrderTerm{}, err
	}
	base.Expression = e
	return base, nil
}

// bindRowCountExpression 要求 LIMIT/OFFSET 是无列/无 aggregate 的整数表达式。
func (b *QueryBinder) bindRowCountExpression(exprs *Binder, raw ast.Expr, label string) (*BoundExpression, error) {
	e, err := exprs.BindExpression(raw)
	if err != nil {
		return nil, err
	}
	if containsColumn(e) || containsAggregate(e) || containsSubquery(e) {
		return nil, b.errorAt(raw.NodeSpan(), label+" must be a local constant integer expression")
	}
	switch e.Type.ID {
	case types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt:
		return e, nil
	default:
		return nil, b.errorAt(raw.NodeSpan(), label+" requires an integer expression")
	}
}

// validateAggregateSemantics 实施 GROUP/aggregate 的核心 SQL 合法性，避免把非法列引用留给 operator 猜测。
func (b *QueryBinder) validateAggregateSemantics(q *BoundQuery) error {
	hasAggregate := false
	for _, item := range q.Projection {
		if err := validateNoNestedAggregate(item.Expression, false); err != nil {
			return b.errorAt(item.Expression.Span, err.Error())
		}
		hasAggregate = hasAggregate || containsAggregate(item.Expression)
	}
	if q.Having != nil {
		if err := validateNoNestedAggregate(q.Having, false); err != nil {
			return b.errorAt(q.Having.Span, err.Error())
		}
		hasAggregate = hasAggregate || containsAggregate(q.Having)
	}
	for _, term := range q.OrderBy {
		if term.UsesOutput {
			continue
		}
		if err := validateNoNestedAggregate(term.Expression, false); err != nil {
			return b.errorAt(term.Expression.Span, err.Error())
		}
		hasAggregate = hasAggregate || containsAggregate(term.Expression)
	}
	if !hasAggregate && len(q.GroupBy) == 0 {
		return nil
	}
	for _, item := range q.Projection {
		if err := validateGroupedExpression(item.Expression, q.GroupBy); err != nil {
			return b.errorAt(item.Expression.Span, err.Error())
		}
	}
	if q.Having != nil {
		if err := validateGroupedExpression(q.Having, q.GroupBy); err != nil {
			return b.errorAt(q.Having.Span, err.Error())
		}
	}
	for _, term := range q.OrderBy {
		if term.UsesOutput {
			continue
		}
		if err := validateGroupedExpression(term.Expression, q.GroupBy); err != nil {
			return b.errorAt(term.Expression.Span, err.Error())
		}
	}
	return nil
}

func validateNoNestedAggregate(e *BoundExpression, insideAggregate bool) error {
	if e == nil {
		return nil
	}
	isAggregate := e.Kind == BoundFunction && e.FunctionKind == function.Aggregate
	if isAggregate && insideAggregate {
		return fmt.Errorf("nested aggregate is not allowed")
	}
	for _, child := range e.Children {
		if err := validateNoNestedAggregate(child, insideAggregate || isAggregate); err != nil {
			return err
		}
	}
	return nil
}

func validateGroupedExpression(e *BoundExpression, groups []*BoundExpression) error {
	if e == nil {
		return nil
	}
	for _, group := range groups {
		if boundExpressionEqual(e, group) {
			return nil
		}
	}
	if e.Kind == BoundSubquery {
		return nil
	}
	if e.Kind == BoundFunction && e.FunctionKind == function.Aggregate {
		return nil
	}
	if e.Kind == BoundColumn {
		return fmt.Errorf("column index %d must appear in GROUP BY or an aggregate", e.ColumnIndex)
	}
	for _, child := range e.Children {
		if err := validateGroupedExpression(child, groups); err != nil {
			return err
		}
	}
	return nil
}

func boundExpressionEqual(a, b *BoundExpression) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Kind != b.Kind || !sameType(a.Type, b.Type) || a.ColumnIndex != b.ColumnIndex || a.Op != b.Op || a.FunctionID != b.FunctionID || a.Literal != b.Literal || len(a.Children) != len(b.Children) {
		return false
	}
	for i := range a.Children {
		if !boundExpressionEqual(a.Children[i], b.Children[i]) {
			return false
		}
	}
	return true
}

func (b *QueryBinder) findCTE(name ast.Identifier) *boundCTE {
	for i := len(b.ctes) - 1; i >= 0; i-- {
		if sameIdentifier(b.ctes[i].Name, name) {
			return &b.ctes[i]
		}
	}
	return nil
}

func (b *QueryBinder) isBlockedCTE(name ast.Identifier) bool {
	for _, blocked := range b.blockedCTEs {
		if sameIdentifier(blocked, name) {
			return true
		}
	}
	return false
}

func sameIdentifier(a, b ast.Identifier) bool {
	return matchesIdentifier(a.Name, b) && matchesIdentifier(b.Name, a)
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
		s.Columns[i] = ColumnBinding{Table: alias, Name: c.Name, Type: cloneLogicalType(c.Type), Nullable: c.Nullable, Index: i}
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
	out := Scope{Columns: make([]ColumnBinding, len(in.Columns))}
	for i, c := range in.Columns {
		out.Columns[i] = c
		out.Columns[i].Type = cloneLogicalType(c.Type)
	}
	return out
}

func cloneRelationColumns(in []RelationColumn) []RelationColumn {
	out := make([]RelationColumn, len(in))
	for i, c := range in {
		out[i] = c
		out[i].Type = cloneLogicalType(c.Type)
	}
	return out
}

func cloneLogicalType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneLogicalType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]types.StructField, len(t.Fields))
		for i, field := range t.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneLogicalType(field.Type)}
		}
	}
	return out
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

func containsSubquery(e *BoundExpression) bool {
	if e == nil {
		return false
	}
	if e.Kind == BoundSubquery {
		return true
	}
	for _, child := range e.Children {
		if containsSubquery(child) {
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
