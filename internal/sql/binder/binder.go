package binder

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

// ExpressionKind 定义 Binder 输出的逻辑表达式类别。
type ExpressionKind uint8

const (
	BoundColumn ExpressionKind = iota
	BoundLiteral
	BoundNull
	BoundUnary
	BoundBinary
	BoundFunction
	BoundIsNull
	BoundCast
	BoundSubquery
	// Runtime constants reserve kind 9 in runtime_constant.go.
	BoundOuterColumn ExpressionKind = BoundRuntimeConstant + 1
)

// ColumnBinding 是输入 relation 中可解析列的控制面描述。
type ColumnBinding struct {
	TableColumnIndex int // TableColumnIndex 保留限定关系中的原始列顺序
	Table    string            // Table 是表名或 alias；空字符串表示无 qualifier。
	Name     string            // Name 是规范化列名。
	Type     types.LogicalType // Type 是列的逻辑类型。
	Nullable bool              // Nullable 来自实际 relation schema；Binder 不再无条件假定列可空。
	Index    int               // Index 是 relation schema 中的列序号。
}

// Scope 保存当前 expression binder 可见列；后续 query binder 负责构造 Scope。
type Scope struct {
	Columns []ColumnBinding // Visible columns retain their physical source indexes.
	Aliases []ColumnBinding // Qualified bindings for merged USING keys.
}

// BoundExpression 是 parser AST 与 planner 之间的强类型逻辑表达式 IR。
type BoundExpression struct {
	TimeZoneName string
	Comparison string
	Distinct     bool
	Kind         ExpressionKind     // Kind 是 bound node 类别。
	Span         ast.Span           // Span 保留源 SQL 位置用于诊断。
	Type         types.LogicalType  // Type 是已解析结果类型；SQLNull=true 时暂为 Invalid。
	SQLNull      bool               // SQLNull 只允许存在于 Binder 内部未消解 NULL literal。
	Nullable     bool               // Nullable 表示运行时结果可能为 NULL。
	ColumnIndex  int                // ColumnIndex 仅对 BoundColumn 有效。
	Op           string             // Op 保存规范化一元/二元运算符。
	FunctionID   function.ID        // FunctionID 仅对 BoundFunction 有效。
	FunctionKind function.Kind      // FunctionKind 区分 scalar/aggregate。
	Literal      string             // Literal 保存已词法解码但未物化的常量文本。
	Children     []*BoundExpression // Children 保存已绑定子表达式。
	Subquery     *BoundQuery        // Subquery 仅对 BoundSubquery 有效；Planner 必须显式 lowering。
	Depth        int                // Outer-column reference depth or aggregate owning-scope depth; zero is local.
}

// SubqueryBinder 是 expression Binder 与 query Binder 的窄桥；调用方负责保证不继承 outer relation Scope。
type SubqueryBinder func(*ast.SelectStmt) (*BoundQuery, error)

// Binder 将 Own AST 绑定为强类型逻辑表达式，不创建 physical operator。
type Binder struct {
	Functions   *function.Catalog // Functions 是 generic FunctionCatalog。
	Scope       Scope             // Scope 是当前 relation 可见列。
	Subqueries  SubqueryBinder    // Subqueries 仅由 QueryBinder 注入，用于 Stable non-correlated scalar subquery。
	OuterScopes []Scope
}

// New 创建 expression Binder。
func New(functions *function.Catalog, scope Scope) *Binder {
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	return &Binder{Functions: functions, Scope: scope}
}

// BindExpression 绑定一个 AST 表达式；未获得上下文的裸 NULL 仅作为 binding-only SQLNull 返回。
func (b *Binder) BindExpression(expr ast.Expr) (*BoundExpression, error) {
	if expr == nil {
		return nil, fmt.Errorf("binder: nil expression")
	}
	switch e := expr.(type) {
	case *ast.InSubqueryExpr:
		return b.bindInSubquery(e)
	case *ast.ParameterExpr:
		if e.Value == nil {
			return nil, b.errorAt(e.Span, "unbound parameter")
		}
		value, err := b.BindExpression(e.Value)
		if err != nil {
			return nil, err
		}
		if literal, ok := e.Value.(*ast.LiteralExpr); ok && literal.Kind == ast.LiteralInteger {
			value.Type = types.LogicalType{ID: types.BigInt}
		}
		value.Span = e.Span
		return value, nil
	case *ast.LiteralExpr:
		return b.bindLiteral(e)
	case *ast.NameExpr:
		return b.bindName(e)
	case *ast.UnaryExpr:
		return b.bindUnary(e)
	case *ast.BinaryExpr:
		return b.bindBinary(e)
	case *ast.CallExpr:
		return b.bindCall(e)
	case *ast.IsNullExpr:
		child, err := b.BindExpression(e.Expr)
		if err != nil {
			return nil, err
		}
		if child.SQLNull {
			child, err = b.coerce(child, types.LogicalType{ID: types.Varchar})
			if err != nil {
				return nil, err
			}
		}
		return &BoundExpression{Kind: BoundIsNull, Span: e.Span, Type: types.LogicalType{ID: types.Boolean}, Nullable: false, Op: nullPredicateOp(e.Not), Children: []*BoundExpression{child}}, nil
	case *ast.SubqueryExpr:
		if b.Subqueries == nil {
			return nil, fmt.Errorf("binder %d:%d: subquery requires query binder", e.Span.Start.Line, e.Span.Start.Column)
		}
		query := e.Query
		if e.Exists && query != nil {
			copyQuery := *query
			copyQuery.Projection = append([]ast.SelectItem(nil), query.Projection...)
			for i, item := range copyQuery.Projection {
				if lit, ok := item.Expr.(*ast.LiteralExpr); ok && lit.Kind == ast.LiteralNull {
					copyQuery.Projection[i].Expr = &ast.CallExpr{Span: lit.Span, Function: ast.QualifiedName{Parts: []ast.Identifier{{Name: "__fssql_cast_varchar"}}}, Args: []ast.Expr{lit}}
				}
			}
			query = &copyQuery
		}
		q, err := b.Subqueries(query)
		if err != nil {
			return nil, err
		}
		if q == nil || (!e.Exists && len(q.Output) != 1) {
			return nil, b.errorAt(e.Span, "scalar subquery must return exactly one column")
		}
		for i, col := range q.Output {
			if col.Type.ID == types.Invalid {
				if err := resolveSubqueryNull(q, i, types.LogicalType{ID: types.Integer}, b); err != nil {
					return nil, err
				}
			}
		}
		if e.Exists {
			return &BoundExpression{Kind: BoundSubquery, Span: e.Span, Type: types.LogicalType{ID: types.Boolean}, Op: "EXISTS", Subquery: q}, nil
		}
		return &BoundExpression{Kind: BoundSubquery, Span: e.Span, Type: cloneLogicalType(q.Output[0].Type), Nullable: true, Subquery: q}, nil
	default:
		return nil, fmt.Errorf("binder: unsupported expression %T", expr)
	}
}

// nullPredicateOp 返回 IS NULL 谓词的规范化操作符文本。
func nullPredicateOp(not bool) string {
	if not {
		return "IS NOT NULL"
	}
	return "IS NULL"
}

// bindLiteral 将语法 literal 转为 binding 类型；SQLNULL 不进入 core LogicalType。
func (b *Binder) bindLiteral(e *ast.LiteralExpr) (*BoundExpression, error) {
	out := &BoundExpression{Kind: BoundLiteral, Span: e.Span, Literal: e.Value}
	switch e.Kind {
	case ast.LiteralNull:
		out.Kind = BoundNull
		out.SQLNull = true
		out.Nullable = true
	case ast.LiteralBool:
		out.Type = types.LogicalType{ID: types.Boolean}
	case ast.LiteralString:
		out.Type = types.LogicalType{ID: types.Varchar}
	case ast.LiteralInteger:
		v, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			return nil, b.errorAt(e.Span, "integer literal out of v1 range")
		}
		if v >= math.MinInt32 && v <= math.MaxInt32 {
			out.Type = types.LogicalType{ID: types.Integer}
		} else {
			out.Type = types.LogicalType{ID: types.BigInt}
		}
	case ast.LiteralDecimal:
		t, err := decimalLiteralType(e.Value)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		out.Type = t
	default:
		return nil, b.errorAt(e.Span, "unknown literal kind")
	}
	return out, nil
}

// bindName 解析限定/未限定列名并检测歧义。
func (b *Binder) bindName(e *ast.NameExpr) (*BoundExpression, error) {
	c, err := b.Scope.Resolve(e.Name)
	if err != nil {
		if missing, ok := err.(*missingColumnError); !ok || missing == nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		for depth, scope := range b.OuterScopes {
			outer, outerErr := scope.Resolve(e.Name)
			if outerErr == nil {
				return &BoundExpression{Kind: BoundOuterColumn, Span: e.Span, Type: cloneLogicalType(outer.Type), Nullable: outer.Nullable, ColumnIndex: outer.Index, Depth: depth + 1}, nil
			}
			if _, missing := outerErr.(*missingColumnError); !missing {
				return nil, b.errorAt(e.Span, outerErr.Error())
			}
		}
		return nil, b.errorAt(e.Span, err.Error())
	}
	return &BoundExpression{Kind: BoundColumn, Span: e.Span, Type: cloneLogicalType(c.Type), Nullable: c.Nullable, ColumnIndex: c.Index}, nil
}

// Resolve 按 SQL 标识符规则解析列；双引号名称区分大小写，未限定名称出现多个匹配时拒绝绑定。
func (s Scope) Resolve(name ast.QualifiedName) (ColumnBinding, error) {
	if len(name.Parts) == 0 {
		return ColumnBinding{}, fmt.Errorf("empty column name")
	}
	column := name.Parts[len(name.Parts)-1]
	var qualifier *ast.Identifier
	if len(name.Parts) >= 2 {
		q := name.Parts[len(name.Parts)-2]
		qualifier = &q
	}
	found := ColumnBinding{}
	matches := 0
	candidates := s.Columns
	if qualifier != nil {
		candidates = append(append([]ColumnBinding(nil), s.Columns...), s.Aliases...)
	}
	for _, c := range candidates {
		if !matchesIdentifier(c.Name, column) {
			continue
		}
		if qualifier != nil && !matchesIdentifier(c.Table, *qualifier) {
			continue
		}
		found = c
		matches++
	}
	if matches == 0 {
		return ColumnBinding{}, &missingColumnError{name: qualifiedText(name)}
	}
	if matches > 1 {
		return ColumnBinding{}, fmt.Errorf("column %s is ambiguous", qualifiedText(name))
	}
	return found, nil
}

// bindUnary 绑定 NOT 与数值正负号。
func (b *Binder) bindUnary(e *ast.UnaryExpr) (*BoundExpression, error) {
	child, err := b.BindExpression(e.Expr)
	if err != nil {
		return nil, err
	}
	op := strings.ToUpper(e.Op)
	switch op {
	case "NOT":
		child, err = b.coerce(child, types.LogicalType{ID: types.Boolean})
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		return &BoundExpression{Kind: BoundUnary, Span: e.Span, Type: types.LogicalType{ID: types.Boolean}, Nullable: child.Nullable, Op: op, Children: []*BoundExpression{child}}, nil
	case "+", "-":
		if child.SQLNull {
			return nil, b.errorAt(e.Span, "cannot infer numeric type for NULL")
		}
		if !child.Type.IsNumeric() {
			return nil, b.errorAt(e.Span, "unary numeric operator requires numeric input")
		}
		return &BoundExpression{Kind: BoundUnary, Span: e.Span, Type: child.Type, Nullable: child.Nullable, Op: op, Children: []*BoundExpression{child}}, nil
	default:
		return nil, b.errorAt(e.Span, "unsupported unary operator "+op)
	}
}

// bindBinary 应用布尔、比较和数值运算的强类型规则并插入显式逻辑 Cast 节点。
func (b *Binder) bindBinary(e *ast.BinaryExpr) (*BoundExpression, error) {
	left, err := b.BindExpression(e.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.BindExpression(e.Right)
	if err != nil {
		return nil, err
	}
	op := strings.ToUpper(e.Op)
	nullable := left.Nullable || right.Nullable || left.SQLNull || right.SQLNull
	switch op {
	case "AND", "OR":
		bt := types.LogicalType{ID: types.Boolean}
		left, err = b.coerce(left, bt)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		right, err = b.coerce(right, bt)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		return binaryNode(e, op, bt, nullable, left, right), nil
	case "=", "<>", "<", "<=", ">", ">=":
		left, right, err = b.coerceComparable(left, right)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		return binaryNode(e, op, types.LogicalType{ID: types.Boolean}, nullable, left, right), nil
	case "+", "-", "*", "%":
		left, right, t, err := b.coerceNumericPair(left, right)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		return binaryNode(e, op, t, nullable, left, right), nil
	case "/":
		left, right, _, err := b.coerceNumericPair(left, right)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		dt := types.LogicalType{ID: types.Double}
		left, err = b.coerce(left, dt)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		right, err = b.coerce(right, dt)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
		return binaryNode(e, op, dt, nullable, left, right), nil
	default:
		return nil, b.errorAt(e.Span, "unsupported binary operator "+op)
	}
}

// binaryNode 构造已完成类型统一的二元表达式。
func binaryNode(e *ast.BinaryExpr, op string, t types.LogicalType, nullable bool, left, right *BoundExpression) *BoundExpression {
	return &BoundExpression{Kind: BoundBinary, Span: e.Span, Type: t, Nullable: nullable, Op: op, Children: []*BoundExpression{left, right}}
}

// bindCall 通过 FunctionCatalog 解析函数类别和返回类型，table function 不允许出现在 scalar expression 位置。
func (b *Binder) bindCall(e *ast.CallExpr) (*BoundExpression, error) {
	name := functionName(e.Function)
	def, ok := b.Functions.Lookup(name)
	if !ok {
		return nil, b.errorAt(e.Span, "unknown function "+name)
	}
	if def.Kind == function.Table {
		return nil, b.errorAt(e.Span, "table function cannot appear in scalar expression")
	}
	if e.Distinct && def.Kind != function.Aggregate {
		return nil, b.errorAt(e.Span, "DISTINCT is only valid for aggregate calls")
	}
	children := make([]*BoundExpression, len(e.Args))
	args := make([]function.ArgumentType, len(e.Args))
	for i, arg := range e.Args {
		x, err := b.BindExpression(arg)
		if err != nil {
			return nil, err
		}
		children[i] = x
		args[i] = function.ArgumentType{Type: x.Type, SQLNull: x.SQLNull || x.Type.NullType}
	}
	_, r, err := b.Functions.BindCall(name, def.Kind, function.BindInput{Arguments: args, Star: e.Star, Distinct: e.Distinct})
	if err != nil {
		return nil, b.errorAt(e.Span, err.Error())
	}
	for i, t := range r.Arguments {
		if t.ID == types.Invalid {
			continue
		}
		children[i], err = b.coerce(children[i], t)
		if err != nil {
			return nil, b.errorAt(e.Span, err.Error())
		}
	}
	if def.ID == function.RoundID && children[0].Type.ID == types.Decimal {
		if len(children)==2 && datePartNullConstant(children[1]) { return &BoundExpression{Kind:BoundNull,Type:types.LogicalType{ID:types.Integer,NullType:true},Nullable:true,Span:e.Span},nil }
		r.ReturnType, err = roundDecimalType(children)
		if err != nil { return nil, b.errorAt(e.Span, err.Error()) }
	}
	if def.ID == function.DatePartID {
		if children[0].Type.ID==types.List{r.ReturnType,err=datePartStructType(children)}else{r.ReturnType, err = datePartType(children[0],children[1])}
		if err != nil { return nil, b.errorAt(e.Span, err.Error()) }
	}
	if def.ID==function.RegexpExtractID&&len(children)>=3&&children[2].Type.ID==types.List {r.ReturnType,err=regexpStructType(children[2]);if err!=nil{return nil,b.errorAt(e.Span,err.Error())}}
	if def.ID==function.StructExtractID {r.ReturnType,err=structExtractType(children);if err!=nil{return nil,b.errorAt(e.Span,err.Error())}}
	if err:=validateConstantRegex(def.ID,children);err!=nil{return nil,b.errorAt(e.Span,err.Error())}
	distinct := e.Distinct && def.ID != function.MinID && def.ID != function.MaxID
	depth := 0
	if def.Kind == function.Aggregate {
		depth = aggregateArgumentDepth(children)
	}
	nullable:=def.ID != function.CountID && def.ID != function.NotDistinctID
 if def.ID>=function.FirstCastFunctionID&&def.ID<function.FirstTryCastFunctionID&&len(children)==1{nullable=children[0].Nullable}
	return &BoundExpression{Kind: BoundFunction, Span: e.Span, Type: r.ReturnType, Nullable: nullable, FunctionID: def.ID, FunctionKind: def.Kind, Children: children, Distinct: distinct, Depth: depth}, nil
}

// coerceComparable 为比较运算找到共同类型；NULL 可隐式转换到任意目标类型。
func (b *Binder) coerceComparable(left, right *BoundExpression) (*BoundExpression, *BoundExpression, error) {
 if left.Type.NullType{copyExpr:=*left;copyExpr.SQLNull=true;left=&copyExpr};if right.Type.NullType{copyExpr:=*right;copyExpr.SQLNull=true;right=&copyExpr}
	if left.SQLNull && right.SQLNull {
		t := types.LogicalType{ID: types.Boolean}
		var err error
		left, err = b.coerce(left, t)
		if err != nil {
			return nil, nil, err
		}
		right, err = b.coerce(right, t)
		return left, right, err
	}
	if left.SQLNull {
		return b.coercePairTo(left, right, right.Type)
	}
	if right.SQLNull {
		return b.coercePairTo(left, right, left.Type)
	}
	if left.Type.IsNumeric() && right.Type.IsNumeric() {
		l, r, _, err := b.coerceNumericPair(left, right)
		return l, r, err
	}
	if sameType(left.Type, right.Type) && isComparable(left.Type) {
		return left, right, nil
	}
	return nil, nil, fmt.Errorf("incomparable types %d and %d", left.Type.ID, right.Type.ID)
}

// coercePairTo 将比较两侧统一到给定类型。
func (b *Binder) coercePairTo(left, right *BoundExpression, t types.LogicalType) (*BoundExpression, *BoundExpression, error) {
	l, err := b.coerce(left, t)
	if err != nil {
		return nil, nil, err
	}
	r, err := b.coerce(right, t)
	if err != nil {
		return nil, nil, err
	}
	return l, r, nil
}

// coerceNumericPair 为算术/数值比较选择共同 logical type。
func (b *Binder) coerceNumericPair(left, right *BoundExpression) (*BoundExpression, *BoundExpression, types.LogicalType, error) {
	if left.SQLNull && right.SQLNull {
		return nil, nil, types.LogicalType{}, fmt.Errorf("cannot infer numeric type from two NULL literals")
	}
	if left.SQLNull {
		leftType := right.Type
		if !leftType.IsNumeric() {
			return nil, nil, types.LogicalType{}, fmt.Errorf("numeric operator requires numeric input")
		}
		l, err := b.coerce(left, leftType)
		return l, right, leftType, err
	}
	if right.SQLNull {
		rightType := left.Type
		if !rightType.IsNumeric() {
			return nil, nil, types.LogicalType{}, fmt.Errorf("numeric operator requires numeric input")
		}
		r, err := b.coerce(right, rightType)
		return left, r, rightType, err
	}
	t, err := commonNumericType(left.Type, right.Type)
	if err != nil {
		return nil, nil, types.LogicalType{}, err
	}
	l, err := b.coerce(left, t)
	if err != nil {
		return nil, nil, types.LogicalType{}, err
	}
	r, err := b.coerce(right, t)
	if err != nil {
		return nil, nil, types.LogicalType{}, err
	}
	return l, r, t, nil
}

// coerce 插入允许的 implicit cast；SQLNULL 可转换到任意目标类型。
func (b *Binder) coerce(expr *BoundExpression, target types.LogicalType) (*BoundExpression, error) {
	if expr.SQLNull || expr.Type.NullType {
		return &BoundExpression{Kind: BoundNull, Span: expr.Span, Type: target, Nullable: true}, nil
	}
	if sameType(expr.Type, target) {
		return expr, nil
	}
	if !canImplicitCast(expr.Type, target) {
		return nil, fmt.Errorf("cannot implicitly cast %d to %d", expr.Type.ID, target.ID)
	}
	return &BoundExpression{Kind: BoundCast, Span: expr.Span, Type: target, Nullable: expr.Nullable, Children: []*BoundExpression{expr}}, nil
}

// commonNumericType 选择 v1 数值运算公共类型；精确 DECIMAL 规则后续由 differential suite 持续校验。
func commonNumericType(a, b types.LogicalType) (types.LogicalType, error) {
	if !a.IsNumeric() || !b.IsNumeric() {
		return types.LogicalType{}, fmt.Errorf("numeric operator requires numeric input")
	}
	if a.ID == types.Double || b.ID == types.Double {
		return types.LogicalType{ID: types.Double}, nil
	}
	if a.ID == types.Float || b.ID == types.Float {
		return types.LogicalType{ID: types.Float}, nil
	}
	if a.ID == types.Decimal || b.ID == types.Decimal {
		scale := uint8(0)
		if a.ID == types.Decimal && a.Decimal.Scale > scale {
			scale = a.Decimal.Scale
		}
		if b.ID == types.Decimal && b.Decimal.Scale > scale {
			scale = b.Decimal.Scale
		}
		return types.NewDecimal(38, scale)
	}
	if a.ID == types.HugeInt || b.ID == types.HugeInt {
		return types.LogicalType{ID: types.HugeInt}, nil
	}
	if a.Unsigned != b.Unsigned {
		if (a.IsUnsignedInteger() && a.ID == types.BigInt) || (b.IsUnsignedInteger() && b.ID == types.BigInt) {
			return types.LogicalType{ID: types.HugeInt}, nil
		}
		return types.LogicalType{ID: types.BigInt}, nil
	}
	rank := numericIntegerRank
	if rank(a.ID) >= rank(b.ID) {
		return a, nil
	}
	return b, nil
}

// canImplicitCast 只允许 NULL 之外的数值 widening，避免旧式任意 VARCHAR 隐式转换。
func canImplicitCast(from, to types.LogicalType) bool {
	if sameType(from, to) {
		return true
	}
	if !from.IsNumeric() || !to.IsNumeric() {
		return false
	}
	if to.ID == types.Double {
		return true
	}
	if to.ID == types.Float {
		return from.ID != types.Double
	}
	if to.ID == types.Decimal {
		return from.ID != types.Float && from.ID != types.Double
	}
	if from.Unsigned != to.Unsigned {
		return from.IsUnsignedInteger() && !to.Unsigned && (to.ID == types.HugeInt || (to.ID == types.BigInt && from.ID != types.BigInt))
	}
	return numericIntegerRank(from.ID) > 0 && numericIntegerRank(to.ID) >= numericIntegerRank(from.ID)
}

// numericIntegerRank 返回整数 widening 顺序；0 表示非整数类型。
func numericIntegerRank(id types.LogicalTypeID) int {
	switch id {
	case types.TinyInt:
		return 1
	case types.SmallInt:
		return 2
	case types.Integer:
		return 3
	case types.BigInt:
		return 4
	case types.HugeInt:
		return 5
	default:
		return 0
	}
}

// isComparable 判断当前 v1 类型是否具有稳定比较语义。
func isComparable(t types.LogicalType) bool {
	switch t.ID {
	case types.Boolean, types.TinyInt, types.SmallInt, types.Integer, types.BigInt, types.HugeInt, types.Float, types.Double, types.Decimal, types.Varchar, types.DateType, types.TimestampType:
		return true
	default:
		return false
	}
}

// sameType 比较 Binder 需要的 logical identity；复杂类型的深比较留给后续类型系统扩展。
func sameType(a, b types.LogicalType) bool {
	if a.ID != b.ID || a.Unsigned != b.Unsigned {
		return false
	}
	if a.ID == types.Decimal {
		return a.Decimal == b.Decimal
	}
	return true
}

// matchesIdentifier 应用 quoted/unquoted identifier 的大小写规则。
func matchesIdentifier(candidate string, id ast.Identifier) bool {
	if id.Quoted {
		return candidate == id.Name
	}
	return strings.EqualFold(candidate, id.Name)
}

// qualifiedText 为 Binder 诊断重建限定名称。
func qualifiedText(n ast.QualifiedName) string {
	parts := make([]string, len(n.Parts))
	for i, p := range n.Parts {
		parts[i] = p.Name
	}
	return strings.Join(parts, ".")
}

// functionName 返回函数限定名的最终名称片段；schema 解析由后续 catalog binder 处理。
func functionName(n ast.QualifiedName) string {
	if len(n.Parts) == 0 {
		return ""
	}
	return n.Parts[len(n.Parts)-1].Name
}

// decimalLiteralType 从十进制 literal 推导 DECIMAL(width, scale)。
func decimalLiteralType(v string) (types.LogicalType, error) {
	p := strings.Split(v, ".")
	if len(p) != 2 {
		return types.LogicalType{}, fmt.Errorf("invalid decimal literal")
	}
	width := len(p[0]) + len(p[1])
	if width == 0 || width > 38 {
		return types.LogicalType{}, fmt.Errorf("decimal literal exceeds DECIMAL(38) range")
	}
	return types.NewDecimal(uint8(width), uint8(len(p[1])))
}

// errorAt 构造带源位置的 Binder 诊断。
func (b *Binder) errorAt(span ast.Span, msg string) error {
	return fmt.Errorf("binder %d:%d: %s", span.Start.Line, span.Start.Column, msg)
}
