package ast

// Position 表示 SQL 源文本中的位置。
type Position struct {
	Offset int // Offset 是 0-based UTF-8 字节偏移。
	Line   int // Line 是 1-based 行号。
	Column int // Column 是 1-based Unicode 字符列号。
}

// Span 表示 AST 节点在源 SQL 中的半开区间 [Start, End)。
type Span struct {
	Start Position // Start 是节点首字符位置。
	End   Position // End 是节点末字符之后的位置。
}

// Node 是所有自研 AST 节点的公共接口。
type Node interface{ NodeSpan() Span }

// Statement 是顶层 SQL 语句节点。
type Statement interface {
	Node
	statementNode()
}

// Expr 是 SQL 表达式节点。
type Expr interface {
	Node
	exprNode()
}

// TableExpr 是 FROM 子句中的表表达式节点。
type TableExpr interface {
	Node
	tableExprNode()
}

// SampleTableExpr samples its input without changing column qualifiers.
type SampleTableExpr struct {
	Span  Span
	Input TableExpr
	Spec  string
	Using bool
}

func (*SampleTableExpr) tableExprNode()   {}
func (s *SampleTableExpr) NodeSpan() Span { return s.Span }

// Identifier 保存标识符原文与规范化名称。
type Identifier struct {
	Span   Span   // Span 是标识符源位置。
	Raw    string // Raw 是包含引号时的原始文本。
	Name   string // Name 是供后续 Binder 使用的规范化名称。
	Quoted bool   // Quoted 表示双引号标识符；为 true 时 Name 保留大小写。
}

// NodeSpan 返回标识符的源位置。
func (i Identifier) NodeSpan() Span { return i.Span }

// QualifiedName 表示 catalog/schema/name 风格的限定名称。
type QualifiedName struct {
	Span  Span         // Span 覆盖完整限定名称。
	Parts []Identifier // Parts 按从左到右顺序保存名称片段。
}

// NodeSpan 返回限定名称的源位置。
func (n QualifiedName) NodeSpan() Span { return n.Span }

// SelectStmt 表示 v1 Stable SELECT 查询骨架。
type SelectStmt struct {
	UnionLeft  *SelectStmt
	UnionRight *SelectStmt
	Span       Span         // Span 覆盖完整 SELECT statement。
	With       *WithClause  // With 是可选 non-recursive CTE。
	Distinct   bool         // Distinct 表示 SELECT DISTINCT。
	Projection []SelectItem // Projection 保存投影列表。
	From       TableExpr    // From 是可选 FROM 表达式。
	Where      Expr         // Where 是可选过滤条件。
	GroupBy    []Expr       // GroupBy 保存 GROUP BY 表达式。
	Having     Expr         // Having 是可选聚合后过滤条件。
	OrderBy    []OrderTerm  // OrderBy 保存排序项。
	Limit      Expr         // Limit 是可选 LIMIT 表达式。
	Offset     Expr         // Offset 是可选 OFFSET 表达式。
}

func (*SelectStmt) statementNode() {}

// NodeSpan 返回 SELECT 语句覆盖的源位置。
func (s *SelectStmt) NodeSpan() Span { return s.Span }

// WithClause 表示 non-recursive CTE 列表。
type WithClause struct {
	Span Span  // Span 覆盖 WITH 到最后一个 CTE。
	CTEs []CTE // CTEs 按声明顺序保存 CTE。
}

// NodeSpan 返回 WITH 子句的源位置。
func (w *WithClause) NodeSpan() Span { return w.Span }

// CTE 表示一个非递归公共表表达式。
type CTE struct {
	Span    Span         // Span 覆盖 CTE 名称至闭合括号。
	Name    Identifier   // Name 是 CTE 名称。
	Columns []Identifier // Columns 是可选显式输出列名。
	Query   *SelectStmt  // Query 是 CTE 查询体。
}

// NodeSpan 返回 CTE 的源位置。
func (c CTE) NodeSpan() Span { return c.Span }

// SelectItem 表示 SELECT 投影项。
type SelectItem struct {
	Span  Span        // Span 覆盖表达式/星号及 alias。
	Expr  Expr        // Expr 是普通投影表达式；Star=true 时为空。
	Star  bool        // Star 表示 SELECT * 投影项。
	Alias *Identifier // Alias 是可选输出别名。
}

// NodeSpan 返回投影项的源位置。
func (s SelectItem) NodeSpan() Span { return s.Span }

// OrderTerm 表示 ORDER BY 排序项。
type OrderTerm struct {
	Span       Span  // Span 覆盖完整排序项。
	Expr       Expr  // Expr 是排序表达式。
	Descending bool  // Descending 表示 DESC；false 表示默认/ASC。
	NullsFirst *bool // NullsFirst 为空时使用执行层默认 NULL 顺序。
}

// NodeSpan 返回排序项的源位置。
func (o OrderTerm) NodeSpan() Span { return o.Span }

// DerivedTable keeps a FROM subquery in place for left-to-right name binding.
type DerivedTable struct {
	Span    Span
	Query   *SelectStmt
	Alias   Identifier
	Columns []Identifier
	Lateral bool
}

func (*DerivedTable) tableExprNode()   {}
func (t *DerivedTable) NodeSpan() Span { return t.Span }

// TableName 表示普通表引用。
type TableName struct {
	Span  Span          // Span 覆盖表名与 alias。
	Name  QualifiedName // Name 是限定表名。
	Alias *Identifier   // Alias 是可选表别名。
}

func (*TableName) tableExprNode() {}

// NodeSpan 返回表引用的源位置。
func (t *TableName) NodeSpan() Span { return t.Span }

// TableFunction 表示 read_parquet/read_csv 等表函数调用。
type TableFunction struct {
	Span  Span        // Span 覆盖函数调用与 alias。
	Call  *CallExpr   // Call 保存函数名和参数。
	Alias *Identifier // Alias 是可选表别名。
}

func (*TableFunction) tableExprNode() {}

// NodeSpan 返回表函数的源位置。
func (t *TableFunction) NodeSpan() Span { return t.Span }

// JoinType 定义 Stable JOIN 类型。
type JoinType uint8

const (
	JoinInner JoinType = iota
	JoinLeft
	JoinRight
	JoinFull
	JoinCross
	JoinSemi
	JoinAnti
)

// JoinExpr 表示 FROM 中的二元 JOIN。
type JoinExpr struct {
	Span      Span         // Span 覆盖左右输入及 join condition。
	Left      TableExpr    // Left 是左输入。
	Right     TableExpr    // Right 是右输入。
	Type      JoinType     // Type 是 Stable JOIN 类型。
	Condition Expr         // Condition 保存 ON 表达式。
	Using     []Identifier // Using 保存 USING 列；与 Condition 互斥。
}

func (*JoinExpr) tableExprNode() {}

// NodeSpan 返回 JOIN 的源位置。
func (j *JoinExpr) NodeSpan() Span { return j.Span }

// LiteralKind 定义字面量类别。
type LiteralKind uint8

const (
	LiteralNull LiteralKind = iota
	LiteralBool
	LiteralInteger
	LiteralDecimal
	LiteralString
)

// LiteralExpr 保存未绑定字面量，具体 LogicalType 由 Binder 决定。
type LiteralExpr struct {
	Span  Span        // Span 是字面量源位置。
	Kind  LiteralKind // Kind 是语法级字面量分类。
	Value string      // Value 是去引号/规范化后的文本值。
}

// ParameterExpr is a prepared-statement value slot. Value is populated by
// binding an immutable statement template, never by parsing parameter text.
type ParameterExpr struct {
	Span  Span
	Slot  int
	Value Expr
}

func (*ParameterExpr) exprNode()        {}
func (e *ParameterExpr) NodeSpan() Span { return e.Span }

func (*LiteralExpr) exprNode() {}

// NodeSpan 返回字面量的源位置。
func (e *LiteralExpr) NodeSpan() Span { return e.Span }

// NameExpr 表示列名或限定列名表达式。
type NameExpr struct {
	Span Span          // Span 覆盖完整名称。
	Name QualifiedName // Name 保存名称片段。
}

func (*NameExpr) exprNode() {}

// NodeSpan 返回名称表达式的源位置。
func (e *NameExpr) NodeSpan() Span { return e.Span }

// UnaryExpr 表示一元运算。
type UnaryExpr struct {
	Span Span   // Span 覆盖运算符与 operand。
	Op   string // Op 是规范化运算符。
	Expr Expr   // Expr 是 operand。
}

func (*UnaryExpr) exprNode() {}

// NodeSpan 返回一元表达式的源位置。
func (e *UnaryExpr) NodeSpan() Span { return e.Span }

// BinaryExpr 表示二元运算。
type BinaryExpr struct {
	Span  Span   // Span 覆盖左右 operand。
	Left  Expr   // Left 是左 operand。
	Op    string // Op 是规范化运算符。
	Right Expr   // Right 是右 operand。
}

func (*BinaryExpr) exprNode() {}

// NodeSpan 返回二元表达式的源位置。
func (e *BinaryExpr) NodeSpan() Span { return e.Span }

// CallExpr 表示标量/聚合/表函数调用；函数分类由 Binder/FunctionCatalog 决定。
type CallExpr struct {
	Span     Span          // Span 覆盖完整函数调用。
	Function QualifiedName // Function 是未绑定函数名称。
	Args     []Expr        // Args 按调用顺序保存参数。
	Star     bool          // Star 表示 count(*) 一类星号参数。
	Distinct bool          // Distinct 表示 aggregate DISTINCT 参数语法。
}

func (*CallExpr) exprNode() {}

// NodeSpan 返回函数调用的源位置。
func (e *CallExpr) NodeSpan() Span { return e.Span }

// IsNullExpr 表示 IS [NOT] NULL。
type IsNullExpr struct {
	Span Span // Span 覆盖完整谓词。
	Expr Expr // Expr 是被检查表达式。
	Not  bool // Not 表示 IS NOT NULL。
}

func (*IsNullExpr) exprNode() {}

// NodeSpan 返回 IS NULL 表达式的源位置。
func (e *IsNullExpr) NodeSpan() Span { return e.Span }

// SubqueryExpr 表示表达式位置上的非相关子查询语法节点；相关性由 Binder 判定。
type SubqueryExpr struct {
	Exists bool
	Span   Span        // Span 包含左右括号。
	Query  *SelectStmt // Query 是子查询 statement。
}

type InSubqueryExpr struct {
	Span  Span
	Value Expr
	Query *SelectStmt
	Not   bool
}

func (*InSubqueryExpr) exprNode()        {}
func (e *InSubqueryExpr) NodeSpan() Span { return e.Span }

func (*SubqueryExpr) exprNode() {}

// NodeSpan 返回子查询表达式的源位置。
func (e *SubqueryExpr) NodeSpan() Span { return e.Span }
