package ast

// CreateViewStmt 表示 CREATE VIEW <name> AS <select>。
type CreateViewStmt struct {
	Replace   bool
	Temporary bool
	Span      Span          // Span 覆盖完整 CREATE VIEW statement。
	Name      QualifiedName // Name 是 catalog relation 名称。
	Query     *SelectStmt   // Query 是使用普通 query Binder 绑定的查询体。
}

func (*CreateViewStmt) statementNode()   {}
func (s *CreateViewStmt) NodeSpan() Span { return s.Span }

// CreateTableAsStmt creates a DB-lifetime typed snapshot, not a writable table.
type CreateTableAsStmt struct {
	Span      Span
	Name      QualifiedName
	Query     *SelectStmt
	Temporary bool
}

func (*CreateTableAsStmt) statementNode()   {}
func (s *CreateTableAsStmt) NodeSpan() Span { return s.Span }

// CreateExternalTableStmt 表示 AD-045 冻结的 external-table DDL。
type CreateExternalTableStmt struct {
	Span     Span          // Span 覆盖完整 DDL。
	Name     QualifiedName // Name 是 Catalog 对象名。
	Function QualifiedName // Function 是 Stable table-function 名称。
	Source   *LiteralExpr  // Source 必须是单个 string literal。
}

func (*CreateExternalTableStmt) statementNode()   {}
func (s *CreateExternalTableStmt) NodeSpan() Span { return s.Span }

// SecretOptionKind 区分 credential 与 AD-045 安全字段。
type SecretOptionKind uint8

const (
	SecretCredential SecretOptionKind = iota
	SecretType
	SecretSubject
	SecretScheme
	SecretHost
)

// SecretOption 保存 CREATE SECRET 中一个有 source span 的 option。
type SecretOption struct {
	Span            Span             // Span 覆盖 key/value。
	Kind            SecretOptionKind // Kind 由语法层区分安全字段。
	Name            Identifier       // Name 保存 option/credential key。
	IdentifierValue *Identifier      // IdentifierValue 仅 TYPE 使用。
	StringValue     *LiteralExpr     // StringValue 用于 credential/ACL 字符串。
}

func (o SecretOption) NodeSpan() Span { return o.Span }

// CreateSecretStmt 保存 SQL secret 输入；credential 只停留在 statement AST/binder 到 SecretManager 的短生命周期路径。
type CreateSecretStmt struct {
	Span    Span           // Span 覆盖完整 CREATE SECRET statement。
	Name    Identifier     // Name 是 secret 名称。
	Options []SecretOption // Options 保留声明顺序供 Binder 检查重复/必需字段。
}

func (*CreateSecretStmt) statementNode()   {}
func (s *CreateSecretStmt) NodeSpan() Span { return s.Span }

// CopyOption 是 COPY TO 的通用 option syntax；具体名称/类型由 Binder registry 验证。
type CopyOption struct {
	Span  Span       // Span 覆盖 option name/value。
	Name  Identifier // Name 是未绑定 option 名称。
	Value Expr       // Value 为空表示 flag option。
}

func (o CopyOption) NodeSpan() Span { return o.Span }

// CopyEncryptionExpr is a bounded COPY-only struct; its values are not evaluated
// by the scalar execution engine. Formatting must never reveal key literals.
type CopyEncryptionExpr struct {
	Span   Span
	Fields []CopyOption
}

func (*CopyEncryptionExpr) exprNode()          {}
func (e *CopyEncryptionExpr) NodeSpan() Span   { return e.Span }
func (*CopyEncryptionExpr) String() string     { return "ENCRYPTION_CONFIG <redacted>" }
func (e *CopyEncryptionExpr) GoString() string { return e.String() }

// CopyStmt 表示 Stable COPY ... TO；Relation 与 Query 恰有一个非 nil/非零。
type CopyStmt struct {
	Span        Span           // Span 覆盖完整 COPY statement。
	Relation    *QualifiedName // Relation 是 catalog relation/view source。
	Query       *SelectStmt    // Query 是 parenthesized SELECT source。
	Destination *LiteralExpr   // Destination 必须是 string literal。
	Options     []CopyOption   // Options 由后续 bounded registry 绑定。
}

func (*CopyStmt) statementNode()   {}
func (s *CopyStmt) NodeSpan() Span { return s.Span }

// DescribeStmt 表示 Stable DESCRIBE relation/query；两种 source 恰有一个。
type DescribeStmt struct {
	Span     Span           // Span 覆盖完整 DESCRIBE statement。
	Relation *QualifiedName // Relation 是 catalog object。
	Query    *SelectStmt    // Query 只 Bind schema，不执行。
}

func (*DescribeStmt) statementNode()   {}
func (s *DescribeStmt) NodeSpan() Span { return s.Span }
