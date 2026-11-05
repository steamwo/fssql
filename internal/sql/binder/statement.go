package binder

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
)

// StatementKind 定义 Stable statement Binder 的输出类别。
type StatementKind uint8

const (
	StatementQuery StatementKind = iota
	StatementCreateView
	StatementCreateExternalTable
	StatementCreateSecret
	StatementCopyTo
	StatementDescribe
)

// ObjectName 是 Binder 层的 schema/object 名称，不依赖具体 Catalog 实现。
type ObjectName struct {
	Schema string // Schema 为空限定时规范化为 main。
	Name   string // Name 保留 quoted identifier 的大小写语义。
}

// BoundStatement 是所有 bound statement 的公共接口。
type BoundStatement interface {
	StatementKind() StatementKind
}

// BoundQueryStatement 包装普通 SELECT 查询。
type BoundQueryStatement struct {
	Query *BoundQuery // Query 是已强类型绑定的查询。
}

func (*BoundQueryStatement) StatementKind() StatementKind { return StatementQuery }

// BoundCreateView 是 CREATE VIEW 的副作用前控制面结果。
type BoundCreateView struct {
	Replace   bool
	Temporary bool
	Name      ObjectName       // Name 是目标 Catalog 对象。
	Query     *BoundQuery      // Query 是实际 QueryBinder 的结果。
	Columns   []RelationColumn // Columns 是 view 创建时冻结的输出 schema。
	SQL       string           // SQL 是 query 子句原文，不包含 CREATE VIEW 前缀。
}

func (*BoundCreateView) StatementKind() StatementKind { return StatementCreateView }

// BoundCreateExternalTable 保存真实 TableFunction.Bind 得到的 schema。
type BoundCreateExternalTable struct {
	Name        ObjectName       // Name 是目标 Catalog 对象。
	FunctionID  function.ID      // FunctionID 是稳定内部 table-function identity。
	Function    string           // Function 是规范化 Stable table-function 名。
	Source      string           // Source 是解析后的 source literal；credential 不得嵌入 Catalog。
	Columns     []RelationColumn // Columns 必须来自真实 TableFunctionResolver。
	RelationID  string           // RelationID 是 bind adapter 返回的 source identity。
	Fingerprint string           // Fingerprint 是实际 source snapshot identity 的文本形式。
}

func (*BoundCreateExternalTable) StatementKind() StatementKind { return StatementCreateExternalTable }

// BoundSecretCredential 保存 SQL secret credential；value 私有以降低意外日志暴露风险。
type BoundSecretCredential struct {
	Key   string // Key 是 SQL identifier 规范化后的 credential key。
	value string // value 是敏感明文，仅执行 adapter 可通过 Value 显式读取。
}

// Value 返回 credential 明文；调用方不得记录、格式化或持久化到 Catalog。
func (c BoundSecretCredential) Value() string { return c.value }

// String 返回固定脱敏文本，避免 fmt 默认路径泄露 credential。
func (c BoundSecretCredential) String() string { return c.Key + "=<redacted>" }

// BoundCreateSecret 是 CREATE SECRET 的已验证安全输入。
type BoundCreateSecret struct {
	Name        string                  // Name 是 secret 稳定名称。
	Type        string                  // Type 是必需 secret adapter metadata。
	Credentials []BoundSecretCredential // Credentials 是已去重的敏感键值。
	Subjects    []string                // Subjects 至少一个；没有隐式 wildcard。
	Schemes     []string                // Schemes 显式映射 SecretScope。
	Hosts       []string                // Hosts 显式映射 SecretScope。
}

func (*BoundCreateSecret) StatementKind() StatementKind { return StatementCreateSecret }

// String 始终脱敏，不输出任何 credential value。
func (s *BoundCreateSecret) String() string {
	if s == nil {
		return "CREATE SECRET <nil>"
	}
	return fmt.Sprintf("CREATE SECRET %s TYPE %s credentials=%d subjects=%d schemes=%d hosts=%d <redacted>", s.Name, s.Type, len(s.Credentials), len(s.Subjects), len(s.Schemes), len(s.Hosts))
}

// CopyFormat 是 SQL COPY Binder 与 Export adapter 之间的格式枚举。
type CopyFormat string

const (
	CopyCSV     CopyFormat = "csv"
	CopyJSON    CopyFormat = "json"
	CopyNDJSON  CopyFormat = "ndjson"
	CopyParquet CopyFormat = "parquet"
)

// BoundCopyOptions 是能够无损映射到现有 Export FileWriterConfig 的 Stable SQL 子集。
type BoundCopyOptions struct {
	ForceQuote  bool
	Format      CopyFormat // Format 是最终目标格式。
	Compression string     // Compression 是 identity/gzip/zstd。
	Header      bool       // Header 仅 CSV 有效；CSV 默认 true 以匹配 DuckDB COPY TO。
	Delimiter   byte       // Delimiter 仅 CSV 有效；默认逗号。
	Quote       byte       // Quote 仅 CSV 有效；默认双引号。
}

// BoundCopyTo 是 COPY source → Export adapter 的副作用前结果。
type BoundCopyTo struct {
	Query       *BoundQuery      // Query 是 relation/query source 统一绑定后的结果。
	Destination string           // Destination 是 SQL string literal；VFS/security adapter 后续验证。
	Options     BoundCopyOptions // Options 已完成格式/压缩/CSV 规则验证。
}

func (*BoundCopyTo) StatementKind() StatementKind { return StatementCopyTo }

// BoundDescribe 是不执行 Query 的 schema introspection 结果。
type BoundDescribe struct {
	Columns []RelationColumn // Columns 直接来自 Catalog resolver 或 QueryBinder 输出。
}

func (*BoundDescribe) StatementKind() StatementKind { return StatementDescribe }

// StatementBinder 负责 AD-045 statement semantic validation；不执行 Catalog/Security/Export 副作用。
type StatementBinder struct {
	Functions      *function.Catalog     // Functions 是统一 FunctionCatalog。
	Relations      RelationResolver      // Relations 是 Catalog relation schema adapter。
	TableFunctions TableFunctionResolver // TableFunctions 必须走真实 Bind/SchemaResolver。
	SourceSQL      string                // SourceSQL 用于 view query SQL 与输出标签。
}

// NewStatementBinder 创建 Stable statement Binder。
func NewStatementBinder(functions *function.Catalog, relations RelationResolver, tableFunctions TableFunctionResolver, sourceSQL string) *StatementBinder {
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	return &StatementBinder{Functions: functions, Relations: relations, TableFunctions: tableFunctions, SourceSQL: sourceSQL}
}

// Bind 绑定一个 Stable statement；所有副作用由后续 adapter 显式执行。
func (b *StatementBinder) Bind(ctx context.Context, stmt ast.Statement) (BoundStatement, error) {
	if b == nil || stmt == nil {
		return nil, fmt.Errorf("statement binder: nil binder/statement")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		q, err := b.queryBinder().BindSelect(ctx, s)
		if err != nil {
			return nil, err
		}
		return &BoundQueryStatement{Query: q}, nil
	case *ast.CreateViewStmt:
		return b.bindCreateView(ctx, s)
	case *ast.CreateExternalTableStmt:
		return b.bindCreateExternalTable(ctx, s)
	case *ast.CreateSecretStmt:
		return b.bindCreateSecret(s)
	case *ast.CopyStmt:
		return b.bindCopy(ctx, s)
	case *ast.DescribeStmt:
		return b.bindDescribe(ctx, s)
	default:
		return nil, fmt.Errorf("statement binder: unsupported statement %T", stmt)
	}
}

// queryBinder 创建共享 Function/Relation/TableFunction contracts 的 QueryBinder。
func (b *StatementBinder) queryBinder() *QueryBinder {
	return NewQueryBinder(b.Functions, b.Relations, b.TableFunctions, b.SourceSQL)
}

// bindCreateView 复用 QueryBinder 并冻结 view 输出 schema。
func (b *StatementBinder) bindCreateView(ctx context.Context, stmt *ast.CreateViewStmt) (*BoundCreateView, error) {
	name, err := bindObjectName(stmt.Name)
	if err != nil {
		return nil, b.errorAt(stmt.Span, err.Error())
	}
	q, err := b.queryBinder().BindSelect(ctx, stmt.Query)
	if err != nil {
		return nil, err
	}
	querySQL, err := b.sourceSlice(stmt.Query.NodeSpan())
	if err != nil {
		return nil, b.errorAt(stmt.Query.NodeSpan(), err.Error())
	}
	return &BoundCreateView{Name: name, Query: q, Columns: cloneRelationColumns(q.Output), SQL: querySQL, Replace: stmt.Replace, Temporary: stmt.Temporary}, nil
}

// bindCreateExternalTable 强制通过 FunctionCatalog + real TableFunctionResolver 获取 schema。
func (b *StatementBinder) bindCreateExternalTable(ctx context.Context, stmt *ast.CreateExternalTableStmt) (*BoundCreateExternalTable, error) {
	name, err := bindObjectName(stmt.Name)
	if err != nil {
		return nil, b.errorAt(stmt.Span, err.Error())
	}
	if len(stmt.Function.Parts) != 1 {
		return nil, b.errorAt(stmt.Function.Span, "external-table function must use an unqualified Stable name")
	}
	fn := stmt.Function.Parts[0].Name
	def, ok := b.Functions.Lookup(fn)
	if !ok || def.Kind != function.Table {
		return nil, b.errorAt(stmt.Function.Span, "unknown Stable table function "+fn)
	}
	argType := types.LogicalType{ID: types.Varchar}
	if _, _, err := b.Functions.BindCall(fn, function.Table, function.BindInput{Arguments: []function.ArgumentType{{Type: argType}}}); err != nil {
		return nil, b.errorAt(stmt.Function.Span, err.Error())
	}
	if stmt.Source == nil || stmt.Source.Kind != ast.LiteralString {
		return nil, b.errorAt(stmt.Span, "external table source must be a string literal")
	}
	if b.TableFunctions == nil {
		return nil, b.errorAt(stmt.Span, "table-function resolver is not configured")
	}
	resolved, err := b.TableFunctions.BindTableFunction(ctx, def, []TableFunctionArgument{{Type: argType, Value: stmt.Source.Value}})
	if err != nil {
		return nil, b.errorAt(stmt.Span, "bind external table source: "+err.Error())
	}
	if resolved.ID == "" || len(resolved.Columns) == 0 {
		return nil, b.errorAt(stmt.Span, "table-function Bind returned no identity/schema")
	}
	return &BoundCreateExternalTable{
		Name: name, FunctionID: def.ID, Function: def.Name, Source: stmt.Source.Value,
		Columns: cloneRelationColumns(resolved.Columns), RelationID: resolved.ID, Fingerprint: string(resolved.Fingerprint),
	}, nil
}

// bindCreateSecret 校验 AD-045 的 TYPE/credential/ACL 约束，不调用 SecretManager。
func (b *StatementBinder) bindCreateSecret(stmt *ast.CreateSecretStmt) (*BoundCreateSecret, error) {
	if strings.TrimSpace(stmt.Name.Name) == "" {
		return nil, b.errorAt(stmt.Span, "secret name is empty")
	}
	out := &BoundCreateSecret{Name: stmt.Name.Name}
	var typeSet bool
	for _, opt := range stmt.Options {
		switch opt.Kind {
		case ast.SecretType:
			if typeSet || opt.IdentifierValue == nil || opt.IdentifierValue.Name == "" {
				return nil, b.errorAt(opt.Span, "CREATE SECRET requires exactly one TYPE identifier")
			}
			out.Type = opt.IdentifierValue.Name
			typeSet = true
		case ast.SecretCredential:
			if opt.StringValue == nil {
				return nil, b.errorAt(opt.Span, "secret credential requires string literal")
			}
			for _, current := range out.Credentials {
				if strings.EqualFold(current.Key, opt.Name.Name) {
					return nil, b.errorAt(opt.Span, "duplicate secret credential key "+opt.Name.Name)
				}
			}
			out.Credentials = append(out.Credentials, BoundSecretCredential{Key: opt.Name.Name, value: opt.StringValue.Value})
		case ast.SecretSubject:
			if opt.StringValue == nil || opt.StringValue.Value == "" {
				return nil, b.errorAt(opt.Span, "SUBJECT requires non-empty string")
			}
			if err := appendUniqueFold(&out.Subjects, opt.StringValue.Value, "SUBJECT"); err != nil {
				return nil, b.errorAt(opt.Span, err.Error())
			}
		case ast.SecretScheme:
			if opt.StringValue == nil || opt.StringValue.Value == "" {
				return nil, b.errorAt(opt.Span, "SCHEME requires non-empty string")
			}
			value := strings.ToLower(opt.StringValue.Value)
			if err := appendUniqueFold(&out.Schemes, value, "SCHEME"); err != nil {
				return nil, b.errorAt(opt.Span, err.Error())
			}
		case ast.SecretHost:
			if opt.StringValue == nil || opt.StringValue.Value == "" {
				return nil, b.errorAt(opt.Span, "HOST requires non-empty string")
			}
			value := strings.ToLower(strings.TrimSuffix(opt.StringValue.Value, "."))
			if err := appendUniqueFold(&out.Hosts, value, "HOST"); err != nil {
				return nil, b.errorAt(opt.Span, err.Error())
			}
		default:
			return nil, b.errorAt(opt.Span, "unknown secret option kind")
		}
	}
	if !typeSet {
		return nil, b.errorAt(stmt.Span, "CREATE SECRET requires TYPE")
	}
	if len(out.Credentials) == 0 {
		return nil, b.errorAt(stmt.Span, "CREATE SECRET requires at least one credential")
	}
	if len(out.Subjects) == 0 {
		return nil, b.errorAt(stmt.Span, "CREATE SECRET requires at least one SUBJECT; wildcard is never implicit")
	}
	return out, nil
}

// bindCopy 将 relation/query source 统一为 BoundQuery，并绑定可无损映射到现有 Export 的 option 子集。
func (b *StatementBinder) bindCopy(ctx context.Context, stmt *ast.CopyStmt) (*BoundCopyTo, error) {
	if stmt.Destination == nil || stmt.Destination.Kind != ast.LiteralString || strings.TrimSpace(stmt.Destination.Value) == "" {
		return nil, b.errorAt(stmt.Span, "COPY destination must be a non-empty string literal")
	}
	var query *BoundQuery
	var err error
	qb := b.queryBinder()
	switch {
	case stmt.Query != nil && stmt.Relation == nil:
		query, err = qb.BindSelect(ctx, stmt.Query)
	case stmt.Query == nil && stmt.Relation != nil:
		table := &ast.TableName{Span: stmt.Relation.Span, Name: *stmt.Relation}
		synthetic := &ast.SelectStmt{
			Span:       stmt.Relation.Span,
			Projection: []ast.SelectItem{{Span: stmt.Relation.Span, Star: true}},
			From:       table,
		}
		query, err = qb.BindSelect(ctx, synthetic)
	default:
		return nil, b.errorAt(stmt.Span, "COPY must have exactly one relation or query source")
	}
	if err != nil {
		return nil, err
	}
	options, err := bindCopyOptions(stmt.Destination.Value, stmt.Options)
	if err != nil {
		return nil, b.errorAt(stmt.Span, err.Error())
	}
	return &BoundCopyTo{Query: query, Destination: stmt.Destination.Value, Options: options}, nil
}

// bindDescribe 复用 relation resolver 或 QueryBinder，不执行查询。
func (b *StatementBinder) bindDescribe(ctx context.Context, stmt *ast.DescribeStmt) (*BoundDescribe, error) {
	switch {
	case stmt.Query != nil && stmt.Relation == nil:
		q, err := b.queryBinder().BindSelect(ctx, stmt.Query)
		if err != nil {
			return nil, err
		}
		return &BoundDescribe{Columns: cloneRelationColumns(q.Output)}, nil
	case stmt.Query == nil && stmt.Relation != nil:
		if b.Relations == nil {
			return nil, b.errorAt(stmt.Span, "relation resolver is not configured")
		}
		r, err := b.Relations.ResolveRelation(ctx, *stmt.Relation)
		if err != nil {
			return nil, b.errorAt(stmt.Span, "resolve relation: "+err.Error())
		}
		if len(r.Columns) == 0 {
			return nil, b.errorAt(stmt.Span, "resolved relation has no schema")
		}
		return &BoundDescribe{Columns: cloneRelationColumns(r.Columns)}, nil
	default:
		return nil, b.errorAt(stmt.Span, "DESCRIBE must have exactly one relation or query source")
	}
}

// bindObjectName 将 v1 Catalog 名称限制为 [schema.]object；ATTACH/catalog-qualified names 不是 Stable v1。
func bindObjectName(name ast.QualifiedName) (ObjectName, error) {
	if len(name.Parts) == 0 || len(name.Parts) > 2 {
		return ObjectName{}, fmt.Errorf("v1 Catalog object name must be [schema.]object")
	}
	if len(name.Parts) == 1 {
		return ObjectName{Schema: "main", Name: name.Parts[0].Name}, nil
	}
	return ObjectName{Schema: name.Parts[0].Name, Name: name.Parts[1].Name}, nil
}

// sourceSlice 从原 SQL 提取 query 原文；Span 越界时必须失败而不是保存伪造 SQL。
func (b *StatementBinder) sourceSlice(span ast.Span) (string, error) {
	if span.Start.Offset < 0 || span.End.Offset <= span.Start.Offset || span.End.Offset > len(b.SourceSQL) {
		return "", fmt.Errorf("query source span is unavailable")
	}
	return strings.TrimSpace(b.SourceSQL[span.Start.Offset:span.End.Offset]), nil
}

// appendUniqueFold 保持 ACL 声明顺序并拒绝大小写等价重复项。
func appendUniqueFold(dst *[]string, value, label string) error {
	for _, current := range *dst {
		if strings.EqualFold(current, value) {
			return fmt.Errorf("duplicate %s %q", label, value)
		}
	}
	*dst = append(*dst, value)
	return nil
}

// bindCopyOptions 只开放当前 Export 可以直接表达且不需要第二套状态机的 Stable option 子集。
func bindCopyOptions(destination string, options []ast.CopyOption) (BoundCopyOptions, error) {
	out := inferCopyOptions(destination)
	seen := make(map[string]struct{}, len(options))
	var csvSpecific bool
	for _, option := range options {
		name := strings.ToLower(option.Name.Name)
		if _, ok := seen[name]; ok {
			return BoundCopyOptions{}, fmt.Errorf("duplicate COPY option %s", option.Name.Name)
		}
		seen[name] = struct{}{}
		switch name {
		case "force_quote":
			value, err := copyStringLike(option.Value)
			if err != nil || value != "*" {
				return BoundCopyOptions{}, fmt.Errorf("FORCE_QUOTE requires *")
			}
			out.ForceQuote = true
			csvSpecific = true
		case "format":
			value, err := copyStringLike(option.Value)
			if err != nil {
				return BoundCopyOptions{}, fmt.Errorf("FORMAT: %w", err)
			}
			switch strings.ToLower(value) {
			case "csv":
				out.Format = CopyCSV
			case "json":
				out.Format = CopyJSON
			case "ndjson":
				out.Format = CopyNDJSON
			case "parquet":
				out.Format = CopyParquet
			default:
				return BoundCopyOptions{}, fmt.Errorf("unsupported COPY FORMAT %q", value)
			}
		case "compression":
			value, err := copyStringLike(option.Value)
			if err != nil {
				return BoundCopyOptions{}, fmt.Errorf("COMPRESSION: %w", err)
			}
			value = strings.ToLower(value)
			if value == "none" || value == "" {
				value = "identity"
			}
			if value != "identity" && value != "gzip" && value != "zstd" && value != "snappy" {
				return BoundCopyOptions{}, fmt.Errorf("unsupported COPY COMPRESSION %q", value)
			}
			out.Compression = value
		case "header":
			value, err := copyBool(option.Value, true)
			if err != nil {
				return BoundCopyOptions{}, fmt.Errorf("HEADER: %w", err)
			}
			out.Header = value
			csvSpecific = true
		case "delimiter":
			value, err := copyRequiredString(option.Value)
			if err != nil || len(value) != 1 {
				return BoundCopyOptions{}, fmt.Errorf("DELIMITER requires exactly one byte string")
			}
			out.Delimiter = value[0]
			csvSpecific = true
		case "quote":
			value, err := copyRequiredString(option.Value)
			if err != nil || len(value) != 1 {
				return BoundCopyOptions{}, fmt.Errorf("QUOTE requires exactly one byte string")
			}
			out.Quote = value[0]
			csvSpecific = true
		default:
			return BoundCopyOptions{}, fmt.Errorf("unsupported COPY option %s", option.Name.Name)
		}
	}
	if out.Compression == "" {
		out.Compression = "identity"
	}
	if out.Format == CopyParquet && out.Compression != "identity" && out.Compression != "snappy" {
		return BoundCopyOptions{}, fmt.Errorf("Parquet whole-file compression %q is unsupported", out.Compression)
	}
	if out.Format != CopyParquet && out.Compression == "snappy" {
		return BoundCopyOptions{}, fmt.Errorf("snappy is only valid for Parquet")
	}
	if csvSpecific && out.Format != CopyCSV {
		return BoundCopyOptions{}, fmt.Errorf("HEADER/DELIMITER/QUOTE are only valid for CSV output")
	}
	if out.Delimiter == out.Quote {
		return BoundCopyOptions{}, fmt.Errorf("CSV DELIMITER and QUOTE must differ")
	}
	return out, nil
}

// inferCopyOptions 按 DuckDB-compatible extension rules 推断格式与压缩；未知扩展默认 CSV。
func inferCopyOptions(destination string) BoundCopyOptions {
	path := destination
	if u, err := url.Parse(destination); err == nil && u.Path != "" {
		path = u.Path
	}
	lower := strings.ToLower(path)
	compression := "identity"
	for _, suffix := range []struct {
		ext string
		cmp string
	}{{".gzip", "gzip"}, {".gz", "gzip"}, {".zstd", "zstd"}, {".zst", "zstd"}} {
		if strings.HasSuffix(lower, suffix.ext) {
			compression = suffix.cmp
			lower = strings.TrimSuffix(lower, suffix.ext)
			break
		}
	}
	format := CopyCSV
	switch strings.ToLower(filepath.Ext(lower)) {
	case ".parquet":
		format = CopyParquet
	case ".json":
		format = CopyJSON
	case ".ndjson":
		format = CopyNDJSON
	case ".csv":
		format = CopyCSV
	}
	return BoundCopyOptions{Format: format, Compression: compression, Header: true, Delimiter: ',', Quote: '"'}
}

// copyStringLike 接受 identifier/string 形式的枚举 option value。
func copyStringLike(expr ast.Expr) (string, error) {
	if expr == nil {
		return "", fmt.Errorf("value is required")
	}
	switch v := expr.(type) {
	case *ast.LiteralExpr:
		if v.Kind == ast.LiteralString {
			return v.Value, nil
		}
	case *ast.NameExpr:
		if len(v.Name.Parts) == 1 {
			return v.Name.Parts[0].Name, nil
		}
	}
	return "", fmt.Errorf("value must be identifier or string literal")
}

// copyRequiredString 只接受 string literal，避免 delimiter/quote 名称被误解释为字符。
func copyRequiredString(expr ast.Expr) (string, error) {
	v, ok := expr.(*ast.LiteralExpr)
	if !ok || v.Kind != ast.LiteralString {
		return "", fmt.Errorf("string literal is required")
	}
	return v.Value, nil
}

// copyBool 解析 DuckDB COPY 常见布尔写法；nil flag 使用 flagDefault。
func copyBool(expr ast.Expr, flagDefault bool) (bool, error) {
	if expr == nil {
		return flagDefault, nil
	}
	switch v := expr.(type) {
	case *ast.LiteralExpr:
		switch v.Kind {
		case ast.LiteralBool:
			return strings.EqualFold(v.Value, "true"), nil
		case ast.LiteralInteger:
			n, err := strconv.Atoi(v.Value)
			if err == nil && (n == 0 || n == 1) {
				return n == 1, nil
			}
		}
	case *ast.NameExpr:
		if len(v.Name.Parts) == 1 {
			switch strings.ToLower(v.Name.Parts[0].Name) {
			case "on", "true":
				return true, nil
			case "off", "false":
				return false, nil
			}
		}
	}
	return false, fmt.Errorf("boolean value must be true/false, on/off, 1/0, or omitted")
}

// errorAt 构造 statement-level 带源位置诊断，消息不得包含 secret credential value。
func (b *StatementBinder) errorAt(span ast.Span, message string) error {
	return fmt.Errorf("statement binder %d:%d: %s", span.Start.Line, span.Start.Column, message)
}
