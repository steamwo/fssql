package catalog

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/steamwo/fssql/internal/core/types"
)

// ObjectKind 定义 Catalog 中的 v1 metadata 对象类别。
type ObjectKind uint8

const (
	ObjectInvalid ObjectKind = iota
	ObjectExternalTable
	ObjectView
)

// ObjectName 是 Catalog 内部已规范化的 schema/object 名称。
type ObjectName struct {
	Schema string // Schema 是已由 Binder 规范化的 schema 名；空值按 main 处理。
	Name   string // Name 是已由 Binder 规范化的对象名。
}

// ColumnDefinition 描述 catalog-visible relation column。
type ColumnDefinition struct {
	Name     string            // Name 是已规范化列名。
	Type     types.LogicalType // Type 是列的 logical type。
	Nullable bool              // Nullable 表示该列允许 NULL。
}

// ExternalTable 保存 CREATE EXTERNAL TABLE 所需的控制面 metadata。
type ExternalTable struct {
	Name          ObjectName         // Name 是对象名称。
	Columns       []ColumnDefinition // Columns 保存已声明/绑定 schema。
	TableFunction string             // TableFunction 是 read_parquet/read_csv 等稳定函数名。
	Source        string             // Source 是 canonical source URI/spec；不保存 credential。
}

// View 保存 CREATE VIEW 的 SQL 与绑定输出 schema。
type View struct {
	Name    ObjectName         // Name 是对象名称。
	SQL     string             // SQL 是创建 view 时保存的原始查询文本。
	Columns []ColumnDefinition // Columns 是创建时绑定得到的输出 schema。
}

// Entry 是 Catalog transaction 读写的不可变 metadata 快照。
type Entry struct {
	Kind          ObjectKind     // Kind 区分 external table/view。
	ExternalTable *ExternalTable // ExternalTable 仅对应 ObjectExternalTable。
	View          *View          // View 仅对应 ObjectView。
}

// Transaction 定义独立 Catalog Transaction；它不与 statement SourceSnapshot/Export commit 混用。
type Transaction interface {
	Get(context.Context, ObjectName) (Entry, bool, error)
	List(context.Context, string) ([]Entry, error)
	CreateExternalTable(context.Context, ExternalTable) error
	CreateView(context.Context, View) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

// Catalog 是 MemoryCatalog/SQLiteCatalog 共享的 metadata transaction contract。
type Catalog interface {
	Begin(context.Context) (Transaction, error)
}

// MemoryCatalog 是有容量上限的线程安全内存 Catalog。
type MemoryCatalog struct {
	mu         sync.RWMutex     // mu 保护 version 与 entries。
	version    uint64           // version 用于 optimistic transaction 冲突检测。
	maxEntries int              // maxEntries 限制 metadata map 的最大对象数，避免无界 Go heap 增长。
	entries    map[string]Entry // entries 保存 committed metadata 的深拷贝。
}

// NewMemoryCatalog 创建 bounded MemoryCatalog。
func NewMemoryCatalog(maxEntries int) (*MemoryCatalog, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("memory catalog max entries must be positive")
	}
	return &MemoryCatalog{maxEntries: maxEntries, entries: make(map[string]Entry)}, nil
}

// Begin 创建 repeatable-read metadata snapshot；写事务提交时使用版本冲突检测。
func (c *MemoryCatalog) Begin(ctx context.Context) (Transaction, error) {
	if c == nil {
		return nil, fmt.Errorf("memory catalog is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := make(map[string]Entry, len(c.entries))
	for k, v := range c.entries {
		snap[k] = cloneEntry(v)
	}
	return &memoryTx{catalog: c, baseVersion: c.version, snapshot: snap, writes: make(map[string]Entry)}, nil
}

// memoryTx 是 MemoryCatalog 的 optimistic metadata transaction。
type memoryTx struct {
	catalog     *MemoryCatalog   // catalog 是 commit target。
	baseVersion uint64           // baseVersion 是 Begin 时的 committed 版本。
	snapshot    map[string]Entry // snapshot 是 repeatable-read 基线。
	writes      map[string]Entry // writes 保存本事务新增对象。
	closed      bool             // closed 防止 Commit/Rollback 后继续使用。
}

// Get 从事务 overlay/snapshot 读取对象。
func (t *memoryTx) Get(ctx context.Context, name ObjectName) (Entry, bool, error) {
	if err := t.check(ctx); err != nil {
		return Entry{}, false, err
	}
	key, err := objectKey(name)
	if err != nil {
		return Entry{}, false, err
	}
	if v, ok := t.writes[key]; ok {
		return cloneEntry(v), true, nil
	}
	v, ok := t.snapshot[key]
	return cloneEntry(v), ok, nil
}

// List 返回指定 schema 的 transaction-visible metadata 快照，并按名称确定性排序。
func (t *memoryTx) List(ctx context.Context, schema string) ([]Entry, error) {
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	schema = normalizeSchema(schema)
	merged := make(map[string]Entry, len(t.snapshot)+len(t.writes))
	for k, v := range t.snapshot {
		merged[k] = v
	}
	for k, v := range t.writes {
		merged[k] = v
	}
	out := make([]Entry, 0, len(merged))
	for _, v := range merged {
		n := entryName(v)
		if normalizeSchema(n.Schema) == schema {
			out = append(out, cloneEntry(v))
		}
	}
	sort.Slice(out, func(i, j int) bool { return objectNameLess(entryName(out[i]), entryName(out[j])) })
	return out, nil
}

// CreateExternalTable 在当前事务创建 external table metadata。
func (t *memoryTx) CreateExternalTable(ctx context.Context, table ExternalTable) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	table.Name.Schema = normalizeSchema(table.Name.Schema)
	if err := validateExternalTable(table); err != nil {
		return err
	}
	return t.create(table.Name, Entry{Kind: ObjectExternalTable, ExternalTable: &table})
}

// CreateView 在当前事务创建 view metadata。
func (t *memoryTx) CreateView(ctx context.Context, view View) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	view.Name.Schema = normalizeSchema(view.Name.Schema)
	if err := validateView(view); err != nil {
		return err
	}
	return t.create(view.Name, Entry{Kind: ObjectView, View: &view})
}

// create 将对象加入 transaction write set，并拒绝 snapshot/overlay 中重复名称。
func (t *memoryTx) create(name ObjectName, entry Entry) error {
	key, err := objectKey(name)
	if err != nil {
		return err
	}
	if _, ok := t.writes[key]; ok {
		return fmt.Errorf("catalog object %s already exists", key)
	}
	if _, ok := t.snapshot[key]; ok {
		return fmt.Errorf("catalog object %s already exists", key)
	}
	t.writes[key] = cloneEntry(entry)
	return nil
}

// Commit 原子提交 write set；并发 committed version 改变时拒绝 stale writer。
func (t *memoryTx) Commit(ctx context.Context) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	c := t.catalog
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(t.writes) > 0 && c.version != t.baseVersion {
		return fmt.Errorf("catalog transaction conflict: base=%d current=%d", t.baseVersion, c.version)
	}
	newCount := len(c.entries) + len(t.writes)
	if newCount > c.maxEntries {
		return fmt.Errorf("memory catalog capacity exceeded: %d > %d", newCount, c.maxEntries)
	}
	for k, v := range t.writes {
		if _, exists := c.entries[k]; exists {
			return fmt.Errorf("catalog object %s already exists", k)
		}
		c.entries[k] = cloneEntry(v)
	}
	if len(t.writes) > 0 {
		c.version++
	}
	t.closed = true
	t.release()
	return nil
}

// Rollback 丢弃 transaction snapshot/write set。
func (t *memoryTx) Rollback(ctx context.Context) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	t.closed = true
	t.release()
	return nil
}

// check 验证 transaction 状态和 context。
func (t *memoryTx) check(ctx context.Context) error {
	if t == nil || t.catalog == nil {
		return fmt.Errorf("catalog transaction is nil")
	}
	if t.closed {
		return fmt.Errorf("catalog transaction is closed")
	}
	return ctx.Err()
}

// release 主动释放 transaction map 引用，避免长生命周期控制面对象持有 metadata。
func (t *memoryTx) release() { t.snapshot = nil; t.writes = nil }

// objectKey 构造不会歧义的 schema/object 内部 key。
func objectKey(name ObjectName) (string, error) {
	schema := normalizeSchema(name.Schema)
	if strings.TrimSpace(name.Name) == "" {
		return "", fmt.Errorf("catalog object name is empty")
	}
	return schema + "\x00" + name.Name, nil
}

// normalizeSchema 将空 schema 规范化为 v1 默认 main schema。
func normalizeSchema(schema string) string {
	if schema == "" {
		return "main"
	}
	return schema
}

// objectNameLess 为 Catalog 列表提供确定性排序。
func objectNameLess(a, b ObjectName) bool {
	as, bs := normalizeSchema(a.Schema), normalizeSchema(b.Schema)
	if as != bs {
		return as < bs
	}
	return a.Name < b.Name
}

// entryName 返回 metadata entry 的对象名。
func entryName(e Entry) ObjectName {
	if e.ExternalTable != nil {
		return e.ExternalTable.Name
	}
	if e.View != nil {
		return e.View.Name
	}
	return ObjectName{}
}

// validateExternalTable 验证 external table metadata 不携带无效/空字段。
func validateExternalTable(t ExternalTable) error {
	if _, err := objectKey(t.Name); err != nil {
		return err
	}
	if strings.TrimSpace(t.TableFunction) == "" {
		return fmt.Errorf("external table function is empty")
	}
	if strings.TrimSpace(t.Source) == "" {
		return fmt.Errorf("external table source is empty")
	}
	return validateColumns(t.Columns)
}

// validateView 验证 view metadata 与已绑定输出 schema。
func validateView(v View) error {
	if _, err := objectKey(v.Name); err != nil {
		return err
	}
	if strings.TrimSpace(v.SQL) == "" {
		return fmt.Errorf("view SQL is empty")
	}
	return validateColumns(v.Columns)
}

// validateColumns 拒绝空名、Invalid 类型和重复列。
func validateColumns(cols []ColumnDefinition) error {
	seen := make(map[string]struct{}, len(cols))
	for _, c := range cols {
		if c.Name == "" {
			return fmt.Errorf("column name is empty")
		}
		if c.Type.ID == types.Invalid {
			return fmt.Errorf("column %s has invalid type", c.Name)
		}
		k := strings.ToLower(c.Name)
		if _, ok := seen[k]; ok {
			return fmt.Errorf("duplicate column %s", c.Name)
		}
		seen[k] = struct{}{}
	}
	return nil
}

// cloneEntry 深拷贝 metadata，防止 caller 修改 committed Catalog 状态。
func cloneEntry(e Entry) Entry {
	out := Entry{Kind: e.Kind}
	if e.ExternalTable != nil {
		x := *e.ExternalTable
		x.Columns = cloneColumns(x.Columns)
		out.ExternalTable = &x
	}
	if e.View != nil {
		x := *e.View
		x.Columns = cloneColumns(x.Columns)
		out.View = &x
	}
	return out
}

// cloneColumns 深拷贝列定义及其复杂 LogicalType。
func cloneColumns(in []ColumnDefinition) []ColumnDefinition {
	if in == nil {
		return nil
	}
	out := make([]ColumnDefinition, len(in))
	for i, c := range in {
		out[i] = c
		out[i].Type = cloneType(c.Type)
	}
	return out
}

// cloneType 递归复制 LIST/STRUCT logical metadata，避免事务快照共享可变 slice/pointer。
func cloneType(t types.LogicalType) types.LogicalType {
	out := t
	if t.Child != nil {
		child := cloneType(*t.Child)
		out.Child = &child
	}
	if t.Fields != nil {
		out.Fields = make([]types.StructField, len(t.Fields))
		for i, f := range t.Fields {
			out.Fields[i] = f
			out.Fields[i].Type = cloneType(f.Type)
		}
	}
	return out
}
