package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

const sqliteCatalogDDL = `CREATE TABLE IF NOT EXISTS fssql_catalog_objects (
	schema_name TEXT NOT NULL,
	object_name TEXT NOT NULL,
	kind INTEGER NOT NULL,
	payload TEXT NOT NULL,
	PRIMARY KEY (schema_name, object_name)
)`

const maxSQLiteCatalogPayloadBytes = 1 << 20

// SQLiteCatalog 使用 database/sql 承载持久化 metadata；具体 SQLite driver 由 engine adapter 注入。
type SQLiteCatalog struct {
	db             *sql.DB // db 是由 Catalog 独占并配置好的 SQLite database/sql handle。
	maxListEntries int     // maxListEntries 限制单次 List 在 Go heap 上物化的 metadata 数量。
}

// NewSQLiteCatalog 初始化 SQLite metadata schema 并返回 Catalog adapter。
// db 由 Catalog 独占；maxOpenConns 和 maxListEntries 必须显式给出以避免无界控制面资源。
func NewSQLiteCatalog(ctx context.Context, db *sql.DB, maxOpenConns, maxListEntries int) (*SQLiteCatalog, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite catalog db is nil")
	}
	if maxOpenConns <= 0 || maxListEntries <= 0 {
		return nil, fmt.Errorf("sqlite catalog limits must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns)
	if _, err := db.ExecContext(ctx, sqliteCatalogDDL); err != nil {
		return nil, fmt.Errorf("initialize sqlite catalog: %w", err)
	}
	return &SQLiteCatalog{db: db, maxListEntries: maxListEntries}, nil
}

// Begin 开启独立 SQLite Catalog Transaction。
func (c *SQLiteCatalog) Begin(ctx context.Context) (Transaction, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("sqlite catalog is nil")
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin sqlite catalog transaction: %w", err)
	}
	return &sqliteTx{tx: tx, maxListEntries: c.maxListEntries}, nil
}

// sqliteTx 是 SQLiteCatalog 的 database/sql transaction adapter。
type sqliteTx struct {
	tx             *sql.Tx // tx 是唯一 catalog transaction ownership。
	maxListEntries int     // maxListEntries 限制当前事务 List 结果物化上限。
	closed         bool    // closed 防止 Commit/Rollback 后继续访问。
}

// Get 按 schema/object 主键读取一个 metadata entry。
func (t *sqliteTx) Get(ctx context.Context, name ObjectName) (Entry, bool, error) {
	if err := t.check(ctx); err != nil {
		return Entry{}, false, err
	}
	name.Schema = normalizeSchema(name.Schema)
	if _, err := objectKey(name); err != nil {
		return Entry{}, false, err
	}
	var kind int
	var payload string
	err := t.tx.QueryRowContext(ctx,
		`SELECT kind, payload FROM fssql_catalog_objects WHERE schema_name = ? AND object_name = ?`,
		name.Schema, name.Name,
	).Scan(&kind, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("read sqlite catalog object: %w", err)
	}
	entry, err := decodeEntry(ObjectKind(kind), []byte(payload))
	if err != nil {
		return Entry{}, false, err
	}
	return entry, true, nil
}

// List 按 object_name 确定性返回指定 schema 中的 metadata，并硬限制 Go heap 物化数量。
func (t *sqliteTx) List(ctx context.Context, schema string) ([]Entry, error) {
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	schema = normalizeSchema(schema)
	rows, err := t.tx.QueryContext(ctx,
		`SELECT kind, payload FROM fssql_catalog_objects WHERE schema_name = ? ORDER BY object_name LIMIT ?`,
		schema, t.maxListEntries+1,
	)
	if err != nil {
		return nil, fmt.Errorf("list sqlite catalog objects: %w", err)
	}
	defer rows.Close()
	out := make([]Entry, 0, minInt(t.maxListEntries, 64))
	for rows.Next() {
		if len(out) >= t.maxListEntries {
			return nil, fmt.Errorf("sqlite catalog list limit exceeded: %d", t.maxListEntries)
		}
		var kind int
		var payload string
		if err := rows.Scan(&kind, &payload); err != nil {
			return nil, fmt.Errorf("scan sqlite catalog object: %w", err)
		}
		entry, err := decodeEntry(ObjectKind(kind), []byte(payload))
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite catalog objects: %w", err)
	}
	return out, nil
}

// CreateExternalTable 持久化 external table metadata；credential 不进入 payload。
func (t *sqliteTx) CreateExternalTable(ctx context.Context, table ExternalTable) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	table.Name.Schema = normalizeSchema(table.Name.Schema)
	if err := validateExternalTable(table); err != nil {
		return err
	}
	return t.insert(ctx, table.Name, Entry{Kind: ObjectExternalTable, ExternalTable: &table})
}

// CreateView 持久化 view SQL 与创建时绑定的输出 schema。
func (t *sqliteTx) CreateView(ctx context.Context, view View) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	view.Name.Schema = normalizeSchema(view.Name.Schema)
	if err := validateView(view); err != nil {
		return err
	}
	return t.insert(ctx, view.Name, Entry{Kind: ObjectView, View: &view})
}

func (t *sqliteTx) ReplaceView(ctx context.Context, view View) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	view.Name.Schema = normalizeSchema(view.Name.Schema)
	if err := validateView(view); err != nil {
		return err
	}
	entry, exists, err := t.Get(ctx, view.Name)
	if err != nil {
		return err
	}
	if exists && entry.Kind != ObjectView {
		return fmt.Errorf("catalog replacement target is not a view")
	}
	if !exists {
		return t.CreateView(ctx, view)
	}
	payload, err := json.Marshal(cloneEntry(Entry{Kind: ObjectView, View: &view}))
	if err != nil {
		return err
	}
	if len(payload) > maxSQLiteCatalogPayloadBytes {
		return fmt.Errorf("sqlite catalog payload exceeds limit")
	}
	_, err = t.tx.ExecContext(ctx, `UPDATE fssql_catalog_objects SET kind=?, payload=? WHERE schema_name=? AND object_name=?`, ObjectView, string(payload), view.Name.Schema, view.Name.Name)
	return err
}

// insert 编码不可变 metadata 并依赖 SQLite 主键约束拒绝重复对象。
func (t *sqliteTx) insert(ctx context.Context, name ObjectName, entry Entry) error {
	payload, err := json.Marshal(cloneEntry(entry))
	if err != nil {
		return fmt.Errorf("encode sqlite catalog object: %w", err)
	}
	if len(payload) > maxSQLiteCatalogPayloadBytes {
		return fmt.Errorf("sqlite catalog payload exceeds %d bytes", maxSQLiteCatalogPayloadBytes)
	}
	if _, err := t.tx.ExecContext(ctx,
		`INSERT INTO fssql_catalog_objects(schema_name, object_name, kind, payload) VALUES(?, ?, ?, ?)`,
		name.Schema, name.Name, int(entry.Kind), string(payload),
	); err != nil {
		return fmt.Errorf("create sqlite catalog object: %w", err)
	}
	return nil
}

// Commit 提交 SQLite Catalog Transaction。
func (t *sqliteTx) Commit(ctx context.Context) error {
	if err := t.check(ctx); err != nil {
		return err
	}
	if err := t.tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite catalog transaction: %w", err)
	}
	t.closed = true
	t.tx = nil
	return nil
}

// Rollback 回滚 SQLite Catalog Transaction；context 已取消时仍执行释放以避免连接/事务泄漏。
func (t *sqliteTx) Rollback(ctx context.Context) error {
	if err := t.checkOpen(); err != nil {
		return err
	}
	err := t.tx.Rollback()
	t.closed = true
	t.tx = nil
	if err != nil && !errors.Is(err, sql.ErrTxDone) {
		return fmt.Errorf("rollback sqlite catalog transaction: %w", err)
	}
	return nil
}

// check 验证 transaction ownership 与 context 状态。
func (t *sqliteTx) check(ctx context.Context) error {
	if err := t.checkOpen(); err != nil {
		return err
	}
	return ctx.Err()
}

// checkOpen 只验证 transaction ownership；供取消后的 Rollback 清理路径使用。
func (t *sqliteTx) checkOpen() error {
	if t == nil || t.tx == nil {
		if t != nil && t.closed {
			return fmt.Errorf("catalog transaction is closed")
		}
		return fmt.Errorf("sqlite catalog transaction is nil")
	}
	if t.closed {
		return fmt.Errorf("catalog transaction is closed")
	}
	return nil
}

// decodeEntry 解码并验证持久化 payload，防止 kind/payload 不一致静默进入 Catalog。
func decodeEntry(kind ObjectKind, payload []byte) (Entry, error) {
	if len(payload) > maxSQLiteCatalogPayloadBytes {
		return Entry{}, fmt.Errorf("sqlite catalog payload exceeds %d bytes", maxSQLiteCatalogPayloadBytes)
	}
	var entry Entry
	if err := json.Unmarshal(payload, &entry); err != nil {
		return Entry{}, fmt.Errorf("decode sqlite catalog object: %w", err)
	}
	if entry.Kind != kind {
		return Entry{}, fmt.Errorf("sqlite catalog kind mismatch: column=%d payload=%d", kind, entry.Kind)
	}
	switch kind {
	case ObjectExternalTable:
		if entry.ExternalTable == nil || entry.View != nil {
			return Entry{}, fmt.Errorf("invalid external table payload")
		}
		if err := validateExternalTable(*entry.ExternalTable); err != nil {
			return Entry{}, fmt.Errorf("invalid external table payload: %w", err)
		}
	case ObjectView:
		if entry.View == nil || entry.ExternalTable != nil {
			return Entry{}, fmt.Errorf("invalid view payload")
		}
		if err := validateView(*entry.View); err != nil {
			return Entry{}, fmt.Errorf("invalid view payload: %w", err)
		}
	default:
		return Entry{}, fmt.Errorf("unknown sqlite catalog object kind %d", kind)
	}
	return cloneEntry(entry), nil
}

// minInt 返回较小整数，用于控制小型控制面预分配。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
