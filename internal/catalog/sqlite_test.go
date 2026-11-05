package catalog

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/steamwo/fssql/internal/core/types"
)

var fakeCatalogDriverSeq atomic.Uint64

// fakeCatalogRow 模拟持久化 SQLite catalog row。
type fakeCatalogRow struct {
	schema  string
	name    string
	kind    int64
	payload string
}

// fakeCatalogStore 是 test-only database/sql driver 的 committed row store。
type fakeCatalogStore struct {
	mu   sync.Mutex
	rows map[string]fakeCatalogRow
}

// fakeCatalogDriver 为 SQLiteCatalog 测试提供真实 database/sql transaction 调用路径。
type fakeCatalogDriver struct{ store *fakeCatalogStore }

func (d *fakeCatalogDriver) Open(string) (driver.Conn, error) {
	return &fakeCatalogConn{store: d.store}, nil
}

// fakeCatalogConn 实现 database/sql 所需连接和 context-aware query/exec。
type fakeCatalogConn struct {
	store  *fakeCatalogStore
	active *fakeCatalogTx
}

func (c *fakeCatalogConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *fakeCatalogConn) Close() error { return nil }
func (c *fakeCatalogConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeCatalogConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.active != nil {
		return nil, errors.New("nested transaction")
	}
	tx := &fakeCatalogTx{conn: c, pending: make(map[string]fakeCatalogRow)}
	c.active = tx
	return tx, nil
}

func (c *fakeCatalogConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.HasPrefix(strings.TrimSpace(query), "CREATE TABLE") {
		return driver.RowsAffected(0), nil
	}
	if strings.HasPrefix(strings.TrimSpace(query), "UPDATE fssql_catalog_objects") {
		if c.active == nil || len(args) != 4 {
			return nil, errors.New("invalid transactional update")
		}
		row := fakeCatalogRow{kind: args[0].Value.(int64), payload: args[1].Value.(string), schema: args[2].Value.(string), name: args[3].Value.(string)}
		key := row.schema + "\x00" + row.name
		if _, exists := c.visibleRow(key); !exists {
			return driver.RowsAffected(0), nil
		}
		if c.active.replaced == nil {
			c.active.replaced = make(map[string]bool)
		}
		c.active.replaced[key] = true
		c.active.pending[key] = row
		return driver.RowsAffected(1), nil
	}
	if strings.HasPrefix(strings.TrimSpace(query), "INSERT INTO") {
		if c.active == nil {
			return nil, errors.New("insert outside transaction")
		}
		if len(args) != 4 {
			return nil, fmt.Errorf("insert args=%d", len(args))
		}
		row := fakeCatalogRow{
			schema:  args[0].Value.(string),
			name:    args[1].Value.(string),
			kind:    args[2].Value.(int64),
			payload: args[3].Value.(string),
		}
		key := row.schema + "\x00" + row.name
		c.store.mu.Lock()
		_, committed := c.store.rows[key]
		c.store.mu.Unlock()
		if committed {
			return nil, errors.New("UNIQUE constraint failed")
		}
		if _, exists := c.active.pending[key]; exists {
			return nil, errors.New("UNIQUE constraint failed")
		}
		c.active.pending[key] = row
		return driver.RowsAffected(1), nil
	}
	return nil, fmt.Errorf("unsupported exec: %s", query)
}

func (c *fakeCatalogConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.Contains(query, "WHERE schema_name = ? AND object_name = ?") {
		if len(args) != 2 {
			return nil, fmt.Errorf("get args=%d", len(args))
		}
		key := args[0].Value.(string) + "\x00" + args[1].Value.(string)
		row, ok := c.visibleRow(key)
		if !ok {
			return &fakeCatalogRows{columns: []string{"kind", "payload"}}, nil
		}
		return &fakeCatalogRows{columns: []string{"kind", "payload"}, values: [][]driver.Value{{row.kind, row.payload}}}, nil
	}
	if strings.Contains(query, "WHERE schema_name = ? ORDER BY object_name LIMIT ?") {
		if len(args) != 2 {
			return nil, fmt.Errorf("list args=%d", len(args))
		}
		schema := args[0].Value.(string)
		limit := int(args[1].Value.(int64))
		rows := c.visibleRows(schema)
		sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
		if len(rows) > limit {
			rows = rows[:limit]
		}
		values := make([][]driver.Value, len(rows))
		for i, row := range rows {
			values[i] = []driver.Value{row.kind, row.payload}
		}
		return &fakeCatalogRows{columns: []string{"kind", "payload"}, values: values}, nil
	}
	return nil, fmt.Errorf("unsupported query: %s", query)
}

// visibleRow 合并 committed 与当前 transaction pending row。
func (c *fakeCatalogConn) visibleRow(key string) (fakeCatalogRow, bool) {
	if c.active != nil {
		if row, ok := c.active.pending[key]; ok {
			return row, true
		}
	}
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	row, ok := c.store.rows[key]
	return row, ok
}

// visibleRows 返回 schema 内 transaction-visible rows。
func (c *fakeCatalogConn) visibleRows(schema string) []fakeCatalogRow {
	merged := make(map[string]fakeCatalogRow)
	c.store.mu.Lock()
	for k, row := range c.store.rows {
		if row.schema == schema {
			merged[k] = row
		}
	}
	c.store.mu.Unlock()
	if c.active != nil {
		for k, row := range c.active.pending {
			if row.schema == schema {
				merged[k] = row
			}
		}
	}
	out := make([]fakeCatalogRow, 0, len(merged))
	for _, row := range merged {
		out = append(out, row)
	}
	return out
}

// fakeCatalogTx 模拟 transaction 的 commit/rollback ownership。
type fakeCatalogTx struct {
	replaced map[string]bool
	conn     *fakeCatalogConn
	pending  map[string]fakeCatalogRow
}

func (t *fakeCatalogTx) Commit() error {
	if t.conn.active != t {
		return driver.ErrBadConn
	}
	t.conn.store.mu.Lock()
	defer t.conn.store.mu.Unlock()
	for k := range t.pending {
		if _, exists := t.conn.store.rows[k]; exists && !t.replaced[k] {
			return errors.New("UNIQUE constraint failed")
		}
	}
	for k, row := range t.pending {
		t.conn.store.rows[k] = row
	}
	t.conn.active = nil
	return nil
}

func (t *fakeCatalogTx) Rollback() error {
	if t.conn.active != t {
		return driver.ErrBadConn
	}
	t.conn.active = nil
	t.pending = nil
	return nil
}

// fakeCatalogRows 是最小 database/sql driver.Rows 实现。
type fakeCatalogRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *fakeCatalogRows) Columns() []string { return r.columns }
func (r *fakeCatalogRows) Close() error      { return nil }
func (r *fakeCatalogRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func openFakeCatalogDB(t *testing.T) (*sql.DB, *fakeCatalogStore) {
	t.Helper()
	store := &fakeCatalogStore{rows: make(map[string]fakeCatalogRow)}
	name := fmt.Sprintf("fssql_catalog_test_%d", fakeCatalogDriverSeq.Add(1))
	sql.Register(name, &fakeCatalogDriver{store: store})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, store
}

func TestSQLiteCatalogPersistsExternalTableAndView(t *testing.T) {
	db, _ := openFakeCatalogDB(t)
	c, err := NewSQLiteCatalog(context.Background(), db, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := c.Begin(context.Background())
	if err := tx.CreateExternalTable(context.Background(), testExternalTable("z")); err != nil {
		t.Fatal(err)
	}
	view := View{
		Name:    ObjectName{Name: "a"},
		SQL:     "SELECT 1",
		Columns: []ColumnDefinition{{Name: "x", Type: types.LogicalType{ID: types.Integer}}},
	}
	if err := tx.CreateView(context.Background(), view); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(context.Background())
	got, ok, err := tx.Get(context.Background(), ObjectName{Name: "z"})
	if err != nil || !ok || got.ExternalTable.Name.Schema != "main" {
		t.Fatalf("get ok=%v err=%v got=%+v", ok, err, got)
	}
	list, err := tx.List(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].View == nil || list[0].View.Name.Name != "a" || list[1].ExternalTable == nil || list[1].ExternalTable.Name.Name != "z" {
		t.Fatalf("list=%+v", list)
	}
}

func TestSQLiteCatalogRollbackAndDuplicate(t *testing.T) {
	db, _ := openFakeCatalogDB(t)
	c, _ := NewSQLiteCatalog(context.Background(), db, 1, 8)
	tx, _ := c.Begin(context.Background())
	_ = tx.CreateExternalTable(context.Background(), testExternalTable("t"))
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(context.Background())
	if _, ok, _ := tx.Get(context.Background(), ObjectName{Name: "t"}); ok {
		t.Fatal("rolled back row became visible")
	}
	_ = tx.CreateExternalTable(context.Background(), testExternalTable("t"))
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(context.Background())
	if err := tx.CreateExternalTable(context.Background(), testExternalTable("t")); err == nil {
		t.Fatal("expected duplicate constraint error")
	}
	_ = tx.Rollback(context.Background())
}

func TestSQLiteCatalogListBound(t *testing.T) {
	db, _ := openFakeCatalogDB(t)
	c, _ := NewSQLiteCatalog(context.Background(), db, 1, 1)
	tx, _ := c.Begin(context.Background())
	_ = tx.CreateExternalTable(context.Background(), testExternalTable("a"))
	_ = tx.CreateExternalTable(context.Background(), testExternalTable("b"))
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx, _ = c.Begin(context.Background())
	if _, err := tx.List(context.Background(), "main"); err == nil {
		t.Fatal("expected bounded list error")
	}
}

func TestCatalogRollbackStillReleasesAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mem, _ := NewMemoryCatalog(2)
	tx, _ := mem.Begin(context.Background())
	cancel()
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("memory rollback after cancellation: %v", err)
	}

	db, _ := openFakeCatalogDB(t)
	sqlite, _ := NewSQLiteCatalog(context.Background(), db, 1, 8)
	tx, _ = sqlite.Begin(context.Background())
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("sqlite rollback after cancellation: %v", err)
	}
}

func TestSQLiteCatalogRejectsCorruptPayloadAndInvalidLimits(t *testing.T) {
	db, store := openFakeCatalogDB(t)
	if _, err := NewSQLiteCatalog(context.Background(), db, 0, 1); err == nil {
		t.Fatal("expected connection limit validation")
	}
	c, err := NewSQLiteCatalog(context.Background(), db, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.rows["main\x00bad"] = fakeCatalogRow{schema: "main", name: "bad", kind: int64(ObjectView), payload: "{"}
	store.mu.Unlock()
	tx, _ := c.Begin(context.Background())
	if _, _, err := tx.Get(context.Background(), ObjectName{Name: "bad"}); err == nil {
		t.Fatal("expected corrupt payload error")
	}
}

func TestSQLiteCatalogClosedTransactionRejectsUse(t *testing.T) {
	db, _ := openFakeCatalogDB(t)
	c, _ := NewSQLiteCatalog(context.Background(), db, 1, 8)
	tx, _ := c.Begin(context.Background())
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tx.Get(context.Background(), ObjectName{Name: "x"}); err == nil {
		t.Fatal("expected closed transaction error")
	}
}
