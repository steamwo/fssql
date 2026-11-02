package session

import (
	"context"
	"errors"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// newControlStatement 创建只持有 Catalog Transaction 的测试 Statement；control adapter 不需要启动 query task。
func newControlStatement(t *testing.T, c catalog.Catalog) *Statement {
	t.Helper()
	tx, err := c.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return &Statement{catalog: tx}
}

func TestSQLControlAdapterCommitsCreateView(t *testing.T) {
	memoryCatalog, err := catalog.NewMemoryCatalog(8)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewSQLControlAdapter(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	statement := newControlStatement(t, memoryCatalog)
	bound := &binder.BoundCreateView{
		Name:  binder.ObjectName{Schema: "main", Name: "v"},
		Query: &binder.BoundQuery{},
		SQL:   "SELECT a FROM t",
		Columns: []binder.RelationColumn{
			{Name: "a", Type: types.LogicalType{ID: types.BigInt}},
		},
	}
	if err := adapter.Apply(context.Background(), statement, bound); err != nil {
		t.Fatal(err)
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}

	read, err := memoryCatalog.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback(context.Background())
	entry, ok, err := read.Get(context.Background(), catalog.ObjectName{Schema: "main", Name: "v"})
	if err != nil || !ok || entry.View == nil {
		t.Fatalf("entry=%+v ok=%v err=%v", entry, ok, err)
	}
	if entry.View.SQL != "SELECT a FROM t" || len(entry.View.Columns) != 1 || entry.View.Columns[0].Type.ID != types.BigInt {
		t.Fatalf("view=%+v", entry.View)
	}
}

func TestSQLControlAdapterCommitsExternalTableRealBoundSchema(t *testing.T) {
	memoryCatalog, err := catalog.NewMemoryCatalog(8)
	if err != nil {
		t.Fatal(err)
	}
	adapter, _ := NewSQLControlAdapter(nil, nil)
	statement := newControlStatement(t, memoryCatalog)
	bound := &binder.BoundCreateExternalTable{
		Name:        binder.ObjectName{Schema: "main", Name: "logs"},
		Function:    "read_ndjson",
		Source:      "s3://bucket/logs.ndjson",
		RelationID:  "tf:read_ndjson:snapshot",
		Fingerprint: "snapshot-1",
		Columns: []binder.RelationColumn{
			{Name: "id", Type: types.LogicalType{ID: types.BigInt}},
			{Name: "message", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
		},
	}
	if err := adapter.Apply(context.Background(), statement, bound); err != nil {
		t.Fatal(err)
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}

	read, _ := memoryCatalog.Begin(context.Background())
	defer read.Rollback(context.Background())
	entry, ok, err := read.Get(context.Background(), catalog.ObjectName{Schema: "main", Name: "logs"})
	if err != nil || !ok || entry.ExternalTable == nil {
		t.Fatalf("entry=%+v ok=%v err=%v", entry, ok, err)
	}
	if entry.ExternalTable.TableFunction != "read_ndjson" || entry.ExternalTable.Source != "s3://bucket/logs.ndjson" || len(entry.ExternalTable.Columns) != 2 {
		t.Fatalf("external=%+v", entry.ExternalTable)
	}
}

func TestSQLControlAdapterCreatesScopedSecretWithoutStoringTypeAsCredential(t *testing.T) {
	manager := security.NewSecretManager(nil)
	adapter, err := NewSQLControlAdapter(manager, []string{"s3", "sftp"})
	if err != nil {
		t.Fatal(err)
	}
	memoryCatalog, _ := catalog.NewMemoryCatalog(4)
	statement := newControlStatement(t, memoryCatalog)

	sql := "CREATE SECRET prod (TYPE s3, key_id 'id-1', secret_key 'secret-1', SUBJECT 'alice', SCHEME 's3', HOST 'data.example.com')"
	parsed, err := parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := binder.NewStatementBinder(nil, nil, nil, sql).Bind(context.Background(), parsed)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(context.Background(), statement, bound); err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.Resolve("prod", "alice", "s3://data.example.com/file")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Type() != "s3" {
		t.Fatalf("TYPE metadata=%q, want s3", resolved.Type())
	}
	if _, ok := resolved.Value("type"); ok {
		t.Fatal("TYPE metadata leaked into credential values")
	}
	if value, ok := resolved.Value("secret_key"); !ok || value != "secret-1" {
		t.Fatalf("secret_key=%q ok=%v", value, ok)
	}
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLControlAdapterRejectsUnsupportedSecretTypeBeforeCreate(t *testing.T) {
	manager := security.NewSecretManager(nil)
	adapter, err := NewSQLControlAdapter(manager, []string{"s3"})
	if err != nil {
		t.Fatal(err)
	}
	memoryCatalog, _ := catalog.NewMemoryCatalog(4)
	statement := newControlStatement(t, memoryCatalog)
	defer statement.Close()

	sql := "CREATE SECRET prod (TYPE unknown, key 'secret', SUBJECT 'alice')"
	parsed, _ := parser.Parse(sql)
	bound, err := binder.NewStatementBinder(nil, nil, nil, sql).Bind(context.Background(), parsed)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Apply(context.Background(), statement, bound); err == nil {
		t.Fatal("expected unsupported TYPE error")
	}
	if _, err := manager.Resolve("prod", "alice", ""); !errors.Is(err, security.ErrSecretNotFound) {
		t.Fatalf("resolve error=%v, want ErrSecretNotFound", err)
	}
}

func TestSQLControlAdapterRejectsDataStatements(t *testing.T) {
	adapter, _ := NewSQLControlAdapter(nil, nil)
	memoryCatalog, _ := catalog.NewMemoryCatalog(4)
	statement := newControlStatement(t, memoryCatalog)
	defer statement.Close()

	bound := &binder.BoundQueryStatement{Query: &binder.BoundQuery{}}
	if err := adapter.Apply(context.Background(), statement, bound); err == nil {
		t.Fatal("SELECT must not execute through control adapter")
	}
}

func TestNewSQLControlAdapterBoundsSecretTypeRegistry(t *testing.T) {
	if _, err := NewSQLControlAdapter(nil, []string{"s3", "S3"}); err == nil {
		t.Fatal("expected duplicate secret type rejection")
	}
	many := make([]string, maxSecretTypes+1)
	for i := range many {
		many[i] = "type-" + string(rune('a'+i%26))
	}
	if _, err := NewSQLControlAdapter(nil, many); err == nil {
		t.Fatal("expected secret type capacity rejection")
	}
}
