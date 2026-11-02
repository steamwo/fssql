package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/security"
)

type statementNoopSink struct{}

func (statementNoopSink) Consume(context.Context, *vector.DataChunk) error { return nil }
func (statementNoopSink) Finalize(context.Context) error                  { return nil }

var _ pipeline.Sink = statementNoopSink{}

type describeStatementSink struct {
	names    []string
	nullable []string
}

func (s *describeStatementSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	for row := 0; row < chunk.Cardinality; row++ {
		name, _ := chunk.Columns[0].String(row)
		nullable, _ := chunk.Columns[2].String(row)
		s.names = append(s.names, name)
		s.nullable = append(s.nullable, nullable)
	}
	return nil
}
func (*describeStatementSink) Finalize(context.Context) error { return nil }

func newStatementEngineHarness(t *testing.T) (*StableStatementEngine, *catalog.MemoryCatalog, *security.SecretManager, *memory.Manager, *memory.BufferManager) {
	t.Helper()
	manager := memory.NewManager(4 << 20)
	buffers := memory.NewBufferManager(nil)
	selects := newSQLTestEngine(t, buffers)
	cat, ok := selects.config.Catalog.(*catalog.MemoryCatalog)
	if !ok {
		t.Fatalf("catalog=%T want *catalog.MemoryCatalog", selects.config.Catalog)
	}
	secrets := security.NewSecretManager(nil)
	control, err := NewSQLControlAdapter(secrets, []string{"s3"})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewStableStatementEngine(selects, control)
	if err != nil {
		t.Fatal(err)
	}
	return engine, cat, secrets, manager, buffers
}

// TestStableStatementEngineCreateExternalTableCommitsOnlyOnRun 验证 planning 不产生 Catalog 副作用，Run 才提交 metadata。
func TestStableStatementEngineCreateExternalTableCommitsOnlyOnRun(t *testing.T) {
	engine, cat, _, manager, _ := newStatementEngineHarness(t)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "CREATE EXTERNAL TABLE ext USING read_csv('mem://rows.csv')")
	if err != nil {
		t.Fatal(err)
	}
	before, err := cat.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := before.Get(context.Background(), catalog.ObjectName{Schema: "main", Name: "ext"}); err != nil || ok {
		_ = before.Rollback(context.Background())
		t.Fatalf("catalog mutated during Prepare: ok=%v err=%v", ok, err)
	}
	if err := before.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, statementNoopSink{}); err != nil {
		t.Fatal(err)
	}
	after, err := cat.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	entry, ok, err := after.Get(context.Background(), catalog.ObjectName{Schema: "main", Name: "ext"})
	if err != nil || !ok || entry.ExternalTable == nil {
		_ = after.Rollback(context.Background())
		t.Fatalf("committed external table missing: ok=%v entry=%+v err=%v", ok, entry, err)
	}
	if entry.ExternalTable.TableFunction != "read_csv" || entry.ExternalTable.Source != "mem://rows.csv" || len(entry.ExternalTable.Columns) != 1 {
		t.Fatalf("entry=%+v", entry.ExternalTable)
	}
	if err := after.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

// TestStableStatementEngineDescribeRelationPreservesNullability 验证 relation form 使用真实 Catalog/Bind nullability，而不是 query form 的统一 YES。
func TestStableStatementEngineDescribeRelationPreservesNullability(t *testing.T) {
	engine, _, _, manager, _ := newStatementEngineHarness(t)
	createQ := query.New(context.Background(), manager)
	create, err := engine.Prepare(context.Background(), createQ, "CREATE EXTERNAL TABLE ext USING read_csv('mem://rows.csv')")
	if err != nil {
		t.Fatal(err)
	}
	if err := create.Run(createQ, statementNoopSink{}); err != nil {
		t.Fatal(err)
	}
	if err := createQ.Close(); err != nil {
		t.Fatal(err)
	}

	describeQ := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), describeQ, "DESCRIBE ext")
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared.Output()) != 6 {
		t.Fatalf("DESCRIBE output columns=%d want=6", len(prepared.Output()))
	}
	sink := &describeStatementSink{}
	if err := prepared.Run(describeQ, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.names) != 1 || sink.names[0] != "id" || len(sink.nullable) != 1 || sink.nullable[0] != "NO" {
		t.Fatalf("names=%v null=%v", sink.names, sink.nullable)
	}
	if err := describeQ.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

// TestStableStatementEngineCreateSecretBypassesSourcePlanning 验证 CREATE SECRET 直接走 security adapter，并保持显式 ACL。
func TestStableStatementEngineCreateSecretBypassesSourcePlanning(t *testing.T) {
	engine, _, secrets, manager, _ := newStatementEngineHarness(t)
	q := query.New(context.Background(), manager)
	prepared, err := engine.Prepare(context.Background(), q, "CREATE SECRET prod (TYPE s3, key_id 'abc', secret_key 'xyz', SUBJECT 'alice', SCHEME 's3', HOST '*.example.com')")
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run(q, statementNoopSink{}); err != nil {
		t.Fatal(err)
	}
	resolved, err := secrets.Resolve("prod", "alice", "s3://bucket.example.com/object")
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Type() != "s3" {
		t.Fatalf("secret type=%q", resolved.Type())
	}
	if key, ok := resolved.Value("key_id"); !ok || key != "abc" {
		t.Fatalf("key_id=%q ok=%v", key, ok)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestStableStatementEngineCopyRemainsADRBlocked 验证统一 dispatch 不会绕过 ADR-0008。
func TestStableStatementEngineCopyRemainsADRBlocked(t *testing.T) {
	engine, _, _, manager, _ := newStatementEngineHarness(t)
	q := query.New(context.Background(), manager)
	if _, err := engine.Prepare(context.Background(), q, "COPY (SELECT 1) TO 'out.csv' (format csv)"); err == nil {
		t.Fatal("expected ADR-0008 COPY blocker")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}
