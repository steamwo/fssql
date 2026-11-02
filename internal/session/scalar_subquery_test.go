package session

import (
	"context"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/query"
)

type nullableIntResultSink struct {
	values []int32
	valid  []bool
}

func (s *nullableIntResultSink) Consume(_ context.Context, chunk *vector.DataChunk) error {
	for row := 0; row < chunk.Cardinality; row++ {
		value, ok := chunk.Columns[0].Int32(row)
		s.values = append(s.values, value)
		s.valid = append(s.valid, ok)
	}
	return nil
}
func (*nullableIntResultSink) Finalize(context.Context) error { return nil }

func runScalarSQL(t *testing.T, sql string) (*nullableIntResultSink, error) {
	t.Helper()
	manager := memory.NewManager(1 << 20)
	buffers := memory.NewBufferManager(nil)
	q := queryForScalarTest(context.Background(), manager)
	engine := newSQLTestEngine(t, buffers)
	prepared, err := engine.Prepare(context.Background(), q, sql)
	if err != nil {
		_ = q.Close()
		return nil, err
	}
	sink := &nullableIntResultSink{}
	runErr := prepared.Run(q, sink)
	closeErr := q.Close()
	if runErr == nil && closeErr != nil {
		runErr = closeErr
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d for %s", manager.Used(), sql)
	}
	return sink, runErr
}

// queryForScalarTest 保持测试 QueryContext 不带 Temp；这些 case 都不需要 blocking/spill operator。
func queryForScalarTest(parent context.Context, manager *memory.Manager) *query.Context {
	return query.New(parent, manager)
}

// TestStableSQLEngineSelectWithoutFrom 验证 OneRow source 让基础 SELECT 常量可执行。
func TestStableSQLEngineSelectWithoutFrom(t *testing.T) {
	sink, err := runScalarSQL(t, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.values) != 1 || !sink.valid[0] || sink.values[0] != 1 {
		t.Fatalf("result=%v valid=%v want [1]", sink.values, sink.valid)
	}
}

// TestStableSQLEngineScalarSubqueryOneRow 验证 non-correlated 单行子查询被 materialize 为 managed runtime constant。
func TestStableSQLEngineScalarSubqueryOneRow(t *testing.T) {
	sink, err := runScalarSQL(t, "SELECT (SELECT 7)")
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.values) != 1 || !sink.valid[0] || sink.values[0] != 7 {
		t.Fatalf("result=%v valid=%v want [7]", sink.values, sink.valid)
	}
}

// TestStableSQLEngineScalarSubqueryZeroRowsIsNull 验证 SQL scalar subquery 0 行语义为 typed NULL。
func TestStableSQLEngineScalarSubqueryZeroRowsIsNull(t *testing.T) {
	sink, err := runScalarSQL(t, "SELECT (SELECT 7 WHERE FALSE)")
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.valid) != 1 || sink.valid[0] {
		t.Fatalf("valid=%v want one NULL", sink.valid)
	}
}

// TestStableSQLEngineScalarSubqueryMoreThanOneRowFails 验证 scalar subquery >1 行显式失败而不是任取一行。
func TestStableSQLEngineScalarSubqueryMoreThanOneRowFails(t *testing.T) {
	if _, err := runScalarSQL(t, "SELECT (SELECT id FROM read_csv('mem://rows.csv'))"); err == nil {
		t.Fatal("expected scalar subquery cardinality error")
	}
}

// TestStableSQLEngineCorrelatedScalarSubqueryRejected 验证 child Binder 不继承 outer Scope，correlation 不会被误当 non-correlated 执行。
func TestStableSQLEngineCorrelatedScalarSubqueryRejected(t *testing.T) {
	manager := memory.NewManager(1 << 20)
	buffers := memory.NewBufferManager(nil)
	q := queryForScalarTest(context.Background(), manager)
	engine := newSQLTestEngine(t, buffers)
	if _, err := engine.Prepare(context.Background(), q, "SELECT id, (SELECT id) FROM read_csv('mem://rows.csv')"); err == nil {
		t.Fatal("expected correlated scalar subquery rejection")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}
