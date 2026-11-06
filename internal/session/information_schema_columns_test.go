package session

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
)

// TestInformationSchemaColumnsVirtualScan 验证 Catalog snapshot → Planner internal scan → managed runtime source 全链路。
func TestInformationSchemaColumnsVirtualScan(t *testing.T) {
	ctx := context.Background()
	store, err := catalog.NewMemoryCatalog(16)
	if err != nil { t.Fatal(err) }
	write, err := store.Begin(ctx)
	if err != nil { t.Fatal(err) }
	if err := write.CreateExternalTable(ctx, catalog.ExternalTable{
		Name: catalog.ObjectName{Schema: "main", Name: "events"},
		Columns: []catalog.ColumnDefinition{
			{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false},
			{Name: "name", Type: types.LogicalType{ID: types.Varchar}, Nullable: true},
		},
		TableFunction: "read_csv", Source: "mem://events.csv",
	}); err != nil { t.Fatal(err) }
	if err := write.CreateView(ctx, catalog.View{
		Name: catalog.ObjectName{Schema: "main", Name: "event_ids"},
		SQL:  "SELECT id FROM events",
		Columns: []catalog.ColumnDefinition{{Name: "id", Type: types.LogicalType{ID: types.BigInt}, Nullable: false}},
	}); err != nil { t.Fatal(err) }
	if err := write.Commit(ctx); err != nil { t.Fatal(err) }

	read, err := store.Begin(ctx)
	if err != nil { t.Fatal(err) }
	defer func() { _ = read.Rollback(context.Background()) }()
	functions := function.NewBuiltinCatalog()
	base, err := NewSourceResolver(read, functions, nil)
	if err != nil { t.Fatal(err) }
	resolver, err := NewPlanningSourceResolver(base)
	if err != nil { t.Fatal(err) }
	name := ast.QualifiedName{Parts: []ast.Identifier{{Name: "information_schema"}, {Name: "columns"}}}
	resolved, err := resolver.ResolveRelation(ctx, name)
	if err != nil { t.Fatal(err) }
	if len(resolved.Columns) != 15 || resolved.Fingerprint == "" {
		t.Fatalf("schema columns=%d fingerprint=%q", len(resolved.Columns), resolved.Fingerprint)
	}
	relation := &binder.BoundRelation{Kind: binder.RelationCatalog, ID: resolved.ID, Columns: resolved.Columns, Fingerprint: resolved.Fingerprint}
	logical, err := resolver.PlanBoundRelation(ctx, relation)
	if err != nil { t.Fatal(err) }
	if logical.Scan == nil || logical.Scan.Function != internalInformationSchemaColumnsFunction {
		t.Fatalf("scan=%+v", logical.Scan)
	}

	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	scans, err := NewRuntimeScanSourceFactory(functions, nil)
	if err != nil { t.Fatal(err) }
	scans, err = scans.WithInternalAdapters([]InternalScanOpenAdapter{informationSchemaScanAdapter(scope, buffers)})
	if err != nil { t.Fatal(err) }
	source, err := scans.Open(ctx, *logical.Scan)
	if err != nil { t.Fatal(err) }
	chunk, ok, err := source.Next(ctx)
	if err != nil { t.Fatal(err) }
	if !ok || chunk == nil || chunk.Cardinality != 3 || len(chunk.Columns) != 15 {
		t.Fatalf("ok=%v chunk=%+v", ok, chunk)
	}
	catalogName, _ := chunk.Columns[0].String(0)
	tableName0, _ := chunk.Columns[2].String(0)
	columnName0, _ := chunk.Columns[3].String(0)
	ordinal0, _ := chunk.Columns[4].Int32(0)
	nullable0, _ := chunk.Columns[6].String(0)
	type0, _ := chunk.Columns[7].String(0)
	precision0, precisionValid := chunk.Columns[10].Int32(0)
	if catalogName != "memory" || tableName0 != "event_ids" || columnName0 != "id" || ordinal0 != 1 || nullable0 != "NO" || type0 != "BIGINT" || !precisionValid || precision0 != 64 {
		t.Fatalf("row0 catalog=%q table=%q column=%q ordinal=%d nullable=%q type=%q precision=(%d,%v)", catalogName, tableName0, columnName0, ordinal0, nullable0, type0, precision0, precisionValid)
	}
	chunk.Release()
	if next, more, err := source.Next(ctx); err != nil || more || next != nil {
		t.Fatalf("terminal next=%v more=%v err=%v", next, more, err)
	}
	if err := scope.Close(); err != nil { t.Fatal(err) }
	if manager.Used() != 0 { t.Fatalf("managed leak=%d", manager.Used()) }

	// Snapshot source 被篡改必须 fail-closed；不能使用同 fingerprint 执行不同 metadata。
	tampered := *logical.Scan
	tampered.Source += "A"
	manager2 := memory.NewManager(1 << 20)
	scope2 := manager2.NewScope()
	scans2, err := NewRuntimeScanSourceFactory(functions, nil)
	if err != nil { t.Fatal(err) }
	scans2, err = scans2.WithInternalAdapters([]InternalScanOpenAdapter{informationSchemaScanAdapter(scope2, buffers)})
	if err != nil { t.Fatal(err) }
	if _, err := scans2.Open(ctx, tampered); err == nil {
		t.Fatal("expected tampered information_schema snapshot rejection")
	}
	if err := scope2.Close(); err != nil { t.Fatal(err) }
}

// TestInformationSchemaColumnsSourceConcurrentNext 验证多 worker 并发调用 Next 时
// 游标只前进一次，不会重复输出同一批或越界 panic（与 globSource 的并发契约一致）。
func TestInformationSchemaColumnsSourceConcurrentNext(t *testing.T) {
	ctx := context.Background()
	manager := memory.NewManager(4 << 20)
	scope := manager.NewScope()
	buffers := memory.NewBufferManager(nil)
	rows := make([]informationSchemaColumnRow, 5)
	for i := range rows {
		rows[i] = informationSchemaColumnRow{
			TableName:  "events",
			ColumnName: fmt.Sprintf("c%d", i),
			TypeName:   "BIGINT",
			Ordinal:    int32(i + 1),
		}
	}
	source, err := newInformationSchemaColumnsSource(scope, buffers, rows)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	start := make(chan struct{})
	errs := make([]error, workers)
	totals := make([]int, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for {
				chunk, ok, err := source.Next(ctx)
				if err != nil {
					errs[worker] = err
					return
				}
				if !ok {
					return
				}
				totals[worker] += chunk.Cardinality
				chunk.Release()
			}
		}(worker)
	}
	close(start)
	wg.Wait()

	total := 0
	for worker := 0; worker < workers; worker++ {
		if errs[worker] != nil {
			t.Fatalf("worker %d: %v", worker, errs[worker])
		}
		total += totals[worker]
	}
	if total != len(rows) {
		t.Fatalf("并发输出总行数=%d，期望 %d（重复或丢失批次）", total, len(rows))
	}
	if err := scope.Close(); err != nil {
		t.Fatal(err)
	}
	if manager.Used() != 0 {
		t.Fatalf("managed memory leak=%d", manager.Used())
	}
}

// TestInformationSchemaColumnsIsNotTableFunction 验证 internal virtual source 不扩大 Stable FunctionCatalog surface。
func TestInformationSchemaColumnsIsNotTableFunction(t *testing.T) {
	functions := function.NewBuiltinCatalog()
	if _, ok := functions.Lookup(internalInformationSchemaColumnsFunction); ok {
		t.Fatal("internal information_schema source must not be SQL table function")
	}
}
