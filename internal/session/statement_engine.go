package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	resultpkg "github.com/steamwo/fssql/internal/execution/result"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// PreparedStatement 是 Native/Server 共用的 SQL statement 执行边界。
// SELECT、DESCRIBE、COPY 与 control statements 都只向 generic Pipeline Sink 输出 native DataChunk。
type PreparedStatement interface {
	Run(*query.Context, pipeline.Sink) error
	Output() []planner.Column
}

// StableStatementEngine 在 StableSQLEngine 之上增加已冻结 control/DESCRIBE/COPY statement composition。
type StableStatementEngine struct {
	materialize MaterializeExecutor
	selects     *StableSQLEngine   // selects 是 SELECT/Query 唯一 production execution path。
	control     *SQLControlAdapter // control 是 Catalog/Secret 副作用唯一 adapter。
	copy        CopyExecutor       // copy 仅适配既有 Export/VFS，不复制 SQL/Query 执行链。
}

// NewStableStatementEngine 创建不含 COPY adapter 的 statement composition root。
// 这保持已有 Native/control 使用者兼容；COPY 请求会显式失败，不能降级到非原子 writer。
func NewStableStatementEngine(selects *StableSQLEngine, control *SQLControlAdapter) (*StableStatementEngine, error) {
	return newStableStatementEngine(selects, control, nil)
}

// NewStableStatementEngineWithCopy 创建包含 Stable COPY TO adapter 的 composition root。
func NewStableStatementEngineWithCopy(selects *StableSQLEngine, control *SQLControlAdapter, copy CopyExecutor) (*StableStatementEngine, error) {
	if copy == nil {
		return nil, fmt.Errorf("session: Stable statement COPY executor is required")
	}
	return newStableStatementEngine(selects, control, copy)
}

func newStableStatementEngine(selects *StableSQLEngine, control *SQLControlAdapter, copy CopyExecutor) (*StableStatementEngine, error) {
	if selects == nil || selects.config.Catalog == nil || selects.config.Sources == nil || selects.functions == nil || selects.config.Buffers == nil {
		return nil, fmt.Errorf("session: Stable statement engine requires initialized StableSQLEngine")
	}
	if control == nil {
		return nil, fmt.Errorf("session: Stable statement engine requires SQLControlAdapter")
	}
	return &StableStatementEngine{selects: selects, control: control, copy: copy}, nil
}

// Prepare 只做 parse/bind/planning，不提前执行 Catalog/Secret/Export 副作用。
func (e *StableStatementEngine) Prepare(ctx context.Context, q *query.Context, sql string) (PreparedStatement, error) {
	parsed, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	return e.PrepareParsed(ctx, q, sql, parsed)
}

// PrepareParsed binds immutable parsed syntax against a fresh statement snapshot.
func (e *StableStatementEngine) PrepareParsed(ctx context.Context, q *query.Context, sql string, parsed ast.Statement) (PreparedStatement, error) {
	if e == nil || e.selects == nil || e.control == nil {
		return nil, fmt.Errorf("session: Stable statement engine is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() {
		return nil, fmt.Errorf("session: statement Prepare requires live QueryContext")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if stmt, ok := parsed.(*ast.CreateTableAsStmt); ok {
		return e.prepareCreateTableAs(ctx, q, sql, stmt)
	}
	if selectQuery, ok := parsed.(*ast.SelectStmt); ok {
		return e.selects.prepareSelect(ctx, q, sql, selectQuery)
	}

	// CREATE SECRET 的 Binder 不需要 Catalog/source resolver，避免无关 VFS/source configuration 阻塞 credential control path。
	if _, ok := parsed.(*ast.CreateSecretStmt); ok {
		bound, err := binder.NewStatementBinder(e.selects.functions, nil, nil, sql).Bind(ctx, parsed)
		if err != nil {
			return nil, err
		}
		return &preparedControlStatement{engine: e, bound: bound}, nil
	}

	bindings, scans, err := e.selects.config.Sources.NewQuerySources(q)
	if err != nil {
		return nil, fmt.Errorf("session: create statement source adapters: %w", err)
	}
	tx, err := e.selects.config.Catalog.Begin(ctx)
	if err != nil {
		return nil, err
	}
	closed := false
	rollback := func() error {
		if closed {
			return nil
		}
		closed = true
		return tx.Rollback(context.Background())
	}
	defer func() { _ = rollback() }()

	base, err := NewSourceResolver(tx, e.selects.functions, bindings)
	if err != nil {
		return nil, err
	}
	planning, err := NewPlanningSourceResolver(base)
	if err != nil {
		return nil, err
	}
	views, err := NewViewPlanningResolver(planning)
	if err != nil {
		return nil, err
	}
	bound, err := binder.NewStatementBinder(e.selects.functions, views, views, sql).Bind(ctx, parsed)
	if err != nil {
		return nil, err
	}

	// COPY must build its physical plan while the statement planning transaction
	// and view/source resolvers are still valid. The returned prepared plan owns
	// no Catalog transaction; rollback happens before Prepare returns.
	var copyPrepared *PreparedSQLQuery
	if value, ok := bound.(*binder.BoundCopyTo); ok {
		if value.Options.Encryption != nil {
			if err := q.AddCleanup(func() error { value.Options.Encryption.Clear(); return nil }); err != nil {
				value.Options.Encryption.Clear()
				return nil, err
			}
		}
		if e.copy == nil {
			return nil, fmt.Errorf("session: COPY executor is not configured")
		}
		if scans == nil {
			return nil, fmt.Errorf("session: COPY query source factory returned nil runtime scan factory")
		}
		copyPrepared, err = e.selects.prepareBoundQuery(ctx, q, value.Query, scans, views)
		if err != nil {
			return nil, err
		}
		if err := ensureFrozenSourceSnapshot(q); err != nil {
			return nil, err
		}
	}

	if err := rollback(); err != nil {
		return nil, fmt.Errorf("session: close statement planning transaction: %w", err)
	}

	switch value := bound.(type) {
	case *binder.BoundCreateView, *binder.BoundCreateExternalTable:
		return &preparedControlStatement{engine: e, bound: bound}, nil
	case *binder.BoundCopyTo:
		return &preparedCopyStatement{executor: e.copy, bound: value, query: copyPrepared, sql: sql}, nil
	case *binder.BoundDescribe:
		describe, ok := parsed.(*ast.DescribeStmt)
		preserve := ok && describe.Relation != nil && describe.Query == nil
		cursor, err := resultpkg.NewDescribeCursorWithNullability(value, preserve)
		if err != nil {
			return nil, err
		}
		return &preparedDescribeStatement{buffers: e.selects.config.Buffers, cursor: cursor}, nil
	default:
		return nil, fmt.Errorf("session: bound statement kind %d has no execution adapter", bound.StatementKind())
	}
}

type preparedControlStatement struct {
	engine *StableStatementEngine // engine 提供 Catalog/SQLControlAdapter。
	bound  binder.BoundStatement  // bound 是无敏感日志输出的 control IR。
}

func (*preparedControlStatement) Output() []planner.Column { return nil }

// Run 在副作用开始时创建独立 write transaction；planning transaction 已经回滚，符合 AD-021 生命周期分离。
func (p *preparedControlStatement) Run(q *query.Context, sink pipeline.Sink) error {
	if p == nil || p.engine == nil || p.bound == nil || q == nil || sink == nil {
		return fmt.Errorf("session: control statement is uninitialized")
	}
	if err := q.Context().Err(); err != nil {
		return err
	}
	tx, err := p.engine.selects.config.Catalog.Begin(q.Context())
	if err != nil {
		return err
	}
	statement := &Statement{catalog: tx}
	applyErr := p.engine.control.Apply(q.Context(), statement, p.bound)
	closeErr := statement.Close()
	if applyErr != nil || closeErr != nil {
		return errors.Join(applyErr, closeErr)
	}
	return sink.Finalize(q.Context())
}

type preparedDescribeStatement struct {
	buffers *memory.BufferManager     // buffers 是 Query MemoryScope 的 managed allocator。
	cursor  *resultpkg.DescribeCursor // cursor 只保存 Binder schema，不执行被描述查询。
}

func (p *preparedDescribeStatement) Output() []planner.Column {
	if p == nil {
		return nil
	}
	schema := resultpkg.DescribeSchema()
	out := make([]planner.Column, len(schema))
	for i, column := range schema {
		out[i] = planner.Column{Name: column.Name, Type: cloneStatementResultType(column.Type), Nullable: column.Nullable}
	}
	return out
}

// Run 流式物化 DESCRIBE schema；ownership-taking Sink 可直接接管 native DataChunk。
func (p *preparedDescribeStatement) Run(q *query.Context, sink pipeline.Sink) error {
	if p == nil || p.buffers == nil || p.cursor == nil || q == nil || q.Memory == nil || sink == nil {
		return fmt.Errorf("session: DESCRIBE statement is uninitialized")
	}
	for {
		chunk, done, err := p.cursor.Next(q.Memory, p.buffers)
		if err != nil {
			return err
		}
		if done {
			break
		}
		if err := sink.Consume(q.Context(), chunk); err != nil {
			chunk.Release()
			return err
		}
		if !pipeline.SinkTakesChunkOwnership(sink) {
			chunk.Release()
		}
	}
	return sink.Finalize(q.Context())
}

func cloneStatementResultType(value types.LogicalType) types.LogicalType {
	out := value
	if value.Child != nil {
		child := cloneStatementResultType(*value.Child)
		out.Child = &child
	}
	if value.Fields != nil {
		out.Fields = make([]types.StructField, len(value.Fields))
		for i, field := range value.Fields {
			out.Fields[i] = types.StructField{Name: field.Name, Type: cloneStatementResultType(field.Type)}
		}
	}
	return out
}
