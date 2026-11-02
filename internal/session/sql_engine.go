package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/steamwo/fssql/internal/catalog"
	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/stats"
	"github.com/steamwo/fssql/internal/execution/expression"
	"github.com/steamwo/fssql/internal/execution/lowering"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/optimizer"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/query"
	"github.com/steamwo/fssql/internal/sql/ast"
	"github.com/steamwo/fssql/internal/sql/binder"
	"github.com/steamwo/fssql/internal/sql/parser"
)

// QuerySourceFactory 为单个 QueryContext 创建真实 Bind adapters 与 runtime scan registry。
// 格式 SourceBinder 持有 Query MemoryScope，因此这些对象严禁跨 Query 复用。
type QuerySourceFactory interface {
	NewQuerySources(*query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error)
}

// QuerySourceFactoryFunc 便于 composition root 用函数实现 query-scoped source 装配。
type QuerySourceFactoryFunc func(*query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error)

// NewQuerySources 实现 QuerySourceFactory。
func (f QuerySourceFactoryFunc) NewQuerySources(q *query.Context) ([]TableBindAdapter, *RuntimeScanSourceFactory, error) {
	if f == nil {
		return nil, nil, fmt.Errorf("session: nil QuerySourceFactoryFunc")
	}
	return f(q)
}

// StableSQLEngineConfig 是 transport-neutral Stable SELECT composition root。
// Query-scoped source objects 通过 Sources 创建；本类型不依赖具体 VFS/format/server transport。
type StableSQLEngineConfig struct {
	Catalog       catalog.Catalog                // Catalog 提供 statement-scoped metadata transaction。
	Functions     *function.Catalog              // Functions 是全链路唯一 FunctionCatalog；nil 使用 builtin Stable catalog。
	Sources       QuerySourceFactory             // Sources 为每个 QueryContext 装配真实 Bind/scan adapters。
	Buffers       *memory.BufferManager          // Buffers 是 expression/blocking operator 的 managed buffer factory。
	Statistics    stats.Provider                 // Statistics 可为空；optimizer 会保持 Unknown confidence。
	Distinct      ExternalDistinctRuntimeConfig  // Distinct 是 query-scoped DISTINCT spill policy。
	Sort          ExternalSortRuntimeConfig      // Sort 是 query-scoped ORDER BY spill policy。
	Aggregate     ExternalAggregateRuntimeConfig // Aggregate 是 query-scoped GROUP/aggregate spill policy。
	Join          ExternalJoinRuntimeConfig      // Join 是 query-scoped Stable join spill policy。
	Workers       int                            // Workers 是单 Pipeline stage 的 CPU worker 上限；<=0 归一为 1。
	BlockingQueue int                            // BlockingQueue 是 blocking stage bounded handoff 容量；<=0 归一为 1。
}

// StableSQLEngine 把 Parser→Binder→Planner→Optimizer→Push Pipeline 组合成唯一 production SELECT path。
type StableSQLEngine struct {
	config    StableSQLEngineConfig // config 构造后只读。
	functions *function.Catalog     // functions 保存归一后的唯一 catalog。
}

// PreparedSQLQuery 保存已经完成 parser/binder/planner/optimizer 的 immutable-by-convention physical plan。
type PreparedSQLQuery struct {
	engine   *StableSQLEngine          // engine 提供 runtime factory policy。
	physical *planner.PhysicalPlan     // physical 不包含已打开 reader/operator。
	scans    *RuntimeScanSourceFactory // scans 与 Prepare 使用的 query-scoped Bind adapters 属于同一 QueryContext。
}

// NewStableSQLEngine 创建 transport-neutral SQL engine；不创建 QueryContext 或 server goroutine。
func NewStableSQLEngine(config StableSQLEngineConfig) (*StableSQLEngine, error) {
	if config.Catalog == nil || config.Sources == nil || config.Buffers == nil {
		return nil, fmt.Errorf("session: Stable SQL engine requires catalog, query source factory and BufferManager")
	}
	if config.Functions == nil {
		config.Functions = function.NewBuiltinCatalog()
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.BlockingQueue <= 0 {
		config.BlockingQueue = 1
	}
	return &StableSQLEngine{config: config, functions: config.Functions}, nil
}

// Prepare 完成 SELECT 的 parse/bind/plan/optimize，并在 QueryContext 冻结全部 source fingerprints。
// Catalog transaction 只覆盖 metadata planning；成功或失败都在返回前 rollback，运行时不持有 Catalog transaction。
func (e *StableSQLEngine) Prepare(ctx context.Context, q *query.Context, sql string) (*PreparedSQLQuery, error) {
	if e == nil || e.config.Catalog == nil || e.config.Sources == nil || e.functions == nil || e.config.Buffers == nil {
		return nil, fmt.Errorf("session: Stable SQL engine is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() {
		return nil, fmt.Errorf("session: Prepare requires live QueryContext")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	selectStatement, ok := statement.(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("session: query execution currently requires SELECT; got %T", statement)
	}

	bindings, scans, err := e.config.Sources.NewQuerySources(q)
	if err != nil {
		return nil, fmt.Errorf("session: create query source adapters: %w", err)
	}
	if scans == nil {
		return nil, fmt.Errorf("session: query source factory returned nil runtime scan factory")
	}

	tx, err := e.config.Catalog.Begin(ctx)
	if err != nil {
		return nil, err
	}
	closedTx := false
	rollback := func() error {
		if closedTx {
			return nil
		}
		closedTx = true
		return tx.Rollback(context.Background())
	}
	defer func() { _ = rollback() }()

	baseResolver, err := NewSourceResolver(tx, e.functions, bindings)
	if err != nil {
		return nil, err
	}
	planningResolver, err := NewPlanningSourceResolver(baseResolver)
	if err != nil {
		return nil, err
	}
	viewResolver, err := NewViewPlanningResolver(planningResolver)
	if err != nil {
		return nil, err
	}
	bound, err := binder.NewQueryBinder(e.functions, viewResolver, viewResolver, sql).BindSelect(ctx, selectStatement)
	if err != nil {
		return nil, err
	}
	logical, err := planner.NewQueryPlanner(e.functions, viewResolver).PlanStable(ctx, bound)
	if err != nil {
		return nil, err
	}
	physical, err := (&optimizer.Optimizer{Statistics: e.config.Statistics}).Optimize(ctx, logical)
	if err != nil {
		return nil, err
	}
	if err := bindPhysicalSourceSnapshot(q, physical); err != nil {
		return nil, err
	}
	if err := rollback(); err != nil {
		return nil, fmt.Errorf("session: close read-only planning transaction: %w", err)
	}
	return &PreparedSQLQuery{engine: e, physical: physical, scans: scans}, nil
}

// Run 把 Prepared physical plan 降低为 Push Pipeline DAG，并由调用方提供最终结果 Sink。
// QueryContext ownership 由调用方保持；本方法不 Close/Cancel QueryContext。
func (p *PreparedSQLQuery) Run(q *query.Context, sink pipeline.Sink) error {
	if p == nil || p.engine == nil || p.physical == nil || p.scans == nil {
		return fmt.Errorf("session: prepared SQL query is uninitialized")
	}
	if q == nil || q.Memory == nil || q.Closed() {
		return fmt.Errorf("session: prepared SQL query requires live QueryContext")
	}
	if sink == nil {
		return fmt.Errorf("session: prepared SQL query requires result sink")
	}
	e := p.engine
	executor := expression.NewExecutor(q.Memory, e.config.Buffers)
	builder, err := lowering.NewBuilder(p.scans, executor)
	if err != nil {
		return err
	}
	builder.BlockingQueue = e.config.BlockingQueue

	// 只为 physical plan 实际需要的 stateful operator 创建 factory；简单 scan/filter/project 不强制要求 Temp scope。
	if physicalContains(p.physical, planner.PhysicalDistinct) {
		factory, err := NewExternalDistinctFactory(q, e.config.Buffers, e.config.Distinct)
		if err != nil {
			return err
		}
		builder.Distincts = factory
	}
	if physicalContains(p.physical, planner.PhysicalSort) {
		factory, err := NewExternalSortFactory(q, e.config.Buffers, e.config.Sort)
		if err != nil {
			return err
		}
		builder.Sorts = factory
	}
	if physicalContains(p.physical, planner.PhysicalHashAggregate) {
		factory, err := NewExternalAggregateFactory(q, e.config.Buffers, e.config.Aggregate)
		if err != nil {
			return err
		}
		builder.Aggregates = factory
	}
	if physicalContains(p.physical, planner.PhysicalHashJoin) || physicalContains(p.physical, planner.PhysicalCrossJoin) {
		factory, err := NewExternalJoinFactory(q, e.config.Buffers, e.config.Join)
		if err != nil {
			return err
		}
		builder.Joins = factory
	}
	root, err := builder.Build(q.Context(), p.physical, sink)
	if err != nil {
		return err
	}
	return pipeline.RunDAG(q.Context(), root, e.config.Workers, q)
}

// Output 返回 prepared query 的结果 schema 副本，供 Native/database/sql API 建立 column metadata。
func (p *PreparedSQLQuery) Output() []planner.Column {
	if p == nil || p.physical == nil {
		return nil
	}
	out := make([]planner.Column, len(p.physical.Output))
	for i, column := range p.physical.Output {
		out[i] = column
		out[i].Type = cloneSourceType(column.Type)
	}
	return out
}

func physicalContains(plan *planner.PhysicalPlan, kind planner.PhysicalKind) bool {
	if plan == nil {
		return false
	}
	if plan.Kind == kind {
		return true
	}
	for _, child := range plan.Children {
		if physicalContains(child, kind) {
			return true
		}
	}
	return false
}

// bindPhysicalSourceSnapshot 将所有 PhysicalScan 的 immutable fingerprint 组合成 Statement-level SourceSnapshot。
// 排序 relation/fingerprint pair 使同一 source set 不受 optimizer tree traversal 顺序影响。
func bindPhysicalSourceSnapshot(q *query.Context, plan *planner.PhysicalPlan) error {
	if q == nil || plan == nil {
		return fmt.Errorf("session: source snapshot requires query and physical plan")
	}
	pairs := make([]string, 0, 4)
	collectPhysicalScanFingerprints(plan, &pairs)
	if len(pairs) == 0 {
		return nil
	}
	sort.Strings(pairs)
	h := sha256.New()
	for _, pair := range pairs {
		_, _ = h.Write([]byte(pair))
		_, _ = h.Write([]byte{0})
	}
	fingerprint := hex.EncodeToString(h.Sum(nil))
	if err := q.BindSourceSnapshot(query.SourceSnapshot{Fingerprint: fingerprint}); err != nil {
		return fmt.Errorf("session: bind statement source snapshot: %w", err)
	}
	return nil
}

func collectPhysicalScanFingerprints(plan *planner.PhysicalPlan, out *[]string) {
	if plan == nil {
		return
	}
	if plan.Kind == planner.PhysicalScan && plan.Scan != nil {
		*out = append(*out, plan.Scan.RelationID+"\x1f"+string(plan.Scan.Fingerprint))
	}
	for _, child := range plan.Children {
		collectPhysicalScanFingerprints(child, out)
	}
}

// AbortPrepared 预留统一 error-join helper，调用方可在后续 result adapter cleanup 中保留执行/清理双错误链。
func AbortPrepared(runErr, cleanupErr error) error { return errors.Join(runErr, cleanupErr) }
