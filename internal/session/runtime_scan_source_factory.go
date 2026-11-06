package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxScanOpenAdapters = 33
const internalScanFunctionPrefix = "__fssql_"

// ScanOpenFunc 是 Engine composition root 注入的 runtime scan callback。
// callback 可以依赖具体格式/VFS，但必须消费传入的 frozen ScanSpec，不能重新做 SQL binding。
type ScanOpenFunc func(context.Context, planner.ScanSpec) (pipeline.Source, error)

// AggregateScanOpenFunc 尝试把无 GROUP BY 的聚合直接交给元数据 source。
type AggregateScanOpenFunc func(context.Context, planner.ScanSpec, []planner.AggregateSpec) (pipeline.Source, bool, error)

// ScanOpenAdapter 将稳定 FunctionID 与 runtime scan callback 关联。
type ScanOpenAdapter struct {
	FunctionID function.ID           // FunctionID 必须对应 FunctionCatalog 中的 Table function。
	Open       ScanOpenFunc          // Open 打开真实 runtime source；返回 nil source 视为错误。
	OpenCount  ScanOpenFunc          // OpenCount optionally returns one exact BIGINT COUNT(*) row.
	OpenStats  AggregateScanOpenFunc // OpenStats optionally answers MIN/MAX from source metadata.
}

// InternalScanOpenAdapter 只用于 Planner 生成的保留 virtual scan 名称。
// 它不会注册到 FunctionCatalog，因此 SQL table-function Binder 无法直接调用这些内部来源。
type InternalScanOpenAdapter struct {
	Function string       // Function 必须使用 __fssql_ 保留前缀。
	Open     ScanOpenFunc // Open 必须消费 frozen ScanSpec，不得重新读取 Catalog metadata。
}

// RuntimeScanSourceFactory 实现 PhysicalPlan lowering 的 generic scan source factory。
// 它只负责 FunctionCatalog 校验与 immutable dispatch，不包含任何格式/协议实现。
type RuntimeScanSourceFactory struct {
	functions *function.Catalog                     // functions 是唯一 FunctionCatalog。
	adapters  map[function.ID]ScanOpenFunc          // adapters 构造后不可变。
	internal  map[string]ScanOpenFunc               // internal 保存 statement-scoped virtual source callback，不构成 SQL FunctionCatalog surface。
	counts    map[function.ID]ScanOpenFunc          // counts 是精确 COUNT(*) metadata source。
	stats     map[function.ID]AggregateScanOpenFunc // stats 是 MIN/MAX metadata source。
}

// NewRuntimeScanSourceFactory 冻结 runtime table-function scan registry。
func NewRuntimeScanSourceFactory(functions *function.Catalog, adapters []ScanOpenAdapter) (*RuntimeScanSourceFactory, error) {
	if functions == nil {
		functions = function.NewBuiltinCatalog()
	}
	if len(adapters) > maxScanOpenAdapters {
		return nil, fmt.Errorf("session: too many scan open adapters: %d > %d", len(adapters), maxScanOpenAdapters)
	}
	bound := make(map[function.ID]ScanOpenFunc, len(adapters))
	counts := make(map[function.ID]ScanOpenFunc)
	stats := make(map[function.ID]AggregateScanOpenFunc)
	for _, adapter := range adapters {
		if adapter.FunctionID == function.InvalidID || adapter.Open == nil {
			return nil, fmt.Errorf("session: invalid scan open adapter")
		}
		if _, exists := bound[adapter.FunctionID]; exists {
			return nil, fmt.Errorf("session: duplicate scan open adapter for function id %d", adapter.FunctionID)
		}
		def, ok := functions.LookupID(adapter.FunctionID)
		if !ok || def.Kind != function.Table {
			return nil, fmt.Errorf("session: scan open adapter references unknown/non-table function id %d", adapter.FunctionID)
		}
		bound[adapter.FunctionID] = adapter.Open
		if adapter.OpenCount != nil {
			counts[adapter.FunctionID] = adapter.OpenCount
		}
		if adapter.OpenStats != nil {
			stats[adapter.FunctionID] = adapter.OpenStats
		}
	}
	return &RuntimeScanSourceFactory{functions: functions, adapters: bound, counts: counts, stats: stats, internal: make(map[string]ScanOpenFunc)}, nil
}

// WithInternalAdapters 返回带 statement-scoped virtual scan callback 的新 immutable registry。
// 原 factory 不被修改，避免跨 Query 泄漏 Catalog snapshot/provenance。
func (f *RuntimeScanSourceFactory) WithInternalAdapters(adapters []InternalScanOpenAdapter) (*RuntimeScanSourceFactory, error) {
	if f == nil || f.functions == nil {
		return nil, fmt.Errorf("session: runtime scan source factory is uninitialized")
	}
	if len(adapters) > maxScanOpenAdapters {
		return nil, fmt.Errorf("session: too many internal scan adapters: %d > %d", len(adapters), maxScanOpenAdapters)
	}
	out := &RuntimeScanSourceFactory{
		functions: f.functions,
		adapters:  make(map[function.ID]ScanOpenFunc, len(f.adapters)),
		internal:  make(map[string]ScanOpenFunc, len(f.internal)+len(adapters)),
		counts:    f.counts,
		stats:     f.stats,
	}
	for id, open := range f.adapters {
		out.adapters[id] = open
	}
	for name, open := range f.internal {
		out.internal[name] = open
	}
	for _, adapter := range adapters {
		name := strings.TrimSpace(adapter.Function)
		if !strings.HasPrefix(name, internalScanFunctionPrefix) || adapter.Open == nil {
			return nil, fmt.Errorf("session: invalid internal scan adapter %q", adapter.Function)
		}
		if _, exists := out.internal[name]; exists {
			return nil, fmt.Errorf("session: duplicate internal scan adapter %s", name)
		}
		out.internal[name] = adapter.Open
	}
	return out, nil
}

func (f *RuntimeScanSourceFactory) OpenAggregateScan(ctx context.Context, spec planner.ScanSpec, aggregates []planner.AggregateSpec) (pipeline.Source, bool, error) {
	if f == nil || f.functions == nil {
		return nil, false, fmt.Errorf("session: uninitialized scan factory")
	}
	if len(aggregates) == 0 || len(spec.Pushdown.Filters) != 0 || spec.Pushdown.Limit != 0 {
		return nil, false, nil
	}
	def, ok := f.functions.Lookup(strings.TrimSpace(spec.Function))
	if !ok || def.Kind != function.Table {
		return nil, false, nil
	}
	if spec.RelationID == "" || spec.Source == "" || spec.Fingerprint == "" {
		return nil, false, fmt.Errorf("session: metadata aggregate requires frozen scan identity")
	}
	prepared := cloneRuntimeScanSpec(spec)
	prepared.Function = def.Name
	if len(aggregates) == 1 && f.counts[def.ID] != nil {
		a := aggregates[0]
		if function.ID(a.FunctionID) == function.CountID && !a.Distinct && len(a.Arguments) == 0 && a.Output.Type.ID == types.BigInt {
			source, err := f.counts[def.ID](ctx, prepared)
			return source, true, err
		}
	}
	if f.stats[def.ID] != nil {
		return f.stats[def.ID](ctx, prepared, aggregates)
	}
	return nil, false, nil
}

// Open 根据 ScanSpec.Function 分派真实 Stable table function 或 Planner-only virtual source。
func (f *RuntimeScanSourceFactory) Open(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
	if f == nil || f.functions == nil {
		return nil, fmt.Errorf("session: runtime scan source factory is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(spec.RelationID) == "" {
		return nil, fmt.Errorf("session: runtime scan requires relation identity")
	}
	functionName := strings.TrimSpace(spec.Function)
	if functionName == "" {
		return nil, fmt.Errorf("session: runtime scan requires table function")
	}
	if strings.TrimSpace(spec.Source) == "" {
		return nil, fmt.Errorf("session: runtime scan %s requires source", spec.RelationID)
	}
	if spec.Fingerprint == "" {
		return nil, fmt.Errorf("session: runtime scan %s requires dataset fingerprint", spec.RelationID)
	}
	prepared := cloneRuntimeScanSpec(spec)
	if strings.HasPrefix(functionName, internalScanFunctionPrefix) {
		open := f.internal[functionName]
		if open == nil {
			return nil, fmt.Errorf("session: no runtime internal scan adapter registered for %s", functionName)
		}
		prepared.Function = functionName
		source, err := open(ctx, prepared)
		if err != nil {
			return nil, fmt.Errorf("session: open internal runtime scan %s: %w", functionName, err)
		}
		if source == nil {
			return nil, fmt.Errorf("session: internal runtime scan adapter %s returned nil source", functionName)
		}
		return source, nil
	}
	def, ok := f.functions.Lookup(functionName)
	if !ok || def.Kind != function.Table {
		return nil, fmt.Errorf("session: runtime scan references unknown/non-table function %s", functionName)
	}
	open := f.adapters[def.ID]
	if open == nil {
		return nil, fmt.Errorf("session: no runtime scan adapter registered for table function %s", def.Name)
	}
	prepared.Function = def.Name
	source, err := open(ctx, prepared)
	if err != nil {
		return nil, fmt.Errorf("session: open runtime scan %s: %w", def.Name, err)
	}
	if source == nil {
		return nil, fmt.Errorf("session: runtime scan adapter %s returned nil source", def.Name)
	}
	return source, nil
}

// cloneRuntimeScanSpec 复制可变 slice backing array，防止 concrete reader callback 修改 PhysicalPlan-owned pushdown metadata。
func cloneRuntimeScanSpec(in planner.ScanSpec) planner.ScanSpec {
	out := in
	out.Pushdown.Projection = append([]int(nil), in.Pushdown.Projection...)
	out.Pushdown.Filters = append([]*binder.BoundExpression(nil), in.Pushdown.Filters...)
	return out
}
