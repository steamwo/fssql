package session

import (
	"context"
	"fmt"
	"strings"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/sql/binder"
)

const maxScanOpenAdapters = 32

// ScanOpenFunc 是 Engine composition root 注入的 runtime scan callback。
// callback 可以依赖具体格式/VFS，但必须消费传入的 frozen ScanSpec，不能重新做 SQL binding。
type ScanOpenFunc func(context.Context, planner.ScanSpec) (pipeline.Source, error)

// ScanOpenAdapter 将稳定 FunctionID 与 runtime scan callback 关联。
type ScanOpenAdapter struct {
	FunctionID function.ID // FunctionID 必须对应 FunctionCatalog 中的 Table function。
	Open       ScanOpenFunc // Open 打开真实 runtime source；返回 nil source 视为错误。
}

// RuntimeScanSourceFactory 实现 PhysicalPlan lowering 的 generic scan source factory。
// 它只负责 FunctionCatalog 校验与 immutable dispatch，不包含任何格式/协议实现。
type RuntimeScanSourceFactory struct {
	functions *function.Catalog            // functions 是唯一 FunctionCatalog。
	adapters  map[function.ID]ScanOpenFunc // adapters 构造后不可变。
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
	}
	return &RuntimeScanSourceFactory{functions: functions, adapters: bound}, nil
}

// Open 根据 ScanSpec.Function 通过唯一 FunctionCatalog 查找内部 ID，再分派到实际 runtime callback。
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
	if strings.TrimSpace(spec.Function) == "" {
		return nil, fmt.Errorf("session: runtime scan requires table function")
	}
	if strings.TrimSpace(spec.Source) == "" {
		return nil, fmt.Errorf("session: runtime scan %s requires source", spec.RelationID)
	}
	if spec.Fingerprint == "" {
		return nil, fmt.Errorf("session: runtime scan %s requires dataset fingerprint", spec.RelationID)
	}
	def, ok := f.functions.Lookup(spec.Function)
	if !ok || def.Kind != function.Table {
		return nil, fmt.Errorf("session: runtime scan references unknown/non-table function %s", spec.Function)
	}
	open := f.adapters[def.ID]
	if open == nil {
		return nil, fmt.Errorf("session: no runtime scan adapter registered for table function %s", def.Name)
	}
	source, err := open(ctx, cloneRuntimeScanSpec(spec))
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
