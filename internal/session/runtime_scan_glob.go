package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/core/types"
	"github.com/steamwo/fssql/internal/core/vector"
	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
	"github.com/steamwo/fssql/internal/storage/fileset"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// globPlanFunc 是 runtime glob adapter 对统一 FileSetPlanner 的最窄依赖。
// 返回 URI 顺序和 fingerprint 必须直接来自同一个 frozen ScanSet。
type globPlanFunc func(context.Context, string) ([]string, string, error)

// NewGlobScanOpenAdapter 创建 glob() 的生产 runtime adapter。
// 每次执行重新规划实际 FileSet，并严格比对 Statement 已冻结的 DatasetFingerprint。
func NewGlobScanOpenAdapter(registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager) (ScanOpenAdapter, error) {
	if registry == nil || scope == nil || buffers == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: glob runtime scan requires VFS registry and managed memory")
	}
	filePlanner := fileset.NewPlanner(registry)
	plan := func(ctx context.Context, source string) ([]string, string, error) {
		scanSet, err := filePlanner.Plan(ctx, source, fileset.PlanOptions{})
		if err != nil {
			return nil, "", err
		}
		if scanSet == nil || scanSet.Fingerprint() == "" {
			return nil, "", fmt.Errorf("session: glob FileSetPlanner returned incomplete ScanSet")
		}
		files := scanSet.Files()
		uris := make([]string, len(files))
		for i := range files {
			uris[i] = files[i].URI.String()
		}
		return uris, scanSet.Fingerprint(), nil
	}
	return newGlobScanOpenAdapter(plan, scope, buffers)
}

// newGlobScanOpenAdapter 绑定已冻结 FileSet 结果到 generic runtime scan callback。
func newGlobScanOpenAdapter(plan globPlanFunc, scope *memory.Scope, buffers *memory.BufferManager) (ScanOpenAdapter, error) {
	if plan == nil || scope == nil || buffers == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: glob runtime scan requires planner and managed memory")
	}
	return ScanOpenAdapter{
		FunctionID: function.GlobID,
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "glob" {
				return nil, fmt.Errorf("session: glob runtime adapter received function %q", spec.Function)
			}
			if len(spec.Pushdown.Filters) != 0 {
				return nil, fmt.Errorf("session: glob runtime filter pushdown is not implemented")
			}
			if len(spec.Pushdown.Projection) > 1 || (len(spec.Pushdown.Projection) == 1 && spec.Pushdown.Projection[0] != 0) {
				return nil, fmt.Errorf("session: glob runtime projection must be empty or [0]")
			}
			uris, fingerprint, err := plan(ctx, spec.Source)
			if err != nil {
				return nil, err
			}
			if fingerprint == "" || fingerprint != string(spec.Fingerprint) {
				return nil, fmt.Errorf("session: glob source snapshot changed: planned=%q runtime=%q", spec.Fingerprint, fingerprint)
			}
			if len(uris) == 0 {
				return nil, fmt.Errorf("session: glob runtime ScanSet has no files")
			}
			limit := len(uris)
			if spec.Pushdown.Limit > 0 && spec.Pushdown.Limit < uint64(limit) {
				limit = int(spec.Pushdown.Limit)
			}
			return &globSource{scope: scope, buffers: buffers, uris: append([]string(nil), uris[:limit]...)}, nil
		},
	}, nil
}

// globSource 把 frozen URI 列表按 Own Vector/DataChunk 分批输出。
// URI 元数据数量受 FileSetPlanner MaxFiles 上限约束；实际字符串结果存入 managed StringHeap。
type globSource struct {
	mu      sync.Mutex            // mu 保护多 worker 并发调用 Next 时的游标。
	scope   *memory.Scope         // scope 是当前 QueryContext 的 managed memory scope。
	buffers *memory.BufferManager // buffers 分配 Vector/StringHeap backing storage。
	uris    []string              // uris 是已通过 fingerprint 校验的稳定顺序文件列表。
	next    int                   // next 是下一条待输出 URI 的序号。
}

var _ pipeline.Source = (*globSource)(nil)

// Next 输出最多一个标准 vector batch；无剩余行时返回 ok=false。
func (s *globSource) Next(ctx context.Context) (*vector.DataChunk, bool, error) {
	if s == nil || s.scope == nil || s.buffers == nil {
		return nil, false, fmt.Errorf("session: glob source is uninitialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= len(s.uris) {
		return nil, false, nil
	}
	count := len(s.uris) - s.next
	if count > vector.DefaultVectorSize {
		count = vector.DefaultVectorSize
	}
	column, err := vector.NewFlat(s.scope, s.buffers, types.LogicalType{ID: types.Varchar}, count)
	if err != nil {
		return nil, false, err
	}
	for row := 0; row < count; row++ {
		if err := ctx.Err(); err != nil {
			column.Release()
			return nil, false, err
		}
		if err := column.SetString(row, s.uris[s.next+row]); err != nil {
			column.Release()
			return nil, false, err
		}
	}
	chunk, err := vector.NewDataChunk(column)
	if err != nil {
		column.Release()
		return nil, false, err
	}
	s.next += count
	return chunk, true, nil
}
