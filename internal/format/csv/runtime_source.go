package csv

import (
	"context"
	"fmt"
)

// RuntimeSourceConfig 描述 Stable read_csv 的 runtime source；Pattern/Fingerprint 必须来自已绑定 ScanSpec。
type RuntimeSourceConfig struct {
	Binder              *SourceBinder // Binder 必须与 SQL Bind/InspectSource 共用真实 CSV SourceBinder。
	Pattern             string        // Pattern 是 Stable read_csv 的 source literal。
	ExpectedFingerprint string        // ExpectedFingerprint 是 Statement 冻结的 DatasetFingerprint。
	Scan                 ScanOptions   // Scan 只控制 runtime range 并行度/粒度，不改变绑定 schema。
}

// NewRuntimeSource 重新走同一 CSV Bind 路径并核对 frozen fingerprint，再复用既有 Source 执行扫描。
// 未接受的 optimizer pushdown 继续由 residual physical operators 执行，runtime source 始终输出完整绑定 schema。
func NewRuntimeSource(ctx context.Context, cfg RuntimeSourceConfig) (*Source, error) {
	if cfg.Binder == nil {
		return nil, fmt.Errorf("csv: runtime source requires SourceBinder")
	}
	if cfg.Pattern == "" || cfg.ExpectedFingerprint == "" {
		return nil, fmt.Errorf("csv: runtime source requires pattern and expected fingerprint")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Stable read_csv 当前只有 source 参数，因此 runtime 必须使用与 TableFunctionResolver 完全相同的默认 Bind options。
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, SourceBindOptions{})
	if err != nil {
		return nil, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, fmt.Errorf("csv: source snapshot changed: planned=%q runtime=%q", cfg.ExpectedFingerprint, binding.Fingerprint())
	}
	return NewSource(ctx, cfg.Binder.registry, binding, cfg.Scan, cfg.Binder.scope, cfg.Binder.bm)
}
