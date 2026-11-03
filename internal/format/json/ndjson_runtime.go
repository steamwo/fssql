package json

import (
	"context"
	"fmt"
)

// NDJSONRuntimeSourceConfig 描述 Stable read_ndjson runtime source；Pattern/Fingerprint 必须来自 Statement 已绑定 ScanSpec。
type NDJSONRuntimeSourceConfig struct {
	Binder              *NDJSONBinder     // Binder 必须与 SQL Bind/InspectSource 共用真实 NDJSONBinder。
	Pattern             string            // Pattern 是 Stable read_ndjson 的 source literal。
	ExpectedFingerprint string            // ExpectedFingerprint 是 Statement 冻结的 DatasetFingerprint。
	Scan                NDJSONScanOptions // Scan 控制 runtime range 并行/粒度。
	Projection          []int             // 绑定 schema 的顶层列索引；输出保留原列位置。
}

// NewNDJSONRuntimeSource 重新走同一 NDJSON Bind 路径并核对 frozen fingerprint，再复用既有 NDJSONSource。
// Projection/Filter 等 optimizer candidate 继续由 residual physical operators 保证语义。
func NewNDJSONRuntimeSource(ctx context.Context, cfg NDJSONRuntimeSourceConfig) (*NDJSONSource, error) {
	if cfg.Binder == nil {
		return nil, fmt.Errorf("json: NDJSON runtime source requires NDJSONBinder")
	}
	if cfg.Pattern == "" || cfg.ExpectedFingerprint == "" {
		return nil, fmt.Errorf("json: NDJSON runtime source requires pattern and expected fingerprint")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	binding, err := cfg.Binder.Bind(ctx, cfg.Pattern, NDJSONBindOptions{})
	if err != nil {
		return nil, err
	}
	if binding.Fingerprint() != cfg.ExpectedFingerprint {
		return nil, fmt.Errorf("json: NDJSON source snapshot changed: planned=%q runtime=%q", cfg.ExpectedFingerprint, binding.Fingerprint())
	}
	// 仅接受绑定列索引，按 runtime schema 翻译为字面顶层字段名。
	// 未读列保留 NULL placeholder，Residual Project/Filter 的索引无需重映射。
	scan := cfg.Scan
	scan.Projection = ProjectedPathSet{}
	fields := binding.Schema().Fields
	for _, index := range cfg.Projection {
		if index < 0 || index >= len(fields) {
			return nil, fmt.Errorf("json: NDJSON projection index %d out of range", index)
		}
		scan.Projection.paths = append(scan.Projection.paths, []string{fields[index].Name})
	}
	return NewNDJSONSource(ctx, cfg.Binder.registry, binding, scan, cfg.Binder.scope, cfg.Binder.bm)
}
