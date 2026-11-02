package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

// NewNDJSONScanOpenAdapter 把 Stable read_ndjson ScanSpec 接到既有 NDJSONSource 执行路径。
func NewNDJSONScanOpenAdapter(binder *jsonformat.NDJSONBinder, scanOptions jsonformat.NDJSONScanOptions) (ScanOpenAdapter, error) {
	if binder == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: read_ndjson runtime scan requires NDJSONBinder")
	}
	return ScanOpenAdapter{
		FunctionID: function.ReadNDJSONID,
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "read_ndjson" {
				return nil, fmt.Errorf("session: read_ndjson runtime adapter received function %q", spec.Function)
			}
			// Filter/Projection/Limit 是 optimizer candidate；当前 source 输出完整绑定 relation，交由 residual operators 保证语义。
			return jsonformat.NewNDJSONRuntimeSource(ctx, jsonformat.NDJSONRuntimeSourceConfig{
				Binder:              binder,
				Pattern:             spec.Source,
				ExpectedFingerprint: string(spec.Fingerprint),
				Scan:                 scanOptions,
			})
		},
	}, nil
}
