package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	jsonformat "github.com/steamwo/fssql/internal/format/json"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

// NewJSONScanOpenAdapter 把 Stable read_json ScanSpec 接到 bounded serial JSON runtime source。
func NewJSONScanOpenAdapter(binder *jsonformat.JSONBinder, rangeBytes int64) (ScanOpenAdapter, error) {
	if binder == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: read_json runtime scan requires JSONBinder")
	}
	return ScanOpenAdapter{
		FunctionID: function.ReadJSONID,
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "read_json" {
				return nil, fmt.Errorf("session: read_json runtime adapter received function %q", spec.Function)
			}
			// 普通 JSON structural parallelism 仍是 Experimental；runtime source 串行 framing，并输出完整 relation 给 residual operators。
			return jsonformat.NewJSONRuntimeSource(ctx, jsonformat.JSONRuntimeSourceConfig{
				Binder:              binder,
				Pattern:             spec.Source,
				ExpectedFingerprint: string(spec.Fingerprint),
				RangeBytes:          rangeBytes,
			})
		},
	}, nil
}
