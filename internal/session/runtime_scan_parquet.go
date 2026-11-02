package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/format/parquet"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

// NewParquetScanOpenAdapter 把 Stable read_parquet ScanSpec 接到真实 Parquet RuntimeSource。
func NewParquetScanOpenAdapter(sourceBinder *parquet.SourceBinder) (ScanOpenAdapter, error) {
	if sourceBinder == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: read_parquet runtime scan requires SourceBinder")
	}
	return ScanOpenAdapter{
		FunctionID: function.ReadParquetID,
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "read_parquet" {
				return nil, fmt.Errorf("session: read_parquet runtime adapter received function %q", spec.Function)
			}
			// Filter/Projection/Limit 目前都只是 optimizer candidate；residual physical operator 仍保留。
			// 在 accepted-pushdown + ColumnIndex remap contract 完成前，Parquet 必须扫描完整绑定 schema。
			return parquet.NewRuntimeSource(ctx, parquet.RuntimeSourceConfig{
				Binder:              sourceBinder,
				Pattern:             spec.Source,
				ExpectedFingerprint: string(spec.Fingerprint),
			})
		},
	}, nil
}
