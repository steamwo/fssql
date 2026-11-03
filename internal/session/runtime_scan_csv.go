package session

import (
	"context"
	"fmt"

	"github.com/steamwo/fssql/internal/execution/pipeline"
	"github.com/steamwo/fssql/internal/format/csv"
	"github.com/steamwo/fssql/internal/function"
	"github.com/steamwo/fssql/internal/planner"
)

// NewCSVScanOpenAdapter 把 Stable read_csv ScanSpec 接到既有 CSV Source 执行路径。
func NewCSVScanOpenAdapter(sourceBinder *csv.SourceBinder, scanOptions csv.ScanOptions) (ScanOpenAdapter, error) {
	if sourceBinder == nil {
		return ScanOpenAdapter{}, fmt.Errorf("session: read_csv runtime scan requires SourceBinder")
	}
	return ScanOpenAdapter{
		FunctionID: function.ReadCSVID,
		OpenCount: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			return csv.NewCountSource(ctx, csv.RuntimeSourceConfig{Binder: sourceBinder, Pattern: spec.Source, ExpectedFingerprint: string(spec.Fingerprint), Scan: scanOptions})
		},
		Open: func(ctx context.Context, spec planner.ScanSpec) (pipeline.Source, error) {
			if spec.Function != "read_csv" {
				return nil, fmt.Errorf("session: read_csv runtime adapter received function %q", spec.Function)
			}
			opts := scanOptions
			opts.Limit = spec.Pushdown.Limit
			opts.Projection = append([]int(nil), spec.Pushdown.Projection...)
			return csv.NewRuntimeSource(ctx, csv.RuntimeSourceConfig{
				Binder:              sourceBinder,
				Pattern:             spec.Source,
				ExpectedFingerprint: string(spec.Fingerprint),
				Scan:                opts,
			})
		},
	}, nil
}
