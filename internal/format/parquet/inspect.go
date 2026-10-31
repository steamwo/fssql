package parquet

import (
	"context"

	"github.com/steamwo/fssql/internal/introspection"
)

// InspectSource returns source schema information projected from the same binding
// used by query execution. It must not infer schema independently of Bind.
func (b *SourceBinder) InspectSource(ctx context.Context, pattern string, opts SourceBindOptions) (introspection.SourceReport, error) {
	binding, err := b.Bind(ctx, pattern, opts)
	if err != nil {
		return introspection.SourceReport{}, err
	}
	return InspectionFromBinding(binding), nil
}

// DescribeSource is the DESCRIBE-facing projection of InspectSource. The returned
// columns and report are derived from one shared binding result.
func (b *SourceBinder) DescribeSource(ctx context.Context, pattern string, opts SourceBindOptions) ([]introspection.Column, introspection.SourceReport, error) {
	report, err := b.InspectSource(ctx, pattern, opts)
	if err != nil {
		return nil, introspection.SourceReport{}, err
	}
	return report.Columns(), report, nil
}

func InspectionFromBinding(binding SourceBinding) introspection.SourceReport {
	schema := binding.Schema()
	return introspection.NewSourceReport(
		schema.Fields,
		introspection.SchemaSourceFileMetadata,
		introspection.ConfidenceExact,
		binding.MatchedFiles(),
		binding.SchemaFiles(),
		binding.Fingerprint(),
		binding.UnionByName(),
		binding.ValidationMode() == SchemaValidationFull,
	)
}
