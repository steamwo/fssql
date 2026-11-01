package csv

import (
	"context"

	"github.com/steamwo/fssql/internal/introspection"
)

// InspectSource projects schema information from the same binding used by CSV
// query execution. No independent introspection-time inference path exists.
func (b *SourceBinder) InspectSource(ctx context.Context, pattern string, opts SourceBindOptions) (introspection.SourceReport, error) {
	binding, err := b.Bind(ctx, pattern, opts)
	if err != nil { return introspection.SourceReport{}, err }
	return InspectionFromBinding(binding), nil
}

func (b *SourceBinder) DescribeSource(ctx context.Context, pattern string, opts SourceBindOptions) ([]introspection.Column, introspection.SourceReport, error) {
	report, err := b.InspectSource(ctx, pattern, opts)
	if err != nil { return nil, introspection.SourceReport{}, err }
	return report.Columns(), report, nil
}

func InspectionFromBinding(binding SourceBinding) introspection.SourceReport {
	schema := binding.Schema()
	return introspection.NewSourceReport(
		schema.Fields,
		introspection.SchemaSourceSampleInferred,
		introspection.ConfidenceSampled,
		binding.MatchedFiles(),
		binding.SchemaFiles(),
		binding.Fingerprint(),
		false,
		false,
	)
}
