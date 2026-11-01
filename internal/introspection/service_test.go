package introspection

import (
	"context"
	"errors"
	"testing"

	coretypes "github.com/steamwo/fssql/internal/core/types"
)

func TestRegistryRoutesNormalizedSourceSpecWithoutFallback(t *testing.T) {
	registry := NewRegistry()
	called := false
	err := registry.Register(SourceFormatCSV, InspectorFunc(func(ctx context.Context, spec SourceSpec) (SourceReport, error) {
		called = true
		if spec.Format != SourceFormatCSV || spec.Pattern != "mem://dataset/*.csv" {
			t.Fatalf("spec=%+v", spec)
		}
		return NewSourceReport(
			[]coretypes.StructField{{Name: "id", Type: coretypes.LogicalType{ID: coretypes.BigInt}}},
			SchemaSourceSampleInferred,
			ConfidenceSampled,
			2,
			1,
			"fingerprint",
			false,
			false,
		), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	report, err := registry.InspectSource(context.Background(), SourceSpec{Format: " CSV ", Pattern: " mem://dataset/*.csv "})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("registered inspector was not called")
	}
	columns := report.Columns()
	if len(columns) != 1 || columns[0].Name != "id" || columns[0].TypeName != "BIGINT" {
		t.Fatalf("columns=%+v", columns)
	}
}

func TestRegistryRejectsUnsupportedAndDuplicateInspectors(t *testing.T) {
	registry := NewRegistry()
	inspector := InspectorFunc(func(context.Context, SourceSpec) (SourceReport, error) { return SourceReport{}, nil })
	if err := registry.Register(SourceFormatParquet, inspector); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(SourceFormatParquet, inspector); err == nil {
		t.Fatal("expected duplicate registration error")
	}
	_, err := registry.InspectSource(context.Background(), SourceSpec{Format: SourceFormatJSON, Pattern: "mem://x/data.json"})
	if !errors.Is(err, ErrUnsupportedSourceFormat) {
		t.Fatalf("error=%v, want unsupported format", err)
	}
}

func TestRegistryPropagatesCancellationBeforeInspector(t *testing.T) {
	registry := NewRegistry()
	called := false
	if err := registry.Register(SourceFormatCSV, InspectorFunc(func(context.Context, SourceSpec) (SourceReport, error) {
		called = true
		return SourceReport{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := registry.InspectSource(ctx, SourceSpec{Format: SourceFormatCSV, Pattern: "mem://x/data.csv"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want canceled", err)
	}
	if called {
		t.Fatal("inspector called after cancellation")
	}
}

func TestSourceSpecValidation(t *testing.T) {
	cases := []SourceSpec{
		{},
		{Format: SourceFormatCSV},
		{Pattern: "mem://x/data.csv"},
	}
	for _, spec := range cases {
		if err := spec.Validate(); !errors.Is(err, ErrInvalidSourceSpec) {
			t.Fatalf("spec=%+v error=%v", spec, err)
		}
	}
}
