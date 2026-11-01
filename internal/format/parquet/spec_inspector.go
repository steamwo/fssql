package parquet

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/introspection"
)

// SpecInspector 把 Parquet SourceBinder 适配到统一 SourceSpec；schema resolution 仍只经过 SourceBinder.Bind。
type SpecInspector struct {
	// Binder 是 Query/Inspect 共用的 Parquet binding 入口。
	Binder *SourceBinder
	// Options 保存 wiring 层提供的 FileSet/metadata 默认设置；SourceSpec 仅覆盖显式 schema 行为。
	Options SourceBindOptions
}

// NewSpecInspector 创建 Parquet SourceSpec adapter。
func NewSpecInspector(binder *SourceBinder, options SourceBindOptions) (*SpecInspector, error) {
	if binder == nil {
		return nil, errors.New("parquet: source binder required for spec inspector")
	}
	return &SpecInspector{Binder: binder, Options: options}, nil
}

// InspectSource 复用 Parquet SourceBinder.InspectSource，并把显式 union/full-validation 请求映射到真实 Bind options。
func (i *SpecInspector) InspectSource(ctx context.Context, spec introspection.SourceSpec) (introspection.SourceReport, error) {
	if i == nil || i.Binder == nil {
		return introspection.SourceReport{}, errors.New("parquet: uninitialized spec inspector")
	}
	spec = spec.Normalize()
	if err := spec.Validate(); err != nil {
		return introspection.SourceReport{}, err
	}
	if spec.Format != introspection.SourceFormatParquet {
		return introspection.SourceReport{}, fmt.Errorf("%w: Parquet inspector received %s", introspection.ErrUnsupportedSourceFormat, spec.Format)
	}
	opts := i.Options
	opts.UnionByName = spec.UnionByName
	if spec.FullValidation {
		opts.ValidationMode = SchemaValidationFull
	} else {
		opts.ValidationMode = SchemaValidationBounded
	}
	return i.Binder.InspectSource(ctx, spec.Pattern, opts)
}
