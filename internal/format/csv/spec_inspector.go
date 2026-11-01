package csv

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/introspection"
)

// SpecInspector 把 CSV SourceBinder 适配到统一 SourceSpec 接口；所有 schema 仍由 SourceBinder.Bind 产生。
type SpecInspector struct {
	// Binder 是 Query/Inspect 共用的 CSV binding 入口。
	Binder *SourceBinder
	// Options 保存 wiring 层提供的 CSV inference/Fileset 默认设置。
	Options SourceBindOptions
}

// NewSpecInspector 创建 CSV SourceSpec adapter。
func NewSpecInspector(binder *SourceBinder, options SourceBindOptions) (*SpecInspector, error) {
	if binder == nil {
		return nil, errors.New("csv: source binder required for spec inspector")
	}
	return &SpecInspector{Binder: binder, Options: options}, nil
}

// InspectSource 复用 CSV SourceBinder.InspectSource；CSV 不接受 parquet 专属的 union/full-validation 选项。
func (i *SpecInspector) InspectSource(ctx context.Context, spec introspection.SourceSpec) (introspection.SourceReport, error) {
	if i == nil || i.Binder == nil {
		return introspection.SourceReport{}, errors.New("csv: uninitialized spec inspector")
	}
	spec = spec.Normalize()
	if err := spec.Validate(); err != nil {
		return introspection.SourceReport{}, err
	}
	if spec.Format != introspection.SourceFormatCSV {
		return introspection.SourceReport{}, fmt.Errorf("%w: CSV inspector received %s", introspection.ErrUnsupportedSourceFormat, spec.Format)
	}
	if spec.UnionByName || spec.FullValidation {
		return introspection.SourceReport{}, fmt.Errorf("%w: CSV does not support union_by_name/full_validation inspection options", introspection.ErrInvalidSourceSpec)
	}
	return i.Binder.InspectSource(ctx, spec.Pattern, i.Options)
}
