package json

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/introspection"
)

// JSONSpecInspector 把普通 JSON JSONBinder 适配到统一 SourceSpec；schema 仍只来自 JSONBinder.Bind。
type JSONSpecInspector struct {
	Binder  *JSONBinder      // Binder 是 Query/Inspect 共用的普通 JSON binding 入口。
	Options JSONBindOptions  // Options 保存 wiring 层提供的 FileSet/inference 默认设置。
}

// NewJSONSpecInspector 创建普通 JSON SourceSpec adapter。
func NewJSONSpecInspector(binder *JSONBinder, options JSONBindOptions) (*JSONSpecInspector, error) {
	if binder == nil {
		return nil, errors.New("json: source binder required for spec inspector")
	}
	return &JSONSpecInspector{Binder: binder, Options: options}, nil
}

// InspectSource 复用 JSONBinder.InspectSource；普通 JSON 不接受 parquet 专属 schema 选项。
func (i *JSONSpecInspector) InspectSource(ctx context.Context, spec introspection.SourceSpec) (introspection.SourceReport, error) {
	if i == nil || i.Binder == nil {
		return introspection.SourceReport{}, errors.New("json: uninitialized spec inspector")
	}
	spec = spec.Normalize()
	if err := spec.Validate(); err != nil {
		return introspection.SourceReport{}, err
	}
	if spec.Format != introspection.SourceFormatJSON {
		return introspection.SourceReport{}, fmt.Errorf("%w: JSON inspector received %s", introspection.ErrUnsupportedSourceFormat, spec.Format)
	}
	if spec.UnionByName || spec.FullValidation {
		return introspection.SourceReport{}, fmt.Errorf("%w: JSON does not support union_by_name/full_validation inspection options", introspection.ErrInvalidSourceSpec)
	}
	return i.Binder.InspectSource(ctx, spec.Pattern, i.Options)
}

// NDJSONSpecInspector 把现有 NDJSONBinder 适配到统一 SourceSpec；不会建立第二套 inference path。
type NDJSONSpecInspector struct {
	Binder  *NDJSONBinder      // Binder 是 Query/Inspect 共用的 NDJSON binding 入口。
	Options NDJSONBindOptions  // Options 保存 bounded inference 默认设置。
}

// NewNDJSONSpecInspector 创建 NDJSON SourceSpec adapter。
func NewNDJSONSpecInspector(binder *NDJSONBinder, options NDJSONBindOptions) (*NDJSONSpecInspector, error) {
	if binder == nil {
		return nil, errors.New("json: NDJSON binder required for spec inspector")
	}
	return &NDJSONSpecInspector{Binder: binder, Options: options}, nil
}

// InspectSource 复用 NDJSONBinder.InspectSource。
func (i *NDJSONSpecInspector) InspectSource(ctx context.Context, spec introspection.SourceSpec) (introspection.SourceReport, error) {
	if i == nil || i.Binder == nil {
		return introspection.SourceReport{}, errors.New("json: uninitialized NDJSON spec inspector")
	}
	spec = spec.Normalize()
	if err := spec.Validate(); err != nil {
		return introspection.SourceReport{}, err
	}
	if spec.Format != introspection.SourceFormatNDJSON {
		return introspection.SourceReport{}, fmt.Errorf("%w: NDJSON inspector received %s", introspection.ErrUnsupportedSourceFormat, spec.Format)
	}
	if spec.UnionByName || spec.FullValidation {
		return introspection.SourceReport{}, fmt.Errorf("%w: NDJSON does not support union_by_name/full_validation inspection options", introspection.ErrInvalidSourceSpec)
	}
	return i.Binder.InspectSource(ctx, spec.Pattern, i.Options)
}
