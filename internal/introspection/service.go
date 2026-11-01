package introspection

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrUnsupportedSourceFormat 表示当前服务没有注册能够复用真实 Bind 路径的 inspector。
	ErrUnsupportedSourceFormat = errors.New("introspection: unsupported source format")
	// ErrInvalidSourceSpec 表示调用方提供了不完整或非法的 SourceSpec。
	ErrInvalidSourceSpec = errors.New("introspection: invalid source spec")
)

// SourceFormat 是 SourceSpec 使用的稳定格式标识；Protocol 仍由 URI/VFS 决定。
type SourceFormat string

const (
	SourceFormatParquet SourceFormat = "parquet"
	SourceFormatCSV     SourceFormat = "csv"
	SourceFormatJSON    SourceFormat = "json"
	SourceFormatNDJSON  SourceFormat = "ndjson"
)

// SourceSpec 描述一次 source schema inspection 请求。
// 它只表达格式/源选择与显式 schema 行为，不携带协议实现细节。
type SourceSpec struct {
	// Format 选择真实 Query Bind/TableFunction Bind 对应的格式 inspector。
	Format SourceFormat
	// Pattern 是交给 FileSetPlanner/Bind 的 canonical URI 或 glob。
	Pattern string
	// UnionByName 显式请求支持该语义的格式按列名合并 schema。
	UnionByName bool
	// FullValidation 显式启用 Experimental 全量多文件 schema validation；默认必须为 false。
	FullValidation bool
}

// Normalize 返回仅规范化格式大小写和前后空白的副本，不改变 Pattern 内容。
func (s SourceSpec) Normalize() SourceSpec {
	s.Format = SourceFormat(strings.ToLower(strings.TrimSpace(string(s.Format))))
	s.Pattern = strings.TrimSpace(s.Pattern)
	return s
}

// Validate 验证通用 SourceSpec；格式专属 option 由对应 inspector 再验证。
func (s SourceSpec) Validate() error {
	s = s.Normalize()
	if s.Format == "" {
		return fmt.Errorf("%w: empty format", ErrInvalidSourceSpec)
	}
	if s.Pattern == "" {
		return fmt.Errorf("%w: empty pattern", ErrInvalidSourceSpec)
	}
	return nil
}

// Inspector 是 Source Introspection 的唯一服务边界。
// 具体实现必须复用真实 Query Bind/TableFunction.Bind，禁止独立维护第二套 schema inference。
type Inspector interface {
	InspectSource(context.Context, SourceSpec) (SourceReport, error)
}

// InspectorFunc 便于 wiring 层把已有格式 Binder 适配到统一 Inspector，而不改变格式 ABI。
type InspectorFunc func(context.Context, SourceSpec) (SourceReport, error)

// InspectSource 调用底层函数实现统一 Inspector。
func (f InspectorFunc) InspectSource(ctx context.Context, spec SourceSpec) (SourceReport, error) {
	if f == nil {
		return SourceReport{}, errors.New("introspection: nil inspector function")
	}
	return f(ctx, spec)
}

// Registry 按格式路由 SourceSpec；map 仅位于 control path，不进入执行热路径。
type Registry struct {
	// mu 保护 inspector 注册与读取，允许 server 初始化期间安全装配。
	mu sync.RWMutex
	// inspectors 保存格式到真实 Bind-backed inspector 的映射。
	inspectors map[SourceFormat]Inspector
}

// NewRegistry 创建空 Source Introspection Registry。
func NewRegistry() *Registry {
	return &Registry{inspectors: make(map[SourceFormat]Inspector)}
}

// Register 注册一个格式 inspector；重复注册必须显式失败，避免运行时覆盖 schema 语义。
func (r *Registry) Register(format SourceFormat, inspector Inspector) error {
	if r == nil {
		return errors.New("introspection: nil registry")
	}
	format = SourceFormat(strings.ToLower(strings.TrimSpace(string(format))))
	if format == "" {
		return fmt.Errorf("%w: empty format", ErrInvalidSourceSpec)
	}
	if inspector == nil {
		return errors.New("introspection: nil inspector")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.inspectors[format]; exists {
		return fmt.Errorf("introspection: duplicate inspector for %s", format)
	}
	r.inspectors[format] = inspector
	return nil
}

// InspectSource 验证并路由请求；未注册格式显式失败，不做 fallback inference。
func (r *Registry) InspectSource(ctx context.Context, spec SourceSpec) (SourceReport, error) {
	if r == nil {
		return SourceReport{}, errors.New("introspection: nil registry")
	}
	if err := ctx.Err(); err != nil {
		return SourceReport{}, err
	}
	spec = spec.Normalize()
	if err := spec.Validate(); err != nil {
		return SourceReport{}, err
	}
	r.mu.RLock()
	inspector := r.inspectors[spec.Format]
	r.mu.RUnlock()
	if inspector == nil {
		return SourceReport{}, fmt.Errorf("%w: %s", ErrUnsupportedSourceFormat, spec.Format)
	}
	return inspector.InspectSource(ctx, spec)
}
