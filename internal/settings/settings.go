package settings

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUnknownSetting 表示调用方引用了未注册的配置项。
	ErrUnknownSetting = errors.New("unknown setting")
	// ErrDuplicateSetting 表示同名配置项被重复注册。
	ErrDuplicateSetting = errors.New("duplicate setting")
	// ErrKindMismatch 表示配置值类型与注册定义不一致。
	ErrKindMismatch = errors.New("setting kind mismatch")
)

// Kind 描述配置值的稳定类型，避免在配置解析路径使用 map[string]any。
type Kind uint8

const (
	// KindInvalid 是未初始化值，不能注册为有效配置类型。
	KindInvalid Kind = iota
	// KindBool 表示布尔配置。
	KindBool
	// KindInt64 表示 64 位整数配置。
	KindInt64
	// KindString 表示字符串配置。
	KindString
)

// Scope 描述 AD-035 冻结的配置覆盖层级。
type Scope uint8

const (
	// ScopeEngine 是全局引擎默认层。
	ScopeEngine Scope = iota
	// ScopeResourceGroup 是资源组覆盖层。
	ScopeResourceGroup
	// ScopeSession 是会话覆盖层。
	ScopeSession
	// ScopeStatement 是单语句覆盖层，优先级最高。
	ScopeStatement
)

// Value 是配置系统内部使用的强类型值容器。
type Value struct {
	// kind 记录值的逻辑类型。
	kind Kind
	// boolValue 保存布尔值。
	boolValue bool
	// int64Value 保存整数值。
	int64Value int64
	// stringValue 保存字符串值。
	stringValue string
}

// Bool 创建布尔配置值。
func Bool(v bool) Value { return Value{kind: KindBool, boolValue: v} }

// Int64 创建整数配置值。
func Int64(v int64) Value { return Value{kind: KindInt64, int64Value: v} }

// String 创建字符串配置值。
func String(v string) Value { return Value{kind: KindString, stringValue: v} }

// Kind 返回配置值类型。
func (v Value) Kind() Kind { return v.kind }

// BoolValue 读取布尔值；类型不匹配时返回错误。
func (v Value) BoolValue() (bool, error) {
	if v.kind != KindBool {
		return false, ErrKindMismatch
	}
	return v.boolValue, nil
}

// Int64Value 读取整数值；类型不匹配时返回错误。
func (v Value) Int64Value() (int64, error) {
	if v.kind != KindInt64 {
		return 0, ErrKindMismatch
	}
	return v.int64Value, nil
}

// StringValue 读取字符串值；类型不匹配时返回错误。
func (v Value) StringValue() (string, error) {
	if v.kind != KindString {
		return "", ErrKindMismatch
	}
	return v.stringValue, nil
}

// Validator 用于在 QuerySettings 冻结前校验配置值。
type Validator func(Value) error

// Definition 描述一个已注册配置项。
type Definition struct {
	// Name 是稳定配置名。
	Name string
	// Kind 是配置项允许的值类型。
	Kind Kind
	// Default 是没有任何覆盖时采用的默认值。
	Default Value
	// Validate 在值写入最终 QuerySettings 前执行额外约束检查。
	Validate Validator
}

// Layer 保存某一配置作用域的覆盖值。
type Layer struct {
	// values 为私有副本，防止调用方在解析期间修改原始 map。
	values map[string]Value
}

// NewLayer 创建不可由外部 map 继续修改的配置层快照。
func NewLayer(values map[string]Value) Layer {
	return Layer{values: cloneValues(values)}
}

// With 返回包含额外覆盖值的新 Layer，不修改原 Layer。
func (l Layer) With(name string, value Value) Layer {
	next := cloneValues(l.values)
	next[name] = value
	return Layer{values: next}
}

// QuerySettings 是绑定到单次 Query 的不可变最终配置快照。
type QuerySettings struct {
	// values 仅在 Resolve 中构造，之后不暴露可写引用。
	values map[string]Value
}

// Get 返回配置值；QuerySettings 本身不会把内部 map 暴露给调用方。
func (q QuerySettings) Get(name string) (Value, bool) {
	v, ok := q.values[name]
	return v, ok
}

// Registry 保存稳定配置定义并负责按 AD-035 层级生成 QuerySettings。
type Registry struct {
	// mu 保护 definitions，允许初始化期注册与运行期并发解析安全共存。
	mu sync.RWMutex
	// definitions 按稳定配置名索引注册定义。
	definitions map[string]Definition
}

// NewRegistry 创建空配置注册表。
func NewRegistry() *Registry {
	return &Registry{definitions: make(map[string]Definition)}
}

// Register 注册配置定义；同名定义、非法类型或默认值类型不一致会被拒绝。
func (r *Registry) Register(def Definition) error {
	if def.Name == "" {
		return fmt.Errorf("setting name must not be empty")
	}
	if def.Kind == KindInvalid || def.Default.Kind() != def.Kind {
		return fmt.Errorf("%w: %s", ErrKindMismatch, def.Name)
	}
	if def.Validate != nil {
		if err := def.Validate(def.Default); err != nil {
			return fmt.Errorf("invalid default for %s: %w", def.Name, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.definitions[def.Name]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateSetting, def.Name)
	}
	r.definitions[def.Name] = def
	return nil
}

// Resolve 按 Engine → ResourceGroup → Session → Statement 的顺序应用覆盖并冻结结果。
func (r *Registry) Resolve(engine, resourceGroup, session, statement Layer) (QuerySettings, error) {
	r.mu.RLock()
	definitions := make(map[string]Definition, len(r.definitions))
	for name, def := range r.definitions {
		definitions[name] = def
	}
	r.mu.RUnlock()

	resolved := make(map[string]Value, len(definitions))
	for name, def := range definitions {
		resolved[name] = def.Default
	}

	layers := []struct {
		scope Scope
		layer Layer
	}{
		{scope: ScopeEngine, layer: engine},
		{scope: ScopeResourceGroup, layer: resourceGroup},
		{scope: ScopeSession, layer: session},
		{scope: ScopeStatement, layer: statement},
	}
	for _, item := range layers {
		if err := applyLayer(definitions, resolved, item.scope, item.layer); err != nil {
			return QuerySettings{}, err
		}
	}
	return QuerySettings{values: resolved}, nil
}

// applyLayer 校验并应用单层覆盖；所有覆盖都必须来自 Registry 的稳定定义。
func applyLayer(definitions map[string]Definition, resolved map[string]Value, scope Scope, layer Layer) error {
	for name, value := range layer.values {
		def, ok := definitions[name]
		if !ok {
			return fmt.Errorf("%w at scope %d: %s", ErrUnknownSetting, scope, name)
		}
		if value.Kind() != def.Kind {
			return fmt.Errorf("%w at scope %d: %s", ErrKindMismatch, scope, name)
		}
		if def.Validate != nil {
			if err := def.Validate(value); err != nil {
				return fmt.Errorf("invalid setting %s at scope %d: %w", name, scope, err)
			}
		}
		resolved[name] = value
	}
	return nil
}

// cloneValues 创建配置 map 的独立副本，保证 Layer 与 QuerySettings 的不可变边界。
func cloneValues(values map[string]Value) map[string]Value {
	if len(values) == 0 {
		return make(map[string]Value)
	}
	out := make(map[string]Value, len(values))
	for name, value := range values {
		out[name] = value
	}
	return out
}
