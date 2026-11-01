package settings

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

var (
	// ErrInvalidConfigVersion 表示配置版本非法或高于当前程序支持版本。
	ErrInvalidConfigVersion = errors.New("invalid config version")
	// ErrMissingMigration 表示旧配置缺少到下一版本的显式迁移步骤。
	ErrMissingMigration = errors.New("missing config migration")
	// ErrDuplicateMigration 表示同一源版本重复注册迁移步骤。
	ErrDuplicateMigration = errors.New("duplicate config migration")
	// ErrConfigConflict 表示迁移期间旧键与新键同时存在，不能静默选择其一。
	ErrConfigConflict = errors.New("config migration conflict")
	// ErrInvalidSettingValue 表示文本配置值不能解析为注册定义要求的类型。
	ErrInvalidSettingValue = errors.New("invalid setting value")
)

// RawConfig 是进入 SettingsRegistry 前的版本化文本配置。
type RawConfig struct {
	// Version 标识配置文件语义版本。
	Version int
	// Values 保存文本键值；迁移与解析都会复制该 map，避免修改调用方输入。
	Values map[string]string
}

// Migration 把一个配置版本显式迁移到下一个版本。
type Migration func(map[string]string) (map[string]string, error)

// Migrator 保存从旧版本到 CurrentVersion 的有序迁移步骤。
type Migrator struct {
	// mu 保护 steps，允许启动期注册和运行期并发迁移。
	mu sync.RWMutex
	// currentVersion 是本程序接受的最终配置版本。
	currentVersion int
	// steps 以源版本索引“version N → N+1”的迁移函数。
	steps map[int]Migration
}

// NewMigrator 创建以 currentVersion 为目标版本的迁移器。
func NewMigrator(currentVersion int) (*Migrator, error) {
	if currentVersion <= 0 {
		return nil, fmt.Errorf("%w: current version must be positive", ErrInvalidConfigVersion)
	}
	return &Migrator{currentVersion: currentVersion, steps: make(map[int]Migration)}, nil
}

// CurrentVersion 返回迁移器支持的最终配置版本。
func (m *Migrator) CurrentVersion() int { return m.currentVersion }

// Register 注册一条 version N → N+1 迁移，禁止重复覆盖已有迁移逻辑。
func (m *Migrator) Register(fromVersion int, migration Migration) error {
	if fromVersion <= 0 || fromVersion >= m.currentVersion || migration == nil {
		return fmt.Errorf("%w: source version %d", ErrInvalidConfigVersion, fromVersion)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.steps[fromVersion]; exists {
		return fmt.Errorf("%w: source version %d", ErrDuplicateMigration, fromVersion)
	}
	m.steps[fromVersion] = migration
	return nil
}

// Migrate 顺序执行所有显式迁移；未来版本和迁移缺口都直接报错，不做静默降级。
func (m *Migrator) Migrate(config RawConfig) (RawConfig, error) {
	if config.Version <= 0 || config.Version > m.currentVersion {
		return RawConfig{}, fmt.Errorf("%w: version %d, current %d", ErrInvalidConfigVersion, config.Version, m.currentVersion)
	}

	values := cloneStrings(config.Values)
	version := config.Version
	for version < m.currentVersion {
		m.mu.RLock()
		migration, ok := m.steps[version]
		m.mu.RUnlock()
		if !ok {
			return RawConfig{}, fmt.Errorf("%w: %d -> %d", ErrMissingMigration, version, version+1)
		}
		next, err := migration(cloneStrings(values))
		if err != nil {
			return RawConfig{}, fmt.Errorf("config migration %d -> %d: %w", version, version+1, err)
		}
		if next == nil {
			return RawConfig{}, fmt.Errorf("config migration %d -> %d returned nil values", version, version+1)
		}
		values = cloneStrings(next)
		version++
	}
	return RawConfig{Version: version, Values: values}, nil
}

// RenameSetting 创建安全的重命名迁移；旧键与新键同时存在时拒绝猜测优先级。
func RenameSetting(oldName, newName string) Migration {
	return func(values map[string]string) (map[string]string, error) {
		out := cloneStrings(values)
		oldValue, hasOld := out[oldName]
		_, hasNew := out[newName]
		if hasOld && hasNew {
			return nil, fmt.Errorf("%w: both %s and %s are set", ErrConfigConflict, oldName, newName)
		}
		if hasOld {
			delete(out, oldName)
			out[newName] = oldValue
		}
		return out, nil
	}
}

// ParseLayer 把文本配置解析成强类型 Layer；未知项和非法值会在进入 QuerySettings 前失败。
func (r *Registry) ParseLayer(values map[string]string) (Layer, error) {
	r.mu.RLock()
	definitions := make(map[string]Definition, len(r.definitions))
	for name, def := range r.definitions {
		definitions[name] = def
	}
	r.mu.RUnlock()

	parsed := make(map[string]Value, len(values))
	for name, raw := range values {
		def, ok := definitions[name]
		if !ok {
			return Layer{}, fmt.Errorf("%w: %s", ErrUnknownSetting, name)
		}
		value, err := parseValue(def.Kind, raw)
		if err != nil {
			return Layer{}, fmt.Errorf("%w for %s: %v", ErrInvalidSettingValue, name, err)
		}
		if def.Validate != nil {
			if err := def.Validate(value); err != nil {
				return Layer{}, fmt.Errorf("invalid setting %s: %w", name, err)
			}
		}
		parsed[name] = value
	}
	return NewLayer(parsed), nil
}

// parseValue 按注册定义类型解析文本值，避免弱类型 map 进入后续配置路径。
func parseValue(kind Kind, raw string) (Value, error) {
	switch kind {
	case KindBool:
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return Value{}, err
		}
		return Bool(v), nil
	case KindInt64:
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Int64(v), nil
	case KindString:
		return String(raw), nil
	default:
		return Value{}, ErrKindMismatch
	}
}

// cloneStrings 创建文本配置 map 的独立副本，保证迁移失败不会污染调用方配置。
func cloneStrings(values map[string]string) map[string]string {
	if len(values) == 0 {
		return make(map[string]string)
	}
	out := make(map[string]string, len(values))
	for name, value := range values {
		out[name] = value
	}
	return out
}
