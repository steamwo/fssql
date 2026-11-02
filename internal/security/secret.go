package security

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrSecretNotFound 表示请求的 secret 不存在。
	ErrSecretNotFound = errors.New("security: secret not found")
	// ErrSecretExists 表示创建了重复 secret 名称。
	ErrSecretExists = errors.New("security: secret already exists")
	// ErrSecretUnauthorized 表示主体或目标 endpoint 不在 secret ACL 范围内。
	ErrSecretUnauthorized = errors.New("security: secret unauthorized")
)

// AuditEvent 是不包含 secret 明文的安全审计记录。
type AuditEvent struct {
	// Time 是安全事件发生时间。
	Time time.Time
	// Action 是稳定动作名，例如 secret.resolve 或 secret.rotate。
	Action string
	// Subject 是发起动作的认证主体。
	Subject string
	// Resource 是被访问的 secret 或安全资源名称。
	Resource string
	// Target 是经过标准化的目标 endpoint；不会写入 credential 内容。
	Target string
	// Allowed 表示安全策略最终是否允许该动作。
	Allowed bool
	// Reason 保存拒绝或状态原因，禁止包含 secret 明文。
	Reason string
	// CredentialVersion 记录操作关联的 credential 版本。
	CredentialVersion string
}

// Auditor 接收安全事件；具体 Prometheus/OTel/日志适配在后续 observability 任务完成。
type Auditor interface {
	Record(AuditEvent)
}

// nopAuditor 在未配置审计适配器时安全丢弃事件，不改变授权结果。
type nopAuditor struct{}

// Record 实现 Auditor。
func (nopAuditor) Record(AuditEvent) {}

// SecretScope 定义 secret 可被哪些主体用于哪些目标。
type SecretScope struct {
	// Subjects 是允许访问 secret 的主体；显式 "*" 表示任意已认证主体。
	Subjects []string
	// Schemes 限制 secret 可用于哪些协议；空列表表示不限制协议。
	Schemes []string
	// Hosts 限制目标主机；支持精确主机和 "*.example.com" 后缀模式。
	Hosts []string
}

// Allows 判断 subject 是否可把 secret 用于指定目标 URL。
func (s SecretScope) Allows(subject string, target *url.URL) bool {
	if subject == "" || !containsSubject(s.Subjects, subject) {
		return false
	}
	if target == nil {
		return len(s.Schemes) == 0 && len(s.Hosts) == 0
	}
	if len(s.Schemes) > 0 && !containsFold(s.Schemes, target.Scheme) {
		return false
	}
	if len(s.Hosts) > 0 && !matchesHost(s.Hosts, target.Hostname()) {
		return false
	}
	return true
}

// SecretSpec 是创建或轮换 secret 时提交的不可持久化明文输入。
type SecretSpec struct {
	// Name 是稳定 secret 名称。
	Name string
	// Type 是非 credential 的 adapter metadata；空值保留旧的 programmatic SecretManager 兼容行为。
	Type string
	// Values 保存 credential 字段；SecretManager 会立即复制，不保留调用方 map 引用。
	Values map[string]string
	// Scope 定义 secret 的 ACL 和 endpoint 范围。
	Scope SecretScope
}

// ResolvedSecret 是授权成功后返回的 credential 快照。
type ResolvedSecret struct {
	// name 是 secret 稳定名称。
	name string
	// typeName 是非 credential adapter metadata，不通过 Value 暴露。
	typeName string
	// version 是轮换后单调增加的版本号。
	version uint64
	// values 是该版本 credential 的私有快照。
	values map[string]string
}

// Name 返回 secret 稳定名称。
func (s ResolvedSecret) Name() string { return s.name }

// Type 返回 secret adapter metadata；它与 credential Values 分离。
func (s ResolvedSecret) Type() string { return s.typeName }

// Version 返回单调 credential 版本号。
func (s ResolvedSecret) Version() uint64 { return s.version }

// VersionString 返回可用于 RemoteClientManager ClientKey 的稳定版本字符串。
func (s ResolvedSecret) VersionString() string { return strconv.FormatUint(s.version, 10) }

// Value 返回指定 credential 字段，避免把内部 map 暴露给调用方。
func (s ResolvedSecret) Value(key string) (string, bool) {
	value, ok := s.values[key]
	return value, ok
}

// secretEntry 是 SecretManager 内部保存的最新 secret 版本。
type secretEntry struct {
	// name 是 secret 稳定名称。
	name string
	// typeName 是与 credential map 分离的 adapter metadata。
	typeName string
	// version 是最新 credential 版本。
	version uint64
	// values 保存最新 credential 的私有副本。
	values map[string]string
	// scope 保存授权与目标限制。
	scope SecretScope
}

// SecretManager 提供 scoped secret、ACL 检查和 credential rotation。
type SecretManager struct {
	// mu 保护 secrets 和版本轮换。
	mu sync.RWMutex
	// secrets 按稳定名称保存最新版本。
	secrets map[string]secretEntry
	// auditor 接收不含明文 credential 的授权与轮换事件。
	auditor Auditor
}

// NewSecretManager 创建 SecretManager；nil auditor 会替换为 no-op 实现。
func NewSecretManager(auditor Auditor) *SecretManager {
	if auditor == nil {
		auditor = nopAuditor{}
	}
	return &SecretManager{secrets: make(map[string]secretEntry), auditor: auditor}
}

// Create 创建 version=1 的新 secret；重复名称必须显式失败。
func (m *SecretManager) Create(spec SecretSpec) error {
	if err := validateSecretSpec(spec); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.secrets[spec.Name]; exists {
		return fmt.Errorf("%w: %s", ErrSecretExists, spec.Name)
	}
	m.secrets[spec.Name] = secretEntry{
		name: spec.Name, typeName: normalizeSecretType(spec.Type), version: 1,
		values: cloneSecretValues(spec.Values), scope: cloneSecretScope(spec.Scope),
	}
	m.auditor.Record(AuditEvent{Time: time.Now(), Action: "secret.create", Resource: spec.Name, Allowed: true, CredentialVersion: "1"})
	return nil
}

// Rotate 原子替换 credential 和 scope，并生成新的单调版本；旧 ResolvedSecret 快照不被修改。
// 已设置的 Type 不允许在 credential rotation 中改变，避免同名 secret 静默切换 adapter 语义。
func (m *SecretManager) Rotate(spec SecretSpec) (uint64, error) {
	if err := validateSecretSpec(spec); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, exists := m.secrets[spec.Name]
	if !exists {
		return 0, fmt.Errorf("%w: %s", ErrSecretNotFound, spec.Name)
	}
	if nextType := normalizeSecretType(spec.Type); nextType != "" {
		if current.typeName != "" && current.typeName != nextType {
			return 0, fmt.Errorf("security: secret type cannot change during rotation: %s -> %s", current.typeName, nextType)
		}
		current.typeName = nextType
	}
	current.version++
	current.values = cloneSecretValues(spec.Values)
	current.scope = cloneSecretScope(spec.Scope)
	m.secrets[spec.Name] = current
	version := strconv.FormatUint(current.version, 10)
	m.auditor.Record(AuditEvent{Time: time.Now(), Action: "secret.rotate", Resource: spec.Name, Allowed: true, CredentialVersion: version})
	return current.version, nil
}

// Resolve 校验主体和 endpoint scope 后返回 credential 的不可变版本快照。
func (m *SecretManager) Resolve(name, subject, targetRaw string) (ResolvedSecret, error) {
	var target *url.URL
	var err error
	if targetRaw != "" {
		target, err = url.Parse(targetRaw)
		if err != nil || target.Scheme == "" || target.Hostname() == "" {
			m.auditResolve(name, subject, targetRaw, false, "invalid target", "")
			return ResolvedSecret{}, fmt.Errorf("%w: invalid target", ErrSecretUnauthorized)
		}
	}

	m.mu.RLock()
	entry, exists := m.secrets[name]
	m.mu.RUnlock()
	if !exists {
		m.auditResolve(name, subject, targetRaw, false, "not found", "")
		return ResolvedSecret{}, fmt.Errorf("%w: %s", ErrSecretNotFound, name)
	}
	version := strconv.FormatUint(entry.version, 10)
	if !entry.scope.Allows(subject, target) {
		m.auditResolve(name, subject, targetRaw, false, "acl denied", version)
		return ResolvedSecret{}, fmt.Errorf("%w: %s", ErrSecretUnauthorized, name)
	}
	m.auditResolve(name, subject, targetRaw, true, "", version)
	return ResolvedSecret{name: entry.name, typeName: entry.typeName, version: entry.version, values: cloneSecretValues(entry.values)}, nil
}

// auditResolve 写入不包含 secret 明文的访问审计。
func (m *SecretManager) auditResolve(name, subject, target string, allowed bool, reason, version string) {
	m.auditor.Record(AuditEvent{
		Time: time.Now(), Action: "secret.resolve", Subject: subject, Resource: name,
		Target: target, Allowed: allowed, Reason: reason, CredentialVersion: version,
	})
}

// validateSecretSpec 拒绝空名称、空 credential、非法 TYPE metadata 和没有主体 ACL 的 secret。
func validateSecretSpec(spec SecretSpec) error {
	if strings.TrimSpace(spec.Name) == "" {
		return errors.New("security: secret name must not be empty")
	}
	if spec.Type != "" && strings.TrimSpace(spec.Type) == "" {
		return errors.New("security: secret type must not be blank")
	}
	if len(spec.Values) == 0 {
		return errors.New("security: secret values must not be empty")
	}
	if len(spec.Scope.Subjects) == 0 {
		return errors.New("security: secret scope must include subjects")
	}
	return nil
}

// normalizeSecretType 规范化非 credential TYPE metadata；空值用于旧 programmatic API 兼容。
func normalizeSecretType(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

// containsSubject 判断主体 ACL，只有显式 "*" 才表示通配。
func containsSubject(subjects []string, subject string) bool {
	for _, allowed := range subjects {
		if allowed == "*" || allowed == subject {
			return true
		}
	}
	return false
}

// containsFold 对协议名执行大小写不敏感比较。
func containsFold(values []string, value string) bool {
	for _, candidate := range values {
		if strings.EqualFold(candidate, value) {
			return true
		}
	}
	return false
}

// matchesHost 对目标主机执行精确或单后缀通配匹配。
func matchesHost(patterns []string, host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, pattern := range patterns {
		pattern = strings.TrimSuffix(strings.ToLower(pattern), ".")
		if pattern == host {
			return true
		}
		if strings.HasPrefix(pattern, "*.") {
			suffix := strings.TrimPrefix(pattern, "*")
			if strings.HasSuffix(host, suffix) && host != strings.TrimPrefix(suffix, ".") {
				return true
			}
		}
	}
	return false
}

// cloneSecretValues 创建 credential map 副本，避免调用方或旧版本快照被后续轮换修改。
func cloneSecretValues(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

// cloneSecretScope 复制 ACL slice，保证 SecretManager 不保留调用方可写引用。
func cloneSecretScope(scope SecretScope) SecretScope {
	return SecretScope{
		Subjects: append([]string(nil), scope.Subjects...),
		Schemes:  append([]string(nil), scope.Schemes...),
		Hosts:    append([]string(nil), scope.Hosts...),
	}
}
