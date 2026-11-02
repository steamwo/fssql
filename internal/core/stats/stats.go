package stats

import (
	"context"
	"fmt"
)

// SourceKind 标识统计信息来源；Optimizer 只依赖此通用来源分类。
type SourceKind uint8

const (
	SourceUnknown SourceKind = iota
	SourceStorageNative
	SourceCatalogPersistent
	SourceSampled
)

// Confidence 描述统计估计可信度，值越大表示越可靠。
type Confidence uint8

const (
	ConfidenceUnknown Confidence = iota
	ConfidenceLow
	ConfidenceMedium
	ConfidenceHigh
	ConfidenceExact
)

// DatasetFingerprint 是一次数据集快照/统计所绑定的稳定指纹。
type DatasetFingerprint string

// RelationKey 是统计查询的逻辑 relation 标识，不包含格式或远端协议类型。
type RelationKey struct {
	Catalog string // Catalog 是逻辑 catalog 名。
	Schema  string // Schema 是逻辑 schema 名。
	Name    string // Name 是 relation/table 名。
}

// Estimate 是 Optimizer 消费的最小统计快照。
type Estimate struct {
	Rows        uint64             // Rows 是估计行数。
	Confidence  Confidence         // Confidence 描述 Rows 可信度。
	Source      SourceKind         // Source 描述统计来源。
	Fingerprint DatasetFingerprint // Fingerprint 绑定数据集版本，避免跨快照误用。
}

// Valid 验证统计快照是否可安全供 Optimizer 使用。
func (e Estimate) Valid() bool {
	if e.Source == SourceUnknown {
		return e.Confidence == ConfidenceUnknown && e.Rows == 0
	}
	return e.Confidence != ConfidenceUnknown && e.Fingerprint != ""
}

// Provider 是 AD-024 的 generic StatisticsProvider contract。
type Provider interface {
	EstimateRelation(context.Context, RelationKey) (Estimate, error)
}

// StaticProvider 是测试、CatalogPersistent 或已冻结 snapshot 可使用的有界静态 provider。
type StaticProvider struct {
	maxEntries int                      // maxEntries 限制控制面统计表大小。
	entries    map[RelationKey]Estimate // entries 保存不可变 relation 统计。
}

// NewStaticProvider 创建有容量上限的静态 StatisticsProvider。
func NewStaticProvider(maxEntries int) (*StaticProvider, error) {
	if maxEntries <= 0 {
		return nil, fmt.Errorf("statistics max entries must be positive")
	}
	return &StaticProvider{maxEntries: maxEntries, entries: make(map[RelationKey]Estimate)}, nil
}

// Put 写入一条 relation 统计；同 key 覆盖不增加容量。
func (p *StaticProvider) Put(key RelationKey, estimate Estimate) error {
	if p == nil {
		return fmt.Errorf("statistics provider is nil")
	}
	if key.Name == "" {
		return fmt.Errorf("statistics relation name is empty")
	}
	if !estimate.Valid() {
		return fmt.Errorf("invalid statistics estimate")
	}
	if _, exists := p.entries[key]; !exists && len(p.entries) >= p.maxEntries {
		return fmt.Errorf("statistics capacity exceeded: %d", p.maxEntries)
	}
	p.entries[key] = estimate
	return nil
}

// EstimateRelation 返回 relation 统计；未命中时返回显式 Unknown，而不是伪造默认 cardinality。
func (p *StaticProvider) EstimateRelation(ctx context.Context, key RelationKey) (Estimate, error) {
	if p == nil {
		return Estimate{}, fmt.Errorf("statistics provider is nil")
	}
	if err := ctx.Err(); err != nil {
		return Estimate{}, err
	}
	if e, ok := p.entries[key]; ok {
		return e, nil
	}
	return Estimate{Source: SourceUnknown, Confidence: ConfidenceUnknown}, nil
}
