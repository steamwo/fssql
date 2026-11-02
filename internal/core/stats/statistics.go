package stats

import "context"

// Source 表示 statistics 的来源；Unknown 不得伪装成高置信度估计。
type Source uint8

const (
	Unknown Source = iota
	StorageNative
	CatalogPersistent
	Sampled
)

// Confidence 是 [0,1] 的统计置信度。
type Confidence float64

// DatasetFingerprint 标识产生 statistics 的不可变数据集快照。
type DatasetFingerprint string

// RelationStatistics 是 optimizer 可消费的 generic relation statistics。
type RelationStatistics struct {
	Source      Source             // Source 记录统计来源。
	Confidence  Confidence         // Confidence 是估计置信度。
	Fingerprint DatasetFingerprint // Fingerprint 必须对应 statement snapshot。
	Rows        uint64             // Rows 是估计或已知 row count。
}

// Valid 验证 statistics 不会把 Unknown 或非法 confidence 当作可信输入。
func (s RelationStatistics) Valid() bool {
	if s.Confidence < 0 || s.Confidence > 1 {
		return false
	}
	if s.Source == Unknown {
		return s.Confidence == 0
	}
	return s.Fingerprint != ""
}

// Provider 是 optimizer 唯一认识的 statistics contract。
type Provider interface {
	Lookup(context.Context, DatasetFingerprint) (RelationStatistics, bool, error)
}
