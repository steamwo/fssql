package export

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrSegmentPlanMismatch 表示恢复作业使用了与 Manifest 不同的稳定分段计划。
	ErrSegmentPlanMismatch = errors.New("export: segment plan mismatch")
	// ErrSegmentPlanRequired 表示当前可恢复布局缺少稳定 SegmentPlan。
	ErrSegmentPlanRequired = errors.New("export: stable segment plan required")
)

const (
	// SegmentStrategyExplicitV1 表示 segmentID 由 Export planner 显式提供，DatasetWriter 只消费稳定身份。
	SegmentStrategyExplicitV1 = "EXPLICIT_V1"
	// SegmentPlanVersionV1 是 Explicit SegmentPlan 的首个持久化版本。
	SegmentPlanVersionV1 = 1
)

// SegmentPlan 描述 resumable MultiFile/Partitioned 导出的稳定分段身份契约。
type SegmentPlan struct {
	// Strategy 指定 Export planner 使用的稳定 segment 策略；Phase 4 初始仅支持 EXPLICIT_V1。
	Strategy string `json:"strategy"`
	// Version 固定该策略的持久化语义版本，恢复时必须完全一致。
	Version int `json:"version"`
	// PlannerFingerprint 标识产生稳定 segmentID 的不可变 planner 配置。
	PlannerFingerprint string `json:"planner_fingerprint"`
}

// NewExplicitSegmentPlan 创建 Phase 4 初始 EXPLICIT_V1 分段计划。
func NewExplicitSegmentPlan(plannerFingerprint string) (SegmentPlan, error) {
	plan := SegmentPlan{
		Strategy:           SegmentStrategyExplicitV1,
		Version:            SegmentPlanVersionV1,
		PlannerFingerprint: strings.TrimSpace(plannerFingerprint),
	}
	if err := plan.Validate(); err != nil {
		return SegmentPlan{}, err
	}
	return plan, nil
}

// Validate 校验 SegmentPlan 的策略、版本和 planner identity。
func (p SegmentPlan) Validate() error {
	if strings.TrimSpace(p.Strategy) == "" || strings.TrimSpace(p.PlannerFingerprint) == "" {
		return fmt.Errorf("%w: missing strategy/planner fingerprint", ErrInvalidManifest)
	}
	if p.Strategy != SegmentStrategyExplicitV1 || p.Version != SegmentPlanVersionV1 {
		return fmt.Errorf("%w: unsupported segment plan %s/v%d", ErrInvalidManifest, p.Strategy, p.Version)
	}
	return nil
}

// Equal 判断两个 SegmentPlan 是否代表完全相同的恢复身份。
func (p SegmentPlan) Equal(other SegmentPlan) bool {
	return p.Strategy == other.Strategy && p.Version == other.Version && p.PlannerFingerprint == other.PlannerFingerprint
}

// BindSegmentPlan 在任何文件写入前把稳定分段计划绑定到 Manifest；重复绑定相同计划是幂等的。
func (m *Manifest) BindSegmentPlan(plan SegmentPlan) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	plan.Strategy = strings.TrimSpace(plan.Strategy)
	plan.PlannerFingerprint = strings.TrimSpace(plan.PlannerFingerprint)
	if err := plan.Validate(); err != nil {
		return err
	}
	if m.SegmentPlan != nil {
		if !m.SegmentPlan.Equal(plan) {
			return fmt.Errorf("%w: existing=%s/v%d/%s new=%s/v%d/%s", ErrSegmentPlanMismatch,
				m.SegmentPlan.Strategy, m.SegmentPlan.Version, m.SegmentPlan.PlannerFingerprint,
				plan.Strategy, plan.Version, plan.PlannerFingerprint)
		}
		return nil
	}
	if m.ExportState != ExportStatePlanned {
		return fmt.Errorf("%w: segment plan must be bound while export is PLANNED", ErrInvalidTransition)
	}
	copied := plan
	m.SegmentPlan = &copied
	return nil
}
