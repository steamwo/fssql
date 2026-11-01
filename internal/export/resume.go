package export

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrResumeIdentityMismatch 表示重启后的 query/source/layout identity 与原作业不同。
	ErrResumeIdentityMismatch = errors.New("export: resume identity mismatch")
	// ErrCommitRecoveryRequired 表示作业在 COMMITTING 崩溃，必须先与 final manifest 对账，不能重写数据文件。
	ErrCommitRecoveryRequired = errors.New("export: commit recovery required")
	// ErrResumeAlreadyCommitted 表示已提交数据集不允许再次进入 WRITING。
	ErrResumeAlreadyCommitted = errors.New("export: dataset already committed")
)

// ResumeIdentity 保存重启 attempt 必须与 Manifest 完全匹配的稳定身份。
type ResumeIdentity struct {
	// QueryFingerprint 是重新绑定后的查询身份。
	QueryFingerprint string
	// SourceSnapshotFingerprint 是重新冻结的 Statement SourceSnapshot 身份。
	SourceSnapshotFingerprint string
	// Layout 决定是否必须提供 Stable SegmentPlan。
	Layout LayoutMode
	// SegmentPlan 是重启后 Export planner 重建的稳定计划；SingleFile 可为空。
	SegmentPlan *SegmentPlan
}

// Validate 校验恢复请求本身是否满足 ADR-0003 的前置条件。
func (i ResumeIdentity) Validate() error {
	if strings.TrimSpace(i.QueryFingerprint) == "" || strings.TrimSpace(i.SourceSnapshotFingerprint) == "" {
		return fmt.Errorf("%w: missing query/source fingerprint", ErrResumeIdentityMismatch)
	}
	switch i.Layout {
	case LayoutSingleFile:
		if i.SegmentPlan != nil {
			if err := i.SegmentPlan.Validate(); err != nil {
				return err
			}
		}
	case LayoutMultiFile, LayoutPartitioned:
		if i.SegmentPlan == nil {
			return ErrSegmentPlanRequired
		}
		if err := i.SegmentPlan.Validate(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unknown layout %q", ErrResumeIdentityMismatch, i.Layout)
	}
	return nil
}

// PrepareResume 在任何新文件打开前验证稳定身份，并把旧 attempt 的 incomplete WRITING 文件转为可整文件重做状态。
func (m *Manifest) PrepareResume(identity ResumeIdentity) error {
	if m == nil {
		return fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if m.QueryFingerprint != strings.TrimSpace(identity.QueryFingerprint) || m.SourceSnapshot.Fingerprint != strings.TrimSpace(identity.SourceSnapshotFingerprint) {
		return fmt.Errorf("%w: query/source changed", ErrResumeIdentityMismatch)
	}
	if err := validateResumeSegmentPlan(m.SegmentPlan, identity.SegmentPlan, identity.Layout); err != nil {
		return err
	}
	switch m.ExportState {
	case ExportStateCommitted:
		return ErrResumeAlreadyCommitted
	case ExportStateCommitting:
		return ErrCommitRecoveryRequired
	case ExportStatePlanned, ExportStateWriting, ExportStateFailed:
	default:
		return fmt.Errorf("%w: unsupported export state %s", ErrInvalidTransition, m.ExportState)
	}

	// 所有 identity 检查完成后才允许修改持久化状态，避免 mismatch 导致部分状态被改写。
	for _, file := range m.FileStates {
		if file.State != FileStateWriting {
			continue
		}
		if err := m.TransitionFile(file.Path, FileStateFailed, 0, ""); err != nil {
			return err
		}
	}

	switch m.ExportState {
	case ExportStatePlanned:
		return m.TransitionExport(ExportStateWriting)
	case ExportStateWriting:
		if err := m.TransitionExport(ExportStateFailed); err != nil {
			return err
		}
		return m.TransitionExport(ExportStateWriting)
	case ExportStateFailed:
		return m.TransitionExport(ExportStateWriting)
	default:
		return fmt.Errorf("%w: cannot resume export state %s", ErrInvalidTransition, m.ExportState)
	}
}

// validateResumeSegmentPlan 确保 MultiFile/Partitioned 始终携带相同计划，SingleFile 则禁止凭空引入或丢失已绑定计划。
func validateResumeSegmentPlan(stored, requested *SegmentPlan, layout LayoutMode) error {
	if layout != LayoutSingleFile && requested == nil {
		return ErrSegmentPlanRequired
	}
	if stored == nil && requested == nil {
		return nil
	}
	if stored == nil || requested == nil {
		return ErrSegmentPlanMismatch
	}
	if !stored.Equal(*requested) {
		return fmt.Errorf("%w: stored=%s/v%d/%s requested=%s/v%d/%s", ErrSegmentPlanMismatch,
			stored.Strategy, stored.Version, stored.PlannerFingerprint,
			requested.Strategy, requested.Version, requested.PlannerFingerprint)
	}
	return nil
}
