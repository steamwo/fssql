package export

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// LoadRecoverAndPrepareResume 按 ADR-0004 顺序执行 durable Manifest load、identity 校验、协议资源 recovery、PrepareResume 与 durable Save。
func LoadRecoverAndPrepareResume(ctx context.Context, store ManifestStore, identity ResumeIdentity, registry *vfs.Registry, scope *memory.Scope) (*Manifest, vfs.RecoveryReport, error) {
	if store == nil {
		return nil, vfs.RecoveryReport{}, errors.New("export: resume recovery requires manifest store")
	}
	if registry == nil || scope == nil {
		return nil, vfs.RecoveryReport{}, errors.New("export: resume recovery requires VFS registry and managed memory")
	}
	if err := ctx.Err(); err != nil {
		return nil, vfs.RecoveryReport{}, err
	}

	manifest, err := store.Load(ctx)
	if err != nil {
		return nil, vfs.RecoveryReport{}, err
	}
	// identity mismatch 必须在任何 remote cleanup 前失败，避免错误 attempt 触碰原作业 staging ownership。
	if err := manifest.validateResumeIdentity(identity); err != nil {
		return nil, vfs.RecoveryReport{}, err
	}

	destination, err := vfs.ParseURI(manifest.Destination.URI)
	if err != nil {
		return nil, vfs.RecoveryReport{}, err
	}
	caps, err := registry.EffectiveCapabilities(ctx, destination)
	if err != nil {
		return nil, vfs.RecoveryReport{}, err
	}
	var report vfs.RecoveryReport
	if caps.Has(vfs.IncompleteWriteRecovery) {
		report, err = registry.RecoverIncompleteWrites(ctx, destination, scope)
		if err != nil {
			// ADR-0004：remote ownership 未完全清理时 Resume 必须停止，不能降级为 warning。
			return nil, vfs.RecoveryReport{}, err
		}
	}

	candidate := cloneManifest(manifest)
	if err := candidate.PrepareResume(identity); err != nil {
		return nil, report, err
	}
	if err := store.Save(ctx, candidate); err != nil {
		return nil, report, err
	}
	return candidate, report, nil
}
