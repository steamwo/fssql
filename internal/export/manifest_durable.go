package export

import "context"

// cloneManifest 深复制 Manifest 中会被状态机修改的 slice/pointer 字段，用于 durable transition 的 copy-on-write。
func cloneManifest(manifest *Manifest) *Manifest {
	if manifest == nil {
		return nil
	}
	cloned := *manifest
	cloned.FileStates = append([]FileEntry(nil), manifest.FileStates...)
	if manifest.SegmentPlan != nil {
		plan := *manifest.SegmentPlan
		cloned.SegmentPlan = &plan
	}
	return &cloned
}

// transitionExportPersisted 先在副本上执行 ExportState transition，再 durable Save，最后替换进程内状态。
// 这样持久化失败时原 Manifest 保持原状态，远端调用方可安全决定是否继续。
func transitionExportPersisted(ctx context.Context, manifest *Manifest, store ManifestStore, next ExportState) error {
	if store == nil {
		return manifest.TransitionExport(next)
	}
	candidate := cloneManifest(manifest)
	if err := candidate.TransitionExport(next); err != nil {
		return err
	}
	if err := store.Save(ctx, candidate); err != nil {
		return err
	}
	*manifest = *candidate
	return nil
}

// LoadAndPrepareResume 从 durable store 重建进程状态，验证 ADR-0003 identity，并把 redo 状态再次 durable 保存后才返回。
func LoadAndPrepareResume(ctx context.Context, store ManifestStore, identity ResumeIdentity) (*Manifest, error) {
	manifest, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if err := manifest.PrepareResume(identity); err != nil {
		return nil, err
	}
	if err := store.Save(ctx, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}
