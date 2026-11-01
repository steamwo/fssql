package export

import (
	"context"
	"errors"
)

// StartDurable 在 SourceSnapshot 校验通过后，把 WRITING 状态持久化到 export staging Manifest。
// 只有 durable Save 成功后调用方才允许打开任何目标文件，保证 Kill/Restart 有可恢复的作业入口。
func (c *Coordinator) StartDurable(ctx context.Context, store ManifestStore) error {
	if c == nil || c.manifest == nil || c.query == nil {
		return errors.New("export: incomplete coordinator")
	}
	if store == nil {
		return errors.New("export: durable coordinator requires manifest store")
	}
	if err := c.ValidateSourceSnapshot(); err != nil {
		return err
	}
	return transitionExportPersisted(ctx, c.manifest, store, ExportStateWriting)
}
