package export

import (
	"context"
	"errors"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

// NewResumableDatasetWriter 创建具备 ADR-0003 file-level resume 语义的 DatasetWriter。
// MultiFile/Partitioned 必须已在 Manifest 中绑定 Stable SegmentPlan；SingleFile 允许无 SegmentPlan 整文件重做。
func NewResumableDatasetWriter(manifest *Manifest, router *FileRouter, factory *FileWriterFactory) (*DatasetWriter, error) {
	if factory == nil {
		return nil, errors.New("export: resumable dataset writer requires file writer factory")
	}
	return newResumableDatasetWriter(manifest, router, func(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions) (FormatWriter, error) {
		return factory.Open(ctx, uri, opts)
	})
}

// newResumableDatasetWriter 是恢复路径的测试注入点；生产路径仍必须通过 FileWriterFactory/VFS capability gate 打开文件。
func newResumableDatasetWriter(manifest *Manifest, router *FileRouter, open fileOpenFunc) (*DatasetWriter, error) {
	if manifest == nil || router == nil || open == nil {
		return nil, errors.New("export: resumable dataset writer requires manifest, router and opener")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if router.config.Mode != LayoutSingleFile {
		if manifest.SegmentPlan == nil {
			return nil, ErrSegmentPlanRequired
		}
		if err := manifest.SegmentPlan.Validate(); err != nil {
			return nil, err
		}
	}
	return newDatasetWriter(manifest, router, open)
}
