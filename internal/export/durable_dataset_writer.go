package export

import (
	"context"
	"errors"
)

var (
	// ErrDurableDatasetWriterTerminal 表示 staging Manifest 持久化失败后当前 writer 已被封死，必须从 durable state 重建。
	ErrDurableDatasetWriterTerminal = errors.New("export: durable dataset writer is terminal")
)

// DurableDatasetWriter 把 resumable DatasetWriter 的文件状态变化绑定到 durable ManifestStore。
type DurableDatasetWriter struct {
	// inner 负责稳定 routing、bounded writer 和文件生命周期。
	inner *DatasetWriter
	// store 保存进程重启后恢复所需的 Manifest snapshot。
	store ManifestStore
	// terminal 表示 durable state 已无法继续安全推进，调用方必须重新 Load/PrepareResume。
	terminal bool
}

// NewDurableResumableDatasetWriter 创建同时满足 ADR-0003 stable segment 与 durable restart state 的 writer。
func NewDurableResumableDatasetWriter(manifest *Manifest, router *FileRouter, factory *FileWriterFactory, store ManifestStore) (*DurableDatasetWriter, error) {
	if factory == nil {
		return nil, errors.New("export: durable dataset writer requires file writer factory")
	}
	return newDurableResumableDatasetWriter(manifest, router, func(ctx context.Context, uriURI interfaceURI, opts interfaceWriteOptions) (FormatWriter, error) {
		return nil, errors.New("export: unreachable durable dataset writer adapter")
	}, store)
}

// interfaceURI/interfaceWriteOptions 仅用于避免暴露额外 public adapter；生产构造器在 initDurableDatasetWriter 中直接绑定 factory。
type interfaceURI = struct{}
type interfaceWriteOptions = struct{}

// initDurableDatasetWriter 是生产路径，保持所有远端打开仍经过 FileWriterFactory/VFS capability gate。
func initDurableDatasetWriter(manifest *Manifest, router *FileRouter, factory *FileWriterFactory, store ManifestStore) (*DurableDatasetWriter, error) {
	if store == nil {
		return nil, errors.New("export: durable dataset writer requires manifest store")
	}
	inner, err := NewResumableDatasetWriter(manifest, router, factory)
	if err != nil {
		return nil, err
	}
	return &DurableDatasetWriter{inner: inner, store: store}, nil
}

// newDurableResumableDatasetWriter 是测试注入点；生产代码不得绕过 initDurableDatasetWriter 的 FileWriterFactory。
func newDurableResumableDatasetWriter(manifest *Manifest, router *FileRouter, open fileOpenFunc, store ManifestStore) (*DurableDatasetWriter, error) {
	if store == nil {
		return nil, errors.New("export: durable dataset writer requires manifest store")
	}
	inner, err := newResumableDatasetWriter(manifest, router, open)
	if err != nil {
		return nil, err
	}
	return &DurableDatasetWriter{inner: inner, store: store}, nil
}

// WriteChunk 写入一个稳定 segment，并把 COMPLETE/FAILED/WRITING 文件状态作为一个 durable snapshot 发布。
func (w *DurableDatasetWriter) WriteChunk(ctx context.Context, chunk *vector.DataChunk, segmentID string) error {
	if w == nil || w.inner == nil || w.store == nil || w.terminal {
		return ErrDurableDatasetWriterTerminal
	}
	before := cloneManifest(w.inner.manifest)
	opErr := w.inner.WriteChunk(ctx, chunk, segmentID)
	if persistErr := w.store.Save(ctx, w.inner.manifest); persistErr != nil {
		abortErr := w.abortOpenSingle()
		*w.inner.manifest = *before
		w.terminal = true
		return errors.Join(opErr, persistErr, abortErr)
	}
	return opErr
}

// Close 完成 SingleFile（若存在）并 durable 保存 COMPLETE/FAILED；之后当前 durable writer 进入终态。
func (w *DurableDatasetWriter) Close(ctx context.Context) error {
	if w == nil || w.inner == nil || w.store == nil || w.terminal {
		return ErrDurableDatasetWriterTerminal
	}
	before := cloneManifest(w.inner.manifest)
	opErr := w.inner.Close(ctx)
	persistErr := w.store.Save(ctx, w.inner.manifest)
	if persistErr != nil {
		*w.inner.manifest = *before
	}
	w.terminal = true
	return errors.Join(opErr, persistErr)
}

// Abort 放弃仍打开的 SingleFile，并 durable 保存 FAILED；之后当前 durable writer 不再接受写入。
func (w *DurableDatasetWriter) Abort(ctx context.Context) error {
	if w == nil || w.inner == nil || w.store == nil {
		return nil
	}
	if w.terminal {
		return nil
	}
	before := cloneManifest(w.inner.manifest)
	opErr := w.inner.Abort(ctx)
	persistErr := w.store.Save(ctx, w.inner.manifest)
	if persistErr != nil {
		*w.inner.manifest = *before
	}
	w.terminal = true
	return errors.Join(opErr, persistErr)
}

// abortOpenSingle 在 durable Save 失败时关闭仍活着的 SingleFile ownership，避免继续写入不可恢复状态。
func (w *DurableDatasetWriter) abortOpenSingle() error {
	if w == nil || w.inner == nil || w.inner.single == nil {
		return nil
	}
	err := w.inner.single.Abort(context.Background())
	w.inner.single = nil
	w.inner.singlePath = ""
	return err
}
