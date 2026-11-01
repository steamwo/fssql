package vfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/steamwo/fssql/internal/core/memory"
)

var (
	ErrWriteUnsupported = errors.New("vfs: write unsupported")
	ErrWriteClosed      = errors.New("vfs: write handle closed")
)

// WriteOptions 描述一次 VFS 对象写入的语义要求；格式与压缩层不得出现在这里。
type WriteOptions struct {
	// IfNoneMatch 要求目标对象此前不存在，用于 final manifest/success marker 的条件提交。
	IfNoneMatch bool
	// Expected 用于需要 compare-and-swap 的条件覆盖；nil 表示不要求既有对象身份。
	Expected *ObjectIdentity
	// Tag 标记驱动内部需要的 Managed Buffer；为空时应使用 ExportBuffer/NetworkBuffer 类别。
	Tag memory.Tag
}

// WriteHandle 表示一个有明确 Close/Abort 生命周期的流式目标对象。
type WriteHandle interface {
	// Write 写入一段数据；实现必须观察 ctx，并且不得把调用方切片无限期保留。
	Write(ctx context.Context, data []byte) (int, error)
	// Close 完成对象并返回最终身份；成功后不得再写入。
	Close(ctx context.Context) (ObjectIdentity, error)
	// Abort 放弃未完成对象并释放远端与本地资源；应当可重复调用。
	Abort(ctx context.Context) error
}

// WritableDriver 是 VFS Driver 的可选写扩展，避免为只读驱动伪造写能力。
type WritableDriver interface {
	Driver
	// OpenWrite 根据驱动真实 capability 打开目标；实现不能绕过 MemoryManager 保存大块缓冲。
	OpenWrite(context.Context, URI, WriteOptions, *memory.Scope, *memory.BufferManager) (WriteHandle, error)
	// Delete 删除 staging/incomplete 对象；它不是 dataset 目录 rename 的替代品。
	Delete(context.Context, URI, *ObjectIdentity) error
}

// WriteDriverFor 返回目标 URI 的可写驱动，并验证端点运行时至少提供一种正式写能力。
func (r *Registry) WriteDriverFor(ctx context.Context, u URI) (WritableDriver, CapabilitySet, error) {
	if r == nil {
		return nil, 0, errors.New("vfs: nil registry")
	}
	d, err := r.DriverFor(u)
	if err != nil {
		return nil, 0, err
	}
	wd, ok := d.(WritableDriver)
	if !ok {
		return nil, 0, fmt.Errorf("%w: scheme %s", ErrWriteUnsupported, u.Scheme)
	}
	caps, err := r.EffectiveCapabilities(ctx, u)
	if err != nil {
		return nil, 0, err
	}
	if !caps.Has(StreamingWrite) && !caps.Has(RandomWrite) && !caps.Has(MultipartWrite) {
		return nil, 0, fmt.Errorf("%w: scheme %s advertises no effective write capability", ErrWriteUnsupported, u.Scheme)
	}
	return wd, caps, nil
}

// OpenWrite 通过 Registry 的 capability gate 打开写句柄，集中防止上层绕过 VFS 能力模型。
func (r *Registry) OpenWrite(ctx context.Context, u URI, opts WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (WriteHandle, error) {
	if scope == nil || bm == nil {
		return nil, errors.New("vfs: managed memory scope and buffer manager are required for write")
	}
	wd, caps, err := r.WriteDriverFor(ctx, u)
	if err != nil {
		return nil, err
	}
	if opts.IfNoneMatch && !caps.Has(ConditionalWrite) {
		return nil, fmt.Errorf("%w: conditional create unavailable for %s", ErrWriteUnsupported, u.Scheme)
	}
	if opts.Expected != nil && !caps.Has(ConditionalWrite) {
		return nil, fmt.Errorf("%w: conditional overwrite unavailable for %s", ErrWriteUnsupported, u.Scheme)
	}
	return wd.OpenWrite(ctx, u, opts, scope, bm)
}
