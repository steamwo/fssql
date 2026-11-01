package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// CommitOptions 描述 dataset commit 的唯一公开控制点；FinalManifestURI 必须由调用方显式给出。
type CommitOptions struct {
	// FinalManifestURI 是 dataset 可见性的最终提交对象，不是伪目录 rename 目标。
	FinalManifestURI string
}

// Committer 使用 VFS ConditionalWrite 原子创建 final manifest，把 staging complete files 变成可见 committed dataset。
type Committer struct {
	// manifest 是导出作业唯一状态源；成功提交后从 WRITING→COMMITTING→COMMITTED。
	manifest *Manifest
	// registry 保证 final manifest 仍经过 VFS capability gate。
	registry *vfs.Registry
	// scope/buffers 管理 final manifest 序列化记账和远端写缓冲。
	scope   *memory.Scope
	buffers *memory.BufferManager
	// finalURI 是调用方显式指定的最终 manifest 对象。
	finalURI vfs.URI
}

// NewCommitter 创建提交器，并拒绝跨 endpoint 的 final manifest，避免把一个 dataset 的提交标记写到无关目标。
func NewCommitter(manifest *Manifest, registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager, opts CommitOptions) (*Committer, error) {
	if manifest == nil || registry == nil || scope == nil || buffers == nil {
		return nil, errors.New("export: committer requires manifest, VFS registry and managed memory")
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	finalURI, err := vfs.ParseURI(strings.TrimSpace(opts.FinalManifestURI))
	if err != nil {
		return nil, err
	}
	destination, err := vfs.ParseURI(manifest.Destination.URI)
	if err != nil {
		return nil, err
	}
	if finalURI.Scheme != destination.Scheme || finalURI.Host != destination.Host {
		return nil, fmt.Errorf("export: final manifest endpoint differs from destination: %s vs %s", finalURI.String(), destination.String())
	}
	return &Committer{manifest: manifest, registry: registry, scope: scope, buffers: buffers, finalURI: finalURI}, nil
}

// Commit 条件创建 committed final manifest；任何写入失败都会 Abort 当前对象并把本地作业置为 FAILED 以供 Resume。
func (c *Committer) Commit(ctx context.Context) error {
	if c == nil || c.manifest == nil || c.registry == nil {
		return errors.New("export: incomplete committer")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.manifest.ExportState == ExportStateCommitted {
		// 同一进程内已确认 COMMITTED 时幂等返回，不重复触碰 final marker。
		return nil
	}
	if c.manifest.ExportState != ExportStateWriting {
		return fmt.Errorf("export: commit requires WRITING manifest, got %s", c.manifest.ExportState)
	}
	if !c.manifest.AllFilesComplete() {
		return fmt.Errorf("%w: cannot commit incomplete dataset", ErrInvalidTransition)
	}

	// 先检查真实 endpoint capability，再改变 Manifest 状态；不允许通过普通 overwrite 模拟原子提交。
	_, caps, err := c.registry.WriteDriverFor(ctx, c.finalURI)
	if err != nil {
		return err
	}
	if !caps.Has(vfs.ConditionalWrite) {
		return fmt.Errorf("%w: dataset commit requires ConditionalWrite for %s", vfs.ErrWriteUnsupported, c.finalURI.Scheme)
	}

	payload, reservation, err := marshalCommittedManifest(c.manifest, c.scope)
	if err != nil {
		return err
	}
	defer reservation.Release()

	if err := c.manifest.TransitionExport(ExportStateCommitting); err != nil {
		return err
	}
	fail := func(cause error) error {
		stateErr := c.manifest.TransitionExport(ExportStateFailed)
		return errors.Join(cause, stateErr)
	}

	handle, err := c.registry.OpenWrite(ctx, c.finalURI, vfs.WriteOptions{IfNoneMatch: true, Tag: memory.TagMetadata}, c.scope, c.buffers)
	if err != nil {
		return fail(err)
	}
	if err := writeHandleAll(ctx, handle, payload); err != nil {
		abortErr := handle.Abort(context.Background())
		return fail(errors.Join(err, abortErr))
	}
	identity, err := handle.Close(ctx)
	if err != nil {
		abortErr := handle.Abort(context.Background())
		return fail(errors.Join(err, abortErr))
	}
	if identity.Size != int64(len(payload)) {
		return fail(fmt.Errorf("%w: final manifest size=%d want=%d", io.ErrShortWrite, identity.Size, len(payload)))
	}
	if err := c.manifest.TransitionExport(ExportStateCommitted); err != nil {
		return err
	}
	return nil
}

// marshalCommittedManifest 在普通 Go heap 序列化前先 Reserve Metadata 上界，满足 allocate-before-accounting 约束。
func marshalCommittedManifest(manifest *Manifest, scope *memory.Scope) ([]byte, *memory.Reservation, error) {
	if manifest == nil || scope == nil {
		return nil, nil, errors.New("export: manifest serialization requires managed memory")
	}
	candidate := *manifest
	candidate.FileStates = append([]FileEntry(nil), manifest.FileStates...)
	candidate.ExportState = ExportStateCommitted
	upper, err := manifestJSONUpperBound(&candidate)
	if err != nil {
		return nil, nil, err
	}
	reservation, err := scope.Reserve(memory.TagMetadata, upper)
	if err != nil {
		return nil, nil, err
	}
	payload, err := candidate.MarshalBinary()
	if err != nil {
		reservation.Release()
		return nil, nil, err
	}
	if int64(len(payload)) > upper {
		reservation.Release()
		return nil, nil, fmt.Errorf("export: manifest encoded size %d exceeds reserved upper bound %d", len(payload), upper)
	}
	return payload, reservation, nil
}

// manifestJSONUpperBound 计算 encoding/json 字符串最坏 6x 转义情况下的保守大小上界。
func manifestJSONUpperBound(manifest *Manifest) (int64, error) {
	if manifest == nil {
		return 0, ErrInvalidManifest
	}
	// 固定字段名、标点、整数和状态字符串预留 4KiB，文件条目另按每项 256B 固定结构预算。
	total := int64(4096)
	stringsToCount := []string{
		manifest.JobID, manifest.QueryFingerprint, manifest.SourceSnapshot.Fingerprint,
		manifest.Destination.URI, manifest.Format, manifest.Compression, manifest.Consistency,
	}
	for _, entry := range manifest.FileStates {
		if total > math.MaxInt64-256 {
			return 0, errors.New("export: manifest size overflow")
		}
		total += 256
		stringsToCount = append(stringsToCount, entry.Path, entry.Checksum)
	}
	for _, value := range stringsToCount {
		if int64(len(value)) > (math.MaxInt64-total)/6 {
			return 0, errors.New("export: manifest string size overflow")
		}
		total += int64(len(value)) * 6
	}
	return total, nil
}

// writeHandleAll 处理合法 short write；零进展且无错误时返回 io.ErrShortWrite，避免静默截断 final manifest。
func writeHandleAll(ctx context.Context, handle vfs.WriteHandle, payload []byte) error {
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := handle.Write(ctx, payload)
		if n < 0 || n > len(payload) {
			return fmt.Errorf("export: invalid VFS write count %d for %d bytes", n, len(payload))
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}
