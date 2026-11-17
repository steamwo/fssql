package export

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

var (
	// ErrCommitConflict covers no-replace conflicts and verified final content mismatches.
	ErrCommitConflict = errors.New("export: final object conflicts with current job")
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
	// scope/buffers 管理 final manifest 序列化、reconciliation read 和远端写缓冲。
	scope   *memory.Scope
	buffers *memory.BufferManager
	// finalURI 是调用方显式指定的最终 manifest 对象。
	finalURI vfs.URI
	// store 非空时把 COMMITTING/COMMITTED transition 作为 durable export-staging state 持久化。
	store ManifestStore
}

// NewCommitter 创建非持久化 Committer；适用于不声明 process-restart resume 的调用路径。
func NewCommitter(manifest *Manifest, registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager, opts CommitOptions) (*Committer, error) {
	return newCommitter(manifest, registry, scope, buffers, opts, nil)
}

// NewDurableCommitter 创建具备 Kill/Restart commit recovery 语义的 Committer；远端提交前必须先持久化 COMMITTING。
func NewDurableCommitter(manifest *Manifest, registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager, opts CommitOptions, store ManifestStore) (*Committer, error) {
	if store == nil {
		return nil, errors.New("export: durable committer requires manifest store")
	}
	return newCommitter(manifest, registry, scope, buffers, opts, store)
}

// newCommitter 共享普通/可恢复 Committer 的 endpoint 与 Manifest 校验逻辑。
func newCommitter(manifest *Manifest, registry *vfs.Registry, scope *memory.Scope, buffers *memory.BufferManager, opts CommitOptions, store ManifestStore) (*Committer, error) {
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
	return &Committer{manifest: manifest, registry: registry, scope: scope, buffers: buffers, finalURI: finalURI, store: store}, nil
}

// Commit 条件创建 committed final manifest；COMMITTING 重试会先尝试同一原子创建，冲突时再读取并核对既有 final manifest。
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
	if c.manifest.ExportState != ExportStateWriting && c.manifest.ExportState != ExportStateCommitting {
		return fmt.Errorf("export: commit requires WRITING/COMMITTING manifest, got %s", c.manifest.ExportState)
	}
	if !c.manifest.AllFilesComplete() {
		return fmt.Errorf("%w: cannot commit incomplete dataset", ErrInvalidTransition)
	}

	// 先检查真实 endpoint capability；不允许通过普通 overwrite 模拟原子提交。
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

	if c.manifest.ExportState == ExportStateWriting {
		// durable 模式必须在任何 final manifest 远端请求之前持久化 COMMITTING。
		if err := transitionExportPersisted(ctx, c.manifest, c.store, ExportStateCommitting); err != nil {
			return err
		}
	}

	handle, err := c.registry.OpenWrite(ctx, c.finalURI, vfs.WriteOptions{IfNoneMatch: true, Tag: memory.TagMetadata}, c.scope, c.buffers)
	if err != nil {
		if errors.Is(err, vfs.ErrObjectChanged) {
			return c.reconcileCommittedManifest(ctx, payload)
		}
		// 一旦进入 COMMITTING，任何远端结果不确定性都保持 COMMITTING，禁止错误回到数据重写阶段。
		return err
	}
	if err := writeHandleAll(ctx, handle, payload); err != nil {
		abortErr := handle.Abort(context.Background())
		return errors.Join(err, abortErr)
	}
	identity, err := handle.Close(ctx)
	if err != nil {
		if errors.Is(err, ErrCommitUncertain) {
			return err
		}
		abortErr := handle.Abort(context.Background())
		if errors.Is(err, vfs.ErrObjectChanged) && abortErr == nil {
			return c.reconcileCommittedManifest(ctx, payload)
		}
		return errors.Join(err, abortErr)
	}
	if identity.Size != int64(len(payload)) {
		// Close 成功但返回 identity 不可信时不降级到 FAILED；读取最终对象决定是否真的提交成功。
		return c.reconcileCommittedManifest(ctx, payload)
	}
	return transitionExportPersisted(ctx, c.manifest, c.store, ExportStateCommitted)
}

// reconcileCommittedManifest 读取已经存在的 final manifest；只有字节完全等于当前 committed payload 才确认 COMMITTED。
func (c *Committer) reconcileCommittedManifest(ctx context.Context, expected []byte) error {
	driver, caps, err := c.registry.WriteDriverFor(ctx, c.finalURI)
	if err != nil {
		return fmt.Errorf("%w: resolve final manifest driver: %v", ErrCommitRecoveryRequired, err)
	}
	if !caps.Has(vfs.RandomRead) {
		return fmt.Errorf("%w: final manifest destination lacks RandomRead", ErrCommitRecoveryRequired)
	}
	info, err := driver.Stat(ctx, c.finalURI)
	if err != nil {
		return fmt.Errorf("%w: stat final manifest: %v", ErrCommitRecoveryRequired, err)
	}
	if info.Size != int64(len(expected)) || info.Size <= 0 {
		return fmt.Errorf("%w: final manifest size=%d expected=%d", ErrCommitConflict, info.Size, len(expected))
	}
	result, err := driver.ReadRange(ctx, vfs.RangeRequest{
		URI:      c.finalURI,
		Offset:   0,
		Length:   info.Size,
		Expected: &info.Identity,
		Tag:      memory.TagMetadata,
	}, c.scope, c.buffers)
	if err != nil {
		return fmt.Errorf("%w: read final manifest: %v", ErrCommitRecoveryRequired, err)
	}
	defer result.Buffer.Release()
	if !bytes.Equal(result.Buffer.Bytes(), expected) {
		return ErrCommitConflict
	}
	return transitionExportPersisted(ctx, c.manifest, c.store, ExportStateCommitted)
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
		manifest.Destination.URI, manifest.Format, manifest.Compression, manifest.Consistency, manifest.Codec, manifest.EncryptionAlgorithm, manifest.KeyID,
	}
	if manifest.Publish != nil {
		p := manifest.Publish
		stringsToCount = append(stringsToCount, p.Intent.JobID, p.Intent.Destination, p.Intent.Protocol, string(p.Intent.Mode), p.Intent.ScopeKey, p.Failure)
		total += int64(len(p.Intent.Token)+len(p.Ownership)) * 6
		if p.Sealed != nil {
			stringsToCount = append(stringsToCount, p.Sealed.Checksum)
			total += int64(len(p.Sealed.RecoveryToken)) * 6
		}
	}
	if manifest.SegmentPlan != nil {
		stringsToCount = append(stringsToCount, manifest.SegmentPlan.Strategy, manifest.SegmentPlan.PlannerFingerprint)
	}
	for _, entry := range manifest.FileStates {
		if total > math.MaxInt64-256 {
			return 0, errors.New("export: manifest size overflow")
		}
		total += 256
		stringsToCount = append(stringsToCount, entry.Path, entry.Checksum)
		if entry.PublishedIdentity != nil {
			stringsToCount = append(stringsToCount, entry.PublishedIdentity.ETag, entry.PublishedIdentity.VersionID)
		}
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
