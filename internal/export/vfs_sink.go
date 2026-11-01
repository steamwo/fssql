package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// VFSByteSink 是 Export 与具体 S3/SFTP 协议之间唯一的字节写入适配层。
type VFSByteSink struct {
	// handle 是已经通过 Registry capability gate 打开的 VFS WriteHandle。
	handle vfs.WriteHandle
	// digest 只保存固定大小 hash state，不缓存整个对象。
	digest hash.Hash
	// written 记录逻辑写入字节数，用于最终 identity/short-write 校验。
	written int64
	// terminal 保证 Close/Abort 之后不可继续写。
	terminal bool
	mu       sync.Mutex
}

// NewVFSByteSink 通过 VFS Registry 打开目标，禁止 export 直接依赖 S3/SFTP SDK 或 session。
func NewVFSByteSink(ctx context.Context, registry *vfs.Registry, uri vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (*VFSByteSink, error) {
	if registry == nil {
		return nil, errors.New("export: VFS registry is required")
	}
	h, err := registry.OpenWrite(ctx, uri, opts, scope, bm)
	if err != nil {
		return nil, err
	}
	return &VFSByteSink{handle: h, digest: sha256.New()}, nil
}

// Write 完整转发字节；short write 被硬失败，不允许格式层产生静默截断文件。
func (s *VFSByteSink) Write(ctx context.Context, data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.terminal || s.handle == nil {
		return 0, ErrWriterClosed
	}
	n, err := s.handle.Write(ctx, data)
	if n > 0 {
		_, _ = s.digest.Write(data[:n])
		s.written += int64(n)
	}
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

// Flush 对 VFS streaming handle 是 no-op；真正 flush/commit 由格式/压缩 Close 与 VFS Close 边界控制。
func (s *VFSByteSink) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.terminal || s.handle == nil {
		return ErrWriterClosed
	}
	return nil
}

// Close 完成单个远端文件，并返回字节数与 SHA-256；它不定义 dataset commit。
func (s *VFSByteSink) Close(ctx context.Context) (WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.terminal || s.handle == nil {
		return WriteResult{}, ErrWriterClosed
	}
	identity, err := s.handle.Close(ctx)
	if err != nil {
		// Close 失败时保留非终态，让上层 PooledWriter 能继续调用 Abort 清理 incomplete file。
		return WriteResult{}, err
	}
	if identity.Size >= 0 && identity.Size != s.written {
		return WriteResult{}, io.ErrShortWrite
	}
	s.terminal = true
	return WriteResult{Bytes: s.written, Checksum: hex.EncodeToString(s.digest.Sum(nil))}, nil
}

// Abort 丢弃 incomplete file；无论底层删除是否成功，本 sink 都进入终态，错误交给 failure gate 处理。
func (s *VFSByteSink) Abort(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s == nil || s.terminal || s.handle == nil {
		return nil
	}
	err := s.handle.Abort(ctx)
	s.terminal = true
	return err
}
