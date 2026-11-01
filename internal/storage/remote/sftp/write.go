package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// WritableSession 是 SFTP Session 的可选写扩展；只读 session 不需要伪造写能力。
type WritableSession interface {
	Session
	// WriteAt 向远端文件指定偏移写入数据；truncate 仅允许第一次写入时使用。
	WriteAt(context.Context, string, []byte, int64, bool) (int, error)
	// Remove 删除 incomplete/staging 文件，用于 Abort 与恢复重做。
	Remove(context.Context, string) error
}

// sftpWriteHandle 把一个 SFTP pool lease 与单个流式目标文件绑定。
type sftpWriteHandle struct {
	// driver 用于 Abort 时从新 session 删除 incomplete 文件。
	driver *Driver
	// pool 拥有当前 leased session。
	pool *Pool
	// session 是当前文件独占的可写 session。
	session WritableSession
	// uri/path 标识当前目标。
	uri  vfs.URI
	path string
	// offset 是下一次顺序写的远端偏移。
	offset int64
	// started 表示第一次 truncate/create 已执行。
	started bool
	// failed 保存首个写失败，Close 不得把失败文件伪装成完成。
	failed error
	// poison 表示当前 session 不能返回 idle pool。
	poison bool
	// terminal 防止 Close/Abort 后再次写入。
	terminal bool
	mu       sync.Mutex
}

// OpenWrite 取得一个 bounded SFTP session lease；写入期间不创建额外无界 goroutine 或连接。
func (d *Driver) OpenWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if err := d.validateURI(u); err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, errors.New("sftp: managed memory required for write")
	}
	if opts.IfNoneMatch || opts.Expected != nil {
		return nil, fmt.Errorf("%w: SFTP conditional write is not advertised", vfs.ErrWriteUnsupported)
	}
	p, err := d.pool()
	if err != nil {
		return nil, err
	}
	s, err := p.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	ws, ok := s.(WritableSession)
	if !ok {
		p.Release(s, false)
		return nil, fmt.Errorf("%w: SFTP session is read-only", vfs.ErrWriteUnsupported)
	}
	return &sftpWriteHandle{driver: d, pool: p, session: ws, uri: u, path: uriPath(u)}, nil
}

// Delete 删除一个 staging/incomplete SFTP 对象；Expected 仅用于未来具备条件删除能力的驱动，当前拒绝。
func (d *Driver) Delete(ctx context.Context, u vfs.URI, expected *vfs.ObjectIdentity) error {
	if err := d.validateURI(u); err != nil {
		return err
	}
	if expected != nil {
		return fmt.Errorf("%w: SFTP conditional delete is not advertised", vfs.ErrWriteUnsupported)
	}
	p, err := d.pool()
	if err != nil {
		return err
	}
	s, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	ws, ok := s.(WritableSession)
	if !ok {
		p.Release(s, false)
		return fmt.Errorf("%w: SFTP session is read-only", vfs.ErrWriteUnsupported)
	}
	err = ws.Remove(ctx, uriPath(u))
	poison := IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	p.Release(s, poison)
	return err
}

// Write 顺序写入 caller-owned bytes；驱动不保留切片，偏移只在完整成功后推进。
func (h *sftpWriteHandle) Write(ctx context.Context, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.session == nil {
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	if err := ctx.Err(); err != nil {
		h.markFailed(err)
		return 0, err
	}
	n, err := h.session.WriteAt(ctx, h.path, data, h.offset, !h.started)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.markFailed(err)
		return n, err
	}
	h.started = true
	h.offset += int64(n)
	return n, nil
}

// Close 确保空文件也被创建，然后 Stat 最终对象身份并归还 session lease。
func (h *sftpWriteHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.session == nil {
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		err := h.failed
		h.releaseLocked(true)
		return vfs.ObjectIdentity{}, err
	}
	if !h.started {
		n, err := h.session.WriteAt(ctx, h.path, nil, 0, true)
		if err != nil || n != 0 {
			if err == nil {
				err = io.ErrShortWrite
			}
			h.markFailed(err)
			h.releaseLocked(true)
			return vfs.ObjectIdentity{}, err
		}
		h.started = true
	}
	info, err := h.session.Stat(ctx, h.path)
	if err != nil {
		h.markFailed(err)
		h.releaseLocked(h.poison)
		return vfs.ObjectIdentity{}, err
	}
	identity := infoIdentity(info)
	if identity.Size != h.offset {
		err := fmt.Errorf("%w: final size=%d written=%d", io.ErrShortWrite, identity.Size, h.offset)
		h.markFailed(err)
		h.releaseLocked(false)
		return vfs.ObjectIdentity{}, err
	}
	h.releaseLocked(false)
	return identity, nil
}

// Abort 归还/丢弃当前 lease 后用独立 session 删除 incomplete 文件；可重复调用。
func (h *sftpWriteHandle) Abort(ctx context.Context) error {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return nil
	}
	h.releaseLocked(h.poison || h.failed != nil)
	h.mu.Unlock()
	return h.driver.Delete(ctx, h.uri, nil)
}

// markFailed 记录首错，并根据失败类别决定当前 session 是否 poison/discard。
func (h *sftpWriteHandle) markFailed(err error) {
	if h.failed == nil {
		h.failed = err
	}
	if IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		h.poison = true
	}
}

// releaseLocked 是唯一 pool lease 释放点；调用方必须持有 h.mu。
func (h *sftpWriteHandle) releaseLocked(poison bool) {
	if h.terminal {
		return
	}
	h.terminal = true
	if h.session != nil {
		h.pool.Release(h.session, poison)
		h.session = nil
	}
}
