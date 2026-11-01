package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// ftpWriteHandle 把一个 pool lease 与单个 STOR data stream 绑定。
type ftpWriteHandle struct {
	// driver 用于 Abort 后删除 incomplete 文件。
	driver *Driver
	// pool 拥有当前 leased session。
	pool *Pool
	// session 是当前文件独占的 control session。
	session Session
	// writer 是当前 STOR data stream。
	writer StreamWriter
	// uri/path 标识远端目标。
	uri  vfs.URI
	path string
	// written 记录成功写入字节数，用于最终 size 校验。
	written int64
	// failed 保存首个失败，Close 不得把失败文件当作完成。
	failed error
	// poison 表示当前 session 不能返回 idle pool。
	poison bool
	// terminal 防止重复 Close/Abort/Write。
	terminal bool
	// mu 串行化单个 WriteHandle 生命周期。
	mu sync.Mutex
}

// OpenWrite 打开 bounded FTP/FTPS STOR stream；不支持条件写语义。
func (d *Driver) OpenWrite(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if err := d.validateURI(uri); err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, errors.New("ftp: managed memory required for write")
	}
	if opts.IfNoneMatch || opts.Expected != nil {
		return nil, fmt.Errorf("%w: FTP conditional write is not advertised", vfs.ErrWriteUnsupported)
	}
	pool, err := d.pool()
	if err != nil {
		return nil, err
	}
	session, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	writer, err := session.OpenWrite(ctx, uriPath(uri))
	if err != nil {
		pool.Release(session, IsRetryable(err))
		return nil, err
	}
	return &ftpWriteHandle{driver: d, pool: pool, session: session, writer: writer, uri: uri, path: uriPath(uri)}, nil
}

// Delete 删除 incomplete/staging 文件；FTP 不宣称条件删除。
func (d *Driver) Delete(ctx context.Context, uri vfs.URI, expected *vfs.ObjectIdentity) error {
	if err := d.validateURI(uri); err != nil {
		return err
	}
	if expected != nil {
		return fmt.Errorf("%w: FTP conditional delete is not advertised", vfs.ErrWriteUnsupported)
	}
	return d.withSession(ctx, func(session Session) error { return session.Delete(ctx, uriPath(uri)) })
}

// Write 把 caller-owned bytes 直接写入 STOR stream；实现不在返回后保留切片。
func (h *ftpWriteHandle) Write(ctx context.Context, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.writer == nil {
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	if err := ctx.Err(); err != nil {
		h.markFailed(err)
		return 0, err
	}
	n, err := h.writer.Write(ctx, data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		h.markFailed(err)
		return n, err
	}
	h.written += int64(n)
	return n, nil
}

// Close 完成 STOR、校验最终 size，并释放 session lease；失败时保留 Abort 清理能力。
func (h *ftpWriteHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.writer == nil || h.session == nil {
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return vfs.ObjectIdentity{}, h.failed
	}
	if err := h.writer.Close(ctx); err != nil {
		h.markFailed(err)
		return vfs.ObjectIdentity{}, err
	}
	info, err := h.session.Stat(ctx, h.path)
	if err != nil {
		h.markFailed(err)
		return vfs.ObjectIdentity{}, err
	}
	identity := infoIdentity(info)
	if identity.Size != h.written {
		err := fmt.Errorf("%w: final size=%d written=%d", io.ErrShortWrite, identity.Size, h.written)
		h.markFailed(err)
		return vfs.ObjectIdentity{}, err
	}
	h.releaseLocked(false)
	return identity, nil
}

// Abort 中止 STOR、丢弃 lease，并通过独立 session 删除 incomplete 文件；可重复调用。
func (h *ftpWriteHandle) Abort(ctx context.Context) error {
	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return nil
	}
	var abortErr error
	if h.writer != nil {
		abortErr = h.writer.Abort(ctx)
	}
	h.releaseLocked(true)
	h.mu.Unlock()
	deleteErr := h.driver.Delete(ctx, h.uri, nil)
	if abortErr != nil {
		return abortErr
	}
	return deleteErr
}

// markFailed 保存首错，并根据连接错误/取消决定 poison 当前 session。
func (h *ftpWriteHandle) markFailed(err error) {
	if h.failed == nil {
		h.failed = err
	}
	if IsRetryable(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		h.poison = true
	}
}

// releaseLocked 是唯一 pool lease 释放点；调用方必须持有 h.mu。
func (h *ftpWriteHandle) releaseLocked(forcePoison bool) {
	if h.terminal {
		return
	}
	h.terminal = true
	if h.session != nil {
		h.pool.Release(h.session, forcePoison || h.poison)
		h.session = nil
	}
	h.writer = nil
}
