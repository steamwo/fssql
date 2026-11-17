package webdav

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// putResult 保存后台 PUT 完成结果；channel 容量固定为 1，避免结果 goroutine 被遗弃句柄永久阻塞。
type putResult struct {
	// response 是 HTTP PUT 响应；调用方负责关闭 Body。
	response *http.Response
	// err 是 transport/request 错误。
	err error
}

// writeHandle 把 bounded streaming PUT 与 staging object 生命周期绑定。
type writeHandle struct {
	putOnly bool
	// driver 提供 MOVE/DELETE/Stat 等 VFS 操作。
	driver *Driver
	// finalURI 是用户可见目标。
	finalURI vfs.URI
	// stagingURI 是同目录唯一临时对象，未成功 MOVE 前不得成为 final。
	stagingURI vfs.URI
	// pipeWriter 把 caller bytes 直接流向 HTTP request body，不做 whole-object buffering。
	pipeWriter *io.PipeWriter
	// cancel 取消 PUT request，并用于 Abort/失败释放 transport ownership。
	cancel context.CancelFunc
	// result 只接收一次后台 PUT 终态。
	result <-chan putResult
	// slotReleased 在 PUT goroutine 返回时关闭，供 leak test/终态等待使用。
	slotReleased <-chan struct{}
	// writeMu 串行化 Write，避免多个调用方并发写破坏流语义。
	writeMu sync.Mutex
	// mu 保护 failed/terminal/putConsumed。
	mu sync.Mutex
	// failed 保存首个写入或 PUT 失败。
	failed error
	// terminal 表示 Close/Abort 已完成。
	terminal bool
	// putConsumed 防止 Close/Abort 重复等待 result。
	putConsumed bool
}

// OpenWrite 启动 bounded staging PUT；条件写当前未通过 capability contract 暴露，因此显式拒绝。
func (d *Driver) OpenWrite(ctx context.Context, uri vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if err := d.validateURI(uri); err != nil {
		return nil, err
	}
	if scope == nil || bm == nil {
		return nil, errors.New("webdav: managed memory scope and buffer manager are required for write")
	}
	if opts.IfNoneMatch || opts.Expected != nil {
		return nil, fmt.Errorf("%w: WebDAV conditional write is not advertised", vfs.ErrWriteUnsupported)
	}
	probe, err := d.Probe(ctx)
	if err != nil {
		return nil, err
	}
	if !probe.Supported.Has(vfs.StreamingWrite) {
		return nil, fmt.Errorf("%w: endpoint lacks PUT+MOVE", vfs.ErrWriteUnsupported)
	}
	staging, err := stagingURI(uri)
	if err != nil {
		return nil, err
	}
	return d.openPUT(ctx, uri, staging, nil, false)
}

func (d *Driver) openPUT(ctx context.Context, uri, stagingURI vfs.URI, headers http.Header, putOnly bool) (*writeHandle, error) {
	select {
	case d.writeSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	pipeReader, pipeWriter := io.Pipe()
	requestCtx, cancel := context.WithCancel(ctx)
	request, err := d.newRequest(requestCtx, http.MethodPut, stagingURI, pipeReader)
	if err != nil {
		cancel()
		_ = pipeReader.CloseWithError(err)
		_ = pipeWriter.CloseWithError(err)
		<-d.writeSlots
		return nil, err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	for k, vs := range headers {
		request.Header[k] = append([]string(nil), vs...)
	}

	result := make(chan putResult, 1)
	slotReleased := make(chan struct{})
	go func() {
		var response *http.Response
		var requestErr error
		if putOnly {
			response, requestErr = d.doPublish(request)
		} else {
			response, requestErr = d.do(request)
		}
		_ = pipeReader.CloseWithError(requestErr)
		result <- putResult{response: response, err: requestErr}
		close(result)
		<-d.writeSlots
		close(slotReleased)
	}()

	return &writeHandle{
		putOnly: putOnly,
		driver:  d, finalURI: uri, stagingURI: stagingURI, pipeWriter: pipeWriter,
		cancel: cancel, result: result, slotReleased: slotReleased,
	}, nil
}

// Write 把 caller-owned bytes 直接写入 io.Pipe；context 取消会终止整个 PUT，避免阻塞 goroutine 泄漏。
func (h *writeHandle) Write(ctx context.Context, data []byte) (int, error) {
	if h == nil {
		return 0, vfs.ErrWriteClosed
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	h.mu.Lock()
	if h.terminal || h.pipeWriter == nil {
		h.mu.Unlock()
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		err := h.failed
		h.mu.Unlock()
		return 0, err
	}
	h.mu.Unlock()
	if err := ctx.Err(); err != nil {
		h.fail(err)
		return 0, err
	}

	stop := context.AfterFunc(ctx, func() {
		h.cancel()
		_ = h.pipeWriter.CloseWithError(ctx.Err())
	})
	n, err := h.pipeWriter.Write(data)
	stop()
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		h.fail(err)
		return n, err
	}
	return n, nil
}

// Close 完成 staging PUT、MOVE 到 final、Stat 最终对象，并确保后台 PUT slot 已释放。
func (h *writeHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	if h == nil {
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	failed := h.failed
	h.mu.Unlock()
	if failed != nil {
		return vfs.ObjectIdentity{}, failed
	}
	if err := h.pipeWriter.Close(); err != nil {
		h.fail(err)
		return vfs.ObjectIdentity{}, err
	}
	response, err := h.waitPUT(ctx)
	if err != nil {
		h.fail(err)
		h.bestEffortCleanup(context.Background())
		return vfs.ObjectIdentity{}, err
	}
	if response != nil {
		defer response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			err = statusError("PUT staging", response.StatusCode)
			h.fail(err)
			h.bestEffortCleanup(context.Background())
			return vfs.ObjectIdentity{}, err
		}
	}
	if h.putOnly {
		h.mu.Lock()
		h.terminal = true
		h.mu.Unlock()
		h.cancel()
		return vfs.ObjectIdentity{}, nil
	}
	if err := h.driver.move(ctx, h.stagingURI, h.finalURI); err != nil {
		h.fail(err)
		h.bestEffortCleanup(context.Background())
		return vfs.ObjectIdentity{}, err
	}
	info, err := h.driver.Stat(ctx, h.finalURI)
	if err != nil {
		h.fail(err)
		return vfs.ObjectIdentity{}, err
	}
	h.mu.Lock()
	h.terminal = true
	h.pipeWriter = nil
	h.mu.Unlock()
	h.cancel()
	return info.Identity, nil
}

// Abort 取消 PUT、等待 bounded goroutine 退出并删除 staging；可重复调用。
func (h *writeHandle) Abort(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()

	h.mu.Lock()
	if h.terminal {
		h.mu.Unlock()
		return nil
	}
	h.mu.Unlock()
	h.cancel()
	_ = h.pipeWriter.CloseWithError(context.Canceled)
	response, waitErr := h.waitPUT(ctx)
	if response != nil {
		_ = response.Body.Close()
	}
	deleteErr := h.driver.deleteInternal(ctx, h.stagingURI, nil, true)
	h.mu.Lock()
	h.terminal = true
	h.pipeWriter = nil
	h.mu.Unlock()
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		return waitErr
	}
	return deleteErr
}

// waitPUT 消费唯一 PUT 结果，并等待 goroutine 归还 write slot。
func (h *writeHandle) waitPUT(ctx context.Context) (*http.Response, error) {
	h.mu.Lock()
	if h.putConsumed {
		h.mu.Unlock()
		return nil, nil
	}
	h.putConsumed = true
	h.mu.Unlock()

	var result putResult
	select {
	case value, ok := <-h.result:
		if ok {
			result = value
		}
	case <-ctx.Done():
		h.cancel()
		<-h.slotReleased
		// PUT goroutine 的结果 channel 是 buffered；取消后主动 drain 并关闭 response，避免连接资源悬挂。
		select {
		case pending, ok := <-h.result:
			if ok && pending.response != nil {
				_ = pending.response.Body.Close()
			}
		default:
		}
		return nil, ctx.Err()
	}
	select {
	case <-h.slotReleased:
	case <-ctx.Done():
		return result.response, ctx.Err()
	}
	return result.response, result.err
}

// fail 保存首错并取消 PUT；后续 Write/Close 不得掩盖首个失败。
func (h *writeHandle) fail(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	if h.failed == nil {
		h.failed = err
	}
	h.mu.Unlock()
	h.cancel()
	if h.pipeWriter != nil {
		_ = h.pipeWriter.CloseWithError(err)
	}
}

// bestEffortCleanup 用于 Close 失败后的 staging 清理；清理失败不覆盖首错。
func (h *writeHandle) bestEffortCleanup(ctx context.Context) {
	h.cancel()
	if h.putOnly {
		return
	}
	_ = h.driver.deleteInternal(ctx, h.stagingURI, nil, true)
}

// Release cancels/drains process resources while preserving staging for recovery.
func (h *writeHandle) Release() error {
	if h == nil {
		return nil
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	h.cancel()
	if h.pipeWriter != nil {
		_ = h.pipeWriter.CloseWithError(context.Canceled)
	}
	response, err := h.waitPUT(context.Background())
	if response != nil {
		_ = response.Body.Close()
	}
	h.mu.Lock()
	h.terminal = true
	h.mu.Unlock()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Delete 删除普通 WebDAV 对象；Expected 仅在有 ETag 时通过 If-Match 表达。
func (d *Driver) Delete(ctx context.Context, uri vfs.URI, expected *vfs.ObjectIdentity) error {
	return d.deleteInternal(ctx, uri, expected, false)
}

// deleteInternal 执行 DELETE；ignoreMissing 仅供 staging Abort/cleanup 使用。
func (d *Driver) deleteInternal(ctx context.Context, uri vfs.URI, expected *vfs.ObjectIdentity, ignoreMissing bool) error {
	request, err := d.newRequest(ctx, http.MethodDelete, uri, nil)
	if err != nil {
		return err
	}
	if expected != nil {
		if expected.ETag == "" {
			return fmt.Errorf("%w: WebDAV conditional delete requires ETag", vfs.ErrWriteUnsupported)
		}
		request.Header.Set("If-Match", expected.ETag)
	}
	response, err := d.do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if ignoreMissing && response.StatusCode == http.StatusNotFound {
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return statusError("DELETE", response.StatusCode)
	}
	return nil
}

// move 把同目录 staging object 原子语义地提交到 final；这只是 writer 内部协议原语，不对 VFS 宣称 AtomicRename。
func (d *Driver) move(ctx context.Context, source, destination vfs.URI) error {
	request, err := d.newRequest(ctx, "MOVE", source, nil)
	if err != nil {
		return err
	}
	target, err := d.targetURL(destination)
	if err != nil {
		return err
	}
	if _, err := d.Policy.ValidateURL(ctx, target.String()); err != nil {
		return err
	}
	request.Header.Set("Destination", target.String())
	request.Header.Set("Overwrite", "T")
	response, err := d.do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusNoContent && response.StatusCode != http.StatusOK {
		return statusError("MOVE", response.StatusCode)
	}
	return nil
}

// stagingURI 生成 final 同目录的唯一 staging 名称，避免跨目录 MOVE 或共享 staging scope。
func stagingURI(final vfs.URI) (vfs.URI, error) {
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return vfs.URI{}, fmt.Errorf("webdav: staging identity: %w", err)
	}
	base := path.Base(final.Path)
	if base == "." || base == "/" || base == "" {
		base = "object"
	}
	stagingPath := path.Join(path.Dir(final.Path), "."+base+".fssql-write-"+hex.EncodeToString(random[:]))
	raw := (&url.URL{Scheme: final.Scheme, Host: final.Host, Path: stagingPath}).String()
	return vfs.ParseURI(raw)
}
