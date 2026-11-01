package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/remote"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	// defaultMultipartPartSize 使用 8MiB，满足 S3 非末尾 part 至少 5MiB 的约束，同时保持 writer 内存有界。
	defaultMultipartPartSize = 8 << 20
	// maxMultipartParts 遵循 S3 multipart upload 的 10,000 parts 上限。
	maxMultipartParts = 10000
)

// multipartPart 保存已完成 part 的小型提交 metadata；payload 已经释放/复用，不保存在这里。
type multipartPart struct {
	// Number 是从 1 开始的 S3 part number。
	Number int
	// ETag 是 UploadPart 返回的实体标签。
	ETag string
}

// s3WriteHandle 把 Managed part buffer、multipart upload identity 与 VFS WriteHandle 生命周期绑定。
type s3WriteHandle struct {
	// driver 提供 endpoint-scoped HTTP client、SigV4 和重试策略。
	driver *Driver
	// uri 是最终对象，而不是伪目录。
	uri vfs.URI
	// opts 保存条件提交要求，只在最终 PUT/Complete 时应用。
	opts vfs.WriteOptions
	// scope/bm 仅用于受管 part buffer 和 metadata reservation。
	scope *memory.Scope
	bm    *memory.BufferManager
	// partBuffer 是固定 8MiB Managed Buffer，整个对象不会驻留 Go heap。
	partBuffer *memory.Buffer
	// used 是当前尚未上传 part 的有效字节数。
	used int
	// uploadID 非空表示远端 multipart 已创建，Abort 必须显式清理。
	uploadID string
	// parts 只保存完成 part 的编号/ETag，增长前由 metadataReservation 记账。
	parts               []multipartPart
	metadataReservation *memory.Reservation
	// total 是逻辑对象已经接受的字节数。
	total int64
	// failed 保存首个失败；Close 失败后仍允许 Abort 清理 multipart。
	failed error
	// terminal 表示成功 Close 或 Abort 后禁止再次使用。
	terminal bool
	mu       sync.Mutex
}

// OpenWrite 创建一个有界 S3 writer；先 Reserve/Allocate part buffer，再允许任何远端 multipart 生命周期开始。
func (d *Driver) OpenWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("s3: unexpected scheme %q", u.Scheme)
	}
	if scope == nil || bm == nil {
		return nil, errors.New("s3: managed memory scope and buffer manager are required for write")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	buf, err := bm.Allocate(scope, memory.TagExportBuffer, defaultMultipartPartSize)
	if err != nil {
		return nil, err
	}
	meta, err := scope.Reserve(memory.TagMetadata, 0)
	if err != nil {
		buf.Release()
		return nil, err
	}
	return &s3WriteHandle{driver: d, uri: u, opts: opts, scope: scope, bm: bm, partBuffer: buf, metadataReservation: meta}, nil
}

// Delete 删除一个 staging/旧对象；Expected 暂不声明为条件删除能力，避免把 ConditionalWrite 扩大解释为 ConditionalDelete。
func (d *Driver) Delete(ctx context.Context, u vfs.URI, expected *vfs.ObjectIdentity) error {
	if u.Scheme != "s3" {
		return fmt.Errorf("s3: unexpected scheme %q", u.Scheme)
	}
	if expected != nil {
		return fmt.Errorf("%w: S3 conditional delete is not part of the Phase 4 write contract", vfs.ErrWriteUnsupported)
	}
	reqURL, err := d.objectURL(u, "")
	if err != nil {
		return err
	}
	resp, err := d.doPayload(ctx, http.MethodDelete, reqURL, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusError(resp)
}

// Write 把 caller bytes 分块复制进固定 Managed part buffer；满 part 立即上传并复用同一 buffer。
func (h *s3WriteHandle) Write(ctx context.Context, data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal || h.partBuffer == nil {
		return 0, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		return 0, h.failed
	}
	written := 0
	for len(data) > 0 {
		if err := ctx.Err(); err != nil {
			h.failLocked(err)
			return written, err
		}
		space := h.partBuffer.Len() - h.used
		if space == 0 {
			if err := h.uploadCurrentPartLocked(ctx); err != nil {
				h.failLocked(err)
				return written, err
			}
			space = h.partBuffer.Len()
		}
		n := len(data)
		if n > space {
			n = space
		}
		copy(h.partBuffer.Bytes()[h.used:h.used+n], data[:n])
		h.used += n
		h.total += int64(n)
		written += n
		data = data[n:]
		if h.used == h.partBuffer.Len() {
			if err := h.uploadCurrentPartLocked(ctx); err != nil {
				h.failLocked(err)
				return written, err
			}
		}
	}
	return written, nil
}

// Close 完成最后 part/empty PUT，并在成功后 HEAD 最终对象 identity；失败时保留 Abort 能力。
func (h *s3WriteHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	if h.terminal || h.partBuffer == nil {
		h.mu.Unlock()
		return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
	}
	if h.failed != nil {
		err := h.failed
		h.mu.Unlock()
		return vfs.ObjectIdentity{}, err
	}
	if h.uploadID == "" && h.total == 0 {
		if err := h.putEmptyLocked(ctx); err != nil {
			h.failLocked(err)
			h.mu.Unlock()
			return vfs.ObjectIdentity{}, err
		}
	} else {
		if h.used > 0 {
			if err := h.uploadCurrentPartLocked(ctx); err != nil {
				h.failLocked(err)
				h.mu.Unlock()
				return vfs.ObjectIdentity{}, err
			}
		}
		if err := h.completeMultipartLocked(ctx); err != nil {
			h.failLocked(err)
			h.mu.Unlock()
			return vfs.ObjectIdentity{}, err
		}
	}
	h.terminal = true
	h.releaseLocked()
	h.mu.Unlock()
	info, err := h.driver.Stat(ctx, h.uri)
	if err != nil {
		// 对象已提交但 HEAD 失败，不能 Abort 已提交数据；把 identity 获取失败向上传播。
		return vfs.ObjectIdentity{}, err
	}
	if info.Size != h.total {
		return vfs.ObjectIdentity{}, fmt.Errorf("%w: S3 final size=%d written=%d", io.ErrShortWrite, info.Size, h.total)
	}
	return info.Identity, nil
}

// Abort 显式 AbortMultipartUpload，保证 Query 终态不遗留 multipart；空对象/未启动 multipart 只释放本地 ownership。
func (h *s3WriteHandle) Abort(ctx context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminal {
		return nil
	}
	var err error
	if h.uploadID != "" {
		err = h.driver.abortMultipart(ctx, h.uri, h.uploadID)
	}
	h.terminal = true
	h.releaseLocked()
	return err
}

// ensureMultipartLocked 懒创建 multipart upload，避免空文件必须制造无意义 multipart。
func (h *s3WriteHandle) ensureMultipartLocked(ctx context.Context) error {
	if h.uploadID != "" {
		return nil
	}
	u, err := h.driver.objectURL(h.uri, "")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("uploads", "")
	u.RawQuery = q.Encode()
	resp, err := h.driver.doPayload(ctx, http.MethodPost, u, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return err
	}
	var result struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return fmt.Errorf("s3: decode CreateMultipartUpload: %w", err)
	}
	if strings.TrimSpace(result.UploadID) == "" {
		return errors.New("s3: CreateMultipartUpload returned empty upload id")
	}
	h.uploadID = result.UploadID
	return nil
}

// uploadCurrentPartLocked 上传当前 Managed part buffer；part metadata 增长前先扩大 reservation。
func (h *s3WriteHandle) uploadCurrentPartLocked(ctx context.Context) error {
	if h.used == 0 {
		return nil
	}
	if len(h.parts) >= maxMultipartParts {
		return errors.New("s3: multipart part limit reached")
	}
	if err := h.ensureMultipartLocked(ctx); err != nil {
		return err
	}
	partNumber := len(h.parts) + 1
	u, err := h.driver.objectURL(h.uri, "")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("partNumber", strconv.Itoa(partNumber))
	q.Set("uploadId", h.uploadID)
	u.RawQuery = q.Encode()
	payload := h.partBuffer.Bytes()[:h.used]
	resp, err := h.driver.doPayload(ctx, http.MethodPut, u, nil, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return err
	}
	etag := trimETag(resp.Header.Get("ETag"))
	if etag == "" {
		return errors.New("s3: UploadPart returned empty ETag")
	}
	if err := h.metadataReservation.Resize(int64((len(h.parts) + 1) * 96)); err != nil {
		return err
	}
	h.parts = append(h.parts, multipartPart{Number: partNumber, ETag: etag})
	h.used = 0
	return nil
}

// completeMultipartLocked 写最终 part manifest；条件写只在原子完成点应用，而不是在 staging 阶段模拟 rename。
func (h *s3WriteHandle) completeMultipartLocked(ctx context.Context) error {
	if h.uploadID == "" || len(h.parts) == 0 {
		return errors.New("s3: cannot complete empty multipart upload")
	}
	// XML 大小随 part 数增长，因此先在 MemoryManager 中 reservation，再允许 Go heap builder 增长。
	estimate := int64(128 + len(h.parts)*128)
	res, err := h.scope.Reserve(memory.TagMetadata, estimate)
	if err != nil {
		return err
	}
	defer res.Release()
	var b strings.Builder
	b.Grow(int(estimate))
	b.WriteString("<CompleteMultipartUpload>")
	for _, part := range h.parts {
		b.WriteString("<Part><PartNumber>")
		b.WriteString(strconv.Itoa(part.Number))
		b.WriteString("</PartNumber><ETag>&quot;")
		xml.EscapeText(&b, []byte(part.ETag))
		b.WriteString("&quot;</ETag></Part>")
	}
	b.WriteString("</CompleteMultipartUpload>")
	body := []byte(b.String())
	u, err := h.driver.objectURL(h.uri, "")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("uploadId", h.uploadID)
	u.RawQuery = q.Encode()
	headers := make(http.Header)
	applyConditionalWriteHeaders(headers, h.opts)
	resp, err := h.driver.doPayload(ctx, http.MethodPost, u, headers, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	h.uploadID = ""
	return nil
}

// putEmptyLocked 使用普通 PutObject 创建空文件，并在同一个原子请求上应用条件写。
func (h *s3WriteHandle) putEmptyLocked(ctx context.Context) error {
	u, err := h.driver.objectURL(h.uri, "")
	if err != nil {
		return err
	}
	headers := make(http.Header)
	applyConditionalWriteHeaders(headers, h.opts)
	resp, err := h.driver.doPayload(ctx, http.MethodPut, u, headers, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusError(resp)
}

// abortMultipart 调用 S3 AbortMultipartUpload；成功后远端不再持有 incomplete multipart ownership。
func (d *Driver) abortMultipart(ctx context.Context, u vfs.URI, uploadID string) error {
	reqURL, err := d.objectURL(u, "")
	if err != nil {
		return err
	}
	q := reqURL.Query()
	q.Set("uploadId", uploadID)
	reqURL.RawQuery = q.Encode()
	resp, err := d.doPayload(ctx, http.MethodDelete, reqURL, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusError(resp)
}

// doPayload 对可重放 byte payload 执行 endpoint-scoped SigV4 请求；每次 retry 都重建 body reader。
func (d *Driver) doPayload(ctx context.Context, method string, u *url.URL, headers http.Header, payload []byte) (*http.Response, error) {
	key := remote.ClientKey{Endpoint: d.endpoint.String(), CredentialScope: d.cfg.CredentialScope, CredentialVersion: d.cfg.CredentialVersion, SecurityProfile: d.cfg.SecurityProfile}
	client, err := d.clients.Client(key)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])
	var lastErr error
	for attempt := 0; attempt <= d.cfg.MaxRetries; attempt++ {
		var body io.Reader
		if len(payload) > 0 {
			body = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(payload))
		for k, values := range headers {
			for _, value := range values {
				req.Header.Add(k, value)
			}
		}
		if err := (SigV4Signer{Region: d.cfg.Region, Creds: d.creds}).Sign(req, d.now(), payloadHash); err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err == nil && !retryableStatus(resp.StatusCode) {
			return resp, nil
		}
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("s3: retryable status %d", resp.StatusCode)
		} else {
			lastErr = err
			if !retryableTransport(err) {
				return nil, err
			}
		}
		if attempt == d.cfg.MaxRetries {
			break
		}
		if err := d.sleep(ctx, time.Duration(1<<attempt)*10*time.Millisecond); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

// applyConditionalWriteHeaders 将 VFS 条件写语义映射到 S3 PutObject/CompleteMultipartUpload 原子完成点。
func applyConditionalWriteHeaders(headers http.Header, opts vfs.WriteOptions) {
	if opts.IfNoneMatch {
		headers.Set("If-None-Match", "*")
	}
	if opts.Expected != nil && opts.Expected.ETag != "" {
		headers.Set("If-Match", quoteETag(opts.Expected.ETag))
	}
}

// failLocked 记录首错；不自动抹掉 uploadID，确保上层 Abort 仍能清理远端 multipart。
func (h *s3WriteHandle) failLocked(err error) {
	if h.failed == nil {
		h.failed = err
	}
}

// releaseLocked 是本地 ownership 唯一释放点；调用方必须持有 h.mu。
func (h *s3WriteHandle) releaseLocked() {
	if h.partBuffer != nil {
		h.partBuffer.Release()
		h.partBuffer = nil
	}
	if h.metadataReservation != nil {
		h.metadataReservation.Release()
		h.metadataReservation = nil
	}
	h.parts = nil
	h.used = 0
}
