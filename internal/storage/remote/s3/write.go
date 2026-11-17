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
	Number int `json:"number"`
	// ETag 是 UploadPart 返回的实体标签。
	ETag              string `json:"etag"`
	Size              int64  `json:"size"`
	ChecksumSHA256    string `json:"checksum_sha256,omitempty"`
	ChecksumSHA1      string `json:"checksum_sha1,omitempty"`
	ChecksumCRC32     string `json:"checksum_crc32,omitempty"`
	ChecksumCRC32C    string `json:"checksum_crc32c,omitempty"`
	ChecksumCRC64NVME string `json:"checksum_crc64nvme,omitempty"`
}

func (p multipartPart) checksums() [5]string {
	return [5]string{p.ChecksumSHA256, p.ChecksumSHA1, p.ChecksumCRC32, p.ChecksumCRC32C, p.ChecksumCRC64NVME}
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
	// stagedRecord makes ownership durable before the next remote payload action.
	stagedRecord    func(context.Context) error
	createAttempted bool
	legacy          vfs.WriteHandle
}

func (h *s3WriteHandle) Release() error {
	if h.legacy != nil {
		return h.legacy.(interface{ Release() error }).Release()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.terminal = true
	h.releaseLocked()
	return nil
}

// OpenWrite 创建一个有界 S3 writer；先 Reserve/Allocate part buffer，再允许任何远端 multipart 生命周期开始。
func (d *Driver) OpenWrite(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
	return d.OpenAtomicPublishWrite(ctx, u, opts, scope, bm)
}

func (d *Driver) openMultipartWriter(ctx context.Context, u vfs.URI, opts vfs.WriteOptions, scope *memory.Scope, bm *memory.BufferManager) (vfs.WriteHandle, error) {
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
	if h.legacy != nil {
		return h.legacy.Write(ctx, data)
	}
	return h.writePayload(ctx, data)
}

func (h *s3WriteHandle) writePayload(ctx context.Context, data []byte) (int, error) {
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

// Close is retained only as the legacy adapter; raw staged writers cannot publish implicitly.
func (h *s3WriteHandle) Close(ctx context.Context) (vfs.ObjectIdentity, error) {
	if h.legacy != nil {
		return h.legacy.Close(ctx)
	}
	return vfs.ObjectIdentity{}, vfs.ErrWriteClosed
}

// Abort 显式 AbortMultipartUpload，保证 Query 终态不遗留 multipart；空对象/未启动 multipart 只释放本地 ownership。
func (h *s3WriteHandle) Abort(ctx context.Context) error {
	if h.legacy != nil {
		return h.legacy.Abort(ctx)
	}
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
	if h.stagedRecord != nil {
		res, err := h.scope.Reserve(memory.TagMetadata, publishControlBytes*4)
		if err != nil {
			return err
		}
		defer res.Release()
	}
	u, err := h.driver.objectURL(h.uri, "")
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("uploads", "")
	u.RawQuery = q.Encode()
	h.createAttempted = true
	request := h.driver.doPayload
	if h.stagedRecord != nil {
		request = h.driver.doPayloadOnce
	}
	resp, err := request(ctx, http.MethodPost, u, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return err
	}
	var result struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		UploadID string   `xml:"UploadId"`
	}
	decode := func() error { return xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result) }
	if h.stagedRecord != nil {
		decode = func() error { return decodePublishXML(resp.Body, &result) }
	}
	if err := decode(); err != nil {
		return fmt.Errorf("s3: decode CreateMultipartUpload: %w", err)
	}
	if strings.TrimSpace(result.UploadID) == "" {
		return errors.New("s3: CreateMultipartUpload returned empty upload id")
	}
	h.uploadID = result.UploadID
	if len(h.uploadID) > 4096 {
		return vfs.ErrInvalidSealedObject
	}
	if h.stagedRecord != nil {
		return h.stagedRecord(ctx)
	}
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
	if len(etag) > 256 {
		return vfs.ErrInvalidSealedObject
	}
	if err := h.metadataReservation.Resize(int64((len(h.parts) + 1) * 4096)); err != nil {
		return err
	}
	part := multipartPart{Number: partNumber, ETag: etag, Size: int64(h.used), ChecksumSHA256: resp.Header.Get("x-amz-checksum-sha256"), ChecksumSHA1: resp.Header.Get("x-amz-checksum-sha1"), ChecksumCRC32: resp.Header.Get("x-amz-checksum-crc32"), ChecksumCRC32C: resp.Header.Get("x-amz-checksum-crc32c"), ChecksumCRC64NVME: resp.Header.Get("x-amz-checksum-crc64nvme")}
	for _, s := range part.checksums() {
		if len(s) > 128 {
			return vfs.ErrInvalidSealedObject
		}
	}
	h.parts = append(h.parts, part)
	h.used = 0
	if h.stagedRecord != nil {
		return h.stagedRecord(ctx)
	}
	return nil
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
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return statusError(resp)
}

// doPayload 对可重放 byte payload 执行 endpoint-scoped SigV4 请求；每次 retry 都重建 body reader。
func (d *Driver) doPayload(ctx context.Context, method string, u *url.URL, headers http.Header, payload []byte) (*http.Response, error) {
	return d.doPayloadAttempts(ctx, method, u, headers, payload, d.cfg.MaxRetries)
}

// Final publication and ownership creation are never replayed implicitly: a
// lost success followed by 412/NoSuchUpload must stay an uncertain result.
func (d *Driver) doPayloadOnce(ctx context.Context, method string, u *url.URL, headers http.Header, payload []byte) (*http.Response, error) {
	return d.doPayloadAttempts(ctx, method, u, headers, payload, 0)
}

func (d *Driver) doPayloadAttempts(ctx context.Context, method string, u *url.URL, headers http.Header, payload []byte, retries int) (*http.Response, error) {
	key := remote.ClientKey{Endpoint: d.endpoint.String(), CredentialScope: d.cfg.CredentialScope, CredentialVersion: d.cfg.CredentialVersion, SecurityProfile: d.cfg.SecurityProfile}
	client, err := d.clients.Client(key)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])
	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
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
		if err == nil && (!retryableStatus(resp.StatusCode) || retries == 0) {
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
		if attempt == retries {
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
