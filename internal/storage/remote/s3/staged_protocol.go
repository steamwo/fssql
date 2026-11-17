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
	"os"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// Bounded control documents are decoded only while this metadata budget is held.
const publishControlBytes int64 = 64 << 10

func decodePublishXML(r io.Reader, value any) error {
	d := xml.NewDecoder(io.LimitReader(r, publishControlBytes+1))
	if err := d.Decode(value); err != nil {
		return err
	}
	// Reject a second document, truncated suffix, non-whitespace suffix, or an
	// oversized whitespace tail. Decode alone accepts trailing Error elements.
	for {
		token, err := d.Token()
		if err == io.EOF {
			if d.InputOffset() > publishControlBytes {
				return errors.New("s3: control response too large")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if chars, ok := token.(xml.CharData); !ok || strings.TrimSpace(string(chars)) != "" {
			return errors.New("s3: trailing control response data")
		}
	}
}

type completePublishResult struct {
	XMLName xml.Name
	ETag    string `xml:"ETag"`
	Code    string `xml:"Code"`
}

func publishResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusPreconditionFailed {
		return errors.Join(vfs.ErrPublishConflict, vfs.ErrObjectChanged)
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return errors.Join(vfs.ErrPublishFailed, vfs.ErrCredential)
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return fmt.Errorf("%w: S3 HTTP status %d", vfs.ErrPublishFailed, resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("s3: uncertain HTTP status %d", resp.StatusCode)
	}
	return nil
}

func (h *s3PublishHandle) Publish(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.validateSealed(s); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	w := h.writer
	if _, err := w.driver.validatePublishIntent(ctx, h.intent); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	reservation, err := w.scope.Reserve(memory.TagMetadata, publishControlBytes*4+int64(len(h.state.Parts))*8192)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer reservation.Release()
	u, err := w.driver.objectURL(w.uri, "")
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	headers := make(http.Header)
	applyConditionalWriteHeaders(headers, w.opts)
	method := http.MethodPut
	var payload []byte
	if h.state.Kind == "multipart" {
		method = http.MethodPost
		q := u.Query()
		q.Set("uploadId", h.state.UploadID)
		u.RawQuery = q.Encode()
		var b bytes.Buffer
		b.WriteString("<CompleteMultipartUpload>")
		for _, p := range h.state.Parts {
			b.WriteString("<Part><PartNumber>")
			b.WriteString(strconv.Itoa(p.Number))
			b.WriteString("</PartNumber><ETag>&quot;")
			_ = xml.EscapeText(&b, []byte(p.ETag))
			b.WriteString("&quot;</ETag></Part>")
		}
		b.WriteString("</CompleteMultipartUpload>")
		payload = b.Bytes()
		headers.Set("Content-Type", "application/xml")
	}
	resp, err := w.driver.doPayloadOnce(ctx, method, u, headers, payload)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer resp.Body.Close()
	if err := publishResponseError(resp); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	identity := vfs.ObjectIdentity{Size: s.Size, ETag: trimETag(resp.Header.Get("ETag")), VersionID: resp.Header.Get("x-amz-version-id")}
	if h.state.Kind == "multipart" {
		var result completePublishResult
		if err := decodePublishXML(resp.Body, &result); err != nil {
			return vfs.ObjectIdentity{}, fmt.Errorf("s3: decode CompleteMultipartUpload response: %w", err)
		}
		if result.XMLName.Local == "Error" {
			if result.Code == "" {
				return vfs.ObjectIdentity{}, errors.New("s3: malformed CompleteMultipartUpload error document")
			}
			if result.Code == "PreconditionFailed" {
				return vfs.ObjectIdentity{}, errors.Join(vfs.ErrPublishConflict, vfs.ErrObjectChanged)
			}
			return vfs.ObjectIdentity{}, fmt.Errorf("%w: S3 CompleteMultipartUpload rejected", vfs.ErrPublishFailed)
		}
		if result.XMLName.Local != "CompleteMultipartUploadResult" || result.Code != "" || trimETag(result.ETag) == "" {
			return vfs.ObjectIdentity{}, errors.New("s3: invalid CompleteMultipartUpload success document")
		}
		identity.ETag = trimETag(result.ETag)
	}
	h.finalIdentity = identity
	return identity, nil
}

func (h *s3PublishHandle) Verify(ctx context.Context, s vfs.SealedObject, final bool) (vfs.ObjectIdentity, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.validateSealed(s); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if _, err := h.writer.driver.validatePublishIntent(ctx, h.intent); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if !final {
		if h.state.Kind == "empty_put" {
			return vfs.ObjectIdentity{Size: 0}, nil
		}
		if err := h.verifyParts(ctx); err != nil {
			return vfs.ObjectIdentity{}, err
		}
		return vfs.ObjectIdentity{Size: s.Size}, nil
	}
	return h.verifyFinal(ctx, s)
}

type listPublishParts struct {
	XMLName     xml.Name `xml:"ListPartsResult"`
	Bucket      string   `xml:"Bucket"`
	Key         string   `xml:"Key"`
	UploadID    string   `xml:"UploadId"`
	IsTruncated bool     `xml:"IsTruncated"`
	Next        int      `xml:"NextPartNumberMarker"`
	Parts       []struct {
		Number            int    `xml:"PartNumber"`
		ETag              string `xml:"ETag"`
		Size              int64  `xml:"Size"`
		ChecksumSHA256    string `xml:"ChecksumSHA256"`
		ChecksumSHA1      string `xml:"ChecksumSHA1"`
		ChecksumCRC32     string `xml:"ChecksumCRC32"`
		ChecksumCRC32C    string `xml:"ChecksumCRC32C"`
		ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME"`
	} `xml:"Part"`
}

func (h *s3PublishHandle) verifyParts(ctx context.Context) error {
	w := h.writer
	reservation, err := w.scope.Reserve(memory.TagMetadata, publishControlBytes*4)
	if err != nil {
		return err
	}
	defer reservation.Release()
	marker, index := 0, 0
	for {
		u, err := w.driver.objectURL(w.uri, "")
		if err != nil {
			return err
		}
		q := u.Query()
		q.Set("uploadId", h.state.UploadID)
		q.Set("max-parts", "100")
		q.Set("part-number-marker", strconv.Itoa(marker))
		u.RawQuery = q.Encode()
		resp, err := w.driver.doPayload(ctx, http.MethodGet, u, nil, nil)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusNotFound {
			_ = resp.Body.Close()
			return vfs.ErrInvalidSealedObject
		}
		if err := statusError(resp); err != nil {
			_ = resp.Body.Close()
			return err
		}
		var result listPublishParts
		err = decodePublishXML(resp.Body, &result)
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		if result.Bucket != w.uri.Host || result.Key != w.uri.ObjectKey() || result.UploadID != h.state.UploadID || len(result.Parts) > 100 {
			return vfs.ErrInvalidSealedObject
		}
		for _, p := range result.Parts {
			if index >= len(h.state.Parts) {
				return vfs.ErrInvalidSealedObject
			}
			expected := h.state.Parts[index]
			checksums := [5]string{p.ChecksumSHA256, p.ChecksumSHA1, p.ChecksumCRC32, p.ChecksumCRC32C, p.ChecksumCRC64NVME}
			if p.Number != expected.Number || trimETag(p.ETag) != expected.ETag || p.Size != expected.Size || checksums != expected.checksums() {
				return vfs.ErrInvalidSealedObject
			}
			index++
		}
		if !result.IsTruncated {
			if index != len(h.state.Parts) {
				return vfs.ErrInvalidSealedObject
			}
			return nil
		}
		if len(result.Parts) == 0 || result.Next <= marker || result.Next != result.Parts[len(result.Parts)-1].Number {
			return vfs.ErrInvalidSealedObject
		}
		marker = result.Next
	}
}

func (h *s3PublishHandle) publishStat(ctx context.Context) (vfs.ObjectIdentity, error) {
	w := h.writer
	u, err := w.driver.objectURL(w.uri, "")
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	resp, err := w.driver.doPayload(ctx, http.MethodHead, u, nil, nil)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return vfs.ObjectIdentity{}, os.ErrNotExist
	}
	if err := statusError(resp); err != nil {
		return vfs.ObjectIdentity{}, err
	}
	id, err := identityFromResponse(resp)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if id.Size < 0 || !id.Stable() {
		return vfs.ObjectIdentity{}, errors.New("s3: final object lacks stable identity or size")
	}
	return id, nil
}

func (h *s3PublishHandle) verifyFinal(ctx context.Context, s vfs.SealedObject) (vfs.ObjectIdentity, error) {
	id, err := h.publishStat(ctx)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if id.Size != s.Size || h.finalIdentity.Stable() && !h.finalIdentity.Matches(id) {
		return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
	}
	w := h.writer
	u, err := w.driver.objectURL(w.uri, id.VersionID)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	headers := make(http.Header)
	headers.Set("Accept-Encoding", "identity")
	if id.ETag != "" {
		headers.Set("If-Match", quoteETag(id.ETag))
	}
	resp, err := w.driver.doPayload(ctx, http.MethodGet, u, headers, nil)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed {
		return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
	}
	if resp.StatusCode != http.StatusOK {
		if err := statusError(resp); err != nil {
			return vfs.ObjectIdentity{}, err
		}
		return vfs.ObjectIdentity{}, errors.New("s3: unexpected final verification response")
	}
	readIdentity, err := identityFromResponse(resp)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if !readIdentity.Matches(id) {
		return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
	}
	buffer, err := w.bm.Allocate(w.scope, memory.TagNetworkBuffer, 64<<10)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	defer buffer.Release()
	digest := sha256.New()
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return vfs.ObjectIdentity{}, err
		}
		n, e := resp.Body.Read(buffer.Bytes())
		if n > 0 {
			size += int64(n)
			_, _ = digest.Write(buffer.Bytes()[:n])
			if size > s.Size {
				return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return vfs.ObjectIdentity{}, e
		}
		if n == 0 {
			return vfs.ObjectIdentity{}, io.ErrNoProgress
		}
	}
	if size != s.Size || hex.EncodeToString(digest.Sum(nil)) != s.Checksum {
		return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
	}
	current, err := h.publishStat(ctx)
	if err != nil {
		return vfs.ObjectIdentity{}, err
	}
	if !current.Matches(id) || current.Size != id.Size {
		return vfs.ObjectIdentity{}, vfs.ErrPublishConflict
	}
	return id, nil
}

func (h *s3PublishHandle) Abort(ctx context.Context, _ *vfs.SealedObject) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.writer
	if _, err := w.driver.validatePublishIntent(ctx, h.intent); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() { w.terminal = true; w.releaseLocked() }()
	if w.uploadID == "" {
		if w.createAttempted || h.state.Kind == "pending" && h.record == nil {
			return vfs.ErrInvalidSealedObject
		}
		return nil
	}
	// Exact UploadID only; never enumerate or delete uploads by final key.
	err := w.driver.abortMultipart(ctx, w.uri, w.uploadID)
	return err
}
