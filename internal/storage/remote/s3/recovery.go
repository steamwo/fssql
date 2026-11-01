package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

const (
	// recoveryMaxUploads 把单页 upload 数限制为 100，使 XML metadata 的 Go heap 上界可由固定 reservation 保守覆盖。
	recoveryMaxUploads = 100
	// recoveryPageMetadataBudget 是每页 ListMultipartUploads XML 解码前预留的 Metadata 预算。
	recoveryPageMetadataBudget int64 = 512 << 10
)

// multipartRecoveryPage 是 S3 ListMultipartUploads 的最小内部表示；uploadID 永远停留在 S3 adapter 边界。
type multipartRecoveryPage struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	IsTruncated        bool     `xml:"IsTruncated"`
	NextKeyMarker      string   `xml:"NextKeyMarker"`
	NextUploadIDMarker string   `xml:"NextUploadIdMarker"`
	Uploads            []struct {
		// Key 是 multipart upload 对应的对象 key。
		Key string `xml:"Key"`
		// UploadID 是协议私有 cleanup token，不得返回给 VFS/Export。
		UploadID string `xml:"UploadId"`
	} `xml:"Upload"`
}

// RecoverIncompleteWrites 发现并清理 job 独占 staging scope 内的 orphan multipart upload。
func (d *Driver) RecoverIncompleteWrites(ctx context.Context, scopeURI vfs.URI, scope *memory.Scope) (vfs.RecoveryReport, error) {
	if d == nil {
		return vfs.RecoveryReport{}, errors.New("s3: nil driver")
	}
	if scopeURI.Scheme != "s3" {
		return vfs.RecoveryReport{}, fmt.Errorf("s3: unexpected scheme %q", scopeURI.Scheme)
	}
	if scope == nil {
		return vfs.RecoveryReport{}, errors.New("s3: managed memory scope is required for multipart recovery")
	}
	if err := ctx.Err(); err != nil {
		return vfs.RecoveryReport{}, err
	}
	base := strings.Trim(strings.TrimSpace(scopeURI.ObjectKey()), "/")
	if base == "" {
		return vfs.RecoveryReport{}, errors.New("s3: multipart recovery requires non-root staging scope")
	}
	// ParseURI 会清理尾部 slash，因此这里显式补回目录分隔符，避免 job-1 误匹配 job-10。
	prefix := base + "/"

	var report vfs.RecoveryReport
	var keyMarker, uploadMarker string
	for {
		if err := ctx.Err(); err != nil {
			return vfs.RecoveryReport{}, err
		}
		page, err := d.listMultipartRecoveryPage(ctx, scopeURI.Host, prefix, keyMarker, uploadMarker, scope)
		if err != nil {
			return vfs.RecoveryReport{}, err
		}
		for _, upload := range page.Uploads {
			if !strings.HasPrefix(upload.Key, prefix) {
				return vfs.RecoveryReport{}, fmt.Errorf("s3: ListMultipartUploads returned out-of-scope key %q for prefix %q", upload.Key, prefix)
			}
			if strings.TrimSpace(upload.UploadID) == "" {
				return vfs.RecoveryReport{}, errors.New("s3: ListMultipartUploads returned empty upload id")
			}
			report.Discovered++
			if err := ctx.Err(); err != nil {
				return vfs.RecoveryReport{}, err
			}
			object := vfs.URI{Scheme: "s3", Host: scopeURI.Host, Path: "/" + upload.Key}
			if err := d.abortMultipart(ctx, object, upload.UploadID); err != nil {
				return vfs.RecoveryReport{}, fmt.Errorf("s3: abort recovered multipart %q: %w", upload.Key, err)
			}
			report.Recovered++
		}
		if !page.IsTruncated {
			break
		}
		nextKey, nextUpload := page.NextKeyMarker, page.NextUploadIDMarker
		if nextKey == "" || (nextKey == keyMarker && nextUpload == uploadMarker) {
			return vfs.RecoveryReport{}, errors.New("s3: ListMultipartUploads pagination made no progress")
		}
		keyMarker, uploadMarker = nextKey, nextUpload
	}
	return report, nil
}

// listMultipartRecoveryPage 读取一页服务端 multipart ownership；解码前先 Reserve 固定 Metadata 上界。
func (d *Driver) listMultipartRecoveryPage(ctx context.Context, bucket, prefix, keyMarker, uploadMarker string, scope *memory.Scope) (multipartRecoveryPage, error) {
	reservation, err := scope.Reserve(memory.TagMetadata, recoveryPageMetadataBudget)
	if err != nil {
		return multipartRecoveryPage{}, err
	}
	defer reservation.Release()

	u, err := d.bucketURL(bucket)
	if err != nil {
		return multipartRecoveryPage{}, err
	}
	q := u.Query()
	q.Set("uploads", "")
	q.Set("prefix", prefix)
	q.Set("max-uploads", strconv.Itoa(recoveryMaxUploads))
	if keyMarker != "" {
		q.Set("key-marker", keyMarker)
	}
	if uploadMarker != "" {
		q.Set("upload-id-marker", uploadMarker)
	}
	u.RawQuery = q.Encode()

	resp, err := d.doPayload(ctx, http.MethodGet, u, nil, nil)
	if err != nil {
		return multipartRecoveryPage{}, err
	}
	defer resp.Body.Close()
	if err := statusError(resp); err != nil {
		return multipartRecoveryPage{}, err
	}

	var page multipartRecoveryPage
	if err := xml.NewDecoder(io.LimitReader(resp.Body, recoveryPageMetadataBudget)).Decode(&page); err != nil {
		return multipartRecoveryPage{}, fmt.Errorf("s3: decode ListMultipartUploads: %w", err)
	}
	return page, nil
}
