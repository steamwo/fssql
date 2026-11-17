package webdav

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/steamwo/fssql/internal/storage/vfs"
)

// identityFromResponse 把 HTTP identity metadata 转成 VFS ObjectIdentity。
func identityFromResponse(response *http.Response) (vfs.ObjectIdentity, error) {
	identity := vfs.ObjectIdentity{ETag: response.Header.Get("ETag")}
	if response.ContentLength >= 0 {
		identity.Size = response.ContentLength
	} else if value := response.Header.Get("Content-Length"); value != "" {
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil || size < 0 {
			return vfs.ObjectIdentity{}, fmt.Errorf("%w: invalid Content-Length %q", ErrProtocol, value)
		}
		identity.Size = size
	}
	if value := response.Header.Get("Last-Modified"); value != "" {
		if parsed, err := http.ParseTime(value); err == nil {
			identity.ModifiedAt = parsed.UTC()
		}
	}
	return identity, nil
}

// validateContentRange 验证服务器返回的 range 起止位置完全等于请求。
func validateContentRange(value string, expectedStart, expectedEnd int64) error {
	if !strings.HasPrefix(strings.ToLower(value), "bytes ") {
		return fmt.Errorf("%w: missing Content-Range", ErrProtocol)
	}
	span := strings.TrimSpace(strings.SplitN(strings.TrimPrefix(strings.ToLower(value), "bytes "), "/", 2)[0])
	parts := strings.SplitN(span, "-", 2)
	if len(parts) != 2 {
		return fmt.Errorf("%w: invalid Content-Range %q", ErrProtocol, value)
	}
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || start != expectedStart || end != expectedEnd {
		return fmt.Errorf("%w: unexpected Content-Range %q", ErrProtocol, value)
	}
	return nil
}

// statusError 构造不读取大响应体的 HTTP 状态错误。
func statusError(operation string, status int) error {
	if status == http.StatusNotFound {
		return fmt.Errorf("%w: %w: %s status=%d", ErrProtocol, os.ErrNotExist, operation, status)
	}
	return fmt.Errorf("%w: %s status=%d", ErrProtocol, operation, status)
}

// headerToken 判断逗号分隔的 HTTP token 列表是否包含目标 token。
func headerToken(header, target string) bool {
	for _, token := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(token), target) {
			return true
		}
	}
	return false
}

// readBounded 在 Go heap 上读取受硬上限保护的小型 metadata body。
func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, ErrListLimit
	}
	return body, nil
}

const propfindBody = `<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/><D:getlastmodified/><D:getetag/><D:resourcetype/></D:prop></D:propfind>`

type multiStatus struct {
	// Responses 保存 DAV:response 元素。
	Responses []propResponse `xml:"response"`
}

type propResponse struct {
	// Href 是资源 URL/path。
	Href string `xml:"href"`
	// PropStats 保存不同 HTTP status 的属性组。
	PropStats []propStat `xml:"propstat"`
}

type propStat struct {
	// Prop 保存当前属性组。
	Prop propValues `xml:"prop"`
	// Status 是 HTTP/1.1 200 OK 等状态文本。
	Status string `xml:"status"`
}

type propValues struct {
	// ContentLength 是文件长度文本。
	ContentLength string `xml:"getcontentlength"`
	// LastModified 是 HTTP date。
	LastModified string `xml:"getlastmodified"`
	// ETag 是远端实体标签。
	ETag string `xml:"getetag"`
	// ResourceType 用于识别 collection。
	ResourceType resourceType `xml:"resourcetype"`
}

type resourceType struct {
	// Collection 非 nil 表示目录集合。
	Collection *struct{} `xml:"collection"`
}

// parseList 把 bounded PROPFIND XML 转换为 VFS list entries，并跳过 collection 自身。
func (d *Driver) parseList(base vfs.URI, body []byte) ([]vfs.ListEntry, error) {
	var document multiStatus
	if err := xml.Unmarshal(body, &document); err != nil {
		return nil, fmt.Errorf("%w: invalid PROPFIND XML: %v", ErrProtocol, err)
	}
	basePath := path.Clean(base.Path)
	entries := make([]vfs.ListEntry, 0, len(document.Responses))
	for _, response := range document.Responses {
		prop, ok := successfulProp(response.PropStats)
		if !ok {
			continue
		}
		href, err := url.Parse(strings.TrimSpace(response.Href))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid href %q", ErrProtocol, response.Href)
		}
		decodedPath, err := url.PathUnescape(href.EscapedPath())
		if err != nil {
			return nil, fmt.Errorf("%w: invalid href path %q", ErrProtocol, response.Href)
		}
		resourcePath := path.Clean("/" + strings.TrimPrefix(decodedPath, "/"))
		if resourcePath == basePath {
			continue
		}
		identity := vfs.ObjectIdentity{ETag: strings.TrimSpace(prop.ETag)}
		if prop.ContentLength != "" {
			size, parseErr := strconv.ParseInt(strings.TrimSpace(prop.ContentLength), 10, 64)
			if parseErr != nil || size < 0 {
				return nil, fmt.Errorf("%w: invalid getcontentlength %q", ErrProtocol, prop.ContentLength)
			}
			identity.Size = size
		}
		if prop.LastModified != "" {
			if parsed, parseErr := http.ParseTime(strings.TrimSpace(prop.LastModified)); parseErr == nil {
				identity.ModifiedAt = parsed.UTC()
			}
		}
		entryURI, err := vfs.ParseURI((&url.URL{Scheme: d.SchemeName, Host: d.Key.Endpoint, Path: resourcePath}).String())
		if err != nil {
			return nil, err
		}
		entries = append(entries, vfs.ListEntry{URI: entryURI, Size: identity.Size, Identity: identity})
	}
	return entries, nil
}

// successfulProp 返回首个 2xx propstat。
func successfulProp(stats []propStat) (propValues, bool) {
	for _, stat := range stats {
		if strings.Contains(stat.Status, " 200 ") || strings.HasSuffix(stat.Status, " 200") || strings.Contains(stat.Status, " 200 OK") {
			return stat.Prop, true
		}
	}
	return propValues{}, false
}
