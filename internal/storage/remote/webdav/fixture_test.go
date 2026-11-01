package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"
)

// davFixture 是受 mutex 保护的确定性 WebDAV HTTP 测试端点。
type davFixture struct {
	// mu 保护 files 与行为开关。
	mu sync.Mutex
	// files 保存路径到对象字节。
	files map[string][]byte
	// ignoreRange 让 GET 故意返回 200 whole-object，用于 no-silent-fallback 回归测试。
	ignoreRange bool
	// redirectRange 让 range GET 重定向到 metadata endpoint，用于 redirect bypass 测试。
	redirectRange bool
	// stallPUTResponse 在读完 request body 后等待客户端取消，用于 Close cancellation/leak 测试。
	stallPUTResponse bool
	// allow 保存 OPTIONS Allow header。
	allow string
	// dav 保存 OPTIONS DAV header。
	dav string
	// username/password 是 Basic Auth 期望值。
	username string
	password string
}

// newDAVFixture 创建默认支持 HEAD/GET/PUT/MOVE/DELETE/PROPFIND 的测试端点。
func newDAVFixture() *davFixture {
	return &davFixture{
		files: map[string][]byte{"/data.bin": []byte("abcdefghij")},
		allow: "OPTIONS, HEAD, GET, PUT, MOVE, DELETE, PROPFIND",
		dav:   "1", username: "user", password: "pass",
	}
}

// ServeHTTP 实现测试所需 WebDAV 子集，并强制 Basic Auth。
func (f *davFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	username, password, ok := r.BasicAuth()
	if !ok || username != f.username || password != f.password {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	ignoreRange := f.ignoreRange
	redirectRange := f.redirectRange
	stallPUTResponse := f.stallPUTResponse
	allow := f.allow
	dav := f.dav
	f.mu.Unlock()

	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", allow)
		w.Header().Set("DAV", dav)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodHead:
		f.serveHead(w, r)
	case http.MethodGet:
		if redirectRange && r.Header.Get("Range") != "" {
			w.Header().Set("Location", "http://169.254.169.254/latest/meta-data")
			w.WriteHeader(http.StatusFound)
			return
		}
		f.serveGet(w, r, ignoreRange)
	case "PROPFIND":
		f.servePropfind(w, r)
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.files[r.URL.Path] = append([]byte(nil), data...)
		f.mu.Unlock()
		if stallPUTResponse {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusCreated)
	case "MOVE":
		destination := r.Header.Get("Destination")
		u, err := url.Parse(destination)
		if err != nil || u.Path == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		data, ok := f.files[r.URL.Path]
		if ok {
			f.files[u.Path] = data
			delete(f.files, r.URL.Path)
		}
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		f.mu.Lock()
		_, ok := f.files[r.URL.Path]
		delete(f.files, r.URL.Path)
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// serveHead 返回稳定测试 identity metadata。
func (f *davFixture) serveHead(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	data, ok := f.files[r.URL.Path]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	etag := fmt.Sprintf("\"%x\"", len(data)*17)
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
}

// serveGet 返回严格 206 range，或按测试开关返回 200 whole-object。
func (f *davFixture) serveGet(w http.ResponseWriter, r *http.Request, ignoreRange bool) {
	f.mu.Lock()
	data, ok := f.files[r.URL.Path]
	f.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	etag := fmt.Sprintf("\"%x\"", len(data)*17)
	if expected := r.Header.Get("If-Match"); expected != "" && expected != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	w.Header().Set("ETag", etag)
	if ignoreRange || r.Header.Get("Range") == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
		return
	}
	start, end, err := parseTestRange(r.Header.Get("Range"), int64(len(data)))
	if err != nil {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	body := data[start : end+1]
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(body)
}

// servePropfind 返回 collection 自身与当前 files 的 DAV multistatus。
func (f *davFixture) servePropfind(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	files := make(map[string][]byte, len(f.files))
	for name, data := range f.files {
		files[name] = append([]byte(nil), data...)
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`)
	_, _ = fmt.Fprintf(w, `<D:response><D:href>%s</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`, r.URL.Path)
	for name, data := range files {
		_, _ = fmt.Fprintf(w, `<D:response><D:href>%s</D:href><D:propstat><D:prop><D:getcontentlength>%d</D:getcontentlength><D:getlastmodified>Fri, 02 Jan 2026 03:04:05 GMT</D:getlastmodified><D:getetag>"%x"</D:getetag><D:resourcetype/></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`, name, len(data), len(data)*17)
	}
	_, _ = io.WriteString(w, `</D:multistatus>`)
}

// parseTestRange 解析测试服务器只需支持的 bytes=start-end 格式。
func parseTestRange(value string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, errors.New("invalid range")
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid range")
	}
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start || end >= size {
		return 0, 0, errors.New("invalid range")
	}
	return start, end, nil
}

// newTestDriver 创建绑定 httptest endpoint 的 WebDAV driver。
func newTestDriver(t *testing.T, fixture *davFixture, maxWrites int) (*Driver, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(fixture)
	t.Cleanup(server.Close)
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	policy := security.NewNetworkPolicy(port, 80)
	policy.AllowLoopback = true
	clients, err := NewClientManager(8, "webdav", policy, nil)
	if err != nil {
		t.Fatal(err)
	}
	key := remote.ClientKey{Endpoint: parsed.Host, CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "loopback-test"}
	credentials := CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (Credentials, error) {
		return Credentials{Username: fixture.username, Password: fixture.password}, nil
	})
	driver, err := NewDriver("webdav", key, clients, credentials, policy, maxWrites)
	if err != nil {
		t.Fatal(err)
	}
	return driver, server
}
