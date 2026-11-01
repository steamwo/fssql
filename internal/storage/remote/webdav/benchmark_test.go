package webdav

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/remote"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// BenchmarkParsePropfind256 覆盖 bounded PROPFIND metadata XML 解析基线。
func BenchmarkParsePropfind256(b *testing.B) {
	fixture := newDAVFixture()
	driver, server := newTestDriverForBenchmark(b, fixture, 2)
	defer server.Close()
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">`)
	builder.WriteString(`<D:response><D:href>/</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`)
	for i := 0; i < 256; i++ {
		fmt.Fprintf(&builder, `<D:response><D:href>/part-%05d.parquet</D:href><D:propstat><D:prop><D:getcontentlength>65536</D:getcontentlength><D:getlastmodified>Fri, 02 Jan 2026 03:04:05 GMT</D:getlastmodified><D:getetag>"abc-%d"</D:getetag><D:resourcetype/></D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`, i, i)
	}
	builder.WriteString(`</D:multistatus>`)
	body := []byte(builder.String())
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/")
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := driver.parseList(uri, body)
		if err != nil {
			b.Fatal(err)
		}
		if len(entries) != 256 {
			b.Fatalf("entries=%d", len(entries))
		}
	}
}

// BenchmarkReadRange64K 覆盖 HTTP range + Managed Buffer 路径的本地 regression baseline。
func BenchmarkReadRange64K(b *testing.B) {
	fixture := newDAVFixture()
	fixture.files["/data.bin"] = make([]byte, 64<<10)
	driver, server := newTestDriverForBenchmark(b, fixture, 2)
	defer server.Close()
	uri, _ := vfs.ParseURI("webdav://" + driver.Key.Endpoint + "/data.bin")
	manager := memory.NewManager(-1)
	scope := manager.NewScope()
	bm := memory.NewBufferManager(nil)
	request := vfs.RangeRequest{URI: uri, Offset: 0, Length: 64 << 10}
	ctx := context.Background()
	b.SetBytes(64 << 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := driver.ReadRange(ctx, request, scope, bm)
		if err != nil {
			b.Fatal(err)
		}
		result.Buffer.Release()
	}
	b.StopTimer()
	if scope.Used() != 0 {
		b.Fatalf("managed bytes leaked: %d", scope.Used())
	}
}

// newTestDriverForBenchmark 创建 benchmark 专用 driver，避免依赖 testing.T Cleanup 生命周期。
func newTestDriverForBenchmark(b *testing.B, fixture *davFixture, maxWrites int) (*Driver, *httptest.Server) {
	b.Helper()
	server := httptest.NewServer(fixture)
	parsed, err := url.Parse(server.URL)
	if err != nil {
		server.Close()
		b.Fatal(err)
	}
	_, portText, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		server.Close()
		b.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		server.Close()
		b.Fatal(err)
	}
	policy := security.NewNetworkPolicy(port, 80)
	policy.AllowLoopback = true
	clients, err := NewClientManager(8, "webdav", policy, nil)
	if err != nil {
		server.Close()
		b.Fatal(err)
	}
	key := remote.ClientKey{Endpoint: parsed.Host, CredentialScope: "bench", CredentialVersion: "1", SecurityProfile: "loopback-bench"}
	credentials := CredentialProviderFunc(func(context.Context, remote.ClientKey, string) (Credentials, error) {
		return Credentials{Username: fixture.username, Password: fixture.password}, nil
	})
	driver, err := NewDriver("webdav", key, clients, credentials, policy, maxWrites)
	if err != nil {
		server.Close()
		b.Fatal(err)
	}
	return driver, server
}
