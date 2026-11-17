package httpremote

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/security"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func testPolicy(raw string) *security.NetworkPolicy {
	u, _ := url.Parse(raw)
	port, _ := strconv.Atoi(u.Port())
	p := security.NewNetworkPolicy(port)
	p.AllowLoopback = true
	return p
}
func testDriver(t *testing.T, configs ...Config) *Driver {
	t.Helper()
	drivers, close, err := NewDrivers(configs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = close() })
	if len(drivers) == 0 {
		t.Fatal("missing drivers")
	}
	return drivers[0].(*Driver)
}
func testURI(t *testing.T, raw string) vfs.URI {
	t.Helper()
	u, err := vfs.ParseURI(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func rangeResponse(w http.ResponseWriter, r *http.Request, data, etag string) {
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-Match") != "" && r.Header.Get("If-Match") != etag {
		w.WriteHeader(412)
		return
	}
	var start, end int
	if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		_, _ = io.WriteString(w, data)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.WriteHeader(206)
	_, _ = io.WriteString(w, data[start:end+1])
}

func TestRangeSnapshotPreservesURIAndRejectsChanges(t *testing.T) {
	var mode atomic.Int32
	var seen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Add(1)
		if r.RequestURI != "/a%2Fb//c?z=secret&z=&x=2" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("request URI/encoding changed")
		}
		if mode.Load() != 0 {
			if r.Header.Get("If-Match") != "\"v1\"" {
				t.Error("validator missing")
			}
			switch mode.Load() {
			case 1:
				w.WriteHeader(200)
				return
			case 2:
				w.WriteHeader(412)
				return
			case 3:
				w.Header().Set("Location", "/changed")
				w.WriteHeader(302)
				return
			case 4:
				rangeResponse(w, r, "abcdef", "\"v2\"")
				return
			case 5:
				rangeResponse(w, r, "abcdefg", "\"v1\"")
				return
			}
		}
		rangeResponse(w, r, "abcdef", "\"v1\"")
	}))
	defer server.Close()
	d := testDriver(t, Config{Endpoint: server.URL, Network: testPolicy(server.URL)})
	u := testURI(t, server.URL+"/a%2Fb//c?z=secret&z=&x=2")
	source, err := d.OpenObject(context.Background(), u)
	if err != nil || source.Stream != nil || source.Info.Size != 6 || !source.Capabilities.Has(vfs.RandomRead) {
		t.Fatalf("source=%+v err=%v", source, err)
	}
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	defer s.Close()
	bm := memory.NewBufferManager(nil)
	req := vfs.RangeRequest{URI: u, Offset: 2, Length: 3, Expected: &source.Info.Identity}
	result, err := source.Reader.ReadRange(context.Background(), req, s, bm)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Buffer.Bytes()) != "cde" {
		t.Fatal("wrong range")
	}
	result.Buffer.Release()
	for i := int32(1); i <= 5; i++ {
		mode.Store(i)
		_, err := source.Reader.ReadRange(context.Background(), req, s, bm)
		if !errors.Is(err, vfs.ErrObjectChanged) {
			t.Fatalf("mode=%d err=%v", i, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("secret leaked")
		}
	}
	if seen.Load() != 7 || m.Used() != 0 {
		t.Fatalf("requests=%d memory=%d", seen.Load(), m.Used())
	}
}

func TestProbeFallbackAndProtocolMatrix(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		status                   int
		cr, etag, encoding, body string
		require                  bool
		want                     error
		gets                     int
	}{
		{"whole", 200, "", "", "", "payload", false, nil, 1},
		{"whole require", 200, "", "", "", "payload", true, vfs.ErrRangeUnsupported, 1},
		{"weak", 206, "bytes 0-0/7", "W/\"v1\"", "", "p", false, nil, 2},
		{"missing", 206, "bytes 0-0/7", "", "", "p", false, nil, 2},
		{"unknown", 206, "bytes 0-0/*", "\"v1\"", "", "p", false, nil, 2},
		{"weak require", 206, "bytes 0-0/7", "W/\"v1\"", "", "p", true, vfs.ErrRangeUnsupported, 1},
		{"unknown require", 206, "bytes 0-0/*", "\"v1\"", "", "p", true, vfs.ErrRangeUnsupported, 1},
		{"empty", 416, "bytes */0", "", "", "error body", true, nil, 1},
		{"other416", 416, "bytes */7", "", "", "", false, ErrProtocol, 1},
		{"bad range", 206, "bytes 1-1/7", "\"v1\"", "", "p", false, ErrProtocol, 1},
		{"bad total", 206, "bytes 0-0/0", "\"v1\"", "", "p", false, ErrProtocol, 1},
		{"bad syntax", 206, "garbage", "\"v1\"", "", "p", false, ErrProtocol, 1},
		{"short", 206, "bytes 0-0/7", "\"v1\"", "", "", false, ErrProtocol, 1},
		{"long", 206, "bytes 0-0/7", "\"v1\"", "", "pp", false, ErrProtocol, 1},
		{"br", 200, "", "", "br", "payload", false, ErrEncodingUnsupported, 1},
		{"stacked", 200, "", "", "identity, gzip", "payload", true, ErrEncodingUnsupported, 1},
		{"gzip source", 200, "", "", "gzip", "payload", false, nil, 1},
		{"auth", 403, "", "", "", "", false, vfs.ErrCredential, 1},
		{"server error", 500, "", "", "", "", false, ErrProtocol, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gets atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Error("HEAD used")
				}
				gets.Add(1)
				if r.Header.Get("Range") == "" {
					_, _ = io.WriteString(w, "payload")
					return
				}
				if tc.cr != "" {
					w.Header().Set("Content-Range", tc.cr)
				}
				if tc.etag != "" {
					w.Header().Set("ETag", tc.etag)
				}
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				w.WriteHeader(tc.status)
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			policy := FallbackMaterialize
			if tc.require {
				policy = FallbackRequireRange
			}
			d := testDriver(t, Config{Endpoint: server.URL, Network: testPolicy(server.URL), FallbackPolicy: policy})
			source, err := d.OpenObject(context.Background(), testURI(t, server.URL+"/data?sig=secret"))
			if tc.want != nil {
				if !errors.Is(err, tc.want) {
					t.Fatalf("want=%v got=%v", tc.want, err)
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("secret leaked")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if source.Stream == nil {
					t.Fatal("stream missing")
				}
				data, err := io.ReadAll(source.Stream)
				if err != nil {
					t.Fatal(err)
				}
				_ = source.Stream.Close()
				want := "payload"
				if tc.status == 416 {
					want = ""
				}
				if string(data) != want {
					t.Fatalf("body=%q", data)
				}
			}
			if int(gets.Load()) != tc.gets {
				t.Fatalf("gets=%d", gets.Load())
			}
		})
	}
}

func TestRedirectAuthenticationAndLimits(t *testing.T) {
	var destinationHits atomic.Int32
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destinationHits.Add(1)
		if r.Header.Get("Authorization") != "Bearer target" || r.Header.Get("X-Key") != "target-key" || r.Header.Get("X-Source") != "" {
			t.Error("credentials crossed endpoints")
		}
		rangeResponse(w, r, "payload", "\"target\"")
	}))
	defer destination.Close()
	originServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "origin" || pass != "password" || r.Header.Get("X-Source") != "source-only" {
			t.Error("source credentials missing")
		}
		switch r.URL.Path {
		case "/loop":
			w.Header().Set("Location", "/loop")
		case "/limit":
			n, _ := strconv.Atoi(r.URL.Query().Get("n"))
			w.Header().Set("Location", fmt.Sprintf("/limit?n=%d", n+1))
		default:
			w.Header().Set("Location", destination.URL+"/data?sig=target-secret")
		}
		w.WriteHeader(302)
	}))
	defer originServer.Close()
	a := Config{Endpoint: originServer.URL, Network: testPolicy(originServer.URL), Username: "origin", Password: "password", Headers: map[string]string{"X-Source": "source-only"}}
	b := Config{Endpoint: destination.URL, Network: testPolicy(destination.URL), BearerToken: "target", Headers: map[string]string{"X-Key": "target-key"}}
	d := testDriver(t, a, b)
	source, err := d.OpenObject(context.Background(), testURI(t, originServer.URL+"/redirect"))
	if err != nil {
		t.Fatal(err)
	}
	m := memory.NewManager(1 << 20)
	s := m.NewScope()
	defer s.Close()
	result, err := source.Reader.ReadRange(context.Background(), vfs.RangeRequest{Length: 1, Expected: &source.Info.Identity}, s, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	result.Buffer.Release()
	for _, path := range []string{"/loop", "/limit"} {
		if _, err := d.OpenObject(context.Background(), testURI(t, originServer.URL+path)); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
	d2 := testDriver(t, a)
	if _, err := d2.OpenObject(context.Background(), testURI(t, originServer.URL+"/redirect")); !errors.Is(err, security.ErrEndpointDenied) {
		t.Fatal(err)
	}
	if destinationHits.Load() != 2 {
		t.Fatal("unauthorized endpoint contacted")
	}
}

func TestHTTPSDowngradeDenied(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://example.test/data")
		w.WriteHeader(302)
	}))
	defer server.Close()
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig
	d := testDriver(t, Config{Endpoint: server.URL, Network: testPolicy(server.URL), TLSConfig: tlsConfig}, Config{Endpoint: "http://example.test"})
	if _, err := d.OpenObject(context.Background(), testURI(t, server.URL+"/data")); !errors.Is(err, security.ErrRedirectDowngrade) {
		t.Fatal(err)
	}
}

func TestConfigurationValidationAndCopy(t *testing.T) {
	for _, c := range []Config{
		{Endpoint: "http://example.test/"}, {Endpoint: "http://example.test?"}, {Endpoint: "http://example.test#"}, {Endpoint: "http://user:secret@example.test"}, {Endpoint: "http://example.test:0"},
		{Endpoint: "http://example.test", Username: "u", BearerToken: "secret"}, {Endpoint: "http://example.test", Password: "secret"}, {Endpoint: "http://example.test", FallbackPolicy: "invalid"},
		{Endpoint: "http://example.test", Headers: map[string]string{"Authorization": "secret"}}, {Endpoint: "http://example.test", Headers: map[string]string{"Range": "bytes=0-0"}}, {Endpoint: "http://example.test", Headers: map[string]string{"X-Key": "secret\r\nx"}},
	} {
		if _, _, err := NewDrivers([]Config{c}); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("bad config: %v", err)
		}
	}
	if _, _, err := NewDrivers([]Config{{Endpoint: "http://EXAMPLE.test"}, {Endpoint: "http://example.test:80"}}); err == nil {
		t.Fatal("duplicate origin accepted")
	}
	for _, name := range []string{"Host", "Proxy-Authorization", "If-None-Match", "If-Match", "If-Range", "If-Modified-Since", "If-Unmodified-Since", "Accept-Encoding", "Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "TE", "Trailer", "Upgrade", "Content-Length"} {
		if _, _, err := NewDrivers([]Config{{Endpoint: "https://example.test", Headers: map[string]string{name: "value"}}}); err == nil {
			t.Fatalf("managed header accepted: %s", name)
		}
	}
	headers := map[string]string{"X-Key": "original"}
	roots := x509.NewCertPool()
	originalTLS := &tls.Config{ServerName: "original", RootCAs: roots}
	d := testDriver(t, Config{Endpoint: "https://example.test", Headers: headers, TLSConfig: originalTLS})
	headers["X-Key"] = "changed"
	originalTLS.ServerName = "changed"
	e := d.endpoints.items["https://example.test:443"]
	if e.config.Headers["X-Key"] != "original" || e.config.TLSConfig.ServerName != "original" || e.config.TLSConfig == originalTLS {
		t.Fatal("configuration was not copied")
	}
	if e.config.TLSConfig.RootCAs != roots {
		t.Fatal("TLS clone must preserve the documented shallow-copy contract")
	}
}

type changingResolver struct{ calls atomic.Int32 }

func (r *changingResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	if r.calls.Add(1) == 1 {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
	}
	return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
}

func TestDNSIsRevalidatedAtDialAndErrorsAreRedacted(t *testing.T) {
	resolver := &changingResolver{}
	p := security.NewNetworkPolicy(80).WithResolver(resolver)
	d := testDriver(t, Config{Endpoint: "http://rebinding.example", Network: p})
	_, err := d.OpenObject(context.Background(), testURI(t, "http://rebinding.example/data?sig=secret"))
	if !errors.Is(err, security.ErrEndpointDenied) || resolver.calls.Load() != 2 || strings.Contains(err.Error(), "secret") {
		t.Fatalf("calls=%d err=%v", resolver.calls.Load(), err)
	}
	loopback := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("default private policy bypassed") }))
	defer loopback.Close()
	d = testDriver(t, Config{Endpoint: loopback.URL})
	if _, err := d.OpenObject(context.Background(), testURI(t, loopback.URL+"/")); !errors.Is(err, security.ErrEndpointDenied) {
		t.Fatal(err)
	}
}

func TestRequestTimeoutAndCancellationReleaseConnectionSlot(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	d := testDriver(t, Config{Endpoint: server.URL, Network: testPolicy(server.URL), RequestTimeout: 40 * time.Millisecond, MaxConnections: 1})
	u := testURI(t, server.URL+"/data?sig=secret")
	if _, err := d.OpenObject(context.Background(), u); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if len(d.endpoints.items[mustOrigin(server.URL)].slots) != 0 {
		t.Fatal("connection slot leaked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.OpenObject(ctx, u); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func mustOrigin(raw string) string { u, _ := url.Parse(raw); key, _ := origin(u); return key }

func TestConnectionLimitWaitAndDriverClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "payload") }))
	defer server.Close()
	d := testDriver(t, Config{Endpoint: server.URL, Network: testPolicy(server.URL), RequestTimeout: 100 * time.Millisecond, MaxConnections: 1})
	u := testURI(t, server.URL+"/data")
	source, err := d.OpenObject(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Stream.Close()
	if _, err := d.OpenObject(context.Background(), u); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slot wait ignored timeout: %v", err)
	}
	_ = source.Stream.Close()
	source, err = d.OpenObject(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	_ = source.Stream.Close()
	d.endpoints.closed.Store(true)
	d.endpoints.cancel()
	if _, err := d.OpenObject(context.Background(), u); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestEndpointScopeSeparatesCredentialsAndTLSRouting(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range []Config{
		{Endpoint: "https://example.test", Headers: map[string]string{"X-Key": "\x80"}},
		{Endpoint: "https://example.test", Headers: map[string]string{"X-Key": "\x81"}},
		{Endpoint: "https://example.test", TLSConfig: &tls.Config{ServerName: "one"}},
		{Endpoint: "https://example.test", TLSConfig: &tls.Config{ServerName: "two"}},
		{Endpoint: "https://example.test", TLSConfig: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{{1}}}}}},
		{Endpoint: "https://example.test", TLSConfig: &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{{2}}}}}},
	} {
		d := testDriver(t, c)
		scope := d.endpoints.items["https://example.test:443"].scope
		if len(scope) != 64 || seen[scope] {
			t.Fatal("endpoint credential/config identity collided")
		}
		seen[scope] = true
	}
}

func FuzzContentRange(f *testing.F) {
	for _, s := range []string{"bytes 0-0/1", "bytes 0-0/*", "bytes */0", "bytes 1-3/4", "bytes 0-1/1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		start, end, total, err := parseContentRange(s)
		if err == nil && (start < -1 || end < start || total < -1 || start >= 0 && total >= 0 && end >= total) {
			t.Fatal("invalid parsed range")
		}
	})
}
