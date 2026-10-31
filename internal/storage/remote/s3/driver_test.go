package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

func newTestDriver(t *testing.T, h http.Handler) (*Driver, *httptest.Server) {
	t.Helper()
	ts := httptest.NewServer(h)
	d, err := NewDriver(Config{Endpoint: ts.URL, Region: "us-east-1", ForcePathStyle: true, AllowInsecureHTTP: true, CredentialScope: "test", CredentialVersion: "1", SecurityProfile: "test", MaxRetries: 2, RequestTimeout: time.Second}, Credentials{AccessKey: "AKID", SecretKey: "SECRET"}, nil)
	if err != nil {
		ts.Close()
		t.Fatal(err)
	}
	d.sleep = func(context.Context, time.Duration) error { return nil }
	d.now = func() time.Time { return time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC) }
	return d, ts
}

func TestReadRangeExactAndManaged(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=2-5" {
			t.Errorf("range=%q", got)
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("missing sigv4")
		}
		w.Header().Set("ETag", "\"abc\"")
		w.Header().Set("x-amz-version-id", "v1")
		w.Header().Set("Content-Range", "bytes 2-5/10")
		w.Header().Set("Content-Length", "4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("cdef"))
	}))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://bucket/key")
	mm := memory.NewManager(8)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	res, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 2, Length: 4, Tag: memory.TagParquetRange}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Buffer.Bytes()); got != "cdef" {
		t.Fatalf("got %q", got)
	}
	if scope.UsedByTag(memory.TagParquetRange) != 4 {
		t.Fatalf("managed=%d", scope.UsedByTag(memory.TagParquetRange))
	}
	res.Buffer.Release()
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestReadRangeRejectsWholeFileFallback(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("whole"))
	}))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://b/k")
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 1, Length: 2}, scope, bm)
	if !errors.Is(err, vfs.ErrRangeUnsupported) {
		t.Fatalf("got %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestReadRangeObjectChanged(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "\"old\"" {
			t.Errorf("if-match=%q", r.Header.Get("If-Match"))
		}
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://b/k")
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	expected := vfs.ObjectIdentity{ETag: "old", Size: 9}
	_, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 0, Length: 2, Expected: &expected}, scope, bm)
	if !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("got %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestReadRangeShortReadReleases(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-3/8")
		w.Header().Set("ETag", "\"e\"")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("ab"))
	}))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://b/k")
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	_, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 0, Length: 4}, scope, bm)
	if !errors.Is(err, vfs.ErrShortRead) {
		t.Fatalf("got %v", err)
	}
	if scope.Used() != 0 {
		t.Fatalf("leaked=%d", scope.Used())
	}
}

func TestRetries503(t *testing.T) {
	var calls atomic.Int64
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Range", "bytes 0-1/2")
		w.Header().Set("Content-Length", "2")
		w.Header().Set("ETag", "\"e\"")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("ok"))
	}))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://b/k")
	mm := memory.NewManager(100)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	res, err := d.ReadRange(context.Background(), vfs.RangeRequest{URI: u, Offset: 0, Length: 2}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	res.Buffer.Release()
	if calls.Load() != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestCredentialFailure(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer ts.Close()
	u, _ := vfs.ParseURI("s3://b/k")
	_, err := d.Stat(context.Background(), u)
	if !errors.Is(err, vfs.ErrCredential) {
		t.Fatalf("got %v", err)
	}
}

func TestListPage(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("list-type") != "2" {
			t.Error("missing list-type")
		}
		if r.URL.Query().Get("continuation-token") != "tok" {
			t.Errorf("token=%q", r.URL.Query().Get("continuation-token"))
		}
		_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>a/x.parquet</Key><LastModified>2024-01-02T03:04:05Z</LastModified><ETag>"abc"</ETag><Size>12</Size></Contents></ListBucketResult>`)
	}))
	defer ts.Close()
	base, _ := vfs.ParseURI("s3://bucket/")
	page, err := d.List(context.Background(), base, vfs.ListOptions{Prefix: "a/", ContinuationToken: "tok", MaxKeys: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Truncated || page.NextContinuationToken != "next" || len(page.Entries) != 1 {
		t.Fatalf("page=%+v", page)
	}
	if page.Entries[0].URI.String() != "s3://bucket/a/x.parquet" {
		t.Fatalf("uri=%s", page.Entries[0].URI.String())
	}
}

func TestObjectURLPreservesEscaping(t *testing.T) {
	d, ts := newTestDriver(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != "/bucket/dir/a%20b%20%C3%BC.parquet" {
			t.Errorf("escaped path=%q", r.URL.EscapedPath())
		}
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", "\"e\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	u, err := vfs.ParseURI("s3://bucket/dir/a%20b%20%C3%BC.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Stat(context.Background(), u); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsInsecureEndpointByDefault(t *testing.T) {
	_, err := NewDriver(Config{Endpoint: "http://example", Region: "us-east-1", CredentialScope: "x", SecurityProfile: "tls"}, Credentials{AccessKey: "a", SecretKey: "b"}, nil)
	if err == nil {
		t.Fatal("expected insecure endpoint rejection")
	}
}
