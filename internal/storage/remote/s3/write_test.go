package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// multipartTestServer 记录 S3 multipart 请求，验证条件提交、Abort 与 retry 行为。
type multipartTestServer struct {
	mu            sync.Mutex
	parts         [][]byte
	aborts        int
	completes     int
	initiate      int
	partFailures  int
	completeCode  int
	ifNoneMatch   string
	ifMatch       string
	stored        []byte
	finalETag     string
	versionID     string
}

// handler 实现测试所需的最小 S3 path-style API。
func (s *multipartTestServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.initiate++
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		body, _ := io.ReadAll(r.Body)
		if s.partFailures > 0 {
			s.partFailures--
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		s.parts = append(s.parts, append([]byte(nil), body...))
		w.Header().Set("ETag", `"part-etag-`+strconv.Itoa(len(s.parts))+`"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		s.completes++
		s.ifNoneMatch = r.Header.Get("If-None-Match")
		s.ifMatch = r.Header.Get("If-Match")
		if s.completeCode != 0 && s.completeCode != http.StatusOK {
			w.WriteHeader(s.completeCode)
			return
		}
		s.stored = s.stored[:0]
		for _, part := range s.parts {
			s.stored = append(s.stored, part...)
		}
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.aborts++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		s.ifNoneMatch = r.Header.Get("If-None-Match")
		s.ifMatch = r.Header.Get("If-Match")
		s.stored = append([]byte(nil), body...)
		w.Header().Set("ETag", `"empty-etag"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodHead:
		w.Header().Set("Content-Length", strconv.Itoa(len(s.stored)))
		if s.finalETag == "" {
			s.finalETag = "final-etag"
		}
		w.Header().Set("ETag", `"`+s.finalETag+`"`)
		if s.versionID != "" {
			w.Header().Set("x-amz-version-id", s.versionID)
		}
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodDelete:
		s.stored = nil
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

// newMultipartTestDriver 创建使用 httptest endpoint 的 path-style S3 driver。
func newMultipartTestDriver(t *testing.T, state *multipartTestServer) (*Driver, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(state.handler))
	d, err := NewDriver(Config{
		Endpoint:          server.URL,
		Region:            "test-region",
		ForcePathStyle:    true,
		AllowInsecureHTTP: true,
		CredentialScope:   "scope",
		CredentialVersion: "v1",
		SecurityProfile:   "test",
		MaxRetries:        2,
	}, Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	// 测试不等待真实 backoff；重试次数和 payload 重放仍走生产逻辑。
	d.sleep = func(context.Context, time.Duration) error { return nil }
	return d, server
}

// TestS3MultipartWriteManagedConditionalCommit 验证小对象也通过 multipart 完成点做条件提交并释放所有 managed memory。
func TestS3MultipartWriteManagedConditionalCommit(t *testing.T) {
	state := &multipartTestServer{versionID: "v7"}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	registry, err := vfs.NewRegistry(d)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := vfs.ParseURI("s3://bucket/out/data.csv")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	bm := memory.NewBufferManager(nil)
	h, err := registry.OpenWrite(context.Background(), u, vfs.WriteOptions{IfNoneMatch: true}, scope, bm)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("abcdef")
	if n, err := h.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	identity, err := h.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	stored := string(state.stored)
	ifNone := state.ifNoneMatch
	initiates, completes := state.initiate, state.completes
	state.mu.Unlock()
	if stored != string(payload) || ifNone != "*" || initiates != 1 || completes != 1 {
		t.Fatalf("stored=%q if-none-match=%q initiate=%d complete=%d", stored, ifNone, initiates, completes)
	}
	if identity.Size != int64(len(payload)) || identity.VersionID != "v7" {
		t.Fatalf("identity=%+v", identity)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}

// TestS3MultipartAllocationFailureBeforeNetwork 验证 part buffer reserve 失败时不会创建远端 multipart。
func TestS3MultipartAllocationFailureBeforeNetwork(t *testing.T) {
	state := &multipartTestServer{}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	u, _ := vfs.ParseURI("s3://bucket/out/data.csv")
	mm := memory.NewManager(1 << 20)
	scope := mm.NewScope()
	_, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if !errors.Is(err, memory.ErrLimitExceeded) {
		t.Fatalf("expected memory limit, got %v", err)
	}
	state.mu.Lock()
	initiates := state.initiate
	state.mu.Unlock()
	if initiates != 0 || scope.Used() != 0 {
		t.Fatalf("network/memory side effect initiate=%d used=%d", initiates, scope.Used())
	}
}

// TestS3MultipartRetryAndAbort 验证 retryable part failure 可重放，AbortMultipartUpload 最终清除远端 ownership。
func TestS3MultipartRetryAndAbort(t *testing.T) {
	state := &multipartTestServer{partFailures: 1}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	u, _ := vfs.ParseURI("s3://bucket/out/data.bin")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	hI, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	h := hI.(*s3WriteHandle)
	payload := make([]byte, defaultMultipartPartSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	if n, err := h.Write(context.Background(), payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	if h.uploadID == "" || len(h.parts) != 1 {
		t.Fatalf("multipart not started/uploaded: id=%q parts=%d", h.uploadID, len(h.parts))
	}
	if err := h.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	aborts := state.aborts
	state.mu.Unlock()
	if aborts != 1 {
		t.Fatalf("abort requests=%d", aborts)
	}
	if scope.Used() != 0 {
		t.Fatalf("abort memory leak=%d", scope.Used())
	}
}

// TestS3ConditionalCompleteFailureRemainsAbortable 验证 412 不会丢失 uploadID，随后 Abort 能清理 incomplete multipart。
func TestS3ConditionalCompleteFailureRemainsAbortable(t *testing.T) {
	state := &multipartTestServer{completeCode: http.StatusPreconditionFailed}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	u, _ := vfs.ParseURI("s3://bucket/out/manifest.json")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	h, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{IfNoneMatch: true}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = h.Write(context.Background(), []byte(`{"job":"x"}`))
	if _, err := h.Close(context.Background()); !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("expected conditional conflict, got %v", err)
	}
	if err := h.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	aborts := state.aborts
	state.mu.Unlock()
	if aborts != 1 || scope.Used() != 0 {
		t.Fatalf("abort=%d used=%d", aborts, scope.Used())
	}
}

// TestS3EmptyObjectUsesAtomicPut 验证空文件不创建 multipart，条件语义直接应用到 PutObject。
func TestS3EmptyObjectUsesAtomicPut(t *testing.T) {
	state := &multipartTestServer{}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	u, _ := vfs.ParseURI("s3://bucket/out/empty")
	mm := memory.NewManager(16 << 20)
	scope := mm.NewScope()
	h, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{Expected: &vfs.ObjectIdentity{ETag: "old"}}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	ifMatch, initiates := state.ifMatch, state.initiate
	state.mu.Unlock()
	if ifMatch != `"old"` || initiates != 0 {
		t.Fatalf("if-match=%q initiates=%d", ifMatch, initiates)
	}
}

// TestS3ExpectedWithoutETagIsRejected 确保 VersionID-only identity 不会被静默忽略为无条件写。
func TestS3ExpectedWithoutETagIsRejected(t *testing.T) {
	state := &multipartTestServer{}
	d, server := newMultipartTestDriver(t, state)
	defer server.Close()
	u, _ := vfs.ParseURI("s3://bucket/out/data")
	mm := memory.NewManager(16 << 20)
	_, err := d.OpenWrite(context.Background(), u, vfs.WriteOptions{Expected: &vfs.ObjectIdentity{VersionID: "v1"}}, mm.NewScope(), memory.NewBufferManager(nil))
	if !errors.Is(err, vfs.ErrWriteUnsupported) || !strings.Contains(err.Error(), "ETag") {
		t.Fatalf("expected ETag requirement, got %v", err)
	}
}
