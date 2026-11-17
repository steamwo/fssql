package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	"github.com/steamwo/fssql/internal/core/memory"
	"github.com/steamwo/fssql/internal/storage/vfs"
)

// conditionalWriteTestServer 只实现条件写测试所需的 S3 multipart API，并记录原子完成请求的条件头。
type conditionalWriteTestServer struct {
	// mu 保护测试服务器在 HTTP handler 与断言之间共享的状态。
	mu sync.Mutex
	// parts 保存已经上传的 part payload，用于成功完成后的 HEAD size。
	parts [][]byte
	// stored 保存成功 CompleteMultipartUpload 后的最终对象内容。
	stored []byte
	// completeIfNoneMatch 记录 CompleteMultipartUpload 收到的 If-None-Match。
	completeIfNoneMatch string
	// completeIfMatch 记录 CompleteMultipartUpload 收到的 If-Match。
	completeIfMatch string
	// rejectComplete 令原子完成点返回 412，模拟条件竞争失败。
	rejectComplete bool
	// aborts 记录 AbortMultipartUpload 请求次数。
	aborts int
}

// handler 实现 CreateMultipartUpload / UploadPart / Complete / Abort / HEAD 的最小测试协议。
func (s *conditionalWriteTestServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>conditional-upload</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		body, _ := io.ReadAll(r.Body)
		s.parts = append(s.parts, append([]byte(nil), body...))
		w.Header().Set("ETag", `"part-`+strconv.Itoa(len(s.parts))+`"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		s.completeIfNoneMatch = r.Header.Get("If-None-Match")
		s.completeIfMatch = r.Header.Get("If-Match")
		if s.rejectComplete {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		s.stored = s.stored[:0]
		for _, part := range s.parts {
			s.stored = append(s.stored, part...)
		}
		w.WriteHeader(http.StatusOK)
		_,_=io.WriteString(w,`<CompleteMultipartUploadResult><ETag>"final"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		s.aborts++
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodHead:
		w.Header().Set("Content-Length", strconv.Itoa(len(s.stored)))
		w.Header().Set("ETag", `"final"`)
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}
}

// newConditionalWriteDriver 创建使用真实 S3 Driver 和 httptest endpoint 的条件写测试环境。
func newConditionalWriteDriver(t *testing.T, state *conditionalWriteTestServer) (*Driver, *httptest.Server) {
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
		MaxRetries:        1,
	}, Credentials{AccessKey: "access", SecretKey: "secret"}, nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return d, server
}

// TestS3ConditionalWriteAtMultipartCommitPoint 验证 capability gate 与 If-None-Match/If-Match 都落在原子 CompleteMultipartUpload 请求。
func TestS3ConditionalWriteAtMultipartCommitPoint(t *testing.T) {
	tests := []struct {
		name            string
		opts            vfs.WriteOptions
		wantIfNoneMatch string
		wantIfMatch     string
	}{
		{name: "create-only", opts: vfs.WriteOptions{IfNoneMatch: true}, wantIfNoneMatch: "*"},
		{name: "compare-and-swap", opts: vfs.WriteOptions{Expected: &vfs.ObjectIdentity{ETag: "old-etag"}}, wantIfMatch: `"old-etag"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &conditionalWriteTestServer{}
			driver, server := newConditionalWriteDriver(t, state)
			defer server.Close()
			if !driver.StaticCapabilities().Has(vfs.ConditionalWrite) {
				t.Fatal("S3 driver must advertise implemented ConditionalWrite")
			}
			registry, err := vfs.NewRegistry(driver)
			if err != nil {
				t.Fatal(err)
			}
			uri, _ := vfs.ParseURI("s3://bucket/final/_manifest.json")
			mm := memory.NewManager(32 << 20)
			scope := mm.NewScope()
			bm := memory.NewBufferManager(nil)
			handle, err := registry.OpenWrite(context.Background(), uri, tt.opts, scope, bm)
			if err != nil {
				t.Fatal(err)
			}
			payload := []byte(`{"job":"j1"}`)
			if n, err := handle.Write(context.Background(), payload); err != nil || n != len(payload) {
				t.Fatalf("write n=%d err=%v", n, err)
			}
			if _, err := handle.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			state.mu.Lock()
			ifNoneMatch, ifMatch := state.completeIfNoneMatch, state.completeIfMatch
			stored := append([]byte(nil), state.stored...)
			state.mu.Unlock()
			if ifNoneMatch != tt.wantIfNoneMatch || ifMatch != tt.wantIfMatch {
				t.Fatalf("conditional headers If-None-Match=%q If-Match=%q", ifNoneMatch, ifMatch)
			}
			if string(stored) != string(payload) {
				t.Fatalf("stored=%q", stored)
			}
			if scope.Used() != 0 {
				t.Fatalf("managed memory leak=%d", scope.Used())
			}
		})
	}
}

// TestS3ConditionalWritePreconditionFailureRemainsAbortable 验证 412 不会丢失 upload ownership，随后 Abort 能清理 multipart 和 Managed Memory。
func TestS3ConditionalWritePreconditionFailureRemainsAbortable(t *testing.T) {
	state := &conditionalWriteTestServer{rejectComplete: true}
	driver, server := newConditionalWriteDriver(t, state)
	defer server.Close()
	registry, err := vfs.NewRegistry(driver)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := vfs.ParseURI("s3://bucket/final/_manifest.json")
	mm := memory.NewManager(32 << 20)
	scope := mm.NewScope()
	handle, err := registry.OpenWrite(context.Background(), uri, vfs.WriteOptions{IfNoneMatch: true}, scope, memory.NewBufferManager(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Write(context.Background(), []byte("manifest")); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Close(context.Background()); !errors.Is(err, vfs.ErrObjectChanged) {
		t.Fatalf("expected precondition failure, got %v", err)
	}
	if err := handle.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	aborts := state.aborts
	header := state.completeIfNoneMatch
	state.mu.Unlock()
	if aborts != 1 || header != "*" {
		t.Fatalf("aborts=%d If-None-Match=%q", aborts, header)
	}
	if scope.Used() != 0 {
		t.Fatalf("managed memory leak=%d", scope.Used())
	}
}
